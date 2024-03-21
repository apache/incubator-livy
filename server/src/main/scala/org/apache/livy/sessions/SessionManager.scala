/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.livy.sessions

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import org.apache.livy.{LivyConf, Logging}
import org.apache.livy.client.common.ClientConf
import org.apache.livy.server.batch.{BatchRecoveryMetadata, BatchSession}
import org.apache.livy.server.interactive.{InteractiveRecoveryMetadata, InteractiveSession, SessionHeartbeatWatchdog}
import org.apache.livy.server.recovery.SessionStore
import org.apache.livy.sessions.Session.RecoveryMetadata

object SessionManager {
  val SESSION_RECOVERY_MODE_OFF = "off"
  val SESSION_RECOVERY_MODE_RECOVERY = "recovery"
}

class BatchSessionManager(
    livyConf: LivyConf,
    sessionStore: SessionStore,
    mockSessions: Option[Seq[BatchSession]] = None)
  extends SessionManager[BatchSession, BatchRecoveryMetadata] (
    livyConf, BatchSession.recover(_, livyConf, sessionStore), sessionStore, "batch", mockSessions)

class InteractiveSessionManager(
  livyConf: LivyConf,
  sessionStore: SessionStore,
  mockSessions: Option[Seq[InteractiveSession]] = None)
  extends SessionManager[InteractiveSession, InteractiveRecoveryMetadata] (
    livyConf,
    InteractiveSession.recover(_, livyConf, sessionStore),
    sessionStore,
    "interactive",
    mockSessions)
  with SessionHeartbeatWatchdog[InteractiveSession, InteractiveRecoveryMetadata]
  {
    start()
  }

class SessionManager[S <: Session, R <: RecoveryMetadata : ClassTag](
    protected val livyConf: LivyConf,
    sessionRecovery: R => S,
    sessionStore: SessionStore,
    sessionType: String,
    mockSessions: Option[Seq[S]] = None)
  extends Logging {

  import SessionManager._

  protected implicit def executor: ExecutionContext = ExecutionContext.global

  protected[this] final val idCounter = new AtomicInteger(0)
  protected[this] final val sessions = mutable.LinkedHashMap[Int, S]()
  private[this] final val sessionsByName = mutable.HashMap[String, S]()


  private[this] final val sessionTimeoutCheck = livyConf.getBoolean(LivyConf.SESSION_TIMEOUT_CHECK)
  private[this] final val sessionTimeoutCheckSkipBusy =
    livyConf.getBoolean(LivyConf.SESSION_TIMEOUT_CHECK_SKIP_BUSY)

  private[this] final val sessionTimeout = livyConf.getTimeAsMs(LivyConf.SESSION_TIMEOUT)

  private[this] final val sessionStateRetainedInSec =
    TimeUnit.MILLISECONDS.toNanos(livyConf.getTimeAsMs(LivyConf.SESSION_STATE_RETAIN_TIME))

  mockSessions.getOrElse(recover()).foreach(register)
  new GarbageCollector().start()

  def nextId(): Int = synchronized {
    val id = idCounter.getAndIncrement()
    sessionStore.saveNextSessionId(sessionType, idCounter.get())
    id
  }

  def register(session: S): S = {
    info(s"Registering new session ${session.id}")
    synchronized {
      session.name.foreach { sessionName =>
        if (sessionsByName.contains(sessionName)) {
          val errMsg = s"Duplicate session name: ${session.name} for session ${session.id}"
          error(errMsg)
          session.stop()
          throw new IllegalArgumentException(errMsg)
        } else {
          sessionsByName.put(sessionName, session)
        }
      }
      sessions.put(session.id, session)
      session.start()
    }
    info(s"Registered new session ${session.id}")
    session
  }

  def get(id: Int): Option[S] = sessions.get(id)

  def get(sessionName: String): Option[S] = sessionsByName.get(sessionName)

  def size(): Int = sessions.size

  def all(): Iterable[S] = sessions.values

  def delete(id: Int): Option[Future[Unit]] = {
    get(id).map(delete)
  }

  def delete(session: S): Future[Unit] = {
    info(s"Deleting ${session}")
    session.stop().map { case _ =>
      try {
        sessionStore.remove(sessionType, session.id)
        synchronized {
          sessions.remove(session.id)
          session.name.foreach(sessionsByName.remove)
        }
      } catch {
        case NonFatal(e) =>
          error("Exception was thrown during stop session:", e)
          throw e
      } finally {
        info(s"Deleted ${session}")
      }
    }
  }

  def shutdown(): Unit = {
    val recoveryEnabled = livyConf.get(LivyConf.RECOVERY_MODE) != SESSION_RECOVERY_MODE_OFF
    if (!recoveryEnabled) {
      sessions.values.map(_.stop).foreach { future =>
        Await.ready(future, Duration.Inf)
      }
    }
  }

  def collectGarbage(): Future[Iterable[Unit]] = {
    def expired(session: Session): Boolean = {
      session.state match {
        case s: FinishedSessionState =>
          val currentTime = System.nanoTime()
          currentTime - s.time > sessionStateRetainedInSec
        case _ =>
          if (!sessionTimeoutCheck) {
            false
          } else if (session.state == SessionState.Busy && sessionTimeoutCheckSkipBusy) {
            false
          } else if (session.isInstanceOf[BatchSession]) {
            false
          } else {
            val currentTime = System.nanoTime()
            var calculatedTimeout = sessionTimeout;
            if (session.idleTimeout.isDefined) {
              calculatedTimeout = ClientConf.getTimeAsMs(session.idleTimeout.get)
            }
            calculatedTimeout = TimeUnit.MILLISECONDS.toNanos(calculatedTimeout)
            if (currentTime - session.lastActivity > calculatedTimeout) {
              return true
            }
            if (session.ttl.isDefined && session.startedOn.isDefined) {
              calculatedTimeout = TimeUnit.MILLISECONDS.toNanos(
                ClientConf.getTimeAsMs(session.ttl.get))
              if (currentTime - session.startedOn.get > calculatedTimeout) {
                return true
              }
            }
            false
          }
      }
    }

    Future.sequence(all().filter(expired).map { s =>
      s.state match {
        case st: FinishedSessionState =>
          info(s"Deleting $s because it finished before ${sessionStateRetainedInSec / 1e9} secs.")
        case _ =>
          info(s"Deleting $s because it was inactive or the time to leave the period is over.")
      }
      delete(s)
    })
  }

  private def recover(): Seq[S] = {
    // Recover next session id from state store and create SessionManager.
    idCounter.set(sessionStore.getNextSessionId(sessionType))

    // Retrieve session recovery metadata from state store.
    val sessionMetadata = sessionStore.getAllSessions[R](sessionType)

    // Recover session from session recovery metadata.
    val recoveredSessions = sessionMetadata.flatMap(_.toOption).map(sessionRecovery)

    info(s"Recovered ${recoveredSessions.length} $sessionType sessions." +
      s" Next session id: $idCounter")

    // Print recovery error.
    val recoveryFailure = sessionMetadata.filter(_.isFailure).map(_.failed.get)
    recoveryFailure.foreach(ex => error(ex.getMessage, ex.getCause))

    recoveredSessions
  }

  private class GarbageCollector extends Thread("session gc thread") {

    setDaemon(true)

    override def run(): Unit = {
      while (true) {
        collectGarbage()
        Thread.sleep(60 * 1000)
      }
    }

  }

}
