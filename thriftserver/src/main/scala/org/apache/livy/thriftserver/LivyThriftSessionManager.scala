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

package org.apache.livy.thriftserver

import java.net.URI
import java.security.PrivilegedExceptionAction
import java.util.{Map => JMap, UUID}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success, Try}

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.shims.Utils
import org.apache.hive.service.cli.SessionHandle
import org.apache.hive.service.cli.session.SessionManager
import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.livy.LivyConf
import org.apache.livy.Logging
import org.apache.livy.server.interactive.{CreateInteractiveRequest, InteractiveSession}
import org.apache.livy.sessions.Spark
import org.apache.livy.thriftserver.SessionStates._
import org.apache.livy.thriftserver.rpc.RpcClient
import org.apache.livy.utils.LivySparkUtils

class LivyThriftSessionManager(val server: LivyThriftServer)
  extends SessionManager(server) with Logging {
  private val sessionHandleToLivySession =
    new ConcurrentHashMap[SessionHandle, Future[InteractiveSession]]()
  private val managedLivySessionActiveUsers =
    new mutable.HashMap[Int, Int]()
  private val maxSessionWait = Duration(
    server.livyConf.getTimeAsMs(LivyConf.THRIFT_SESSION_CREATION_TIMEOUT),
    scala.concurrent.duration.MILLISECONDS)

  val supportUseDatabase: Boolean = {
    val sparkVersion = server.livyConf.get(LivyConf.LIVY_SPARK_VERSION)
    val (sparkMajorVersion, _) = LivySparkUtils.formatSparkVersion(sparkVersion)
    sparkMajorVersion > 1 || server.livyConf.getBoolean(LivyConf.ENABLE_HIVE_CONTEXT)
  }

  def getLivySession(sessionHandle: SessionHandle): InteractiveSession = {
    val future = sessionHandleToLivySession.get(sessionHandle)
    assert(future != null, s"Looking for not existing session: $sessionHandle.")

    if (!future.isCompleted) {
      Try(Await.result(future, maxSessionWait)) match {
        case Success(session) => session
        case Failure(e) => throw e
      }
    } else {
      future.value match {
        case Some(Success(session)) => session
        case Some(Failure(e)) => throw e
        case None => throw new RuntimeException("Future cannot be None when it is completed")
      }
    }
  }

  def livySessionId(sessionHandle: SessionHandle): Option[Int] = {
    sessionHandleToLivySession.get(sessionHandle).value.filter(_.isSuccess).map(_.get.id)
  }

  def livySessionState(sessionHandle: SessionHandle): SessionStates = {
    sessionHandleToLivySession.get(sessionHandle).value match {
      case Some(Success(_)) => CREATION_SUCCESS
      case Some(Failure(_)) => CREATION_FAILED
      case None => CREATION_IN_PROGRESS
    }
  }

  def numberOfActiveUsers(livySessionId: Int): Int = synchronized[Int] {
    managedLivySessionActiveUsers.getOrElse(livySessionId, 0)
  }

  def onLivySessionOpened(livySession: InteractiveSession): Unit = {
    server.livySessionManager.register(livySession)
  }

  private def incrementManagedSessionActiveUsers(livySessionId: Int): Unit = synchronized {
    managedLivySessionActiveUsers(livySessionId) = numberOfActiveUsers(livySessionId) + 1
  }

  def onUserSessionClosed(sessionHandle: SessionHandle, livySession: InteractiveSession): Unit = {
    val closeSession = synchronized[Boolean] {
      val activeUsers = managedLivySessionActiveUsers(livySession.id)
      if (activeUsers == 1) {
        // it was the last user, so we can close the LivySession
        managedLivySessionActiveUsers -= livySession.id
        true
      } else {
        managedLivySessionActiveUsers(livySession.id) = activeUsers - 1
        false
      }
    }
    if (closeSession) {
      server.livySessionManager.delete(livySession)
    } else {
      // We unregister the session only if we don't close it, as it is unnecessary in that case
      val rpcClient = new RpcClient(livySession)
      try {
        rpcClient.executeUnregisterSession(sessionHandle).get()
      } catch {
        case e: Exception => warn(s"Unable to unregister session $sessionHandle", e)
      }
    }
  }

  override def init(hiveConf: HiveConf): Unit = {
    operationManager = new LivyOperationManager(this)
    super.init(hiveConf)
  }

  private def getOrCreateLivySession(
      sessionHandle: SessionHandle,
      sessionId: Option[Int],
      username: String,
      createLivySession: () => InteractiveSession): InteractiveSession = {
    sessionId match {
      case Some(id) =>
        server.livySessionManager.get(id) match {
          case None =>
            warn(s"Session id $id doesn't exist, so we will ignore it.")
            createLivySession()
          case Some(session) if !server.isAllowedToUse(username, session) =>
            warn(s"Session id $id doesn't belong to $username, so we will ignore it.")
            createLivySession()
          case Some(session) => if (session.state.isActive) {
            info(s"Reusing Session $id for $sessionHandle.")
            session
          } else {
            warn(s"Session id $id is not active anymore, so we will ignore it.")
            createLivySession()
          }
        }
      case None =>
        createLivySession()
    }
  }

  /**
   * Performs the initialization of the new Thriftserver session:
   *  - adds the Livy thrifserver JAR to the Spark application;
   *  - register the new Thriftserver session in the Spark application;
   *  - runs the initialization statements;
   */
  private def initSession(
      sessionHandle: SessionHandle,
      livySession: InteractiveSession,
      initStatements: List[String]): Unit = {
    // Add the thriftserver jar to Spark application as we need to deserialize there the classes
    // which handle the job submission.
    // Note: if this is an already existing session, adding the JARs multiple times is not a
    // problem as Spark ignores JARs which have already been added.
    try {
      livySession.addJar(LivyThriftSessionManager.thriftserverJarLocation(server.livyConf))
    } catch {
      case e: java.util.concurrent.ExecutionException
          if Option(e.getCause).forall(_.getMessage.contains("has already been uploaded")) =>
        // We have already uploaded the jar to this session, we can ignore this error
        debug(e.getMessage, e)
    }

    val rpcClient = new RpcClient(livySession)
    rpcClient.executeRegisterSession(sessionHandle).get()
    initStatements.foreach { statement =>
      val statementId = UUID.randomUUID().toString
      try {
        rpcClient.executeSql(sessionHandle, statementId, statement).get()
      } finally {
        Try(rpcClient.cleanupStatement(statementId).get()).failed.foreach { e =>
          error(s"Failed to close init operation $statementId", e)
        }
      }
    }
  }

  override def openSession(
      protocol: TProtocolVersion,
      username: String,
      password: String,
      ipAddress: String,
      sessionConf: JMap[String, String],
      withImpersonation: Boolean,
      delegationToken: String): SessionHandle = {
    val sessionHandle = super.openSession(
      protocol, username, password, ipAddress, sessionConf, withImpersonation, delegationToken)
    val (initStatements, createInteractiveRequest, sessionId) =
      LivyThriftSessionManager.processSessionConf(sessionConf, supportUseDatabase)
    val createLivySession = () => {
      createInteractiveRequest.kind = Spark
      val newSession = InteractiveSession.create(
        server.livySessionManager.nextId(),
        username,
        None,
        server.livyConf,
        createInteractiveRequest,
        server.sessionStore)
      onLivySessionOpened(newSession)
      newSession
    }
    val futureLivySession = Future({
      val livyServiceUGI = Utils.getUGI
      livyServiceUGI.doAs(new PrivilegedExceptionAction[InteractiveSession] {
        override def run(): InteractiveSession = {
          val livySession =
            getOrCreateLivySession(sessionHandle, sessionId, username, createLivySession)
          incrementManagedSessionActiveUsers(livySession.id)
          initSession(sessionHandle, livySession, initStatements)
          livySession
        }
      })
    })
    sessionHandleToLivySession.put(sessionHandle, futureLivySession)
    sessionHandle
  }

  override def closeSession(sessionHandle: SessionHandle): Unit = {
    super.closeSession(sessionHandle)
    val removedSession = sessionHandleToLivySession.remove(sessionHandle)
    removedSession.value match {
      case Some(Success(interactiveSession)) =>
        onUserSessionClosed(sessionHandle, interactiveSession)
      case None =>
        removedSession.onSuccess {
          case interactiveSession => onUserSessionClosed(sessionHandle, interactiveSession)
        }
      case _ => // nothing to do
    }
  }
}

object LivyThriftSessionManager extends Logging {
  // Users can explicitly set the Livy connection id they want to connect to using this hiveconf
  // variable
  private val livySessionIdConfigKey = "set:hiveconf:livy.server.sessionId"
  private val livySessionConfRegexp = "set:hiveconf:livy.session.conf.(.*)".r
  private val JAR_LOCATION = getClass.getProtectionDomain.getCodeSource.getLocation.toURI

  def thriftserverJarLocation(livyConf: LivyConf): URI = {
    Option(livyConf.get(LivyConf.THRIFT_SERVER_JAR_LOCATION)).map(new URI(_))
      .getOrElse(JAR_LOCATION)
  }

  private def convertConfValueToInt(key: String, value: String) = {
    val res = Try(value.toInt)
    if (res.isFailure) {
      warn(s"Ignoring $key = $value as it is not a valid integer")
      None
    } else {
      Some(res.get)
    }
  }

  private def processSessionConf(
      sessionConf: JMap[String, String],
      supportUseDatabase: Boolean): (List[String], CreateInteractiveRequest, Option[Int]) = {
    if (null != sessionConf && !sessionConf.isEmpty) {
      val statements = new mutable.ListBuffer[String]
      val extraLivyConf = new mutable.ListBuffer[(String, String)]
      val createInteractiveRequest = new CreateInteractiveRequest
      sessionConf.asScala.foreach {
        case (key, value) =>
          key match {
            case v if v.startsWith("use:") && supportUseDatabase =>
              statements += s"use $value"
            // Process session configs for Livy session creation request
            case "set:hiveconf:livy.session.driverMemory" =>
              createInteractiveRequest.driverMemory = Some(value)
            case "set:hiveconf:livy.session.driverCores" =>
              createInteractiveRequest.driverCores = convertConfValueToInt(key, value)
            case "set:hiveconf:livy.session.executorMemory" =>
              createInteractiveRequest.executorMemory = Some(value)
            case "set:hiveconf:livy.session.executorCores" =>
              createInteractiveRequest.executorCores = convertConfValueToInt(key, value)
            case "set:hiveconf:livy.session.queue" =>
              createInteractiveRequest.queue = Some(value)
            case "set:hiveconf:livy.session.name" =>
              createInteractiveRequest.name = Some(value)
            case "set:hiveconf:livy.session.heartbeatTimeoutInSecond" =>
              convertConfValueToInt(key, value).foreach { heartbeatTimeoutInSecond =>
                createInteractiveRequest.heartbeatTimeoutInSecond = heartbeatTimeoutInSecond
              }
            case livySessionConfRegexp(livyConfKey) => extraLivyConf += (livyConfKey -> value)
            case _ if key == livySessionIdConfigKey => // Ignore it, we handle it later
            case _ =>
              info(s"Ignoring key: $key = '$value'")
          }
      }
      createInteractiveRequest.conf = extraLivyConf.toMap
      val sessionId = Option(sessionConf.get(livySessionIdConfigKey)).flatMap { id =>
        val res = Try(id.toInt)
        if (res.isFailure) {
          warn(s"Ignoring $livySessionIdConfigKey=$id as it is not an int.")
          None
        } else {
          Some(res.get)
        }
      }
      (statements.toList, createInteractiveRequest, sessionId)
    } else {
      (List(), new CreateInteractiveRequest, None)
    }
  }
}
