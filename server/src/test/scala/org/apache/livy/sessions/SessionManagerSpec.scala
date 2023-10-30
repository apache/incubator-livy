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

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Try}

import org.mockito.Mockito.{doReturn, never, verify, when}
import org.scalatest.{FunSpec, Matchers}
import org.scalatest.concurrent.Eventually._
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.livy.{LivyBaseUnitTestSuite, LivyConf}
import org.apache.livy.server.batch.{BatchRecoveryMetadata, BatchSession}
import org.apache.livy.server.interactive.{InteractiveRecoveryMetadata, InteractiveSession}
import org.apache.livy.server.recovery.SessionStore
import org.apache.livy.sessions.Session.RecoveryMetadata

class SessionManagerSpec extends FunSpec with Matchers with LivyBaseUnitTestSuite {
  implicit def executor: ExecutionContext = ExecutionContext.global

  private def createSessionManager(livyConf: LivyConf = new LivyConf())
    : (LivyConf, SessionManager[MockSession, RecoveryMetadata]) = {
    livyConf.set(LivyConf.SESSION_TIMEOUT, "100ms")
    val manager = new SessionManager[MockSession, RecoveryMetadata](
      livyConf,
      { _ => assert(false).asInstanceOf[MockSession] },
      mock[SessionStore],
      "test",
      Some(Seq.empty))
    (livyConf, manager)
  }

  describe("SessionManager") {
    it("should garbage collect old sessions") {
      val (livyConf, manager) = createSessionManager()
      val session = manager.register(new MockSession(manager.nextId(), null, livyConf))
      manager.get(session.id).isDefined should be(true)
      eventually(timeout(5 seconds), interval(100 millis)) {
        Await.result(manager.collectGarbage(), Duration.Inf)
        manager.get(session.id) should be(None)
      }
    }

    it("should garbage collect old sessions with ttl") {
      val (livyConf, manager) = createSessionManager()
      val session = manager.register(new MockSession(manager.nextId(), null, livyConf,
        None, Some("4s"), None))
      manager.get(session.id).isDefined should be(true)
      eventually(timeout(5 seconds), interval(100 millis)) {
        Await.result(manager.collectGarbage(), Duration.Inf)
        manager.get(session.id) should be(None)
      }
    }

    it("should garbage collect old sessions with idleTimeout") {
      val (livyConf, manager) = createSessionManager()
      val session = manager.register(new MockSession(manager.nextId(), null, livyConf,
        None, None, Some("4s")))
      manager.get(session.id).isDefined should be(true)
      eventually(timeout(5 seconds), interval(100 millis)) {
        Await.result(manager.collectGarbage(), Duration.Inf)
        manager.get(session.id) should be(None)
      }
    }

    it("should not garbage collect busy sessions if skip-busy configured") {
      val lc = new LivyConf()
      lc.set(LivyConf.SESSION_TIMEOUT_CHECK_SKIP_BUSY, true)
      val (livyConf, manager) = createSessionManager(lc)
      val session1 = manager.register(new MockSession(manager.nextId(), null, livyConf))
      val session2 = manager.register(new MockSession(manager.nextId(), null, livyConf))
      manager.get(session1.id).isDefined should be(true)
      manager.get(session2.id).isDefined should be(true)
      session2.serverState = SessionState.Busy
      eventually(timeout(5 seconds), interval(100 millis)) {
        Await.result(manager.collectGarbage(), Duration.Inf)
        (manager.get(session1.id).isDefined, manager.get(session2.id).isDefined) should
          be (false, true)
      }
    }

    it("should create sessions with names") {
      val (livyConf, manager) = createSessionManager()
      val name = "Mock-session"
      val session = manager.register(new MockSession(manager.nextId(), null, livyConf, Some(name)))
      manager.get(session.id).isDefined should be(true)
      manager.get(name).isDefined should be(true)
    }

    it("should not create sessions with duplicate names") {
      val (livyConf, manager) = createSessionManager()
      val name = "Mock-session"
      val session1 = new MockSession(manager.nextId(), null, livyConf, Some(name))
      val session2 = new MockSession(manager.nextId(), null, livyConf, Some(name))
      manager.register(session1)
      an[IllegalArgumentException] should be thrownBy manager.register(session2)
      manager.get(session1.id).isDefined should be(true)
      manager.get(session2.id).isDefined should be(false)
      eventually(timeout(10 seconds), interval(100 millis)) {
        session1.stopped should be(false)
        session2.stopped should be(true)
        manager.shutdown()
      }
    }

    it("batch session should not be gc-ed until application is finished") {
      var sessionId = 24
      val conf = new LivyConf().set(LivyConf.SESSION_STATE_RETAIN_TIME, "1s")
      val sessionStore = mock[SessionStore]
      when(sessionStore.getAllSessions[BatchRecoveryMetadata]("batch"))
        .thenReturn(Seq.empty)
      val sm = new BatchSessionManager(conf, sessionStore)

      // Batch session should not be gc-ed when alive
      for (s <- Seq(SessionState.Running,
        SessionState.Idle,
        SessionState.Recovering,
        SessionState.NotStarted,
        SessionState.Busy,
        SessionState.ShuttingDown)) {
          sessionId = sessionId + 1
          val session = mock[BatchSession]
          mockSessionFieldAndMethod(session, s, sessionId)
          sm.register(session)

          Await.result(sm.collectGarbage(), Duration.Inf)
          sm.get(session.id) should be (Some(session))
      }

      // Stopped session should be gc-ed after retained timeout
      for (s <- Seq(SessionState.Error(),
        SessionState.Success(),
        SessionState.Dead())) {
          sessionId = sessionId + 1
          val session = mock[BatchSession]
          mockSessionFieldAndMethod(session, s, sessionId)
          sm.register(session)

          eventually(timeout(30 seconds), interval(100 millis)) {
            Await.result(sm.collectGarbage(), Duration.Inf)
            sm.get(session.id) should be (None)
          }
      }
    }

    it("interactive session should not gc-ed if session timeout check is off") {
      var sessionId = 24
      val conf = new LivyConf().set(LivyConf.SESSION_TIMEOUT_CHECK, false)
        .set(LivyConf.SESSION_STATE_RETAIN_TIME, "1s")
      val sessionStore = mock[SessionStore]
      when(sessionStore.getAllSessions[InteractiveRecoveryMetadata]("interactive"))
        .thenReturn(Seq.empty)
      val sm = new InteractiveSessionManager(conf, sessionStore)

      // Batch session should not be gc-ed when alive
      for (s <- Seq(SessionState.Running,
        SessionState.Idle,
        SessionState.Recovering,
        SessionState.NotStarted,
        SessionState.Busy,
        SessionState.ShuttingDown)) {
          sessionId = sessionId + 1
          val session = mock[InteractiveSession]
          mockSessionFieldAndMethod(session, s, sessionId)
          sm.register(session)

          Await.result(sm.collectGarbage(), Duration.Inf)
          sm.get(session.id) should be (Some(session))
      }

      // Stopped session should be gc-ed after retained timeout
      for (s <- Seq(SessionState.Error(),
        SessionState.Success(),
        SessionState.Dead())) {
          sessionId = sessionId + 1
          val session = mock[InteractiveSession]
          mockSessionFieldAndMethod(session, s, sessionId)
          sm.register(session)

          eventually(timeout(30 seconds), interval(100 millis)) {
            Await.result(sm.collectGarbage(), Duration.Inf)
            sm.get(session.id) should be (None)
          }
      }
    }

    def mockSessionFieldAndMethod(session: Session, state: SessionState, sessionId: Int) : Unit = {
      when(session.id).thenReturn(sessionId)
      when(session.name).thenReturn(None)
      when(session.stop()).thenReturn(Future {})
      when(session.lastActivity).thenReturn(System.nanoTime())
      when(session.state).thenReturn(state)
    }
  }

  describe("BatchSessionManager") {
    implicit def executor: ExecutionContext = ExecutionContext.global

    def makeMetadata(id: Int, appTag: String): BatchRecoveryMetadata = {
      BatchRecoveryMetadata(id, Some(s"test-session-$id"), None, appTag, null, None)
    }

    def mockSession(id: Int): BatchSession = {
      val session = mock[BatchSession]
      when(session.id).thenReturn(id)
      when(session.name).thenReturn(None)
      when(session.stop()).thenReturn(Future {})
      when(session.lastActivity).thenReturn(System.nanoTime())

      session
    }

    it("should not fail if state store is empty") {
      val conf = new LivyConf()

      val sessionStore = mock[SessionStore]
      when(sessionStore.getAllSessions[BatchRecoveryMetadata]("batch"))
        .thenReturn(Seq.empty)

      val sm = new BatchSessionManager(conf, sessionStore)
      sm.nextId() shouldBe 0
    }

    it("should recover sessions from state store") {
      val conf = new LivyConf()
      conf.set(LivyConf.LIVY_SPARK_MASTER.key, "yarn-cluster")

      val sessionType = "batch"
      val nextId = 99

      val validMetadata = List(makeMetadata(0, "t1"), makeMetadata(77, "t2")).map(Try(_))
      val invalidMetadata = List(Failure(new Exception("Fake invalid metadata")))
      val sessionStore = mock[SessionStore]
      when(sessionStore.getNextSessionId(sessionType)).thenReturn(nextId)
      when(sessionStore.getAllSessions[BatchRecoveryMetadata](sessionType))
        .thenReturn(validMetadata ++ invalidMetadata)

      val sm = new BatchSessionManager(conf, sessionStore)
      sm.nextId() shouldBe nextId
      validMetadata.foreach { m =>
        sm.get(m.get.id) shouldBe defined
      }
      sm.size shouldBe validMetadata.size
    }

    it("should delete sessions from state store") {
      val conf = new LivyConf()

      val sessionType = "batch"
      val sessionId = 24
      val sessionStore = mock[SessionStore]
      val session = mockSession(sessionId)

      val sm = new BatchSessionManager(conf, sessionStore, Some(Seq(session)))
      sm.get(sessionId) shouldBe defined

      Await.ready(sm.delete(sessionId).get, 30 seconds)

      verify(sessionStore).remove(sessionType, sessionId)
      sm.get(sessionId) shouldBe None
    }

    it("should delete sessions on shutdown when recovery is off") {
      val conf = new LivyConf()
      val sessionId = 24
      val sessionStore = mock[SessionStore]
      val session = mockSession(sessionId)

      val sm = new BatchSessionManager(conf, sessionStore, Some(Seq(session)))
      sm.get(sessionId) shouldBe defined
      sm.shutdown()

      verify(session).stop()
    }

    it("should not delete sessions on shutdown with recovery is on") {
      val conf = new LivyConf()
      conf.set(LivyConf.RECOVERY_MODE, SessionManager.SESSION_RECOVERY_MODE_RECOVERY)

      val sessionId = 24
      val sessionStore = mock[SessionStore]
      val session = mockSession(sessionId)

      val sm = new BatchSessionManager(conf, sessionStore, Some(Seq(session)))
      sm.get(sessionId) shouldBe defined
      sm.shutdown()

      verify(session, never).stop()
    }
  }
}
