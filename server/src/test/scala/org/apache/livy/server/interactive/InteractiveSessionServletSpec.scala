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

package org.apache.livy.server.interactive

import java.util.concurrent.atomic.AtomicInteger
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.language.postfixOps

import org.json4s.jackson.Json4sScalaModule
import org.mockito.Matchers._
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.Entry
import org.scalatest.concurrent.Eventually._
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.livy.{ExecuteRequest, LivyConf}
import org.apache.livy.client.common.HttpMessages.SessionInfo
import org.apache.livy.rsc.driver.{Statement, StatementState}
import org.apache.livy.server.AccessManager
import org.apache.livy.server.recovery.SessionStore
import org.apache.livy.sessions._
import org.apache.livy.utils.AppInfo

class InteractiveSessionServletSpec extends BaseInteractiveServletSpec {

  mapper.registerModule(new Json4sScalaModule())

  class MockInteractiveSessionServlet(
      sessionManager: InteractiveSessionManager,
      conf: LivyConf,
      accessManager: AccessManager)
    extends InteractiveSessionServlet(sessionManager, mock[SessionStore], conf, accessManager) {

    private var statements = IndexedSeq[Statement]()

    override protected def createSession(req: HttpServletRequest): InteractiveSession = {
      super.createSession(req)

      val statementCounter = new AtomicInteger()

      val session = mock[InteractiveSession]
      when(session.kind).thenReturn(Spark)
      when(session.name).thenReturn(None)
      when(session.appId).thenReturn(None)
      when(session.appInfo).thenReturn(AppInfo())
      when(session.logLines()).thenReturn(IndexedSeq())
      when(session.state).thenReturn(SessionState.Idle)
      when(session.stop()).thenReturn(Future.successful(()))
      when(session.proxyUser).thenReturn(None)
      when(session.heartbeatExpired).thenReturn(false)
      when(session.ttl).thenReturn(None)
      when(session.statements).thenAnswer(
        new Answer[IndexedSeq[Statement]]() {
          override def answer(args: InvocationOnMock): IndexedSeq[Statement] = statements
        })
      when(session.executeStatement(any(classOf[ExecuteRequest]))).thenAnswer(
        new Answer[Statement]() {
          override def answer(args: InvocationOnMock): Statement = {
            val id = statementCounter.getAndIncrement
            val statement = new Statement(id, "1+1", StatementState.Available, "1")

            statements :+= statement
            statement
          }
        })
      when(session.cancelStatement(anyInt())).thenAnswer(
        new Answer[Unit] {
          override def answer(args: InvocationOnMock): Unit = {
            statements = IndexedSeq(
              new Statement(statementCounter.get(), null, StatementState.Cancelled, null))
          }
        }
      )

      session
    }

  }

  override def createServlet(): InteractiveSessionServlet = {
    val conf = createConf()
    val sessionManager = new InteractiveSessionManager(conf, mock[SessionStore], Some(Seq.empty))
    val accessManager = new AccessManager(conf)
    new MockInteractiveSessionServlet(sessionManager, conf, accessManager)
  }

  it("should setup and tear down an interactive session") {
    jget[Map[String, Any]]("/") { data =>
      data("sessions") should equal(Seq())
    }

    jpost[Map[String, Any]]("/", createRequest()) { data =>
      header("Location") should equal("/0")
      data("id") should equal (0)

      val session = servlet.sessionManager.get(0)
      session should be (defined)
    }

    jget[Map[String, Any]]("/0") { data =>
      data("id") should equal (0)
      data("state") should equal ("idle")

      val batch = servlet.sessionManager.get(0)
      batch should be (defined)
    }

    jpost[Map[String, Any]]("/0/statements", ExecuteRequest("foo", Some("spark"))) { data =>
      data("id") should be (0)
      data("code") shouldBe "1+1"
      data("progress") should be (0.0)
      data("output") shouldBe 1
    }

    jget[Map[String, Any]]("/0/statements") { data =>
      data("total_statements") should be (1)
      data("statements").asInstanceOf[Seq[Map[String, Any]]](0)("id") should be (0)
    }

    jpost[Map[String, Any]]("/0/statements/0/cancel", null, HttpServletResponse.SC_OK) { data =>
      data should equal(Map("msg" -> "canceled"))
    }

    jget[Map[String, Any]]("/0/statements") { data =>
      data("total_statements") should be (1)
      data("statements").asInstanceOf[Seq[Map[String, Any]]](0)("state") should be ("cancelled")
    }

    jdelete[Map[String, Any]]("/0") { data =>
      data should equal (Map("msg" -> "deleted"))

      val session = servlet.sessionManager.get(0)
      session should not be defined
    }
  }

  Seq(Some("TEST-interactive-session"), None)
    .foreach { case name =>
      it(s"should show session properties (name =$name") {
        testShowSessionProperties(name: Option[String])
      }
    }

  def testShowSessionProperties(name: Option[String]): Unit = {
    val id = 0
    val appId = "appid"
    val owner = "owner"
    val proxyUser = "proxyUser"
    val state = SessionState.Running
    val kind = Spark
    val appInfo = AppInfo(Some("DRIVER LOG URL"), Some("SPARK UI URL"))
    val log = IndexedSeq[String]("log1", "log2")

    val session = mock[InteractiveSession]
    when(session.id).thenReturn(id)
    when(session.name).thenReturn(name)
    when(session.appId).thenReturn(Some(appId))
    when(session.owner).thenReturn(owner)
    when(session.proxyUser).thenReturn(Some(proxyUser))
    when(session.state).thenReturn(state)
    when(session.kind).thenReturn(kind)
    when(session.appInfo).thenReturn(appInfo)
    when(session.logLines()).thenReturn(log)
    when(session.heartbeatExpired).thenReturn(false)
    when(session.ttl).thenReturn(None)

    val req = mock[HttpServletRequest]

    val view = servlet.asInstanceOf[InteractiveSessionServlet].clientSessionView(session, req)
      .asInstanceOf[SessionInfo]

    view.id shouldEqual id
    Option(view.name) shouldEqual name
    view.appId shouldEqual appId
    view.owner shouldEqual owner
    view.proxyUser shouldEqual proxyUser
    view.state shouldEqual state.toString
    view.kind shouldEqual kind.toString
    view.appInfo should contain (Entry(AppInfo.DRIVER_LOG_URL_NAME, appInfo.driverLogUrl.get))
    view.appInfo should contain (Entry(AppInfo.SPARK_UI_URL_NAME, appInfo.sparkUiUrl.get))
    view.log shouldEqual log.asJava
  }

  private def waitSession(): Unit = {
    eventually(timeout(1 minute), interval(100 millis)) {
      servlet.tooManySessions should be(true)
    }
  }

  it("should failed create session when too many creating session ") {
    var id = 1
    jpost[SessionInfo]("/", createRequest(inProcess = false)) { data =>
      id = data.id
    }

    servlet.livyConf.set(LivyConf.SESSION_MAX_CREATION, 1)

    waitSession
    jpost[Map[String, Any]]("/", createRequest(), HttpServletResponse.SC_BAD_REQUEST) { data =>
      None
    }

    jdelete[Map[String, Any]](s"/${id}") { _ => }
  }
}
