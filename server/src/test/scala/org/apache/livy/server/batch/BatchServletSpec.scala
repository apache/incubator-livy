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

package org.apache.livy.server.batch

import java.io.FileWriter
import java.nio.file.{Files, Path}
import java.util.concurrent.TimeUnit
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse._

import scala.concurrent.duration.Duration

import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.livy.{LivyConf, Utils}
import org.apache.livy.server.{AccessManager, BaseSessionServletSpec}
import org.apache.livy.server.recovery.SessionStore
import org.apache.livy.sessions.{BatchSessionManager, SessionState}
import org.apache.livy.utils.AppInfo

class BatchServletSpec extends BaseSessionServletSpec[BatchSession, BatchRecoveryMetadata] {

  val script: Path = {
    val script = Files.createTempFile("livy-test", ".py")
    script.toFile.deleteOnExit()
    val writer = new FileWriter(script.toFile)
    try {
      writer.write(
        """
          |print("hello world")
        """.stripMargin)
    } finally {
      writer.close()
    }
    script
  }

  override def createServlet(): BatchSessionServlet = {
    val livyConf = createConf()
    val sessionStore = mock[SessionStore]
    val accessManager = new AccessManager(livyConf)
    new BatchSessionServlet(
      new BatchSessionManager(livyConf, sessionStore, Some(Seq.empty)),
      sessionStore,
      livyConf,
      accessManager)
  }

  def testShowSessionProperties(name: Option[String]): Unit = {
    val id = 0
    val state = SessionState.Running
    val appId = "appid"
    val owner = "owner"
    val proxyUser = "proxyUser"
    val appInfo = AppInfo(Some("DRIVER LOG URL"), Some("SPARK UI URL"))
    val log = IndexedSeq[String]("log1", "log2")

    val session = mock[BatchSession]
    when(session.id).thenReturn(id)
    when(session.name).thenReturn(name)
    when(session.state).thenReturn(state)
    when(session.appId).thenReturn(Some(appId))
    when(session.appInfo).thenReturn(appInfo)
    when(session.logLines()).thenReturn(log)
    when(session.owner).thenReturn(owner)
    when(session.proxyUser).thenReturn(Some(proxyUser))

    val req = mock[HttpServletRequest]

    val view = servlet.asInstanceOf[BatchSessionServlet].clientSessionView(session, req)
      .asInstanceOf[BatchSessionView]

    view.id shouldEqual id
    view.name shouldEqual name
    view.state shouldEqual state.toString
    view.appId shouldEqual Some(appId)
    view.owner shouldEqual owner
    view.proxyUser shouldEqual Some(proxyUser)
    view.appInfo shouldEqual appInfo
    view.log shouldEqual log
  }

  describe("Batch Servlet") {
    it("should create and tear down a batch") {
      jget[Map[String, Any]]("/") { data =>
        data("sessions") should equal (Seq())
      }

      val createRequest = new CreateBatchRequest()
      createRequest.file = script.toString
      createRequest.conf = Map("spark.driver.extraClassPath" -> sys.props("java.class.path"))

      jpost[Map[String, Any]]("/", createRequest) { data =>
        header("Location") should equal("/0")
        data("id") should equal (0)

        val batch = servlet.sessionManager.get(0)
        batch should be (defined)
      }

      // Wait for the process to finish.
      {
        val batch = servlet.sessionManager.get(0).get
        Utils.waitUntil({ () => !batch.state.isActive }, Duration(10, TimeUnit.SECONDS))
        (batch.state match {
          case SessionState.Success(_) => true
          case _ => false
        }) should be (true)
      }

      jget[Map[String, Any]]("/0") { data =>
        data("id") should equal (0)
        data("state") should equal ("success")

        val batch = servlet.sessionManager.get(0)
        batch should be (defined)
      }

      jget[Map[String, Any]]("/0/log?size=1000") { data =>
        data("id") should equal (0)
        data("log").asInstanceOf[Seq[String]] should contain ("hello world")

        val batch = servlet.sessionManager.get(0)
        batch should be (defined)
      }

      jdelete[Map[String, Any]]("/0") { data =>
        data should equal (Map("msg" -> "deleted"))

        val batch = servlet.sessionManager.get(0)
        batch should not be defined
      }
    }

    it("should respect config black list") {
      val createRequest = new CreateBatchRequest()
      createRequest.name = Some("TEST-BatchServletSpec-Session-0")
      createRequest.file = script.toString
      createRequest.conf = BLACKLISTED_CONFIG
      jpost[Map[String, Any]]("/", createRequest, expectedStatus = SC_BAD_REQUEST) { _ => }
    }

    Seq(None, Some("TEST-batch-session"))
      .foreach { name =>
        it(s"should show session properties (name = $name)") {
          testShowSessionProperties(name)
        }
      }

    it("should fail session creation when max session creation is hit") {
      val createRequest = new CreateBatchRequest()
      createRequest.file = script.toString
      createRequest.conf = Map("spark.driver.extraClassPath" -> sys.props("java.class.path"))

      jpost[Map[String, Any]]("/", createRequest) { data =>
        header("Location") should equal("/2")
        data("id") should equal (2)

        val batch = servlet.sessionManager.get(2)
        batch should be (defined)
      }

      servlet.livyConf.set(LivyConf.SESSION_MAX_CREATION, 1)
      jpost[Map[String, Any]]("/", createRequest, SC_BAD_REQUEST) { data => None }

      jdelete[Map[String, Any]]("/2") { data =>
        data should equal (Map("msg" -> "deleted"))

        val batch = servlet.sessionManager.get(2)
        batch should not be defined
      }
    }
  }
}
