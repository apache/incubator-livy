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

import java.io.FileWriter
import java.nio.file.{Files, Path}

import org.apache.spark.launcher.SparkLauncher
import org.json4s.jackson.Json4sScalaModule
import org.scalatest.mock.MockitoSugar.mock

import org.apache.livy.LivyConf
import org.apache.livy.rsc.RSCConf
import org.apache.livy.server.{AccessManager, BaseSessionServletSpec}
import org.apache.livy.server.batch.{BatchRecoveryMetadata, BatchSession}
import org.apache.livy.server.batch.BatchSessionServlet
import org.apache.livy.server.batch.CreateBatchRequest
import org.apache.livy.server.recovery.SessionStore
import org.apache.livy.sessions.{BatchSessionManager, InteractiveSessionManager}
import org.apache.livy.sessions.{Kind, SessionKindModule, Spark}

class FailedInteractiveSessionServletSpec
  extends BaseSessionServletSpec[BatchSession, BatchRecoveryMetadata] {

  mapper.registerModule(new Json4sScalaModule())

  val script: Path = {
    val script = Files.createTempFile("livy-test", ".py")
    script.toFile.deleteOnExit()
    val writer = new FileWriter(script.toFile)
    try {
      writer.write(
        """
          |print "hello world"
        """.stripMargin)
    } finally {
      writer.close()
    }
    script
  }

  protected def createRequest(
                               inProcess: Boolean = true,
                               extraConf: Map[String, String] = Map(),
                               kind: Kind = Spark()): CreateInteractiveRequest = {
    val classpath = sys.props("java.class.path")
    val request = new CreateInteractiveRequest()
    request.kind = kind
    request.conf = extraConf ++ Map(
      RSCConf.Entry.LIVY_JARS.key() -> "",
      RSCConf.Entry.CLIENT_IN_PROCESS.key() -> inProcess.toString,
      SparkLauncher.SPARK_MASTER -> "local",
      SparkLauncher.DRIVER_EXTRA_CLASSPATH -> classpath,
      SparkLauncher.EXECUTOR_EXTRA_CLASSPATH -> classpath
    )
    request
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

  describe("Interactive Servlet") {
    it("should failed to create interactive session") {
      jget[Map[String, Any]]("/") { data =>
        data("sessions") should equal(Seq())
      }

      val createBatchRequest = new CreateBatchRequest()
      createBatchRequest.file = script.toString
      createBatchRequest.conf = Map("spark.driver.extraClassPath" -> sys.props("java.class.path"))

      val createInteractiveRequest = createRequest()

      jpost[Map[String, Any]]("/", createBatchRequest) { data => None }

      val tmp = servlet.livyConf.getInt(LivyConf.MAX_CREATING_SESSION)
      servlet.livyConf.set(LivyConf.MAX_CREATING_SESSION, 1)
      jpost[Map[String, Any]]("/", createInteractiveRequest, 400) { data => None }
      servlet.livyConf.set(LivyConf.MAX_CREATING_SESSION, tmp)

      jdelete[Map[String, Any]]("/0") { data => None }
    }
  }
}
