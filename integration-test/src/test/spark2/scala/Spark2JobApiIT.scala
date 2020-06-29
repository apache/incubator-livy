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

package org.apache.livy.test

import java.io.File
import java.net.URI
import java.util.concurrent.{TimeUnit, Future => JFuture}
import javax.servlet.http.HttpServletResponse

import scala.util.Properties

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.http.client.methods.HttpGet
import org.scalatest.BeforeAndAfterAll

import org.apache.livy._
import org.apache.livy.client.common.HttpMessages._
import org.apache.livy.sessions.SessionKindModule
import org.apache.livy.test.framework.BaseIntegrationTestSuite
import org.apache.livy.test.jobs.spark2._
import org.apache.livy.utils.LivySparkUtils

class Spark2JobApiIT extends BaseIntegrationTestSuite with BeforeAndAfterAll with Logging {

  private var client: LivyClient = _
  private var sessionId: Int = _
  private val mapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .registerModule(new SessionKindModule())

  override def afterAll(): Unit = {
    super.afterAll()

    if (client != null) {
      client.stop(true)
    }

    livyClient.connectSession(sessionId).stop()
  }

  scalaTest("create a new session and upload test jar") {
    val prevSessionCount = sessionList().total
    val tempClient = createClient(livyEndpoint)

    try {
      // Figure out the session ID by poking at the REST endpoint. We should probably expose this
      // in the Java API.
      val list = sessionList()
      assert(list.total === prevSessionCount + 1)
      val tempSessionId = list.sessions(0).id

      livyClient.connectSession(tempSessionId).verifySessionIdle()
      waitFor(tempClient.uploadJar(new File(testLib)))

      client = tempClient
      sessionId = tempSessionId
    } finally {
      if (client == null) {
        try {
          if (tempClient != null) {
            tempClient.stop(true)
          }
        } catch {
          case e: Exception => warn("Error stopping client.", e)
        }
      }
    }
  }

  scalaTest("run spark2 job") {
    assume(client != null, "Client not active.")
    val result = waitFor(client.submit(new SparkSessionTest()))
    assert(result === 3)
  }

  scalaTest("run spark2 dataset job") {
    assume(client != null, "Client not active.")
    val result = waitFor(client.submit(new DatasetTest()))
    assert(result === 2)
  }

  private def waitFor[T](future: JFuture[T]): T = {
    future.get(60, TimeUnit.SECONDS)
  }

  private def sessionList(): SessionList = {
    val httpGet = new HttpGet(s"$livyEndpoint/sessions/")
    val r = livyClient.httpClient.execute(httpGet)
    val statusCode = r.getStatusLine().getStatusCode()
    val responseBody = r.getEntity().getContent
    val sessionList = mapper.readValue(responseBody, classOf[SessionList])
    r.close()

    assert(statusCode ==  HttpServletResponse.SC_OK)
    sessionList
  }

  private def createClient(uri: String): LivyClient = {
    new LivyClientBuilder().setURI(new URI(uri)).build()
  }

  protected def scalaTest(desc: String)(testFn: => Unit): Unit = {
    test(desc) {
      val livyConf = new LivyConf()
      val (sparkVersion, scalaVersion) = LivySparkUtils.sparkSubmitVersion(livyConf)
      val formattedSparkVersion = LivySparkUtils.formatSparkVersion(sparkVersion)
      val versionString =
        LivySparkUtils.sparkScalaVersion(formattedSparkVersion, scalaVersion, livyConf)

      assume(versionString == LivySparkUtils.formatScalaVersion(Properties.versionNumberString),
        s"Scala test can only be run with ${Properties.versionString}")
      testFn
    }
  }
}
