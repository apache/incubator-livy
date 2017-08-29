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
package org.apache.livy.utils

import java.io.{ByteArrayInputStream, InputStream}
import java.nio.charset.StandardCharsets
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import scala.concurrent.duration._
import scala.language.postfixOps

import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.util.ConverterUtils
import org.apache.http.{HttpEntity, HttpHost, HttpRequest, HttpResponse}
import org.apache.http.client.HttpClient
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, render}
import org.mockito.Matchers.any
import org.mockito.Mockito.{times, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.concurrent.Eventually.{eventually, interval, timeout}
import org.scalatest.mock.MockitoSugar.mock
import org.scalatest.FunSpec
import org.scalatest.Matchers._

import org.apache.livy.{LivyBaseUnitTestSuite, LivyConf}
import org.apache.livy.utils.SparkApp._

class SparkYarnAppSpec extends FunSpec with LivyBaseUnitTestSuite {

  /**
    * Creates an `InputStream` with the given parameters to mock an `HttpRespose`.
    *
    * @param id
    * @param tag
    * @param state
    * @param finalStatus
    * @param trackingUrl
    * @param sparkUiUrl
    * @return
    */
  private def jsonApp(id: String, tag: String, state: String, finalStatus: String,
                      trackingUrl: String, sparkUiUrl: String, diagnostics: String): InputStream = {
    val jsonResponse = render("apps" -> (
      "app" ->
        List(
          // first object
          ("allocatedMB" -> -1) ~
            ("allocatedVCores" -> -1) ~
            ("amContainerLogs" -> trackingUrl) ~
            ("amHostHttpAddress" -> "rm_server:8042") ~
            ("amNodeLabelExpression" -> "") ~
            ("applicationTags" -> tag) ~
            ("applicationType" -> "SPARK") ~
            ("clusterId" -> 1501814742838L) ~
            ("clusterUsagePercentage" -> 0.0) ~
            ("diagnostics" -> diagnostics) ~
            ("elapsedTime" -> 60179) ~
            ("finalStatus" -> finalStatus) ~
            ("finishedTime" -> 1503686236168L) ~
            ("id" -> id) ~
            ("logAggregationStatus" -> "TIME_OUT") ~
            ("memorySeconds" -> 698448L) ~
            ("name" -> "livy--test-app-1") ~
            ("numAMContainerPreempted" -> 0) ~
            ("numNonAMContainerPreempted" -> 0) ~
            ("preemptedResourceMB" -> 0) ~
            ("preemptedResourceVCores" -> 0) ~
            ("priority" -> 0) ~
            ("progress" -> 100.0) ~
            ("queue" -> "default") ~
            ("queueUsagePercentage" -> 0.0) ~
            ("runningContainers" -> -1) ~
            ("startedTime" -> 1503686175989L) ~
            ("state" -> state) ~
            ("trackingUI" -> "History") ~
            ("trackingUrl" -> sparkUiUrl) ~
            ("unmanagedApplication" -> false) ~
            ("user" -> "livy") ~
            ("vcoreSeconds" -> 26)
          ,
          ("allocatedMB" -> -1) ~
            ("allocatedVCores" -> -1) ~
            ("amContainerLogs" -> "http://rm_server:8080/spark/ui/url") ~
            ("amHostHttpAddress" -> "rm_server:8042") ~
            ("amNodeLabelExpression" -> "") ~
            ("applicationTags" -> "an-app-tag-that-we-do-not-care-about") ~
            ("applicationType" -> "SPARK") ~
            ("clusterId" -> 1501814742838L) ~
            ("clusterUsagePercentage" -> 0.0) ~
            ("diagnostics" -> "fake-yarn-diagnostics") ~
            ("elapsedTime" -> 60179) ~
            ("finalStatus" -> "SUCCEEDED") ~
            ("finishedTime" -> 1503686236168L) ~
            ("id" -> "application_0000000000000_0000") ~
            ("logAggregationStatus" -> "TIME_OUT") ~
            ("memorySeconds" -> 698448L) ~
            ("name" -> "livy--test-app-1") ~
            ("numAMContainerPreempted" -> 0) ~
            ("numNonAMContainerPreempted" -> 0) ~
            ("preemptedResourceMB" -> 0) ~
            ("preemptedResourceVCores" -> 0) ~
            ("priority" -> 0) ~
            ("progress" -> 100.0) ~
            ("queue" -> "default") ~
            ("queueUsagePercentage" -> 0.0) ~
            ("runningContainers" -> -1) ~
            ("startedTime" -> 1503686175989L) ~
            ("state" -> "FINISHED") ~
            ("trackingUI" -> "History") ~
            ("trackingUrl" -> "http://rm_server:8080/tracking/url") ~
            ("unmanagedApplication" -> false) ~
            ("user" -> "livy") ~
            ("vcoreSeconds" -> 26)
        )
      ))

    new ByteArrayInputStream(compact(jsonResponse).getBytes(StandardCharsets.UTF_8))
  }

  describe("SparkYarnApp") {
    val TEST_TIMEOUT = timeout(10 seconds)
    val RETRY_DELAY = interval(500 milliseconds)
    val appIdString = "application_1467912463905_0021"
    val appId = ConverterUtils.toApplicationId(appIdString)
    val appIdOption = Some(appId.toString)
    val appTag = "fakeTag"
    val diag = "diag"
    val driverLogUrl = "log://driver.log/url"
    val sparkUiUrl = "log://spark.ui/url"
    val livyConf = new LivyConf()
    livyConf.set(LivyConf.YARN_APP_LOOKUP_TIMEOUT, "30s")
    livyConf.set(LivyConf.YARN_POLL_INTERVAL, "500ms")

    it("should poll YARN state and terminate") {
      val mockAppListener = mock[SparkAppListener]
      val mockYarnClient = mock[YarnClient]
      val mockHttpClient = mock[HttpClient]
      val mockHttpResponse = mock[HttpResponse]
      val mockHttpEntity = mock[HttpEntity]
      when(mockHttpClient.execute(any[HttpHost], any[HttpRequest])).thenReturn(mockHttpResponse)
      when(mockHttpResponse.getEntity).thenReturn(mockHttpEntity)

      val responses = List(
        jsonApp(appIdString, appTag, "NEW", "UNDEFINED", driverLogUrl, sparkUiUrl, diag),
        jsonApp(appIdString, appTag, "RUNNING", "UNDEFINED", driverLogUrl, sparkUiUrl, diag),
        jsonApp(appIdString, appTag, "RUNNING", "UNDEFINED", driverLogUrl, sparkUiUrl, diag),
        jsonApp(appIdString, appTag, "RUNNING", "UNDEFINED", driverLogUrl, sparkUiUrl, diag),
        jsonApp(appIdString, appTag, "RUNNING", "UNDEFINED", driverLogUrl, sparkUiUrl, diag),
        jsonApp(appIdString, appTag, "RUNNING", "UNDEFINED", driverLogUrl, sparkUiUrl, diag),
        jsonApp(appIdString, appTag, "RUNNING", "UNDEFINED", driverLogUrl, sparkUiUrl, diag),
        jsonApp(appIdString, appTag, "RUNNING", "UNDEFINED", driverLogUrl, sparkUiUrl, diag),
        jsonApp(appIdString, appTag, "FINISHED", "SUCCEEDED", driverLogUrl, sparkUiUrl, diag)
      )

      when(mockHttpEntity.getContent).thenAnswer {
        new Answer[InputStream] {
          // will be incremented to 0 on the fist call to `incrementAndGet`
          val count = new AtomicInteger(-1)

          override def answer(invocationOnMock: InvocationOnMock): InputStream = {
            val index = count.incrementAndGet()
            val inputStream = responses(Math.min(index, responses.size - 1))
            inputStream.reset() // reset needed because we are reusing input streams
            inputStream
          }
        }
      }
      val yarnInterface = new YarnInterface(livyConf, mockYarnClient, mockHttpClient)

      eventually(TEST_TIMEOUT, RETRY_DELAY) {
        yarnInterface.isRunning.get shouldBe true
      }

      val app = new SparkYarnApp(
        appTag,
        appIdOption,
        None,
        Some(mockAppListener),
        livyConf,
        yarnInterface)

      yarnInterface.checkStatus(app)

      eventually(TEST_TIMEOUT, RETRY_DELAY) {
        app.state shouldBe (SparkApp.State.FINISHED)
      }

      yarnInterface.shutdown

      eventually(TEST_TIMEOUT, RETRY_DELAY) {
        yarnInterface.isRunning.get shouldBe false
      }
    }

    it("should kill yarn app") {
      val mockAppListener = mock[SparkAppListener]
      val mockYarnClient = mock[YarnClient]
      val mockHttpClient = mock[HttpClient]
      val mockHttpResponse = mock[HttpResponse]
      val mockHttpEntity = mock[HttpEntity]
      when(mockHttpClient.execute(any[HttpHost], any[HttpRequest])).thenReturn(mockHttpResponse)
      when(mockHttpResponse.getEntity).thenReturn(mockHttpEntity)


      val appKilled = new AtomicBoolean(false)
      when(mockHttpEntity.getContent).thenAnswer {
        new Answer[InputStream] {
          override def answer(invocation: InvocationOnMock): InputStream = {
            val inputStream = if (!appKilled.get) {
              jsonApp(appIdString, appTag, "RUNNING", "UNDEFINED", driverLogUrl, sparkUiUrl, diag)
            } else {
              jsonApp(appIdString, appTag, "KILLED", "KILLED", driverLogUrl, sparkUiUrl, diag)
            }
            inputStream.reset()
            inputStream
          }
        }
      }

      val yarnInterface = new YarnInterface(livyConf, mockYarnClient, mockHttpClient)

      eventually(TEST_TIMEOUT, RETRY_DELAY) {
        yarnInterface.isRunning.get shouldBe true
      }

      val app = new SparkYarnApp(
        appTag,
        appIdOption,
        None,
        Some(mockAppListener),
        livyConf,
        yarnInterface)

      yarnInterface.checkStatus(app)

      eventually(TEST_TIMEOUT, RETRY_DELAY) {
        app.state shouldBe (SparkApp.State.RUNNING)
      }
      appKilled.set(true)
      app.kill()

      eventually(TEST_TIMEOUT, RETRY_DELAY) {
        app.state shouldBe (SparkApp.State.KILLED)
      }

      verify(mockYarnClient).killApplication(appId)
      assert(app.log().mkString.contains(diag))

      yarnInterface.shutdown

      eventually(TEST_TIMEOUT, RETRY_DELAY) {
        yarnInterface.isRunning.get shouldBe false
      }
    }

    it("should return spark-submit log") {
      val mockYarnClient = mock[YarnClient]
      val mockHttpClient = mock[HttpClient]
      val mockHttpResponse = mock[HttpResponse]
      val mockHttpEntity = mock[HttpEntity]
      when(mockHttpClient.execute(any[HttpHost], any[HttpRequest])).thenReturn(mockHttpResponse)
      when(mockHttpResponse.getEntity).thenReturn(mockHttpEntity)

      when(mockHttpEntity.getContent).thenAnswer {
        new Answer[InputStream] {
          override def answer(invocationOnMock: InvocationOnMock): InputStream = {
            val inputStream =
              jsonApp(appIdString, appTag, "STARTING", "UNDEFINED", driverLogUrl, sparkUiUrl, "")
            inputStream.reset()
            inputStream
          }
        }
      }
      val mockSparkSubmit = mock[LineBufferedProcess]
      val sparkSubmitInfoLog = IndexedSeq("SPARK-SUBMIT", "LOG")
      val sparkSubmitErrorLog = IndexedSeq("SPARK-SUBMIT", "error log")
      val sparkSubmitLog = ("stdout: " +: sparkSubmitInfoLog) ++
        ("\nstderr: " +: sparkSubmitErrorLog) :+ "\nYARN Diagnostics: "
      when(mockSparkSubmit.inputLines).thenReturn(sparkSubmitInfoLog)
      when(mockSparkSubmit.errorLines).thenReturn(sparkSubmitErrorLog)
      val waitForCalledLatch = new CountDownLatch(1)
      when(mockSparkSubmit.waitFor()).thenAnswer(new Answer[Int]() {
        override def answer(invocation: InvocationOnMock): Int = {
          waitForCalledLatch.countDown()
          0
        }
      })

      val yarnInterface = new YarnInterface(livyConf, mockYarnClient, mockHttpClient)

      eventually(TEST_TIMEOUT, RETRY_DELAY) {
        yarnInterface.isRunning.get shouldBe true
      }

      val app = new SparkYarnApp(
        appTag,
        appIdOption,
        Some(mockSparkSubmit),
        None,
        livyConf,
        yarnInterface)

      yarnInterface.checkStatus(app)

      waitForCalledLatch.await(TEST_TIMEOUT.value.millisPart, TimeUnit.MILLISECONDS)
      assert(app.log() == sparkSubmitLog)
      assert(app.log() == sparkSubmitLog, "Expect spark-submit log")

      yarnInterface.shutdown

      eventually(TEST_TIMEOUT, RETRY_DELAY) {
        yarnInterface.isRunning.get shouldBe false
      }
    }

    it("can kill spark-submit while it's running") {

      val livyConf = new LivyConf()
      livyConf.set(LivyConf.YARN_APP_LOOKUP_TIMEOUT, "0")
      val mockYarnClient = mock[YarnClient]
      val mockHttpClient = mock[HttpClient]
      val mockHttpResponse = mock[HttpResponse]
      val mockHttpEntity = mock[HttpEntity]
      val mockSparkSubmit = mock[LineBufferedProcess]

      when(mockHttpClient.execute(any[HttpHost], any[HttpRequest])).thenReturn(mockHttpResponse)
      when(mockHttpResponse.getEntity).thenReturn(mockHttpEntity)

      when(mockHttpEntity.getContent).thenAnswer {
        new Answer[InputStream] {
          override def answer(invocationOnMock: InvocationOnMock): InputStream = {
            val inputStream =
              jsonApp(appIdString, appTag, "STARTING", "UNDEFINED", driverLogUrl, sparkUiUrl, "")
            inputStream.reset()
            inputStream
          }
        }
      }

      val sparkSubmitRunningLatch = new CountDownLatch(1)
      // Simulate a running spark-submit
      when(mockSparkSubmit.waitFor()).thenAnswer(new Answer[Int]() {
        override def answer(invocation: InvocationOnMock): Int = {
          sparkSubmitRunningLatch.await(TEST_TIMEOUT.value.toMillis, TimeUnit.MILLISECONDS)
          0
        }
      })

      val yarnInterface = new YarnInterface(livyConf, mockYarnClient, mockHttpClient)

      eventually(TEST_TIMEOUT, RETRY_DELAY) {
        yarnInterface.isRunning.get shouldBe true
      }

      val app = new SparkYarnApp(
        appTag,
        appIdOption,
        Some(mockSparkSubmit),
        None,
        livyConf,
        yarnInterface
      )
      app.kill()
      verify(mockSparkSubmit, times(1)).destroy()
      sparkSubmitRunningLatch.countDown()

      yarnInterface.shutdown

      eventually(TEST_TIMEOUT, RETRY_DELAY) {
        yarnInterface.isRunning.get shouldBe false
      }
    }

    it("should map YARN state to SparkApp.State correctly") {
      val app = new SparkYarnApp(appTag, appIdOption, None, None, livyConf, null)
      assert(app.mapYarnState(appIdString, "NEW", "UNDEFINED") == State.STARTING)
      assert(app.mapYarnState(appIdString, "NEW_SAVING", "UNDEFINED") == State.STARTING)
      assert(app.mapYarnState(appIdString, "SUBMITTED", "UNDEFINED") == State.STARTING)
      assert(app.mapYarnState(appIdString, "ACCEPTED", "UNDEFINED") == State.STARTING)
      assert(app.mapYarnState(appIdString, "RUNNING", "UNDEFINED") == State.RUNNING)
      assert(
        app.mapYarnState(appIdString, "FINISHED", "SUCCEEDED") == State.FINISHED)
      assert(app.mapYarnState(appIdString, "FAILED", "FAILED") == State.FAILED)
      assert(app.mapYarnState(appIdString, "KILLED", "KILLED") == State.KILLED)

      // none of the (state , finalStatus) combination below should happen
      assert(app.mapYarnState(appIdString, "FINISHED", "UNDEFINED") == State.FAILED)
      assert(app.mapYarnState(appIdString, "FINISHED", "FAILED") == State.FAILED)
      assert(app.mapYarnState(appIdString, "FINISHED", "KILLED") == State.FAILED)
      assert(app.mapYarnState(appIdString, "FAILED", "UNDEFINED") == State.FAILED)
      assert(app.mapYarnState(appIdString, "KILLED", "UNDEFINED") == State.FAILED)
      assert(app.mapYarnState(appIdString, "FAILED", "SUCCEEDED") == State.FAILED)
      assert(app.mapYarnState(appIdString, "KILLED", "SUCCEEDED") == State.FAILED)
    }

    it("should expose driver log url and Spark UI url") {
      val mockYarnClient = mock[YarnClient]
      val mockHttpClient = mock[HttpClient]
      val mockHttpResponse = mock[HttpResponse]
      val mockHttpEntity = mock[HttpEntity]
      when(mockHttpClient.execute(any[HttpHost], any[HttpRequest])).thenReturn(mockHttpResponse)
      when(mockHttpResponse.getEntity).thenReturn(mockHttpEntity)

      @volatile var done = false
      when(mockHttpEntity.getContent).thenAnswer {
        new Answer[InputStream] {

          override def answer(invocationOnMock: InvocationOnMock): InputStream = {
            val inputStream = if (!done) {
              jsonApp(appIdString, appTag, "RUNNING", "UNDEFINED", driverLogUrl, sparkUiUrl, diag)
            } else {
              jsonApp(appIdString, appTag, "FINISHED", "FINISHED", driverLogUrl, sparkUiUrl, diag)
            }
            inputStream.reset()
            inputStream
          }
        }
      }

      val yarnInterface = new YarnInterface(livyConf, mockYarnClient, mockHttpClient)

      eventually(TEST_TIMEOUT, RETRY_DELAY) {
        yarnInterface.isRunning.get shouldBe true
      }

      val mockListener = mock[SparkAppListener]

      val app = new SparkYarnApp(
        appTag, appIdOption, None, Some(mockListener), livyConf, yarnInterface)
      yarnInterface.checkStatus(app)

      done = true

      eventually(TEST_TIMEOUT, RETRY_DELAY) {
        verify(mockListener).appIdKnown(appId.toString)
        verify(mockListener).infoChanged(AppInfo(Some(driverLogUrl), Some(sparkUiUrl)))
      }

      yarnInterface.shutdown

      eventually(TEST_TIMEOUT, RETRY_DELAY) {
        yarnInterface.isRunning.get shouldBe false
      }
    }

    // This test is irrelevant after refactoring YARN polling to use YARN's REST API
    it("should not die on YARN-4411") {
      val mockYarnClient = mock[YarnClient]
      val mockHttpClient = mock[HttpClient]
      val mockHttpResponse = mock[HttpResponse]
      val mockHttpEntity = mock[HttpEntity]
      when(mockHttpClient.execute(any[HttpHost], any[HttpRequest])).thenReturn(mockHttpResponse)
      when(mockHttpResponse.getEntity).thenReturn(mockHttpEntity)
      when(mockHttpClient.execute(any[HttpHost], any[HttpRequest])).thenReturn(mockHttpResponse)
      when(mockHttpResponse.getEntity).thenReturn(mockHttpEntity)

      when(mockHttpEntity.getContent).thenAnswer {
        new Answer[InputStream] {

          override def answer(invocationOnMock: InvocationOnMock): InputStream = {
            throw new IllegalArgumentException("No enum constant " +
              "org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState.FINAL_SAVING")
          }
        }
      }
      // Block test until getApplicationReport is called 10 times.
      val pollCountDown = new CountDownLatch(10)
      when(mockYarnClient.getApplicationReport(appId)).thenAnswer(new Answer[ApplicationReport] {
        override def answer(invocation: InvocationOnMock): ApplicationReport = {
          pollCountDown.countDown()
          throw new IllegalArgumentException("No enum constant " +
            "org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState.FINAL_SAVING")
        }
      })

      val yarnInterface = new YarnInterface(livyConf, mockYarnClient, mockHttpClient)

      eventually(TEST_TIMEOUT, RETRY_DELAY) {
        yarnInterface.isRunning.get shouldBe true
      }

      val app = new SparkYarnApp(appTag, appIdOption, None, None, livyConf, yarnInterface)
      pollCountDown.await(TEST_TIMEOUT.value.millisPart, TimeUnit.MILLISECONDS)
      eventually(TEST_TIMEOUT, RETRY_DELAY) {
        assert(app.state == SparkApp.State.STARTING)
      }
      eventually(TEST_TIMEOUT, RETRY_DELAY) {
        app.state = SparkApp.State.FINISHED
      }

      yarnInterface.shutdown

      eventually(TEST_TIMEOUT, RETRY_DELAY) {
        yarnInterface.isRunning.get shouldBe false
      }
    }

    // This test is irrelevant after refactoring YARN polling to use YARN's REST API
    it("should not die on ApplicationAttemptNotFoundException") {
      val mockYarnClient = mock[YarnClient]
      val mockHttpClient = mock[HttpClient]
      val mockHttpResponse = mock[HttpResponse]
      val mockHttpEntity = mock[HttpEntity]
      when(mockHttpClient.execute(any[HttpHost], any[HttpRequest])).thenReturn(mockHttpResponse)
      when(mockHttpResponse.getEntity).thenReturn(mockHttpEntity)

      when(mockHttpEntity.getContent).thenAnswer {
        new Answer[InputStream] {
          override def answer(invocationOnMock: InvocationOnMock): InputStream = {
            // return an empty response, so the LIVY never finds the application on YARN
            new ByteArrayInputStream("".getBytes(StandardCharsets.UTF_8))
          }
        }
      }
      val yarnInterface = new YarnInterface(livyConf, mockYarnClient, mockHttpClient)

      eventually(TEST_TIMEOUT, RETRY_DELAY) {
        yarnInterface.isRunning.get shouldBe true
      }

      val app = new SparkYarnApp(appTag, appIdOption, None, None, livyConf, yarnInterface)

      eventually(TEST_TIMEOUT, RETRY_DELAY) {
        assert(app.state == SparkApp.State.STARTING)
      }

      yarnInterface.shutdown

      eventually(TEST_TIMEOUT, RETRY_DELAY) {
        yarnInterface.isRunning.get shouldBe false
      }
    }
  }
}
