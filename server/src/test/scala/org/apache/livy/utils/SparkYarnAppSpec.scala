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

import java.util.concurrent.CountDownLatch

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.language.postfixOps

import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.api.records.YarnApplicationState._
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.util.ConverterUtils
import org.mockito.Matchers.{any, anyString}
import org.mockito.Mockito.{atLeast, doReturn, never, times, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.{FunSpec, FunSpecLike}
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually.{eventually, interval, timeout}
import org.scalatest.mock.MockitoSugar.mock

import org.apache.livy.{LivyBaseUnitTestSuite, LivyConf}
import org.apache.livy.utils.SparkApp._


class SparkYarnAppSpec extends FunSpec with FunSpecLike with LivyBaseUnitTestSuite {

  private def mockSleep(ms: Long) = {
    Thread.sleep(ms)
  }

  describe("SparkYarnApp") {
    val TEST_TIMEOUT = timeout(50 seconds)
    val RETRY_DELAY = interval(200 milliseconds)
    val appId = ConverterUtils.toApplicationId("application_1467912463905_0021")
    val appIdOption = Some(appId.toString)
    val appTag = "fakeTag"
    val diagnostics = "diagnostics"
    val driverLogUrl = "log://driver.log/url"
    val sparkUiUrl = "log://spark.ui/url"
    val livyConf = new LivyConf()
    livyConf.set(LivyConf.YARN_APP_LOOKUP_TIMEOUT, "30s")
    livyConf.set(LivyConf.YARN_POLL_INTERVAL, "500ms")

    it("should poll YARN state and terminate") {
      Clock.withSleepMethod(mockSleep) {

        val mockAppListener = mock[SparkAppListener]
        val mockYarnClient = mock[YarnClient]
        val mockAppReport = mock[ApplicationReport]
        val mockAppReports = List[ApplicationReport](mockAppReport).asJava
        val mockAttemptId = mock[ApplicationAttemptId]
        val mockAttemptReport = mock[ApplicationAttemptReport]
        val mockContainerId = mock[ContainerId]
        val mockContainerReport = mock[ContainerReport]

        val yarnInterface = new YarnInterface(livyConf, mockYarnClient)

        val app = new SparkYarnApp(
          appTag,
          appIdOption,
          None,
          Some(mockAppListener),
          livyConf,
          yarnInterface)

        when(mockYarnClient.getApplications(any[java.util.Set[String]]()))
          .thenReturn(mockAppReports)
        when(mockAppReport.getApplicationId)
          .thenReturn(appId)
        when(mockAppReport.getName)
          .thenReturn("Test-app")
        when(mockAppReport.getApplicationTags)
          .thenReturn(Set(appTag, "not used tag").asJava)
        when(mockAppReport.getDiagnostics)
          .thenReturn(diagnostics)
        when(mockAppReport.getYarnApplicationState)
          .thenReturn(YarnApplicationState.NEW)
          .thenReturn(YarnApplicationState.NEW_SAVING)
          .thenReturn(YarnApplicationState.SUBMITTED)
          .thenReturn(YarnApplicationState.ACCEPTED)
          .thenReturn(YarnApplicationState.RUNNING)
          .thenReturn(YarnApplicationState.RUNNING)
          .thenReturn(YarnApplicationState.RUNNING)
          .thenReturn(YarnApplicationState.RUNNING)
          .thenReturn(YarnApplicationState.FINISHED)

        when(mockAppReport.getFinalApplicationStatus).thenAnswer(
          new Answer[FinalApplicationStatus] {
            override def answer(invocation: InvocationOnMock): FinalApplicationStatus = {
              mockAppReport.getYarnApplicationState match {
                case FINISHED =>
                  FinalApplicationStatus.SUCCEEDED
                case _ =>
                  FinalApplicationStatus.UNDEFINED
              }
            }
          })
        when(mockAppReport.getCurrentApplicationAttemptId)
          .thenReturn(mockAttemptId)
        when(mockYarnClient.getApplicationAttemptReport(mockAttemptId))
          .thenReturn(mockAttemptReport)
        when(mockAttemptReport.getAMContainerId)
          .thenReturn(mockContainerId)
        when(mockYarnClient.getContainerReport(mockContainerId))
          .thenReturn(mockContainerReport)
        when(mockContainerReport.getLogUrl).thenReturn(driverLogUrl)

        yarnInterface.checkStatus(app)
        eventually(TEST_TIMEOUT, interval(100 millis)) {
          app.state shouldBe (SparkApp.State.FINISHED)
        }
        yarnInterface.shutdown
      }
    }

    it("should kill yarn app") {
      Clock.withSleepMethod(mockSleep) {
        val killLatch = new CountDownLatch(1)
        val mockAppListener = mock[SparkAppListener]
        val mockYarnClient = mock[YarnClient]
        val mockAppReport = mock[ApplicationReport]
        val mockAppReports = List[ApplicationReport](mockAppReport).asJava
        val mockAttemptId = mock[ApplicationAttemptId]
        val mockAttemptReport = mock[ApplicationAttemptReport]
        val mockContainerId = mock[ContainerId]
        val mockContainerReport = mock[ContainerReport]

        val yarnInterface = new YarnInterface(livyConf, mockYarnClient)

        val app = new SparkYarnApp(
          appTag,
          appIdOption,
          None,
          Some(mockAppListener),
          livyConf,
          yarnInterface)

        doReturn(mockAppReports).when(mockYarnClient).getApplications(any[java.util.Set[String]]())
        doReturn(appId).when(mockAppReport).getApplicationId
        doReturn("Test-app").when(mockAppReport).getName
        doReturn(Set(appTag, "not used tag").asJava).when(mockAppReport).getApplicationTags
        doReturn(diagnostics).when(mockAppReport).getDiagnostics
        when(mockAppReport.getYarnApplicationState).thenAnswer(
          new Answer[YarnApplicationState] {
            override def answer(invocation: InvocationOnMock): YarnApplicationState = {
              killLatch.getCount match {
                case 0 =>
                  YarnApplicationState.KILLED
                case other =>
                  YarnApplicationState.SUBMITTED
              }
            }
          })

        when(mockAppReport.getFinalApplicationStatus).thenAnswer(
          new Answer[FinalApplicationStatus] {
            override def answer(invocation: InvocationOnMock): FinalApplicationStatus = {
              mockAppReport.getYarnApplicationState match {
                case FINISHED =>
                  FinalApplicationStatus.SUCCEEDED
                case KILLED =>
                  FinalApplicationStatus.KILLED
                case FAILED =>
                  FinalApplicationStatus.FAILED
                case _ =>
                  FinalApplicationStatus.UNDEFINED
              }
            }
          })

        when(mockYarnClient.killApplication(any[ApplicationId]())).thenAnswer(
          new Answer[Unit] {
            override def answer(invocation: InvocationOnMock): Unit = {
              killLatch.countDown
            }
          }
        )

        doReturn(mockAttemptId).when(mockAppReport).getCurrentApplicationAttemptId
        doReturn(mockAttemptReport).when(mockYarnClient).getApplicationAttemptReport(mockAttemptId)
        doReturn(mockContainerId).when(mockAttemptReport).getAMContainerId
        doReturn(mockContainerReport).when(mockYarnClient).getContainerReport(mockContainerId)
        doReturn(driverLogUrl).when(mockContainerReport).getLogUrl

        yarnInterface.checkStatus(app)

        eventually(TEST_TIMEOUT, RETRY_DELAY) {
          app.state shouldBe (SparkApp.State.STARTING)
          app.isRunning shouldBe true
        }

        app.kill()
        killLatch.await

        eventually(TEST_TIMEOUT, RETRY_DELAY) {
          app.state shouldBe (SparkApp.State.KILLED)
          app.isRunning shouldBe false
        }

        app.process.foreach { p =>
          verify(p).destroy()
        }
        verify(mockYarnClient).killApplication(appId)

        assert(app.log().mkString.contains(diagnostics))
        yarnInterface.shutdown
      }
    }

    it("should return spark-submit log") {
      Clock.withSleepMethod(mockSleep) {
        val mockYarnClient = mock[YarnClient]
        val yarnInterface = mock[YarnInterface]
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

        val mockAppReport = mock[ApplicationReport]
        when(mockAppReport.getApplicationId).thenReturn(appId)
        when(mockAppReport.getYarnApplicationState).thenReturn(YarnApplicationState.FINISHED)
        when(mockAppReport.getDiagnostics).thenReturn(null)
        when(mockYarnClient.getApplicationReport(appId)).thenReturn(mockAppReport)

        val app = new SparkYarnApp(
          appTag,
          appIdOption,
          Some(mockSparkSubmit),
          None,
          livyConf,
          yarnInterface)
      }
    }

    it("can kill spark-submit while it's running") {
      Clock.withSleepMethod(mockSleep) {
        val livyConf = new LivyConf()
        livyConf.set(LivyConf.YARN_APP_LOOKUP_TIMEOUT, "0")

        val mockYarnClient = mock[YarnClient]
        val mockYarnInterface = new YarnInterface(livyConf, mockYarnClient)
        val mockSparkSubmit = mock[LineBufferedProcess]

        val sparkSubmitRunningLatch = new CountDownLatch(1)
        // Simulate a running spark-submit
        when(mockSparkSubmit.waitFor()).thenAnswer(new Answer[Int]() {
          override def answer(invocation: InvocationOnMock): Int = {
            sparkSubmitRunningLatch.await()
            0
          }
        })

        val app = new SparkYarnApp(
          appTag,
          appIdOption,
          Some(mockSparkSubmit),
          None,
          livyConf,
          mockYarnInterface)
        app.kill()
        verify(mockSparkSubmit, times(1)).destroy()
        sparkSubmitRunningLatch.countDown()
      }
    }

    it("should map YARN state to SparkApp.State correctly") {
      val mockYarnInterface = mock[YarnInterface]
      val app = new SparkYarnApp(appTag, appIdOption, None, None, livyConf, mockYarnInterface)
      app.mapYarnState(appId, NEW, FinalApplicationStatus.UNDEFINED) shouldBe State.STARTING
      app.mapYarnState(appId, NEW_SAVING, FinalApplicationStatus.UNDEFINED) shouldBe State.STARTING
      app.mapYarnState(appId, SUBMITTED, FinalApplicationStatus.UNDEFINED) shouldBe State.STARTING
      app.mapYarnState(appId, ACCEPTED, FinalApplicationStatus.UNDEFINED) shouldBe State.STARTING
      app.mapYarnState(appId, RUNNING, FinalApplicationStatus.UNDEFINED) shouldBe State.RUNNING
      app.mapYarnState(appId, FINISHED, FinalApplicationStatus.SUCCEEDED) shouldBe State.FINISHED
      app.mapYarnState(appId, FINISHED, FinalApplicationStatus.FAILED) shouldBe State.FAILED
      app.mapYarnState(appId, FINISHED, FinalApplicationStatus.KILLED) shouldBe State.KILLED
      app.mapYarnState(appId, FINISHED, FinalApplicationStatus.UNDEFINED) shouldBe State.FAILED
      app.mapYarnState(appId, FAILED, FinalApplicationStatus.UNDEFINED) shouldBe State.FAILED
      app.mapYarnState(appId, KILLED, FinalApplicationStatus.UNDEFINED) shouldBe State.KILLED
    }

    it("should expose driver log url and Spark UI url") {
      Clock.withSleepMethod(mockSleep) {

        val mockAppListener = mock[SparkAppListener]
        val mockYarnClient = mock[YarnClient]
        val mockAppReport = mock[ApplicationReport]
        val mockAppReports = List[ApplicationReport](mockAppReport).asJava
        val mockAttemptId = mock[ApplicationAttemptId]
        val mockAttemptReport = mock[ApplicationAttemptReport]
        val mockContainerId = mock[ContainerId]
        val mockContainerReport = mock[ContainerReport]

        val yarnInterface = new YarnInterface(livyConf, mockYarnClient)

        val app = new SparkYarnApp(
          appTag,
          appIdOption,
          None,
          Some(mockAppListener),
          livyConf,
          yarnInterface)

        doReturn(mockAppReports).when(mockYarnClient).getApplications(any[java.util.Set[String]])

        mockYarnClient.getApplications(YarnInterface.appType) shouldBe mockAppReports

        doReturn(appId)
          .when(mockAppReport)
          .getApplicationId
        doReturn("Test-app")
          .when(mockAppReport)
          .getName
        doReturn(Set(appTag, "not used tag").asJava).when(mockAppReport).getApplicationTags
        doReturn(diagnostics).when(mockAppReport).getDiagnostics
        doReturn(sparkUiUrl).when(mockAppReport).getTrackingUrl
        doReturn(RUNNING).when(mockAppReport).getYarnApplicationState

        doReturn(FinalApplicationStatus.UNDEFINED).when(mockAppReport).getFinalApplicationStatus
        doReturn(mockAttemptId).when(mockAppReport).getCurrentApplicationAttemptId
        doReturn(mockAttemptReport).when(mockYarnClient).getApplicationAttemptReport(mockAttemptId)
        doReturn(mockContainerId).when(mockAttemptReport).getAMContainerId
        doReturn(mockContainerReport).when(mockYarnClient).getContainerReport(mockContainerId)
        doReturn(driverLogUrl).when(mockContainerReport).getLogUrl

        @volatile var appIdStr: String = null
        when(mockAppListener.appIdKnown(anyString)).thenAnswer(new Answer[Unit]() {
          override def answer(invocation: InvocationOnMock): Unit = {
            appIdStr = invocation.getArguments.head.asInstanceOf[String]
          }
        })
        @volatile var info: AppInfo = null
        when(mockAppListener.infoChanged(any[AppInfo])).thenAnswer(new Answer[Unit]() {
          override def answer(invocation: InvocationOnMock): Unit = {
            info = invocation.getArguments.head.asInstanceOf[AppInfo]
          }
        })

        yarnInterface.checkStatus(app)

        eventually(TEST_TIMEOUT, RETRY_DELAY) {
          app.isRunning shouldBe true
        }

        eventually(TEST_TIMEOUT, RETRY_DELAY) {
          info shouldNot be(null)
          appIdStr should be(appId.toString)
        }

        verify(mockAppListener, atLeast(1)).infoChanged(any[AppInfo])
        verify(mockAppListener).appIdKnown(anyString)
        verify(mockAppReport, atLeast(1)).getTrackingUrl()
        verify(mockContainerReport, atLeast(1)).getLogUrl()
        verify(mockAppReport, atLeast(1)).getTrackingUrl()
        verify(mockContainerReport, atLeast(1)).getLogUrl()
        verify(mockYarnClient, never).getApplicationReport(appId)

        yarnInterface.shutdown

        yarnInterface.isRunning.get shouldBe false
      }
    }
  }
}
