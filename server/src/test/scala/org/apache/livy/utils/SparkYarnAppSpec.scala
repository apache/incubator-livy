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

import java.util.ArrayList
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.language.postfixOps

import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus.UNDEFINED
import org.apache.hadoop.yarn.api.records.YarnApplicationState._
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException
import org.apache.hadoop.yarn.util.ConverterUtils
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.concurrent.Eventually
import org.scalatest.FunSpec
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.livy.{LivyBaseUnitTestSuite, LivyConf, Utils}
import org.apache.livy.utils.SparkApp._

class SparkYarnAppSpec extends FunSpec with LivyBaseUnitTestSuite {
  private def cleanupThread(t: Thread)(f: => Unit) = {
    try { f } finally { t.interrupt() }
  }

  private def mockSleep(ms: Long) = {
    Thread.`yield`()
  }

  describe("SparkYarnApp") {
    val TEST_TIMEOUT = 30 seconds
    val appId = ConverterUtils.toApplicationId("application_1467912463905_0021")
    val appIdOption = Some(appId.toString)
    val appTag = "fakeTag"
    val livyConf = new LivyConf()
    livyConf.set(LivyConf.YARN_APP_LOOKUP_TIMEOUT, "30s")

    it("should poll YARN state and terminate") {
      Clock.withSleepMethod(mockSleep) {
        val mockYarnClient = mock[YarnClient]
        val mockAppListener = mock[SparkAppListener]

        val mockAppReport = mock[ApplicationReport]
        when(mockAppReport.getApplicationId).thenReturn(appId)

        // Simulate YARN app state progression.
        val applicationStateList = List(
          ACCEPTED,
          RUNNING,
          FINISHED
        )
        val finalApplicationStatusList = List(
          FinalApplicationStatus.UNDEFINED,
          FinalApplicationStatus.UNDEFINED,
          FinalApplicationStatus.SUCCEEDED
       )
        val stateIndex = new AtomicInteger(-1)
        when(mockAppReport.getYarnApplicationState).thenAnswer(
          // get and increment
          new Answer[YarnApplicationState] {
            override def answer(invocationOnMock: InvocationOnMock): YarnApplicationState = {
              stateIndex.incrementAndGet match {
                case i if i < applicationStateList.size =>
                  applicationStateList(i)
                case _ =>
                  applicationStateList.last
              }
            }
          }
        )
        when(mockAppReport.getFinalApplicationStatus).thenAnswer(
          new Answer[FinalApplicationStatus] {
            override def answer(invocationOnMock: InvocationOnMock): FinalApplicationStatus = {
              // do not increment here, only get
              stateIndex.get match {
                case i if i < applicationStateList.size =>
                  finalApplicationStatusList(i)
                case _ =>
                  finalApplicationStatusList.last
              }
            }
          }
        )

        when(mockYarnClient.getApplicationReport(appId)).thenReturn(mockAppReport)

        val app = new SparkYarnApp(
          appTag,
          appIdOption,
          None,
          Some(mockAppListener),
          livyConf,
          mockYarnClient)
        cleanupThread(app.yarnAppMonitorThread) {
          app.yarnAppMonitorThread.join(TEST_TIMEOUT.toMillis)
          assert(!app.yarnAppMonitorThread.isAlive,
            "YarnAppMonitorThread should terminate after YARN app is finished.")
          verify(mockYarnClient, atLeast(1)).getApplicationReport(appId)
          verify(mockAppListener).stateChanged(State.STARTING, State.RUNNING)
          verify(mockAppListener).stateChanged(State.RUNNING, State.FINISHED)
        }
      }
    }

    it("should kill yarn app") {
      Clock.withSleepMethod(mockSleep) {
        val diag = "DIAG"
        val mockYarnClient = mock[YarnClient]

        val mockAppReport = mock[ApplicationReport]
        when(mockAppReport.getApplicationId).thenReturn(appId)
        when(mockAppReport.getDiagnostics).thenReturn(diag)
        when(mockAppReport.getFinalApplicationStatus).thenReturn(FinalApplicationStatus.SUCCEEDED)

        var appKilled = false
        when(mockAppReport.getYarnApplicationState).thenAnswer(new Answer[YarnApplicationState]() {
          override def answer(invocation: InvocationOnMock): YarnApplicationState = {
            if (!appKilled) {
              RUNNING
            } else {
              KILLED
            }
          }
        })
        when(mockYarnClient.getApplicationReport(appId)).thenReturn(mockAppReport)

        val app = new SparkYarnApp(appTag, appIdOption, None, None, livyConf, mockYarnClient)
        Utils.waitUntil({ () => app.isRunning }, Duration(10, TimeUnit.SECONDS))
        cleanupThread(app.yarnAppMonitorThread) {
          app.kill()
          appKilled = true

          app.yarnAppMonitorThread.join(TEST_TIMEOUT.toMillis)
          assert(!app.yarnAppMonitorThread.isAlive,
            "YarnAppMonitorThread should terminate after YARN app is finished.")
          verify(mockYarnClient, atLeast(1)).getApplicationReport(appId)
          verify(mockYarnClient).killApplication(appId)
          assert(app.log().mkString.contains(diag))
        }
      }
    }

    it("should return spark-submit log") {
      Clock.withSleepMethod(mockSleep) {
        val mockYarnClient = mock[YarnClient]
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
          mockYarnClient)
        cleanupThread(app.yarnAppMonitorThread) {
          waitForCalledLatch.await(TEST_TIMEOUT.toMillis, TimeUnit.MILLISECONDS)
          assert(app.log() == sparkSubmitLog, "Expect spark-submit log")
        }
      }
    }

    it("can kill spark-submit while it's running") {
      Clock.withSleepMethod(mockSleep) {
        val diag = "DIAG"
        val livyConf = new LivyConf()
        livyConf.set(LivyConf.YARN_APP_LOOKUP_TIMEOUT, "0")

        val mockAppReport = mock[ApplicationReport]
        when(mockAppReport.getApplicationId).thenReturn(appId)
        when(mockAppReport.getDiagnostics).thenReturn(diag)
        when(mockAppReport.getFinalApplicationStatus).thenReturn(FinalApplicationStatus.SUCCEEDED)
        when(mockAppReport.getYarnApplicationState).thenReturn(RUNNING)

        val mockYarnClient = mock[YarnClient]
        when(mockYarnClient.getApplicationReport(appId)).thenReturn(mockAppReport)

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
          mockYarnClient)

        Eventually.eventually(Eventually.timeout(10 seconds), Eventually.interval(100 millis)) {
          assert(app.isRunning)
          cleanupThread(app.yarnAppMonitorThread) {
            app.kill()
            verify(mockSparkSubmit, times(1)).destroy()
            sparkSubmitRunningLatch.countDown()
          }
        }
      }
    }

    it("should end with state failed when spark submit start failed") {
      Clock.withSleepMethod(mockSleep) {
        val livyConf = new LivyConf()
        val mockSparkSubmit = mock[LineBufferedProcess]
        when(mockSparkSubmit.isAlive).thenReturn(false)
        when(mockSparkSubmit.exitValue).thenReturn(-1)

        val app = new SparkYarnApp(
          appTag,
          None,
          Some(mockSparkSubmit),
          None,
          livyConf)

        cleanupThread(app.yarnAppMonitorThread) {
          app.yarnAppMonitorThread.join(TEST_TIMEOUT.toMillis)
          assert(!app.yarnAppMonitorThread.isAlive,
            "YarnAppMonitorThread should terminate after YARN app is finished.")
          assert(app.state == SparkApp.State.FAILED,
            "SparkYarnApp should end with state failed when spark submit start failed")
        }
      }
    }

    // LIVY-896
    it("should end with state failed when spark submit failed but Yarn reports SUCCESS") {
      Clock.withSleepMethod(mockSleep) {
        val diag = "DIAG"
        val mockYarnClient = mock[YarnClient]

        val mockAppReport = mock[ApplicationReport]
        when(mockAppReport.getApplicationId).thenReturn(appId)
        when(mockAppReport.getDiagnostics).thenReturn(diag)
        when(mockAppReport.getFinalApplicationStatus).thenReturn(FinalApplicationStatus.SUCCEEDED)
        when(mockAppReport.getYarnApplicationState).thenReturn(YarnApplicationState.FINISHED)
        when(mockYarnClient.getApplicationReport(appId)).thenReturn(mockAppReport)

        val mockSparkSubmit = mock[LineBufferedProcess]
        when(mockSparkSubmit.isAlive).thenReturn(false)
        when(mockSparkSubmit.exitValue).thenReturn(-1)

        val listener = new SparkAppListener {
          var nStateChanged = 0
          override def stateChanged(oldState: SparkApp.State, newState: SparkApp.State): Unit = {
            nStateChanged += 1
          }

        }
        val app = new SparkYarnApp(
          appTag,
          None,
          Some(mockSparkSubmit),
          Some(listener),
          livyConf,
          mockYarnClient)

        cleanupThread(app.yarnAppMonitorThread) {
          app.yarnAppMonitorThread.join(TEST_TIMEOUT.toMillis)
          assert(!app.yarnAppMonitorThread.isAlive,
            "YarnAppMonitorThread should terminate after YARN app is finished.")
          assert(app.state == SparkApp.State.FAILED,
            "SparkYarnApp should end with state failed when spark submit failed")
          assert(listener.nStateChanged == 1,
            "SparkYarnApp should make state change only once.")
        }
      }
    }

    // LIVY-896
    it("should never change state if spark submit not finished") {
      Clock.withSleepMethod(mockSleep) {
        val diag = "DIAG"
        val mockYarnClient = mock[YarnClient]

        val mockAppReport = mock[ApplicationReport]
        when(mockAppReport.getApplicationId).thenReturn(appId)
        when(mockAppReport.getDiagnostics).thenReturn(diag)
        when(mockAppReport.getFinalApplicationStatus).thenReturn(FinalApplicationStatus.SUCCEEDED)
        when(mockAppReport.getYarnApplicationState).thenReturn(YarnApplicationState.FINISHED)
        when(mockYarnClient.getApplicationReport(appId)).thenReturn(mockAppReport)

        val mockSparkSubmit = mock[LineBufferedProcess]
        when(mockSparkSubmit.isAlive).thenReturn(true)

        val listener = new SparkAppListener {
          var nStateChanged = 0
          override def stateChanged(oldState: SparkApp.State, newState: SparkApp.State): Unit = {
            nStateChanged += 1
          }

        }
        val app = new SparkYarnApp(
          appTag,
          None,
          Some(mockSparkSubmit),
          Some(listener),
          livyConf,
          mockYarnClient)

        cleanupThread(app.yarnAppMonitorThread) {
          // 2 seconds is sufficient - yarn poll interval should be 200ms
          app.yarnAppMonitorThread.join((2 seconds).toMillis)
          assert(app.yarnAppMonitorThread.isAlive,
            "YarnAppMonitorThread should not terminate if spark-submit is still alive.")
          assert(listener.nStateChanged == 0,
            "SparkYarnApp should not make state change.")
        }
      }
    }

    it("should map YARN state to SparkApp.State correctly") {
      val app = new SparkYarnApp(appTag, appIdOption, None, None, livyConf)
      cleanupThread(app.yarnAppMonitorThread) {
        assert(app.mapYarnState(appId, NEW, UNDEFINED) == State.STARTING)
        assert(app.mapYarnState(appId, NEW_SAVING, UNDEFINED) == State.STARTING)
        assert(app.mapYarnState(appId, SUBMITTED, UNDEFINED) == State.STARTING)
        assert(app.mapYarnState(appId, ACCEPTED, UNDEFINED) == State.STARTING)
        assert(app.mapYarnState(appId, RUNNING, UNDEFINED) == State.RUNNING)
        assert(
          app.mapYarnState(appId, FINISHED, FinalApplicationStatus.SUCCEEDED) == State.FINISHED)
        assert(app.mapYarnState(appId, FAILED, FinalApplicationStatus.FAILED) == State.FAILED)
        assert(app.mapYarnState(appId, KILLED, FinalApplicationStatus.KILLED) == State.KILLED)

        // none of the (state , finalStatus) combination below should happen
        assert(app.mapYarnState(appId, FINISHED, UNDEFINED) == State.FAILED)
        assert(app.mapYarnState(appId, FINISHED, FinalApplicationStatus.FAILED) == State.FAILED)
        assert(app.mapYarnState(appId, FINISHED, FinalApplicationStatus.KILLED) == State.FAILED)
        assert(app.mapYarnState(appId, FAILED, UNDEFINED) == State.FAILED)
        assert(app.mapYarnState(appId, KILLED, UNDEFINED) == State.FAILED)
        assert(app.mapYarnState(appId, FAILED, FinalApplicationStatus.SUCCEEDED) == State.FAILED)
        assert(app.mapYarnState(appId, KILLED, FinalApplicationStatus.SUCCEEDED) == State.FAILED)
      }
    }

    it("should get App Id") {
      Clock.withSleepMethod(mockSleep) {
        val mockYarnClient = mock[YarnClient]
        val mockAppReport = mock[ApplicationReport]

        when(mockAppReport.getApplicationTags).thenReturn(Set(appTag.toLowerCase).asJava)
        when(mockAppReport.getApplicationId).thenReturn(appId)
        when(mockAppReport.getFinalApplicationStatus).thenReturn(FinalApplicationStatus.SUCCEEDED)
        when(mockAppReport.getYarnApplicationState).thenReturn(YarnApplicationState.FINISHED)
        when(mockYarnClient.getApplicationReport(appId)).thenReturn(mockAppReport)
        when(mockYarnClient.getApplications(Set("SPARK").asJava))
          .thenReturn(List(mockAppReport).asJava)

        val mockListener = mock[SparkAppListener]
        val mockSparkSubmit = mock[LineBufferedProcess]
        val app = new SparkYarnApp(
          appTag, None, Some(mockSparkSubmit), Some(mockListener), livyConf, mockYarnClient)

        cleanupThread(app.yarnAppMonitorThread) {
          app.yarnAppMonitorThread.join(TEST_TIMEOUT.toMillis)
          assert(!app.yarnAppMonitorThread.isAlive,
            "YarnAppMonitorThread should terminate after YARN app is finished.")

          verify(mockYarnClient, atLeast(1)).getApplicationReport(appId)
          verify(mockListener).appIdKnown(appId.toString)
        }
      }
    }

    it("should expose driver log url and Spark UI url") {
      Clock.withSleepMethod(mockSleep) {
        val mockYarnClient = mock[YarnClient]
        val driverLogUrl = "DRIVER LOG URL"
        val sparkUiUrl = "SPARK UI URL"

        val mockApplicationAttemptId = mock[ApplicationAttemptId]
        val mockAppReport = mock[ApplicationReport]
        when(mockAppReport.getApplicationId).thenReturn(appId)
        when(mockAppReport.getFinalApplicationStatus).thenReturn(FinalApplicationStatus.SUCCEEDED)
        when(mockAppReport.getTrackingUrl).thenReturn(sparkUiUrl)
        when(mockAppReport.getCurrentApplicationAttemptId).thenReturn(mockApplicationAttemptId)
        var done = false
        when(mockAppReport.getYarnApplicationState).thenAnswer(new Answer[YarnApplicationState]() {
          override def answer(invocation: InvocationOnMock): YarnApplicationState = {
            if (!done) {
              RUNNING
            } else {
              FINISHED
            }
          }
        })
        when(mockYarnClient.getApplicationReport(appId)).thenReturn(mockAppReport)

        val mockAttemptReport = mock[ApplicationAttemptReport]
        val mockContainerId = mock[ContainerId]
        when(mockAttemptReport.getAMContainerId).thenReturn(mockContainerId)
        when(mockYarnClient.getApplicationAttemptReport(mockApplicationAttemptId))
          .thenReturn(mockAttemptReport)

        val mockContainerReport = mock[ContainerReport]
        when(mockYarnClient.getContainerReport(mockContainerId)).thenReturn(mockContainerReport)

        // Block test until getLogUrl is called 10 times.
        val getLogUrlCountDown = new CountDownLatch(10)
        when(mockContainerReport.getLogUrl).thenAnswer(new Answer[String] {
          override def answer(invocation: InvocationOnMock): String = {
            getLogUrlCountDown.countDown()
            driverLogUrl
          }
        })

        val mockListener = mock[SparkAppListener]

        val app = new SparkYarnApp(
          appTag, appIdOption, None, Some(mockListener), livyConf, mockYarnClient)
        cleanupThread(app.yarnAppMonitorThread) {
          getLogUrlCountDown.await(TEST_TIMEOUT.length, TEST_TIMEOUT.unit)
          done = true

          app.yarnAppMonitorThread.join(TEST_TIMEOUT.toMillis)
          assert(!app.yarnAppMonitorThread.isAlive,
            "YarnAppMonitorThread should terminate after YARN app is finished.")

          verify(mockYarnClient, atLeast(1)).getApplicationReport(appId)
          verify(mockAppReport, atLeast(1)).getTrackingUrl()
          verify(mockContainerReport, atLeast(1)).getLogUrl()
          verify(mockListener).appIdKnown(appId.toString)
          verify(mockListener).infoChanged(AppInfo(Some(driverLogUrl), Some(sparkUiUrl)))
        }
      }
    }

    it("should expose driver log url and Spark UI url in KILLED state") {
      Clock.withSleepMethod(mockSleep) {
        val mockYarnClient = mock[YarnClient]
        val driverLogUrl = "DRIVER LOG URL"
        val sparkUiUrl = "SPARK UI URL"

        val mockApplicationAttemptId = mock[ApplicationAttemptId]
        val mockAppReport = mock[ApplicationReport]
        when(mockAppReport.getApplicationId).thenReturn(appId)
        when(mockAppReport.getFinalApplicationStatus).thenReturn(FinalApplicationStatus.SUCCEEDED)
        when(mockAppReport.getTrackingUrl).thenReturn(sparkUiUrl)
        when(mockAppReport.getCurrentApplicationAttemptId).thenReturn(mockApplicationAttemptId)
        var done = false
        when(mockAppReport.getYarnApplicationState).thenAnswer(new Answer[YarnApplicationState]() {
          override def answer(invocation: InvocationOnMock): YarnApplicationState = {
            KILLED
          }
        })
        when(mockYarnClient.getApplicationReport(appId)).thenReturn(mockAppReport)

        val mockAttemptReport = mock[ApplicationAttemptReport]
        val mockContainerId = mock[ContainerId]
        when(mockAttemptReport.getAMContainerId).thenReturn(mockContainerId)
        when(mockYarnClient.getApplicationAttemptReport(mockApplicationAttemptId))
          .thenReturn(mockAttemptReport)

        val mockContainerReport = mock[ContainerReport]
        when(mockYarnClient.getContainerReport(mockContainerId)).thenReturn(mockContainerReport)

        // Block test until getLogUrl is called 10 times.
        val getLogUrlCountDown = new CountDownLatch(10)
        when(mockContainerReport.getLogUrl).thenAnswer(new Answer[String] {
          override def answer(invocation: InvocationOnMock): String = {
            getLogUrlCountDown.countDown()
            driverLogUrl
          }
        })

        val mockListener = mock[SparkAppListener]

        val app = new SparkYarnApp(
          appTag, appIdOption, None, Some(mockListener), livyConf, mockYarnClient)
        cleanupThread(app.yarnAppMonitorThread) {
          getLogUrlCountDown.await(TEST_TIMEOUT.length, TEST_TIMEOUT.unit)
          done = true

          app.yarnAppMonitorThread.join(TEST_TIMEOUT.toMillis)
          assert(!app.yarnAppMonitorThread.isAlive,
            "YarnAppMonitorThread should terminate after YARN app is finished.")

          verify(mockYarnClient, atLeast(1)).getApplicationReport(appId)
          verify(mockAppReport, atLeast(1)).getTrackingUrl()
          verify(mockContainerReport, atLeast(1)).getLogUrl()
          verify(mockListener).appIdKnown(appId.toString)
          verify(mockListener).infoChanged(AppInfo(Some(driverLogUrl), Some(sparkUiUrl)))
        }
      }
    }

    it("should not expose driver log url and Spark UI url in FINISHED state") {
      Clock.withSleepMethod(mockSleep) {
        val mockYarnClient = mock[YarnClient]
        val driverLogUrl = "DRIVER LOG URL"
        val sparkUiUrl = "SPARK UI URL"

        val mockApplicationAttemptId = mock[ApplicationAttemptId]
        val mockAppReport = mock[ApplicationReport]
        when(mockAppReport.getApplicationId).thenReturn(appId)
        when(mockAppReport.getFinalApplicationStatus).thenReturn(FinalApplicationStatus.SUCCEEDED)
        when(mockAppReport.getTrackingUrl).thenReturn(sparkUiUrl)
        when(mockAppReport.getCurrentApplicationAttemptId).thenReturn(mockApplicationAttemptId)
        var done = false
        when(mockAppReport.getYarnApplicationState).thenAnswer(new Answer[YarnApplicationState]() {
          override def answer(invocation: InvocationOnMock): YarnApplicationState = {
            FINISHED
          }
        })
        when(mockYarnClient.getApplicationReport(appId)).thenReturn(mockAppReport)

        val mockAttemptReport = mock[ApplicationAttemptReport]
        val mockContainerId = mock[ContainerId]
        when(mockAttemptReport.getAMContainerId).thenReturn(mockContainerId)
        when(mockYarnClient.getApplicationAttemptReport(mockApplicationAttemptId))
          .thenReturn(mockAttemptReport)

        val mockContainerReport = mock[ContainerReport]
        when(mockYarnClient.getContainerReport(mockContainerId)).thenReturn(mockContainerReport)

        // Block test until getLogUrl is called 10 times.
        val getLogUrlCountDown = new CountDownLatch(10)
        when(mockContainerReport.getLogUrl).thenAnswer(new Answer[String] {
          override def answer(invocation: InvocationOnMock): String = {
            getLogUrlCountDown.countDown()
            driverLogUrl
          }
        })

        val mockListener = mock[SparkAppListener]

        val app = new SparkYarnApp(
          appTag, appIdOption, None, Some(mockListener), livyConf, mockYarnClient)
        cleanupThread(app.yarnAppMonitorThread) {
          getLogUrlCountDown.await(TEST_TIMEOUT.length, TEST_TIMEOUT.unit)
          done = true

          app.yarnAppMonitorThread.join(TEST_TIMEOUT.toMillis)
          assert(!app.yarnAppMonitorThread.isAlive,
            "YarnAppMonitorThread should terminate after YARN app is finished.")

          verify(mockYarnClient, atLeast(1)).getApplicationReport(appId)
          verify(mockAppReport, atLeast(1)).getTrackingUrl()
          verify(mockContainerReport, never()).getLogUrl()
          verify(mockListener).appIdKnown(appId.toString)
          verify(mockListener, never()).infoChanged(AppInfo(Some(driverLogUrl), Some(sparkUiUrl)))
        }
      }
    }

    it("should not die on YARN-4411") {
      Clock.withSleepMethod(mockSleep) {
        val mockYarnClient = mock[YarnClient]

        // Block test until getApplicationReport is called 10 times.
        val pollCountDown = new CountDownLatch(10)
        when(mockYarnClient.getApplicationReport(appId)).thenAnswer(new Answer[ApplicationReport] {
          override def answer(invocation: InvocationOnMock): ApplicationReport = {
            pollCountDown.countDown()
            throw new IllegalArgumentException("No enum constant " +
              "org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState.FINAL_SAVING")
          }
        })

        val app = new SparkYarnApp(appTag, appIdOption, None, None, livyConf, mockYarnClient)
        cleanupThread(app.yarnAppMonitorThread) {
          pollCountDown.await(TEST_TIMEOUT.length, TEST_TIMEOUT.unit)
          assert(app.state == SparkApp.State.STARTING)

          app.state = SparkApp.State.FINISHED
          app.yarnAppMonitorThread.join(TEST_TIMEOUT.toMillis)
        }
      }
    }

    it("should not die on ApplicationAttemptNotFoundException") {
      Clock.withSleepMethod(mockSleep) {
        val mockYarnClient = mock[YarnClient]
        val mockAppReport = mock[ApplicationReport]
        val mockApplicationAttemptId = mock[ApplicationAttemptId]
        val done = new AtomicBoolean(false)

        when(mockAppReport.getApplicationId).thenReturn(appId)
        when(mockAppReport.getYarnApplicationState).thenAnswer(
          new Answer[YarnApplicationState]() {
            override def answer(invocation: InvocationOnMock): YarnApplicationState = {
              if (done.get()) {
                FINISHED
              } else {
                RUNNING
              }
            }
          })
        when(mockAppReport.getFinalApplicationStatus).thenAnswer(
          new Answer[FinalApplicationStatus]() {
            override def answer(invocation: InvocationOnMock): FinalApplicationStatus = {
              if (done.get()) {
                FinalApplicationStatus.SUCCEEDED
              } else {
                FinalApplicationStatus.UNDEFINED
              }
            }
          })

        when(mockAppReport.getCurrentApplicationAttemptId).thenReturn(mockApplicationAttemptId)
        when(mockYarnClient.getApplicationReport(appId)).thenReturn(mockAppReport)

        // Block test until getApplicationReport is called 10 times.
        val pollCountDown = new CountDownLatch(10)
        when(mockYarnClient.getApplicationAttemptReport(mockApplicationAttemptId)).thenAnswer(
          new Answer[ApplicationReport] {
            override def answer(invocation: InvocationOnMock): ApplicationReport = {
              pollCountDown.countDown()
              throw new ApplicationAttemptNotFoundException("unit test")
            }
          })

        val app = new SparkYarnApp(appTag, appIdOption, None, None, livyConf, mockYarnClient)
        cleanupThread(app.yarnAppMonitorThread) {
          pollCountDown.await(TEST_TIMEOUT.length, TEST_TIMEOUT.unit)
          assert(app.state == SparkApp.State.RUNNING)
          done.set(true)

          app.yarnAppMonitorThread.join(TEST_TIMEOUT.toMillis)
        }
      }
    }

    it("should delete leak app when timeout") {
      Clock.withSleepMethod(mockSleep) {
        livyConf.set(LivyConf.YARN_APP_LEAKAGE_CHECK_INTERVAL, "100ms")
        livyConf.set(LivyConf.YARN_APP_LEAKAGE_CHECK_TIMEOUT, "1000ms")

        val client = mock[YarnClient]
        when(client.getApplications(SparkYarnApp.appType)).
          thenReturn(new ArrayList[ApplicationReport]())

        SparkYarnApp.init(livyConf, Some(client))

        SparkYarnApp.leakedAppTags.clear()
        SparkYarnApp.leakedAppTags.put("leakApp", System.currentTimeMillis())

        Eventually.eventually(Eventually.timeout(TEST_TIMEOUT), Eventually.interval(100 millis)) {
          assert(SparkYarnApp.leakedAppTags.size() == 0)
        }
      }
    }
  }
}
