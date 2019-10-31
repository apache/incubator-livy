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

import java.util.Objects._
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._

import io.fabric8.kubernetes.api.model._
import org.mockito.Matchers.{eq => eqs, _}
import org.mockito.Mockito.{atLeast, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.FunSpec
import org.scalatest.mock.MockitoSugar.mock

import org.apache.livy.{LivyBaseUnitTestSuite, LivyConf}
import org.apache.livy.utils.SparkApp.State
import org.apache.livy.utils.SparkKubernetesApp.KubernetesApplicationState._

class SparkKubernetesAppSpec extends FunSpec with LivyBaseUnitTestSuite {

  private def cleanupThread(t: Thread)(f: => Unit) = {
    try { f } finally { t.interrupt() }
  }

  private def mockSleep(ms: Long) = {
    Thread.`yield`()
  }

  describe("SparkKubernetesApp") {
    val TEST_TIMEOUT = 30 seconds
    val appId = "app_id"
    val appTag = "app_tag"
    val trackingUrl = "tracking_url"

    def initMockApp: KubernetesApplication = {
      val mockApp = mock[KubernetesApplication]
      when(mockApp.getApplicationId).thenReturn(appId)
      when(mockApp.getApplicationTag).thenReturn(appTag)
      mockApp
    }

    def initMockClient(mockApp: KubernetesApplication): LivyKubernetesClient = {
      val mockAppReport = mock[KubernetesAppReport]
      when(mockAppReport.getApplicationLog).thenReturn(IndexedSeq("app", "log"))
      when(mockAppReport.getApplicationDiagnostics).thenReturn(IndexedSeq("app", "diagnostics"))
      when(mockAppReport.getTrackingUrl).thenReturn(Some(trackingUrl))
      val mockClient = mock[LivyKubernetesClient]
      when(mockClient.getApplications(any(), any(), any())).thenReturn(Seq(mockApp))
      when(mockClient.getApplicationReport(eqs(mockApp), any(), any())).thenReturn(mockAppReport)

      // Simulate Kubernetes app state progression.
      val applicationStateList = List(
        PENDING,
        RUNNING,
        SUCCEEDED
      )
      val stateIndex = new AtomicInteger(0)
      when(mockAppReport.getApplicationState).thenAnswer(
        new Answer[String] {
          override def answer(inv: InvocationOnMock): String = {
            stateIndex.getAndIncrement() match {
              case i if i < applicationStateList.size =>
                applicationStateList(i)
              case _ =>
                applicationStateList.last
            }
          }
        }
      )
      mockClient
    }

    it("should poll Kubernetes state and terminate") {
      val livyConf = new LivyConf(false)
      livyConf.set(LivyConf.KUBERNETES_APP_LOOKUP_TIMEOUT, "30s")
      Clock.withSleepMethod(mockSleep) {
        val mockListener = mock[SparkAppListener]
        val mockApp = initMockApp
        val mockClient = initMockClient(mockApp)
        val app = new SparkKubernetesApp(
          appTag, None, None, Some(mockListener), livyConf, mockClient)
        cleanupThread(app.kubernetesAppMonitorThread) {
          app.kubernetesAppMonitorThread.join(TEST_TIMEOUT.toMillis)
          assert(!app.kubernetesAppMonitorThread.isAlive,
            "KubernetesAppMonitorThread should terminate after Kubernetes app is finished")
          verify(mockClient, atLeast(1)).getApplications(any(), anyString(), anyString())
          verify(mockClient, atLeast(1))
            .getApplicationReport(eqs(mockApp), anyInt(), anyString())
          verify(mockListener).appIdKnown(appId)
          verify(mockListener).stateChanged(State.STARTING, State.RUNNING)
          verify(mockListener).stateChanged(State.RUNNING, State.FINISHED)
        }
      }
    }

    it("should build sparkUiUrl and update AppInfo") {
      val livyConf = new LivyConf(false)
      livyConf.set(LivyConf.KUBERNETES_APP_LOOKUP_TIMEOUT, "30s")
      Clock.withSleepMethod(mockSleep) {
        val mockListener = mock[SparkAppListener]
        val mockApp = initMockApp
        val mockClient = initMockClient(mockApp)
        val app = new SparkKubernetesApp(
          appTag, None, None, Some(mockListener), livyConf, mockClient)
        cleanupThread(app.kubernetesAppMonitorThread) {
          app.kubernetesAppMonitorThread.join(TEST_TIMEOUT.toMillis)
          assert(!app.kubernetesAppMonitorThread.isAlive,
            "KubernetesAppMonitorThread should terminate after Kubernetes app is finished.")
          verify(mockListener)
            .infoChanged(eqs(AppInfo(sparkUiUrl = Some(trackingUrl))))
          verify(mockListener).infoChanged(eqs(AppInfo()))
        }
      }
    }
  }

  describe("KubernetesAppReport") {

    import scala.collection.JavaConverters._
    import KubernetesConstants._

    def driverMock(state: String): Pod = {
      val status = when(mock[PodStatus].getPhase).thenReturn(state).getMock[PodStatus]
      when(mock[Pod].getStatus).thenReturn(status).getMock[Pod]
    }

    def witLabels(pod: Pod, labelMap: Map[String, String]): Pod = {
      val metaWithLabel = when(mock[ObjectMeta].getLabels).thenReturn(labelMap.asJava)
        .getMock[ObjectMeta]
      when(pod.getMetadata).thenReturn(metaWithLabel).getMock[Pod]
    }

    def report(driver: Option[Pod], livyConf: LivyConf): KubernetesAppReport =
      KubernetesAppReport(driver, Set.empty, IndexedSeq.empty, livyConf)

    it("should return application state") {
      val livyConf = new LivyConf(false)
      val driver = driverMock("State")
      assertResult("state") {
        KubernetesAppReport(Some(driver), Set.empty, IndexedSeq.empty, livyConf).getApplicationState
      }
      assertResult("unknown") {
        KubernetesAppReport(None, Set.empty, IndexedSeq.empty, livyConf).getApplicationState
      }
    }

    it("should return Spark UI url") {

      def test(expected: Option[String], driver: Option[Pod], livyConf: LivyConf): Unit =
        assertResult(expected) {
          report(driver, livyConf).getTrackingUrl
        }

      val appId = "app_id"
      val livyConf = new LivyConf(false)
      livyConf.set(LivyConf.UI_KUBERNETES_SPARK_UI_ENABLED, true)
      livyConf.set(LivyConf.UI_KUBERNETES_SPARK_UI_LINK_FORMAT, "https://cluser.com/spark/%s")

      test(Some(s"https://cluser.com/spark/$appId"),
        Some(witLabels(driverMock("Running"), Map(SPARK_APP_ID_LABEL -> appId))), livyConf)
      test(Some(s"https://cluser.com/spark/unknown"),
        Some(witLabels(driverMock("Running"), Map.empty)), livyConf)
      test(None, Some(witLabels(driverMock("State"), Map.empty)), livyConf)
      test(None, None, livyConf)
      test(None, Some(witLabels(driverMock("Running"), Map(SPARK_APP_ID_LABEL -> appId))),
        new LivyConf(false))
    }
  }

  describe("KubernetesClientFactory") {
    it("should build KubernetesApi url from LivyConf master url") {
      def actual(sparkMaster: String): String =
        KubernetesClientFactory.sparkMasterToKubernetesApi(sparkMaster)

      val masterUrl = "kubernetes.default.svc:443"

      assertResult(s"https://local")(actual(s"https://local"))
      assertResult(s"https://$masterUrl")(actual(s"k8s://$masterUrl"))
      assertResult(s"http://$masterUrl")(actual(s"k8s://http://$masterUrl"))
      assertResult(s"https://$masterUrl")(actual(s"k8s://https://$masterUrl"))
      assertResult(s"http://$masterUrl")(actual(s"http://$masterUrl"))
      assertResult(s"https://$masterUrl")(actual(s"https://$masterUrl"))
    }

    it("should create KubernetesClient with default configs") {
      assert(nonNull(KubernetesClientFactory.createKubernetesClient(new LivyConf(false))))
    }

    it("should throw IllegalArgumentException if both oauth file and token provided") {
      val conf = new LivyConf(false)
        .set(LivyConf.KUBERNETES_OAUTH_TOKEN_FILE, "dummy_path")
        .set(LivyConf.KUBERNETES_OAUTH_TOKEN_VALUE, "dummy_value")
      intercept[IllegalArgumentException] {
        KubernetesClientFactory.createKubernetesClient(conf)
      }
    }
  }
}
