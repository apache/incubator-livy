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

import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.api.model.networking.v1.{Ingress, IngressRule, IngressSpec}
import org.mockito.Mockito.when
import org.scalatest.FunSpec
import org.scalatestplus.mockito.MockitoSugar._

import org.apache.livy.{LivyBaseUnitTestSuite, LivyConf}
import org.apache.livy.utils.KubernetesConstants.SPARK_APP_TAG_LABEL

class SparkKubernetesAppSpec extends FunSpec with LivyBaseUnitTestSuite {

  describe("KubernetesAppReport") {
    import scala.collection.JavaConverters._

    it("should return application state") {
      val status = when(mock[PodStatus].getPhase).thenReturn("Status").getMock[PodStatus]
      val driver = when(mock[Pod].getStatus).thenReturn(status).getMock[Pod]
      assertResult("status") {
        KubernetesAppReport(Option(driver), Seq.empty, IndexedSeq.empty, None, new LivyConf(false))
          .getApplicationState
      }
      assertResult("unknown") {
        KubernetesAppReport(None, Seq.empty, IndexedSeq.empty, None, new LivyConf(false))
          .getApplicationState
      }
    }

    def livyConf(lokiEnabled: Boolean): LivyConf = new LivyConf(false)
      .set(LivyConf.KUBERNETES_GRAFANA_LOKI_ENABLED, lokiEnabled)

    def podMockWithLabels(labelMap: Map[String, String]): Pod = {
      val metaWithLabel = when(mock[ObjectMeta].getLabels).thenReturn(labelMap.asJava)
        .getMock[ObjectMeta]
      when(mock[Pod].getMetadata).thenReturn(metaWithLabel).getMock[Pod]
    }

    def driverMock(labelExists: Boolean): Option[Pod] = {
      val labels = if (labelExists) Map(KubernetesConstants.SPARK_APP_TAG_LABEL -> "app_tag")
      else Map.empty[String, String]
      Some(podMockWithLabels(labels))
    }

    it("should return driver log url") {

      def test(labelExists: Boolean, lokiEnabled: Boolean, shouldBeDefined: Boolean): Unit =
        assertResult(shouldBeDefined) {
          KubernetesAppReport(
            driverMock(labelExists), Seq.empty, IndexedSeq.empty, None, livyConf(lokiEnabled)
          ).getDriverLogUrl.isDefined
        }

      test(labelExists = false, lokiEnabled = false, shouldBeDefined = false)
      test(labelExists = false, lokiEnabled = true, shouldBeDefined = false)
      test(labelExists = true, lokiEnabled = false, shouldBeDefined = false)
      test(labelExists = true, lokiEnabled = true, shouldBeDefined = true)
      assert(KubernetesAppReport(None, Seq.empty, IndexedSeq.empty, None, livyConf(true))
        .getDriverLogUrl.isEmpty)
    }

    it("should return executors log urls") {
      def executorMock(labelsExist: Boolean): Option[Pod] = {
        val labels = if (labelsExist) {
          Map(KubernetesConstants.SPARK_APP_TAG_LABEL -> "app_tag",
            KubernetesConstants.SPARK_EXEC_ID_LABEL -> "exec-1")
        } else {
          Map.empty[String, String]
        }
        Some(podMockWithLabels(labels))
      }

      def test(labelExists: Boolean, lokiEnabled: Boolean, shouldBeDefined: Boolean): Unit =
        assertResult(shouldBeDefined) {
          KubernetesAppReport(
            None, Seq(executorMock(labelExists).get), IndexedSeq.empty, None, livyConf(lokiEnabled)
          ).getExecutorsLogUrls.isDefined
        }

      test(labelExists = false, lokiEnabled = false, shouldBeDefined = false)
      test(labelExists = false, lokiEnabled = true, shouldBeDefined = false)
      test(labelExists = true, lokiEnabled = false, shouldBeDefined = false)
      test(labelExists = true, lokiEnabled = true, shouldBeDefined = true)
      assert(KubernetesAppReport(None, Seq.empty, IndexedSeq.empty, None, livyConf(true))
        .getExecutorsLogUrls.isEmpty)
    }

    it("should return driver ingress url") {

      def livyConf(protocol: Option[String]): LivyConf = {
        val conf = new LivyConf()
        protocol.map(conf.set(LivyConf.KUBERNETES_INGRESS_PROTOCOL, _)).getOrElse(conf)
      }

      def ingressMock(host: Option[String]): Ingress = {
        val ingressRules = host.map(h =>
          List(when(mock[IngressRule].getHost).thenReturn(h).getMock[IngressRule]))
          .getOrElse(List.empty).asJava
        val ingressSpec = when(mock[IngressSpec].getRules)
          .thenReturn(ingressRules).getMock[IngressSpec]
        when(mock[Ingress].getSpec).thenReturn(ingressSpec).getMock[Ingress]
      }

      def test(driver: Option[Pod], ingress: Option[Ingress],
               protocol: Option[String], shouldBeDefined: Boolean): Unit = {
        assertResult(shouldBeDefined) {
          KubernetesAppReport(driver, Seq.empty, IndexedSeq.empty, ingress, livyConf(protocol))
            .getTrackingUrl.isDefined
        }
      }

      val hostname = Some("hostname")
      val protocol = Some("protocol")

      test(None, None, None, shouldBeDefined = false)
      test(None, None, protocol, shouldBeDefined = false)
      test(None, Some(ingressMock(None)), None, shouldBeDefined = false)
      test(None, Some(ingressMock(None)), protocol, shouldBeDefined = false)
      test(None, Some(ingressMock(hostname)), None, shouldBeDefined = false)
      test(None, Some(ingressMock(hostname)), protocol, shouldBeDefined = false)

      test(driverMock(true), None, None, shouldBeDefined = false)
      test(driverMock(true), None, protocol, shouldBeDefined = false)
      test(driverMock(true), Some(ingressMock(None)), None, shouldBeDefined = false)
      test(driverMock(true), Some(ingressMock(None)), protocol, shouldBeDefined = false)
      test(driverMock(true), Some(ingressMock(hostname)), None, shouldBeDefined = true)
      test(driverMock(true), Some(ingressMock(hostname)), protocol, shouldBeDefined = true)

      test(driverMock(false), None, None, shouldBeDefined = false)
      test(driverMock(false), None, protocol, shouldBeDefined = false)
      test(driverMock(false), Some(ingressMock(None)), None, shouldBeDefined = false)
      test(driverMock(false), Some(ingressMock(None)), protocol, shouldBeDefined = false)
      test(driverMock(false), Some(ingressMock(hostname)), None, shouldBeDefined = true)
      test(driverMock(false), Some(ingressMock(hostname)), protocol, shouldBeDefined = true)

      assertResult(s"${protocol.get}://${hostname.get}/app_tag") {
        KubernetesAppReport(driverMock(true), Seq.empty, IndexedSeq.empty,
          Some(ingressMock(hostname)), livyConf(protocol)).getTrackingUrl.get
      }
      assertResult(s"${protocol.get}://${hostname.get}/unknown") {
        KubernetesAppReport(driverMock(false), Seq.empty, IndexedSeq.empty,
          Some(ingressMock(hostname)), livyConf(protocol)).getTrackingUrl.get
      }
    }

  }

  describe("KubernetesClientFactory") {
    it("should build KubernetesApi url from LivyConf masterUrl") {
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

    it("should throw IllegalArgumentException in both oauth file and token provided") {
      val conf = new LivyConf(false)
        .set(LivyConf.KUBERNETES_OAUTH_TOKEN_FILE, "dummy_path")
        .set(LivyConf.KUBERNETES_OAUTH_TOKEN_VALUE, "dummy_value")
      intercept[IllegalArgumentException] {
        KubernetesClientFactory.createKubernetesClient(conf)
      }
    }
  }

  describe("KubernetesClientExtensions") {
    it("should build an ingress from the supplied KubernetesApplication") {
      def test(app: KubernetesApplication, expectedAnnotations: Map[String, String]): Unit = {
        import scala.collection.JavaConverters._
        val livyConf = new LivyConf(false)
        val client = KubernetesClientFactory.createKubernetesClient(livyConf)
        val clientExtensions = KubernetesExtensions.KubernetesClientExtensions(client)
        val ingress = clientExtensions.buildSparkUIIngress(app, "ingress-class", "https",
          "cluster.example.com", "tlsSecret", "")
        val diff = expectedAnnotations.toSet diff ingress.getMetadata.getAnnotations.asScala.toSet
        assert(ingress.getMetadata.getName == s"${app.getApplicationPod.getMetadata.getName}-svc")
        assert(diff.isEmpty)
      }

      def mockPod(name: String, namespace: String, tag: String): Pod = {
        new PodBuilder().withNewMetadata().withName(name).withNamespace(namespace).
          addToLabels(SPARK_APP_TAG_LABEL, tag).endMetadata().withNewSpec().endSpec().build()
      }

      def app(name: String, namespace: String, tag: String): KubernetesApplication = {
        new KubernetesApplication(mockPod(name, namespace, tag))
      }

      test(app("app1", "ns-1", "tag-1"), Map(
        "nginx.ingress.kubernetes.io/rewrite-target" -> "/$1",
        "nginx.ingress.kubernetes.io/proxy-redirect-to" -> s"/tag-1/",
        "nginx.ingress.kubernetes.io/x-forwarded-prefix" -> s"/tag-1",
        "nginx.ingress.kubernetes.io/proxy-redirect-from" ->
          s"http://app1-svc.ns-1.svc.cluster.local/",
        "nginx.ingress.kubernetes.io/upstream-vhost" ->
          s"app1-svc.ns-1.svc.cluster.local",
        "nginx.ingress.kubernetes.io/service-upstream" -> "true"
      ))

      test(app("app2", "ns-2", "tag-2"), Map(
        "nginx.ingress.kubernetes.io/rewrite-target" -> "/$1",
        "nginx.ingress.kubernetes.io/proxy-redirect-to" -> s"/tag-2/",
        "nginx.ingress.kubernetes.io/x-forwarded-prefix" -> s"/tag-2",
        "nginx.ingress.kubernetes.io/proxy-redirect-from" ->
          s"http://app2-svc.ns-2.svc.cluster.local/",
        "nginx.ingress.kubernetes.io/upstream-vhost" ->
          s"app2-svc.ns-2.svc.cluster.local",
        "nginx.ingress.kubernetes.io/service-upstream" -> "true"
      ))
    }
  }

}
