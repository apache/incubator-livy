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
import org.mockito.Mockito.when
import org.scalatest.FunSpec
import org.scalatest.mock.MockitoSugar._

import org.apache.livy.{LivyBaseUnitTestSuite, LivyConf}

class SparkKubernetesAppSpec extends FunSpec with LivyBaseUnitTestSuite {

  describe("KubernetesAppReport") {
    it("should return application state") {
      val status = when(mock[PodStatus].getPhase).thenReturn("Status").getMock[PodStatus]
      val driver = when(mock[Pod].getStatus).thenReturn(status).getMock[Pod]
      assertResult("status") {
        KubernetesAppReport(Some(driver), Seq.empty, IndexedSeq.empty).getApplicationState
      }
      assertResult("unknown") {
        KubernetesAppReport(None, Seq.empty, IndexedSeq.empty).getApplicationState
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
}
