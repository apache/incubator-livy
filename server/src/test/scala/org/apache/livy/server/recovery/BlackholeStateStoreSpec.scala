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

package org.apache.livy.server.recovery

import org.scalatest.FunSpec
import org.scalatest.Matchers._

import org.apache.livy.{LivyBaseUnitTestSuite, LivyConf}
import org.apache.livy.server.batch.BatchRecoveryMetadata

class BlackholeStateStoreSpec extends FunSpec with LivyBaseUnitTestSuite {
  describe("BlackholeStateStore") {
    val stateStore = new BlackholeStateStore(new LivyConf())

    it("set should not throw") {
      stateStore.set("", 1.asInstanceOf[Object])
    }

    it("get should return None") {
      val v = stateStore.get[Object]("")
      v shouldBe None
    }

    it("getChildren should return empty list") {
      val c = stateStore.getChildren("")
      c shouldBe empty
    }

    it("remove should not throw") {
      stateStore.remove("")
    }

    it("should deserialize sessions without name") {
      val jsonbytes =
        """
          |{
          |  "id": 408107,
          |  "appId": "application_1541532370353_1465148",
          |  "state": "running",
          |  "appTag": "livy-batch-408107-2jAOFzDy",
          |  "owner": "batch_admin",
          |  "proxyUser": "batch_opts",
          |  "version": 1
          |}
        """.stripMargin.getBytes("UTF-8")
      val batchRecoveryMetadata = stateStore.deserialize[BatchRecoveryMetadata](jsonbytes)
      batchRecoveryMetadata.id shouldBe 408107
      batchRecoveryMetadata.appId shouldBe Some("application_1541532370353_1465148")
      batchRecoveryMetadata.name shouldBe None
    }
  }
}
