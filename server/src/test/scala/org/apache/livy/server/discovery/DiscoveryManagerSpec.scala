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
package org.apache.livy.server.discovery

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.api.{ExistsBuilder, GetDataBuilder, SetDataBuilder, UnhandledErrorListener}
import org.apache.curator.framework.listen.Listenable
import org.apache.livy.{LivyBaseUnitTestSuite, LivyConf}
import org.apache.livy.LivyConf.LIVY_ZOOKEEPER_URL
import org.apache.livy.server.LivyServer
import org.apache.zookeeper.data.Stat
import org.mockito.Mockito.{never, verify, when}
import org.scalatest.FunSpec
import org.scalatest.mock.MockitoSugar.mock


class DiscoveryManagerSpec extends FunSpec with LivyBaseUnitTestSuite
  with JsonMapper {
  describe("DiscoveryManagerSpec") {
    case class TestFixture(discoveryManager: DiscoveryManager, curatorClient: CuratorFramework)
    val conf = new LivyConf()
    conf.set(LivyConf.LIVY_ZOOKEEPER_URL, "host")
    val key = DiscoveryManager.LIVY_SERVER_URL_KEY
    val prefixedKey = s"/livy/$key"
    val testAddress = s"0.0.0.0:${conf.getInt(LivyConf.SERVER_PORT)}"
    val testData: Array[Byte] = serializeToBytes(testAddress)

    def withMock[R](testBody: TestFixture => R): R = {
      val curatorClient = mock[CuratorFramework]
      when(curatorClient.getUnhandledErrorListenable())
        .thenReturn(mock[Listenable[UnhandledErrorListener]])
      val discoveryManager = DiscoveryManager(conf, Some(curatorClient))
      testBody(TestFixture(discoveryManager, curatorClient))
    }

    def mockExistsBuilder(curatorClient: CuratorFramework, exists: Boolean): Unit = {
      val existsBuilder = mock[ExistsBuilder]
      when(curatorClient.checkExists()).thenReturn(existsBuilder)
      if (exists) {
        when(existsBuilder.forPath(prefixedKey)).thenReturn(mock[Stat])
      }
    }

    it("setAddress should use curatorClient") {
      withMock { f =>
        mockExistsBuilder(f.curatorClient, exists = true)

        val setDataBuilder = mock[SetDataBuilder]
        when(f.curatorClient.setData()).thenReturn(setDataBuilder)

        f.discoveryManager.setAddress(testAddress)

        verify(f.curatorClient).start()
        verify(setDataBuilder).forPath(prefixedKey, testData)
      }
    }

    it("getAddress should use curatorClient") {
      withMock { f =>
        mockExistsBuilder(f.curatorClient, exists = true)
        val getDataBuilder = mock[GetDataBuilder]
        when(f.curatorClient.getData()).thenReturn(getDataBuilder)
        when(getDataBuilder.forPath(prefixedKey)).thenReturn(testData)

        f.discoveryManager.getAddress()

        verify(f.curatorClient).start()
        verify(getDataBuilder).forPath(prefixedKey)
      }
    }

    it("Livy Server should use DiscoveryManager") {
      withMock { f =>
        val livyConf = new LivyConf()
        livyConf.set(LIVY_ZOOKEEPER_URL, "host:port")
        val s = new LivyServer()

        mockExistsBuilder(f.curatorClient, exists = true)

        val setDataBuilder = mock[SetDataBuilder]
        when(f.curatorClient.setData()).thenReturn(setDataBuilder)

        s.setServerAddress(livyConf, Some(testAddress), Some(f.curatorClient))
        verify(setDataBuilder).forPath(prefixedKey, testData)
      }
    }

    it("Livy Server should skip DiscoveryManager if ZooKeeper url isn't defined") {
      withMock { f =>
        val livyConf = new LivyConf()
        val s = new LivyServer()

        s.setServerAddress(livyConf, Some(testAddress), Some(f.curatorClient))
        verify(f.curatorClient, never).setData()
      }
    }
  }

}
