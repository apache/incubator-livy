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

import scala.collection.JavaConverters._

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.api._
import org.apache.curator.framework.listen.Listenable
import org.apache.curator.framework.recipes.leader.LeaderLatch
import org.apache.zookeeper.data.Stat
import org.mockito.Mockito._
import org.scalatest.FunSpec
import org.scalatest.Matchers._
import org.scalatest.mock.MockitoSugar.mock

import org.apache.livy.{LivyBaseUnitTestSuite, LivyConf}
import org.apache.livy.server.CuratorElectorService
import org.apache.livy.server.HAState
import org.apache.livy.server.LivyServer

class CuratorElectorServiceSpec extends FunSpec with LivyBaseUnitTestSuite {
  describe("CuratorElectorService") {
    case class TestFixture(electorService: CuratorElectorService)
    val conf = new LivyConf()
    conf.set(LivyConf.HA_ZOOKEEPER_URL, "host")

    // Need to create mock leader latches and their associated functions
    def withMock[R](testBody: TestFixture => R): R = {
      val curatorClient = mock[CuratorFramework]
      when(curatorClient.getUnhandledErrorListenable())
        .thenReturn(mock[Listenable[UnhandledErrorListener]])
      val leaderLatch = mock[LeaderLatch]

      val server = mock[LivyServer]
      val electorService = new CuratorElectorService(conf, server,
                               Some(curatorClient), Some(leaderLatch))

      testBody(TestFixture(electorService))
    }

    it("should not start the server until it acquires leadership") {
      withMock { f =>
        f.electorService.currentState shouldBe HAState.Standby
        verify(f.electorService.server, times(0)).start()
      }
    }

    it("should restart the livy server after acquiring leadership") {
      withMock { f =>
        f.electorService.isLeader()
        f.electorService.currentState shouldBe HAState.Active
        verify(f.electorService.server, times(1)).restart()
      }
    }

    it("should be in standy state if loses leadership") {
      withMock { f =>
        f.electorService.isLeader()
        f.electorService.notLeader()
        f.electorService.currentState shouldBe HAState.Standby
      }
    }

    it("should restart the Livy Server again after reacquiring leadership") {
      withMock { f =>
        f.electorService.isLeader()
        f.electorService.notLeader()
        f.electorService.isLeader()
        f.electorService.currentState shouldBe HAState.Active
        verify(f.electorService.server, times(2)).restart()
        }
    }
  }
}
