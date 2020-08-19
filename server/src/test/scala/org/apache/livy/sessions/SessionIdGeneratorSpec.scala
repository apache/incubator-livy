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

package org.apache.livy.sessions

import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex
import org.mockito.Mockito._
import org.scalatest.FunSpec
import org.scalatest.Matchers
import org.scalatest.mock.MockitoSugar.mock

import org.apache.livy.LivyBaseUnitTestSuite
import org.apache.livy.server.recovery.{SessionStore, ZooKeeperManager, ZooKeeperStateStore}

class SessionIdGeneratorSpec extends FunSpec with Matchers with LivyBaseUnitTestSuite {
  describe("SessionIdGenerator") {
    it("should generate right session id when use LocalSessionIdGenerator") {
      val sessionStore = mock[SessionStore]
      val idGenerator = new LocalSessionIdGenerator("interactive", sessionStore)

      assert(idGenerator.getNextSessionId() == 0)
      assert(idGenerator.getNextSessionId() == 1)
    }

    it("should acquire and release lock when use DistributedSessionIdGenerator") {
      val sessionStore = mock[SessionStore]
      val store = mock[ZooKeeperStateStore]
      when(sessionStore.getStore).thenReturn(store)
      when(store.isDistributed()).thenReturn(true)

      val zkManager = mock[ZooKeeperManager]
      val lock = mock[InterProcessSemaphoreMutex]
      val idGenerator = new DistributedSessionIdGenerator("interactive",
        sessionStore, zkManager, Some(lock))

      idGenerator.getNextSessionId()

      verify(lock).acquire
      verify(lock).release
    }
  }
}
