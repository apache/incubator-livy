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

import java.util.concurrent.atomic.AtomicInteger

import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex

import org.apache.livy.server.recovery.{SessionStore, ZooKeeperManager}

/**
  * Interface of session id generator.
  */
abstract class SessionIdGenerator(
    sessionType: String,
    sessionStore: SessionStore) {

  /**
    * Get next session id, then increase next session id and save it in store.
    */
  def getNextSessionId(): Int

  protected def getAndIncreaseId(): Int

  protected def persist(id: Int): Unit = {
    sessionStore.saveNextSessionId(sessionType, id)
  }
}

class LocalSessionIdGenerator(
    sessionType: String,
    sessionStore: SessionStore) extends SessionIdGenerator(sessionType, sessionStore) {

  private val idCounter = new AtomicInteger(0)
  idCounter.set(sessionStore.getNextSessionId(sessionType))

  override def getNextSessionId(): Int = {
    val result = getAndIncreaseId()
    persist(result + 1)
    result
  }

  override def getAndIncreaseId(): Int = {
    idCounter.getAndIncrement()
  }
}

class DistributedSessionIdGenerator(
    sessionType: String,
    sessionStore: SessionStore,
    zkManager: ZooKeeperManager,
    mockLock: Option[InterProcessSemaphoreMutex] = None)
  extends SessionIdGenerator(sessionType, sessionStore) {

  require(sessionStore.getStore.isDistributed(),
    "Choose a distributed store such as hdfs or zookeeper")

  val distributedLock = mockLock.getOrElse(
    zkManager.createLock(SessionStore.sessionIdLockPath(sessionType)))

  override def getNextSessionId(): Int = {
    distributedLock.acquire()
    val result = getAndIncreaseId()
    persist(result + 1)
    distributedLock.release()
    result
  }

  override def getAndIncreaseId(): Int = {
    sessionStore.getNextSessionId(sessionType)
  }
}
