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

import scala.reflect.ClassTag

import org.apache.livy.LivyConf
import org.apache.livy.LivyConf.Entry

class ZooKeeperStateStore(
    livyConf: LivyConf,
    zkManager: ZooKeeperManager)
  extends StateStore(livyConf) {

  val ZK_KEY_PREFIX_CONF = Entry("livy.server.recovery.zk-state-store.key-prefix", "livy")
  private val zkKeyPrefix = livyConf.get(ZK_KEY_PREFIX_CONF)
  private def prefixKey(key: String) = s"/$zkKeyPrefix/$key"

  override def set(key: String, value: Object): Unit = {
    zkManager.set(prefixKey(key), value)
  }

  override def get[T: ClassTag](key: String): Option[T] = {
    zkManager.get(prefixKey(key))
  }

  override def getChildren(key: String): Seq[String] = {
    zkManager.getChildren(prefixKey(key))
  }

  override def remove(key: String): Unit = {
    zkManager.remove(prefixKey(key))
  }

  def getZooKeeperManager(): ZooKeeperManager = {
    zkManager
  }
}
