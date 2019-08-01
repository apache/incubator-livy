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

import org.apache.curator.framework.CuratorFramework

import org.apache.livy.{LivyConf, Logging}
import org.apache.livy.server.discovery.ZooKeeperManager

/**
  * Implementation for Livy State Store which uses Zookeeper as backend storage.
  * Set {@code livy.server.recovery.mode} to ``recovery``
  * and {@code livy.server.recovery.state-store} to ``zookeeper`` to enable ZooKeeper state store.
  * Also need to set {@code livy.server.zookeeper.url} to be able to get information from ZooKeeper.
  *
  * @param livyConf
  * @param mockCuratorClient
  */
class ZooKeeperStateStore(livyConf: LivyConf,
                          mockCuratorClient: Option[CuratorFramework] = None) // For testing
  extends StateStore(livyConf) with Logging {

  val zooKeeperManager = ZooKeeperManager(livyConf, mockCuratorClient)

  // Constructor defined for StateStore factory to new this class using reflection.
  def this(livyConf: LivyConf) {
    this(livyConf, None)
  }

  override def set(key: String, value: Object): Unit = {
    zooKeeperManager.setData(key, value)
  }

  override def get[T: ClassTag](key: String): Option[T] = {
    zooKeeperManager.getData[T](key)
  }

  override def getChildren(key: String): Seq[String] = {
    zooKeeperManager.getChildren(key)
  }

  override def remove(key: String): Unit = {
    zooKeeperManager.delete(key)
  }
}
