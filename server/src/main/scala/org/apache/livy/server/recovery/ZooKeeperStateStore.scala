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

import org.apache.livy.{LivyConf, Logging}

class ZooKeeperStateStore(livyConf: LivyConf) extends StateStore(livyConf) {
  require(ZooKeeperManager.get != null)

  override def set(key: String, value: Object): Unit = {
    ZooKeeperManager.get.set(key, value)
  }

  override def get[T: ClassTag](key: String): Option[T] = {
    ZooKeeperManager.get.get(key)
  }

  override def getChildren(key: String): Seq[String] = {
    ZooKeeperManager.get.getChildren(key)
  }

  override def remove(key: String): Unit = {
    ZooKeeperManager.get.remove(key)
  }
}
