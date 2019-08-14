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

import java.net.URI

import org.apache.curator.framework.CuratorFramework

import org.apache.livy.LivyConf

/**
  * Livy Server Discovery manager.
  * Stores information about Livy Server location in ZooKeeper.
  * The address will be stored in
  * "/{@code LIVY_ZOOKEEPER_NAMESPACE}/{@code LIVY_SERVER_ZOOKEEPER_NAMESPACE}" znode
  * By default, the full path to znode is /livy/server.uri.
  * Need to set {@code livy.zookeeper.url} to be able to get information from ZooKeeper.
  *
  * @param livyConf - Livy configurations
  * @param mockCuratorClient - used for testing
  */
class LivyDiscoveryManager(livyConf: LivyConf,
                           mockCuratorClient: Option[CuratorFramework] = None)
  extends ZooKeeperManager(livyConf, mockCuratorClient) {

  private val LIVY_SERVER_URI_KEY = livyConf.get(LivyConf.LIVY_SERVER_ZOOKEEPER_NAMESPACE)

  /**
    * Save Livy Server URI to ZooKeeper.
    * @param address - URI address of Livy Server
    */
  def setServerUri(address: URI): Unit = {
    setData(LIVY_SERVER_URI_KEY, address)
  }

  /**
    * Get Livy Server URI from ZooKeeper.
    * @return Livy Server URI
    */
  def getServerUri(): URI = {
    getData[URI](LIVY_SERVER_URI_KEY).getOrElse(URI.create(""))
  }
}

object LivyDiscoveryManager {

  def apply(livyConf: LivyConf,
            mockCuratorClient: Option[CuratorFramework] = None): LivyDiscoveryManager = {
    new LivyDiscoveryManager(livyConf, mockCuratorClient)
  }
}
