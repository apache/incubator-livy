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

package org.apache.livy.thriftserver

import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry

import org.apache.livy.{LivyConf, Logging}

object LivyThriftZookeeperManager {
  val ZOOKEEPER_PATH_SEPARATOR = "/"
}

class LivyThriftZookeeperManager(server: LivyThriftServer)
  extends ThriftService(classOf[LivyThriftZookeeperManager].getName) with Logging {

  private var zookeeperClient: CuratorFramework = null

  override def start(): Unit = {
    val livyConf = server.livyConf
    val quorum = livyConf.get(LivyConf.THRIFT_ZOOKEEPER_QUORUM).trim
    if (!quorum.isEmpty) {
      val port = livyConf.get(LivyConf.THRIFT_ZOOKEEPER_CLIENT_PORT)
      val quorumServers = getQuorumServers(quorum, port)

      val sessionTimeout = livyConf.getTimeAsMs(LivyConf.THRIFT_ZOOKEEPER_SESSION_TIMEOUT)
        .asInstanceOf[Int]
      val baseSleepTime = livyConf.getTimeAsMs(LivyConf.THRIFT_ZOOKEEPER_CONNECTION_BASESLEEPTIME)
        .asInstanceOf[Int]
      val maxRetries = livyConf.getInt(LivyConf.THRIFT_ZOOKEEPER_CONNECTION_MAX_RETRIES)

      zookeeperClient = CuratorFrameworkFactory.builder()
        .connectString(quorumServers)
        .sessionTimeoutMs(sessionTimeout)
        .retryPolicy(new ExponentialBackoffRetry(baseSleepTime, maxRetries))
        .build()
      zookeeperClient.start()
    }

    super.start()
  }

  private[thriftserver] def getQuorumServers(quorum: String, clientPort: String): String = {
    val quorumServers = quorum.split(",").foldLeft(new StringBuilder()) { (sb, x) =>
      sb.append(x)
      if (!x.contains(":")) {
        // if the quorum doesn't contain a port, add the configured port to quorum
        sb.append(":").append(clientPort)
      }
      sb.append(",")
    }

    if (quorumServers.length > 0) {
      quorumServers.substring(0, quorumServers.length - 1)
    } else {
      ""
    }
  }

  def getClient(): CuratorFramework = {
    if (zookeeperClient == null) {
      throw new RuntimeException(s"Zookeeper client is not initialized, " +
        s"please make sure ${LivyConf.THRIFT_ZOOKEEPER_QUORUM.key} is configured correctly.")
    }
    zookeeperClient
  }
}
