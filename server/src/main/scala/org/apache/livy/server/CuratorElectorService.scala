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

package org.apache.livy.server

import java.io.Closeable
import java.io.IOException
import java.net.InetAddress
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.recipes.leader.LeaderLatch
import org.apache.curator.framework.recipes.leader.LeaderLatchListener
import org.apache.curator.retry.RetryNTimes

import org.apache.livy.{LivyConf, Logging}
import org.apache.livy.LivyConf.Entry

object CuratorElectorService {
  val HA_KEY_PREFIX_CONF = Entry("livy.server.ha.key-prefix", "livy_ha")
  val HA_RETRY_CONF = Entry("livy.server.ha.retry-policy", "5,100")
}

object HAState extends Enumeration{
  type HAState = Value
  val Active, Standby = Value
}


class CuratorElectorService(livyConf : LivyConf, livyServer : LivyServer,
    mockCuratorClient: Option[CuratorFramework] = None,
    mockLeaderLatch: Option[LeaderLatch] = None)
  extends LeaderLatchListener
  with Logging
{
  import CuratorElectorService._
  import HAState._

  val haAddress = livyConf.get(LivyConf.HA_ZOOKEEPER_URL)
  require(!haAddress.isEmpty, s"Please configure ${LivyConf.HA_ZOOKEEPER_URL.key}.")
  val haKeyPrefix = livyConf.get(HA_KEY_PREFIX_CONF)
  val retryValue = livyConf.get(HA_RETRY_CONF)
  // a regex to match patterns like "m, n" where m and n both are integer values
  val retryPattern = """\s*(\d+)\s*,\s*(\d+)\s*""".r
  val retryPolicy = retryValue match {
    case retryPattern(n, sleepMs) => new RetryNTimes(n.toInt, sleepMs.toInt)
    case _ => throw new IllegalArgumentException(
      s"$HA_KEY_PREFIX_CONF contains bad value: $retryValue. " +
        "Correct format is <max retry count>,<sleep ms between retry>. e.g. 5,100")
  }

  var server : LivyServer = livyServer

  val client: CuratorFramework = mockCuratorClient.getOrElse {
    CuratorFrameworkFactory.newClient(haAddress, retryPolicy)
  }
  val leaderKey = s"/$haKeyPrefix/leader"

  val leaderIds = livyConf.configToSeq(LivyConf.HA_SERVER_IDS)
  val leaderHostnames = livyConf.configToSeq(LivyConf.HA_SERVER_HOSTNAMES)
  val leaderAddresses = livyConf.configToSeq(LivyConf.HA_SERVER_HOSTNAMES)

  var leaderLatch = mockLeaderLatch.getOrElse {
    new LeaderLatch(client, leaderKey, getCurrentId())
  }
  leaderLatch.addListener(this)

  var currentState = HAState.Standby
  def isLeader() {
    transitionToActive()
  }

  def notLeader(){
    transitionToStandby()
  }

  def getCurrentId(): String = {
    val currentHostname = java.net.InetAddress.getLocalHost().getHostName();
    debug("This server's current hostname is: " + currentHostname)
    debug("This hostnames for valid leaders are: " + leaderHostnames)
    val currentId = leaderIds(leaderHostnames indexOf currentHostname)
    debug("This server's designated ID is: " + currentId)
    currentId
  }

  def getActiveAddress(): String = {
    val activeLeaderId = leaderLatch.getLeader().getId()
    val activeAddress = leaderAddresses(leaderIds indexOf activeLeaderId)
    activeAddress
  }

  def start(): Unit = {
    transitionToStandby()

    server.start()
    client.start()
    leaderLatch.start()

    try {
      Thread.currentThread.join()
    } finally {
      transitionToStandby()
    }
  }

  def close(): Unit = {
    transitionToStandby()
    leaderLatch.close()
  }

  def transitionToActive(): Unit = {
    info("Transitioning to Active state")
    if(currentState == HAState.Active) {
      info("Already in Active State")
    }
    else {
      server.restart()
      currentState = HAState.Active
      info("Transition complete")
    }
  }

  def transitionToStandby(): Unit = {
    info("Transitioning to Standby state")
    if(currentState == HAState.Standby) {
      info("Already in Standby State")
    }
    else {
      currentState = HAState.Standby
      info("Transition complete")
    }
  }
}
