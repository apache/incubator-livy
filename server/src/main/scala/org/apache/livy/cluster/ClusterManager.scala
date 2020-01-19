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
package org.apache.livy.cluster

import java.util.UUID

import scala.collection.mutable.{ArrayBuffer, Set}
import org.apache.livy.LivyConf
import org.apache.livy.LivyConf._
import org.apache.livy.Logging
import org.apache.livy.rsc.RSCConf.Entry.LAUNCHER_ADDRESS
import org.apache.livy.server.recovery.ZooKeeperManager

import scala.collection.mutable

case class ServiceNode(host: String, port: Int, UUID: String)

class ClusterManager(livyConf: LivyConf, zkManager: ZooKeeperManager) extends Logging {
  private val serverIP = livyConf.get(LAUNCHER_ADDRESS)
  require(serverIP != null, "Please config the livy.rsc.launcher.address")

  private val port = livyConf.getInt(SERVER_PORT)

  private val serviceDir = livyConf.get(ZK_SERVICE_DIR)
  require(serviceDir != null, "Please config the livy.server.zk.services")

  private val nodes = new mutable.HashSet[ServiceNode]()
  private val nodeJoinListeners = new ArrayBuffer[ServiceNode => Unit]()
  private val nodeLeaveListeners = new ArrayBuffer[ServiceNode => Unit]()

  private val separator = "#"

  zkManager.getChildren(serviceDir).foreach(node => {
    val serviceNode = zkManager.get[ServiceNode](serviceDir + "/" + node).get
    nodes.add(serviceNode)
  })

  zkManager.watchAddNode(serviceDir, nodeAddHandler)
  zkManager.watchRemoveNode(serviceDir, nodeRemoveHandler)

  def register(): Unit = {
    val node = ServiceNode(serverIP, port, UUID.randomUUID().toString)
    zkManager.createEphemeralNode(serviceDir + "/" + serverIP + ":" + port, node)
  }

  def getNodes(): Set[ServiceNode] = {
    nodes
  }

  def registerNodeJoinListener(listener: ServiceNode => Unit): Unit = {
    nodeJoinListeners.append(listener)
  }

  def registerNodeLeaveListener(listener : ServiceNode => Unit): Unit = {
    nodeLeaveListeners.append(listener)
  }

  private def nodeAddHandler(path: String, node: ServiceNode): Unit = {
    logger.info("Detect new node join: " + node)
    nodes.add(node)
    nodeJoinListeners.foreach(_(node))
  }

  private def nodeRemoveHandler(path: String, node: ServiceNode): Unit = {
    logger.info("Detect node leave: " + node)
    nodes.remove(node)
    nodeLeaveListeners.foreach(_(node))
  }
}

