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

import scala.collection.mutable.Set

case class ServiceNode(host: String, port: Int, UUID: String)

/**
 * Interface for cluster management.
 */
abstract class ClusterManager {
  /**
   * Register current node into the cluster. It should be invoked when the node is ready
   * for service.
   */
  def register(): Unit

  /**
   * Get the nodes in the cluster.
   * @return
   */
  def getNodes(): Set[ServiceNode]

  /**
   * Add a listener which will be notified when a new node join the cluster.
   * @param listener
   */
  def registerNodeJoinListener(listener: ServiceNode => Unit): Unit

  /**
   * Add a listener which will be notified when a node leave the cluster.
   * @param listener
   */
  def registerNodeLeaveListener(listener : ServiceNode => Unit): Unit
}

