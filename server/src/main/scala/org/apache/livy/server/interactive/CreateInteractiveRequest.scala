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

package org.apache.livy.server.interactive

import org.apache.livy.sessions.{Kind, Shared}

class CreateInteractiveRequest {
  var kind: Kind = Shared
  var proxyUser: Option[String] = None
  var jars: List[String] = List()
  var pyFiles: List[String] = List()
  var files: List[String] = List()
  var driverMemory: Option[String] = None
  var driverCores: Option[Int] = None
  var executorMemory: Option[String] = None
  var executorCores: Option[Int] = None
  var numExecutors: Option[Int] = None
  var archives: List[String] = List()
  var queue: Option[String] = None
  var name: Option[String] = None
  var conf: Map[String, String] = Map()
  var heartbeatTimeoutInSecond: Int = 0
  var driverMemoryOverhead:Option[String] = None
  var executorMemoryOverhead:Option[String] = None
  var dynamicAllocationEnabled:Option[Boolean] = None
  var shuffleServiceEnabled:Option[Boolean] = None
  var dynamicAllocationMinExecutors:Option[Int] = None
  var dynamicAllocationMaxExecutors:Option[Int] = None
  var dynamicAllocationInitialExecutors:Option[Int] = None
  var dynamicAllocationSchedulerBacklogTimeout:Option[Int] = None
  var dynamicAllocationSustainedSchedulerBacklogTimeout:Option[Int] = None
  var dynamicAllocationExecutorIdleTimeout:Option[Int] = None

  override def toString: String = {
    s"[kind: $kind, proxyUser: $proxyUser, " +
      (if (jars.nonEmpty) s"jars: ${jars.mkString(",")}, " else "") +
      (if (pyFiles.nonEmpty) s"pyFiles: ${pyFiles.mkString(",")}, " else "") +
      (if (files.nonEmpty) s"files: ${files.mkString(",")}, " else "") +
      (if (archives.nonEmpty) s"archives: ${archives.mkString(",")}, " else "") +
      (if (driverMemory.isDefined) s"driverMemory: ${driverMemory.get}, " else "") +
      (if (driverCores.isDefined) s"driverCores: ${driverCores.get}, " else "") +
      (if (executorMemory.isDefined) s"executorMemory: ${executorMemory.get}, " else "") +
      (if (executorCores.isDefined) s"executorCores: ${executorCores.get}, " else "") +
      (if (numExecutors.isDefined) s"numExecutors: ${numExecutors.get}, " else "") +
      (if (queue.isDefined) s"queue: ${queue.get}, " else "") +
      (if (name.isDefined) s"name: ${name.get}, " else "") +
      (if (driverMemoryOverhead.isDefined) s"driverMemoryOverhead: ${driverMemoryOverhead.get}, " else "") +
      (if (executorMemoryOverhead.isDefined) s"executorMemoryOverhead: ${executorMemoryOverhead.get}, " else "") +
      (if (dynamicAllocationEnabled.isDefined) s"dynamicAllocationEnabled: " +
        s"${dynamicAllocationEnabled.get}, " else "") +
      (if (shuffleServiceEnabled.isDefined) s"shuffleServiceEnabled: ${shuffleServiceEnabled.get}, " else "") +
      (if (dynamicAllocationMinExecutors.isDefined) s"dynamicAllocationMinExecutors: " +
        s"${dynamicAllocationMinExecutors.get}, " else "") +
      (if (dynamicAllocationMaxExecutors.isDefined) s"dynamicAllocationMaxExecutors: " +
        s"${dynamicAllocationMaxExecutors.get}, " else "") +
      (if (dynamicAllocationInitialExecutors.isDefined) s"dynamicAllocationInitialExecutors: " +
        s"${dynamicAllocationInitialExecutors.get}, " else "") +
      (if (dynamicAllocationSchedulerBacklogTimeout.isDefined) s"dynamicAllocationSchedulerBacklogTimeout: " +
        s"${dynamicAllocationSchedulerBacklogTimeout.get}, " else "") +
      (if (dynamicAllocationSustainedSchedulerBacklogTimeout.isDefined)
        s"dynamicAllocationSustainedSchedulerBacklogTimeout: " +
          s"${dynamicAllocationSustainedSchedulerBacklogTimeout.get}, " else "") +
      (if (dynamicAllocationExecutorIdleTimeout.isDefined)
        s"dynamicAllocationExecutorIdleTimeout: ${dynamicAllocationExecutorIdleTimeout.get}, " else "") +
      (if (conf.nonEmpty) s"conf: ${conf.mkString(",")}, " else "") +
      s"heartbeatTimeoutInSecond: $heartbeatTimeoutInSecond]"
  }
}
