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

package org.apache.livy.utils

/**
  * A case class to keep all Application data that we get form YARN.
  */
case class ApplicationReport(
                              id: String,
                              name: String,
                              user: String,
                              applicationTags: Option[String],
                              applicationType: String,
//                              clusterId: Long,
//                              clusterUsagePercentage: Float,
                              diagnostics: String,
                              elapsedTime: Long,
                              finalStatus: String,
                              finishedTime: Long,
                              amContainerLogs: Option[String],
//                              logAggregationStatus: String,
//                              priority: Int,
//                              progress: Float,
                              queue: String,
//                              runningContainers: Int,
                              startedTime: Long,
                              state: String,
                              trackingUI: Option[String],
                              trackingUrl: Option[String]
                            )
