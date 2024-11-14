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

import java.io.IOException
import java.util

import org.apache.hadoop.yarn.api.protocolrecords.{GetApplicationsRequest, GetApplicationsResponse}
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl
import org.apache.hadoop.yarn.exceptions.YarnException

import org.apache.livy.Logging



/**
 * Provides an extended implementation of @YarnClientImpl with required methods.
 * Uses the protected rmClient for any custom methods.
 */
class YarnClientExt extends YarnClientImpl with Logging {


  /**
   * Returns the list of Yarn applications based on the
   * GetApplicationsRequest.
   * @param request GetApplicationsRequest
   * @return List of ApplicationReport matching the GetApplicationsRequest.
   */
  @throws[YarnException]
  @throws[IOException]
  def getApplications(request: GetApplicationsRequest): util
  .List[ApplicationReport] = {
    logger.trace("getApplications called in YarnClientExt with " +
      "GetApplicationsRequest, calling rmClient to get Applications")
    val response: GetApplicationsResponse = rmClient.getApplications(request)
    response.getApplicationList
  }
}
