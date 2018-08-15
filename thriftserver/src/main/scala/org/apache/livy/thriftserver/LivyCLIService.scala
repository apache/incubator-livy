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

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hive.service.cli.{CLIService, GetInfoType, GetInfoValue, SessionHandle}

import org.apache.livy.LIVY_VERSION

class LivyCLIService(server: LivyThriftServer) extends CLIService(server) {
  override def init(hiveConf: HiveConf): Unit = {
    this.sessionManager = new LivyThriftSessionManager(server)
    super.init(hiveConf)
  }

  override def getInfo(sessionHandle: SessionHandle, getInfoType: GetInfoType): GetInfoValue = {
    getInfoType match {
      case GetInfoType.CLI_SERVER_NAME => new GetInfoValue("Livy JDBC")
      case GetInfoType.CLI_DBMS_NAME => new GetInfoValue("Livy JDBC")
      case GetInfoType.CLI_DBMS_VER => new GetInfoValue(LIVY_VERSION)
      case _ => super.getInfo(sessionHandle, getInfoType)
    }
  }
}
