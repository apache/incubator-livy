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

import java.util.{Map => JMap}

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hive.service.cli.{FetchOrientation, OperationHandle, RowSet}
import org.apache.hive.service.cli.operation.{ExecuteStatementOperation, OperationManager}
import org.apache.hive.service.cli.session.HiveSession

import org.apache.livy.Logging

class LivyOperationManager(
    val livyThriftSessionManager: LivyThriftSessionManager)
  extends OperationManager with Logging {

  override def newExecuteStatementOperation(
      parentSession: HiveSession,
      statement: String,
      confOverlay: JMap[String, String],
      runAsync: Boolean,
      queryTimeout: Long): ExecuteStatementOperation = {
    val op = new LivyExecuteStatementOperation(
      parentSession,
      statement,
      confOverlay,
      runAsync,
      livyThriftSessionManager)
    addOperation(op)
    debug(s"Created Operation for $statement with session=$parentSession, " +
      s"runInBackground=$runAsync")
    op
  }

  override def getOperationLogRowSet(
      opHandle: OperationHandle,
      orientation: FetchOrientation,
      maxRows: Long,
      hConf: HiveConf): RowSet = {
    val logs = super.getOperationLogRowSet(opHandle, orientation, maxRows, hConf)
    val op = getOperation(opHandle).asInstanceOf[LivyExecuteStatementOperation]
    op.getOperationMessages.foreach { l =>
      logs.addRow(Array(l))
    }
    logs
  }
}
