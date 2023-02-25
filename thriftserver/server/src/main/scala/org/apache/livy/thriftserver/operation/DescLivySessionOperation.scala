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

package org.apache.livy.thriftserver.operation

import org.apache.hive.service.cli.{FetchOrientation, OperationState, OperationType, SessionHandle}

import org.apache.livy.thriftserver.LivyThriftSessionManager
import org.apache.livy.thriftserver.serde.ThriftResultSet
import org.apache.livy.thriftserver.types.{BasicDataType, Field, Schema}

class DescLivySessionOperation(sessionHandle: SessionHandle,
     sessionManager: LivyThriftSessionManager)
  extends Operation(sessionHandle, OperationType.EXECUTE_STATEMENT) {

  private var hasNext: Boolean = true

  override protected def runInternal(): Unit = {
    setState(OperationState.PENDING)
    setHasResultSet(true) // avoid no resultset for async run
    setState(OperationState.RUNNING)
    setState(OperationState.FINISHED)
  }

  override def cancel(stateAfterCancel: OperationState): Unit = {
    setState(OperationState.CANCELED)
  }

  override def close(): Unit = {
    setState(OperationState.CLOSED)
  }

  override def getResultSetSchema: Schema = {
    assertState(Seq(OperationState.FINISHED))
    DescLivySessionOperation.SCHEMA
  }

  override def getNextRowSet(orientation: FetchOrientation,
      maxRows: Long): ThriftResultSet = {

    validateFetchOrientation(orientation)
    assertState(Seq(OperationState.FINISHED))
    setHasResultSet(true)

    val sessionVar = ThriftResultSet(this.getResultSetSchema,
      sessionManager.getSessionInfo(sessionHandle).protocolVersion)
    val session = sessionManager.getLivySession(sessionHandle)
    if (hasNext) {
      sessionVar.addRow(
        Array(
          session.id.toString,
          session.appId.orNull,
          session.state.state,
          session.logLines().mkString("\n")
        )
      )
      hasNext = false
    }
    sessionVar
  }
}

object DescLivySessionOperation {
  val SCHEMA: Schema = Schema(
    Field("id", BasicDataType("string"), "Livy session id."),
    Field("appId", BasicDataType("string"), "Spark application id."),
    Field("state", BasicDataType("string"), "Livy session state"),
    Field("logs", BasicDataType("string"), "Spark application logs.")
  )
}
