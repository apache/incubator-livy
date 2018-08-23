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

import java.util
import java.util.{Map => JMap}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable

import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.metastore.api.{FieldSchema, Schema}
import org.apache.hive.service.cli._
import org.apache.hive.service.cli.operation.{GetCatalogsOperation, GetTableTypesOperation, GetTypeInfoOperation, Operation}

import org.apache.livy.Logging

class LivyOperationManager(val livyThriftSessionManager: LivyThriftSessionManager)
  extends Logging {

  private val handleToOperation = new ConcurrentHashMap[OperationHandle, Operation]()
  private val sessionToOperationHandles =
    new mutable.HashMap[SessionHandle, mutable.Set[OperationHandle]]()

  private def addOperation(operation: Operation, sessionHandle: SessionHandle): Unit = {
    handleToOperation.put(operation.getHandle, operation)
    sessionToOperationHandles.synchronized {
      val set = sessionToOperationHandles.getOrElseUpdate(sessionHandle,
        new mutable.HashSet[OperationHandle])
      set += operation.getHandle
    }
  }

  @throws[HiveSQLException]
  private def removeOperation(operationHandle: OperationHandle): Operation = {
    val operation = handleToOperation.remove(operationHandle)
    if (operation == null) {
      throw new HiveSQLException(s"Operation does not exist: $operationHandle")
    }
    val sessionHandle = operation.getSessionHandle
    sessionToOperationHandles.synchronized {
      sessionToOperationHandles(sessionHandle) -= operationHandle
      if (sessionToOperationHandles(sessionHandle).isEmpty) {
        sessionToOperationHandles.remove(sessionHandle)
      }
    }
    operation
  }

  def getOperations(sessionHandle: SessionHandle): Set[OperationHandle] = {
    sessionToOperationHandles.synchronized {
      sessionToOperationHandles(sessionHandle).toSet
    }
  }

  def getTimedOutOperations(sessionHandle: SessionHandle): Set[Operation] = {
    val opHandles = getOperations(sessionHandle)
    val currentTime = System.currentTimeMillis()
    opHandles.flatMap { handle =>
      // Some operations may have finished and been removed since we got them.
      Option(handleToOperation.get(handle))
    }.filter(_.isTimedOut(currentTime))
  }

  @throws[HiveSQLException]
  def getOperation(operationHandle: OperationHandle): Operation = {
    val operation = handleToOperation.get(operationHandle)
    if (operation == null) {
      throw new HiveSQLException(s"Invalid OperationHandle: $operationHandle")
    }
    operation
  }

  def newExecuteStatementOperation(
      sessionHandle: SessionHandle,
      statement: String,
      confOverlay: JMap[String, String],
      runAsync: Boolean,
      queryTimeout: Long): Operation = {
    val op = new LivyExecuteStatementOperation(
      sessionHandle,
      statement,
      confOverlay,
      runAsync,
      livyThriftSessionManager)
    addOperation(op, sessionHandle)
    debug(s"Created Operation for $statement with session=$sessionHandle, " +
      s"runInBackground=$runAsync")
    op
  }

  def getOperationLogRowSet(
      opHandle: OperationHandle,
      orientation: FetchOrientation,
      maxRows: Long): RowSet = {
    val tableSchema = new TableSchema(LivyOperationManager.LOG_SCHEMA)
    val session = livyThriftSessionManager.getSessionInfo(getOperation(opHandle).getSessionHandle)
    val logs = RowSetFactory.create(tableSchema, session.protocolVersion, false)

    if (!livyThriftSessionManager.getHiveConf.getBoolVar(
        ConfVars.HIVE_SERVER2_LOGGING_OPERATION_ENABLED)) {
      warn("Try to get operation log when " +
        ConfVars.HIVE_SERVER2_LOGGING_OPERATION_ENABLED.varname +
        " is false, no log will be returned. ")
    } else {
      // Get the operation log. This is implemented only for LivyExecuteStatementOperation
      val operation = getOperation(opHandle)
      if (operation.isInstanceOf[LivyExecuteStatementOperation]) {
        val op = getOperation(opHandle).asInstanceOf[LivyExecuteStatementOperation]
        op.getOperationMessages.foreach { l =>
          logs.addRow(Array(l))
        }
      }
    }
    logs
  }

  @throws[HiveSQLException]
  def executeStatement(
      sessionHandle: SessionHandle,
      statement: String,
      confOverlay: util.Map[String, String],
      runAsync: Boolean,
      queryTimeout: Long): OperationHandle = {
    executeOperation(sessionHandle, {
      newExecuteStatementOperation(sessionHandle, statement, confOverlay, runAsync, queryTimeout)
    })
  }

  @throws[HiveSQLException]
  private def executeOperation(
      sessionHandle: SessionHandle,
      operationCreator: => Operation): OperationHandle = {
    var opHandle: OperationHandle = null
    try {
      val operation = operationCreator
      opHandle = operation.getHandle
      operation.run()
      opHandle
    } catch {
      case e: HiveSQLException =>
        if (opHandle != null) {
          closeOperation(opHandle)
        }
        throw e
    }
  }

  @throws[HiveSQLException]
  def getTypeInfo(sessionHandle: SessionHandle): OperationHandle = {
    executeOperation(sessionHandle, {
      val op = new GetTypeInfoOperation(sessionHandle)
      addOperation(op, sessionHandle)
      op
    })
  }

  @throws[HiveSQLException]
  def getCatalogs(sessionHandle: SessionHandle): OperationHandle = {
    executeOperation(sessionHandle, {
      val op = new GetCatalogsOperation(sessionHandle)
      addOperation(op, sessionHandle)
      op
    })
  }

  @throws[HiveSQLException]
  def getTableTypes(sessionHandle: SessionHandle): OperationHandle = {
    executeOperation(sessionHandle, {
      val op = new GetTableTypesOperation(sessionHandle)
      addOperation(op, sessionHandle)
      op
    })
  }

  /**
   * Cancel the running operation unless it is already in a terminal state
   */
  @throws[HiveSQLException]
  def cancelOperation(opHandle: OperationHandle, errMsg: String): Unit = {
    val operation = getOperation(opHandle)
    val opState = operation.getStatus.getState
    if (opState.isTerminal) { // Cancel should be a no-op in either cases
      debug(s"$opHandle: Operation is already aborted in state - $opState")
    } else {
      debug(s"$opHandle: Attempting to cancel from state - $opState")
      val operationState = OperationState.CANCELED
      operationState.setErrorMessage(errMsg)
      operation.cancel(operationState)
    }
  }

  @throws[HiveSQLException]
  def cancelOperation(opHandle: OperationHandle): Unit = {
    cancelOperation(opHandle, "")
  }

  @throws[HiveSQLException]
  def closeOperation(opHandle: OperationHandle): Unit = {
    info("Closing operation: " + opHandle)
    val operation = removeOperation(opHandle)
    operation.close()
  }

  @throws[HiveSQLException]
  def fetchResults(
      opHandle: OperationHandle,
      orientation: FetchOrientation,
      maxRows: Long,
      fetchType: FetchType): RowSet = {
    if (fetchType == FetchType.QUERY_OUTPUT) {
      getOperation(opHandle).getNextRowSet(orientation, maxRows)
    } else {
      getOperationLogRowSet(opHandle, orientation, maxRows)
    }
  }
}

object LivyOperationManager {
 val LOG_SCHEMA: Schema = {
    val schema = new Schema
    val fieldSchema = new FieldSchema
    fieldSchema.setName("operation_log")
    fieldSchema.setType("string")
    schema.addToFieldSchemas(fieldSchema)
    schema
  }
}
