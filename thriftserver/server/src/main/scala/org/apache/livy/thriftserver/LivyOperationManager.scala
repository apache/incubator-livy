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

import org.apache.hive.service.cli._

import org.apache.livy.{LivyConf, Logging}
import org.apache.livy.thriftserver.operation._
import org.apache.livy.thriftserver.serde.ThriftResultSet
import org.apache.livy.thriftserver.session.DataType

class LivyOperationManager(val livyThriftSessionManager: LivyThriftSessionManager)
  extends Logging {

  private val handleToOperation = new ConcurrentHashMap[OperationHandle, Operation]()
  private val sessionToOperationHandles =
    new mutable.HashMap[SessionHandle, mutable.Set[OperationHandle]]()

  private val operationTimeout =
    livyThriftSessionManager.livyConf.getTimeAsMs(LivyConf.THRIFT_IDLE_OPERATION_TIMEOUT)

  private def addOperation(operation: Operation, sessionHandle: SessionHandle): Unit = {
    handleToOperation.put(operation.opHandle, operation)
    sessionToOperationHandles.synchronized {
      val set = sessionToOperationHandles.getOrElseUpdate(sessionHandle,
        new mutable.HashSet[OperationHandle])
      set += operation.opHandle
    }
  }

  @throws[HiveSQLException]
  private def removeOperation(operationHandle: OperationHandle): Operation = {
    val operation = handleToOperation.remove(operationHandle)
    if (operation == null) {
      throw new HiveSQLException(s"Operation does not exist: $operationHandle")
    }
    val sessionHandle = operation.sessionHandle
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
    }.filter(_.isTimedOut(currentTime, operationTimeout))
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
      maxRows: Long): ThriftResultSet = {
    val session = livyThriftSessionManager.getSessionInfo(getOperation(opHandle).sessionHandle)
    val logs = ThriftResultSet(LivyOperationManager.LOG_SCHEMA, session.protocolVersion)

    if (!livyThriftSessionManager.livyConf.getBoolean(LivyConf.THRIFT_LOG_OPERATION_ENABLED)) {
      warn(s"Try to get operation log when ${LivyConf.THRIFT_LOG_OPERATION_ENABLED.key} is " +
        "false, no log will be returned.")
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
      opHandle = operation.opHandle
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

  @throws[HiveSQLException]
  def getTables(
      sessionHandle: SessionHandle,
      catalogName: String,
      schemaName: String,
      tableName: String,
      tableTypes: util.List[String]): OperationHandle = {
    executeOperation(sessionHandle, {
      val op = new GetTablesOperation(
        sessionHandle, schemaName, tableName, tableTypes, livyThriftSessionManager)
      addOperation(op, sessionHandle)
      op
    })
  }

  @throws[HiveSQLException]
  def getFunctions(
      sessionHandle: SessionHandle,
      catalogName: String,
      schemaName: String,
      functionName: String): OperationHandle = {
    executeOperation(sessionHandle, {
      val op = new GetFunctionsOperation(
        sessionHandle, schemaName, functionName, livyThriftSessionManager)
      addOperation(op, sessionHandle)
      op
    })
  }

  @throws[HiveSQLException]
  def getSchemas(
      sessionHandle: SessionHandle,
      catalogName: String,
      schemaName: String): OperationHandle = {
    executeOperation(sessionHandle, {
      val op = new GetSchemasOperation(sessionHandle, schemaName, livyThriftSessionManager)
      addOperation(op, sessionHandle)
      op
    })
  }

  @throws[HiveSQLException]
  def getColumns(
      sessionHandle: SessionHandle,
      catalogName: String,
      schemaName: String,
      tableName: String,
      columnName: String): OperationHandle = {
    executeOperation(sessionHandle, {
      val op = new GetColumnsOperation(
        sessionHandle, schemaName, tableName, columnName, livyThriftSessionManager)
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
    val opState = operation.getStatus.state
    if (opState.isTerminal) {
      // Cancel should be a no-op either case
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
      fetchType: FetchType): ThriftResultSet = {
    if (fetchType == FetchType.QUERY_OUTPUT) {
      getOperation(opHandle).getNextRowSet(orientation, maxRows)
    } else {
      getOperationLogRowSet(opHandle, orientation, maxRows)
    }
  }
}

object LivyOperationManager {
  val LOG_SCHEMA: Array[DataType] = Array(DataType.STRING)
}
