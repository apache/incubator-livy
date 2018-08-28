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

import java.security.PrivilegedExceptionAction
import java.util
import java.util.{Map => JMap}
import java.util.concurrent.{ConcurrentLinkedQueue, RejectedExecutionException}

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.hadoop.hive.serde2.thrift.ColumnBuffer
import org.apache.hadoop.hive.shims.Utils
import org.apache.hive.service.cli._
import org.apache.hive.service.cli.operation.Operation

import org.apache.livy.Logging
import org.apache.livy.thriftserver.SessionStates._
import org.apache.livy.thriftserver.rpc.RpcClient
import org.apache.livy.thriftserver.types.DataTypeUtils._

class LivyExecuteStatementOperation(
    sessionHandle: SessionHandle,
    statement: String,
    confOverlay: JMap[String, String],
    runInBackground: Boolean = true,
    sessionManager: LivyThriftSessionManager)
  extends Operation(sessionHandle, confOverlay, OperationType.EXECUTE_STATEMENT)
    with Logging {

  /**
   * Contains the messages which have to be sent to the client.
   */
  private val operationMessages = new ConcurrentLinkedQueue[String]

  // The initialization need to be lazy in order not to block when the instance is created
  private lazy val rpcClient = {
    val sessionState = sessionManager.livySessionState(sessionHandle)
    if (sessionState == CREATION_IN_PROGRESS) {
      operationMessages.offer(
        "Livy session has not yet started. Please wait for it to be ready...")
    }
    // This call is blocking, we are waiting for the session to be ready.
    new RpcClient(sessionManager.getLivySession(sessionHandle))
  }
  private var rowOffset = 0L

  private def statementId: String = getHandle.getHandleIdentifier.toString

  private def rpcClientValid: Boolean =
    sessionManager.livySessionState(sessionHandle) == CREATION_SUCCESS && rpcClient.isValid

  override def getNextRowSet(order: FetchOrientation, maxRowsL: Long): RowSet = {
    validateDefaultFetchOrientation(order)
    assertState(util.Arrays.asList(OperationState.FINISHED))
    setHasResultSet(true)

    // maxRowsL here typically maps to java.sql.Statement.getFetchSize, which is an int
    val maxRows = maxRowsL.toInt
    val jsonSchema = rpcClient.fetchResultSchema(statementId).get()
    val types = getInternalTypes(jsonSchema)
    val livyColumnResultSet = rpcClient.fetchResult(statementId, types, maxRows).get()

    val thriftColumns = livyColumnResultSet.columns.map { col =>
      new ColumnBuffer(toHiveThriftType(col.dataType), col.getNulls, col.getColumnValues)
    }
    val result = new ColumnBasedSet(tableSchemaFromSparkJson(jsonSchema).toTypeDescriptors,
      thriftColumns.toList.asJava,
      rowOffset)
    livyColumnResultSet.columns.headOption.foreach { c =>
      rowOffset += c.size
    }
    result
  }

  override def runInternal(): Unit = {
    setState(OperationState.PENDING)
    setHasResultSet(true) // avoid no resultset for async run

    if (!runInBackground) {
      execute()
    } else {
      val livyServiceUGI = Utils.getUGI

      // Runnable impl to call runInternal asynchronously,
      // from a different thread
      val backgroundOperation = new Runnable() {

        override def run(): Unit = {
          val doAsAction = new PrivilegedExceptionAction[Unit]() {
            override def run(): Unit = {
              try {
                execute()
              } catch {
                case e: HiveSQLException =>
                  setOperationException(e)
                  error("Error running hive query: ", e)
              }
            }
          }

          try {
            livyServiceUGI.doAs(doAsAction)
          } catch {
            case e: Exception =>
              setOperationException(new HiveSQLException(e))
              error("Error running hive query as user : " +
                livyServiceUGI.getShortUserName, e)
          }
        }
      }
      try {
        // This submit blocks if no background threads are available to run this operation
        val backgroundHandle = sessionManager.submitBackgroundOperation(backgroundOperation)
        setBackgroundHandle(backgroundHandle)
      } catch {
        case rejected: RejectedExecutionException =>
          setState(OperationState.ERROR)
          throw new HiveSQLException("The background threadpool cannot accept" +
            " new task for execution, please retry the operation", rejected)
        case NonFatal(e) =>
          error(s"Error executing query in background", e)
          setState(OperationState.ERROR)
          throw e
      }
    }
  }

  protected def execute(): Unit = {
    info(s"Running query '$statement' with id $statementId")
    setState(OperationState.RUNNING)

    try {
      rpcClient.executeSql(sessionHandle, statementId, statement).get()
    } catch {
      case e: Throwable =>
        val currentState = getStatus.getState
        info(s"Error executing query, currentState $currentState, ", e)
        setState(OperationState.ERROR)
        throw new HiveSQLException(e)
    }
    setState(OperationState.FINISHED)
  }

  def close(): Unit = {
    info(s"Close $statementId")
    cleanup(OperationState.CLOSED)
  }

  override def cancel(state: OperationState): Unit = {
    info(s"Cancel $statementId with state $state")
    cleanup(state)
  }

  def getResultSetSchema: TableSchema = {
    val tableSchema = tableSchemaFromSparkJson(rpcClient.fetchResultSchema(statementId).get())
    // Workaround for operations returning an empty schema (eg. CREATE, INSERT, ...)
    if (tableSchema.getSize == 0) {
      tableSchema.addStringColumn("Result", "")
    }
    tableSchema
  }

  private def cleanup(state: OperationState) {
    if (statementId != null && rpcClientValid) {
      rpcClient.cleanupStatement(statementId).get()
    }
    setState(state)
  }

  /**
   * Returns the messages that should be sent to the client and removes them from the queue in
   * order not to send them twice.
   */
  def getOperationMessages: Seq[String] = {
    def fetchNext: (mutable.ListBuffer[String]) => Boolean = (acc: mutable.ListBuffer[String]) => {
      val m = operationMessages.poll()
      if (m == null) {
        false
      } else {
        acc += m
        true
      }
    }
    val res = new mutable.ListBuffer[String]
    while (fetchNext(res)) {}
    res
  }
}
