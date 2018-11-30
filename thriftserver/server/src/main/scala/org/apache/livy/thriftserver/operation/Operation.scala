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

import java.util
import java.util.concurrent.Future

import org.apache.hive.service.cli.FetchOrientation
import org.apache.hive.service.cli.HiveSQLException
import org.apache.hive.service.cli.OperationHandle
import org.apache.hive.service.cli.OperationState
import org.apache.hive.service.cli.OperationType
import org.apache.hive.service.cli.SessionHandle
import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.livy.Logging
import org.apache.livy.thriftserver.serde.ThriftResultSet
import org.apache.livy.thriftserver.types.Schema

abstract class Operation(
    val sessionHandle: SessionHandle,
    val opType: OperationType) extends Logging {

  @volatile private var state = OperationState.INITIALIZED
  val opHandle = new OperationHandle(opType, sessionHandle.getProtocolVersion)

  protected var resultSetPresent: Boolean = false
  @volatile private var operationException: HiveSQLException = _
  @volatile protected var backgroundHandle: Future[_] = _

  private val beginTime = System.currentTimeMillis()
  @volatile private var lastAccessTime = beginTime

  protected var operationStart: Long = _
  protected var operationComplete: Long = _

  def getBackgroundHandle: Future[_] = backgroundHandle

  protected def setBackgroundHandle(backgroundHandle: Future[_]): Unit = {
    this.backgroundHandle = backgroundHandle
  }

  def shouldRunAsync: Boolean = false

  def protocolVersion: TProtocolVersion = opHandle.getProtocolVersion

  def getStatus: OperationStatus = {
    // TODO: get and return also the task status
    OperationStatus(state, operationStart, operationComplete, resultSetPresent, operationException)
  }

  def hasResultSet: Boolean = resultSetPresent

  protected def setHasResultSet(hasResultSet: Boolean): Unit = {
    this.resultSetPresent = hasResultSet
    opHandle.setHasResultSet(hasResultSet)
  }

  @throws[HiveSQLException]
  protected def setState(newState: OperationState): OperationState = {
    state.validateTransition(newState)
    val prevState = state
    state = newState
    onNewState(state, prevState)
    this.lastAccessTime = System.currentTimeMillis()
    state
  }

  def isTimedOut(current: Long, operationTimeout: Long): Boolean = {
    if (operationTimeout == 0) {
      false
    } else if (operationTimeout > 0) {
      // check only when it's in terminal state
      state.isTerminal && lastAccessTime + operationTimeout <= current
    } else {
      lastAccessTime + (- operationTimeout) <= current
    }
  }

  protected def setOperationException(operationException: HiveSQLException): Unit = {
    this.operationException = operationException
  }

  protected def assertState(states: Seq[OperationState]): Unit = {
    if (!states.contains(state)) {
      throw new HiveSQLException(s"Expected states: $states, but found $state")
    }
    this.lastAccessTime = System.currentTimeMillis()
  }

  def isDone: Boolean = state.isTerminal

  /**
   * Implemented by subclass of Operation class to execute specific behaviors.
   */
  @throws[HiveSQLException]
  protected def runInternal(): Unit

  // As of now, run does nothing else than calling runInternal. This may change in the future as
  // additional operation before and after running may be added (as it happens in Hive).
  @throws[HiveSQLException]
  def run(): Unit = runInternal()

  @throws[HiveSQLException]
  def cancel(stateAfterCancel: OperationState): Unit

  @throws[HiveSQLException]
  def close(): Unit

  @throws[HiveSQLException]
  def getResultSetSchema: Schema

  @throws[HiveSQLException]
  def getNextRowSet(orientation: FetchOrientation, maxRows: Long): ThriftResultSet

  /**
   * Verify if the given fetch orientation is part of the supported orientation types.
   */
  @throws[HiveSQLException]
  protected def validateFetchOrientation(orientation: FetchOrientation): Unit = {
    if (!Operation.DEFAULT_FETCH_ORIENTATION_SET.contains(orientation)) {
      throw new HiveSQLException(
        s"The fetch type $orientation is not supported for this resultset", "HY106")
    }
  }

  def getBeginTime: Long = beginTime

  protected def getState: OperationState = state

  protected def onNewState(newState: OperationState, prevState: OperationState): Unit = {
    newState match {
      case OperationState.RUNNING =>
        markOperationStartTime()
      case OperationState.ERROR | OperationState.FINISHED | OperationState.CANCELED =>
        markOperationCompletedTime()
      case _ => // Do nothing.
    }
  }

  def getOperationComplete: Long = operationComplete

  def getOperationStart: Long = operationStart

  protected def markOperationStartTime(): Unit = {
    operationStart = System.currentTimeMillis()
  }

  protected def markOperationCompletedTime(): Unit = {
    operationComplete = System.currentTimeMillis()
  }
}

object Operation {
  val DEFAULT_FETCH_ORIENTATION_SET: util.EnumSet[FetchOrientation] =
    util.EnumSet.of(FetchOrientation.FETCH_NEXT, FetchOrientation.FETCH_FIRST)
  val DEFAULT_FETCH_ORIENTATION = FetchOrientation.FETCH_NEXT
}

case class OperationStatus(
  state: OperationState,
  operationStarted: Long,
  operationCompleted: Long,
  hasResultSet: Boolean,
  operationException: HiveSQLException)
