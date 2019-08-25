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

import org.apache.hive.service.cli.{FetchOrientation, HiveSQLException, OperationState, OperationType, SessionHandle}

import org.apache.livy.thriftserver.serde.ThriftResultSet

/**
  * MetadataOperation is the base class for operations which do not perform any call on Spark side
  *
  * @param sessionHandle
  * @param opType
  */
abstract class MetadataOperation(sessionHandle: SessionHandle, opType: OperationType)
  extends Operation(sessionHandle, opType) {
  setHasResultSet(true)

  protected def rowSet: ThriftResultSet

  @throws[HiveSQLException]
  override def close(): Unit = {
    setState(OperationState.CLOSED)
  }

  @throws[HiveSQLException]
  override def cancel(stateAfterCancel: OperationState): Unit = {
    throw new UnsupportedOperationException("MetadataOperation.cancel()")
  }

  @throws(classOf[HiveSQLException])
  override def getNextRowSet(orientation: FetchOrientation, maxRows: Long): ThriftResultSet = {
    assertState(Seq(OperationState.FINISHED))
    validateFetchOrientation(orientation)
    if (orientation.equals(FetchOrientation.FETCH_FIRST)) {
      rowSet.setRowOffset(0)
    }
    rowSet.extractSubset(maxRows.toInt)
  }
}
