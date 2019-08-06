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

import org.apache.commons.lang.StringUtils
import org.apache.hive.service.cli.{FetchOrientation, HiveSQLException, OperationState, OperationType, SessionHandle}

import org.apache.livy.thriftserver.serde.ThriftResultSet

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
    rowSet.extractSubset(maxRows)
  }

  /**
    * Convert wildchars and escape sequence from JDBC format to datanucleous/regex
    */
  protected def convertIdentifierPattern(pattern: String, datanucleusFormat: Boolean): String = {
    if (pattern == null) {
      convertPattern("%", datanucleusFormat = true)
    } else {
      convertPattern(pattern, datanucleusFormat)
    }
  }

  /**
    * Convert wildchars and escape sequence of schema pattern from JDBC format to datanucleous/regex
    * The schema pattern treats empty string also as wildchar
    */
  protected def convertSchemaPattern(pattern: String): String = {
    if (StringUtils.isEmpty(pattern)) {
      convertPattern("%", datanucleusFormat = true)
    } else {
      convertPattern(pattern, datanucleusFormat = true)
    }
  }

  private def convertPattern(pattern: String, datanucleusFormat: Boolean): String = {
    val wStr = if (datanucleusFormat) "*" else ".*"
    pattern
      .replaceAll("([^\\\\])%", "$1" + wStr)
      .replaceAll("\\\\%", "%")
      .replaceAll("^%", wStr)
      .replaceAll("([^\\\\])_", "$1.")
      .replaceAll("\\\\_", "_")
      .replaceAll("^_", ".")
  }
}
