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
import org.apache.hive.service.cli._

import org.apache.livy.thriftserver.LivyThriftSessionManager
import org.apache.livy.thriftserver.serde.ThriftResultSet
import org.apache.livy.thriftserver.session.{CleanupCatalogResultJob, FetchCatalogResultJob}

/**
 * SparkCatalogOperation is the base class for operations which need to fetch catalog information
 * from spark session.
 */
abstract class SparkCatalogOperation(
    sessionHandle: SessionHandle,
    opType: OperationType,
    sessionManager: LivyThriftSessionManager)
  extends Operation(sessionHandle, opType) {

  // The initialization need to be lazy in order not to block when the instance is created
  protected lazy val rscClient = {
    // This call is blocking, we are waiting for the session to be ready.
    sessionManager.getLivySession(sessionHandle).client.get
  }

  protected lazy val jobId = {
    this.opHandle.getHandleIdentifier.getPublicId.toString + "-" +
      this.opHandle.getHandleIdentifier.getSecretId.toString
  }

  protected lazy val sessionId = {
    sessionHandle.getSessionId.toString
  }

  @throws[HiveSQLException]
  override def close(): Unit = {
    val cleaned = rscClient.submit(new CleanupCatalogResultJob(sessionId, jobId)).get()
    if (!cleaned) {
      warn(s"Fail to cleanup fetch catalog job (session = ${sessionId}), " +
        "this message can be ignored if the job failed.")
    }
    setState(OperationState.CLOSED)
  }

  @throws[HiveSQLException]
  override def cancel(stateAfterCancel: OperationState): Unit = {
    setState(OperationState.CANCELED)
    // Spark fetch schema is not a really spark job. It only run on driver and cannot be cancelled
    throw new UnsupportedOperationException("SparkCatalogOperation.cancel()")
  }

  /**
   * Convert wildchars and escape sequence from JDBC format to datanucleous/regex
   *
   * This is ported from Spark Hive Thrift MetaOperation.
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
   * The schema pattern treats empty string also as wildchar.
   *
   * This is ported from Spark Hive Thrift MetaOperation.
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

  override def getNextRowSet(orientation: FetchOrientation, maxRowsL: Long): ThriftResultSet = {
    validateFetchOrientation(orientation)
    assertState(Seq(OperationState.FINISHED))
    setHasResultSet(true)
    val maxRows = maxRowsL.toInt
    val results = rscClient.submit(new FetchCatalogResultJob(sessionId, jobId, maxRows)).get()

    val rowSet = ThriftResultSet.apply(getResultSetSchema, protocolVersion)
    import scala.collection.JavaConverters._
    results.asScala.foreach { r => rowSet.addRow(r.asInstanceOf[Array[Any]]) }
    return rowSet
  }
}
