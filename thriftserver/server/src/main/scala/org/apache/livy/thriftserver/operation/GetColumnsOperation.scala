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

import org.apache.hive.service.cli.{HiveSQLException, OperationState, OperationType, SessionHandle}

import org.apache.livy.Logging
import org.apache.livy.thriftserver.types.{BasicDataType, Field, Schema}
import org.apache.livy.thriftserver.LivyThriftSessionManager
import org.apache.livy.thriftserver.session.{GetColumnsJob, GetFunctionsJob}

class GetColumnsOperation(
    sessionHandle: SessionHandle,
    schemaPattern: String,
    tablePattern: String,
    columnPattern: String,
    sessionManager: LivyThriftSessionManager)
  extends SparkCatalogOperation(
    sessionHandle, OperationType.GET_COLUMNS, sessionManager) with Logging {

  @throws(classOf[HiveSQLException])
  override protected def runInternal(): Unit = {
    setState(OperationState.RUNNING)
    try {
      rscClient.submit(new GetColumnsJob(
        schemaPattern,
        tablePattern,
        columnPattern,
        sessionId,
        jobId,
        GetColumnsOperation.SCHEMA.fields.map(_.fieldType.dataType))).get()

      setState(OperationState.FINISHED)
    } catch {
      case e: Throwable =>
        error("Remote job meet an exception: ", e)
        setState(OperationState.ERROR)
        throw new HiveSQLException(e)
    }
  }

  @throws(classOf[HiveSQLException])
  override def getResultSetSchema: Schema = {
    assertState(Seq(OperationState.FINISHED))
    GetColumnsOperation.SCHEMA
  }
}

object GetColumnsOperation {
  val SCHEMA = Schema(
    Field("TABLE_CAT", BasicDataType("string"), "Catalog name. NULL if not applicable."),
    Field("TABLE_SCHEM", BasicDataType("string"), "Schema name."),
    Field("TABLE_NAME", BasicDataType("string"), "Table name."),
    Field("COLUMN_NAME", BasicDataType("string"), "Column name"),
    Field("DATA_TYPE", BasicDataType("integer"), "SQL type from java.sql.Types"),
    Field("TYPE_NAME", BasicDataType("string"),
      "Data source dependent type name, for a UDT the type name is fully qualified"),
    Field("COLUMN_SIZE", BasicDataType("integer"), "Column size. For char or date types this is " +
      "the maximum number of characters, for numeric or decimal types this is precision."),
    Field("BUFFER_LENGTH", BasicDataType("byte"), "Unused"),
    Field("DECIMAL_DIGITS", BasicDataType("integer"), "The number of fractional digits"),
    Field("NUM_PREC_RADIX", BasicDataType("integer"), "Radix (typically either 10 or 2)"),
    Field("NULLABLE", BasicDataType("integer"), "Is NULL allowed"),
    Field("REMARKS", BasicDataType("string"), "Comment describing column (may be null)"),
    Field("COLUMN_DEF", BasicDataType("string"), "Default value (may be null)"),
    Field("SQL_DATA_TYPE", BasicDataType("integer"), "Unused"),
    Field("SQL_DATETIME_SUB", BasicDataType("integer"), "Unused"),
    Field("CHAR_OCTET_LENGTH", BasicDataType("integer"), "For char types the maximum number of " +
      "bytes in the column"),
    Field("ORDINAL_POSITION", BasicDataType("integer"), "Index of column in table (starting at 1)"),
    Field("IS_NULLABLE", BasicDataType("string"), "\"NO\" means column definitely does not " +
      "allow NULL values; \"YES\" means the column might allow NULL values. An empty string " +
      "means nobody knows."),
    Field("SCOPE_CATALOG", BasicDataType("string"), "Catalog of table that is the scope of a " +
      "reference attribute (null if DATA_TYPE isn't REF)"),
    Field("SCOPE_SCHEMA", BasicDataType("string"), "Schema of table that is the scope of a " +
      "reference attribute (null if the DATA_TYPE isn't REF)"),
    Field("SCOPE_TABLE", BasicDataType("string"), "Table name that this the scope of a " +
      "reference attribure (null if the DATA_TYPE isn't REF)"),
    Field("SOURCE_DATA_TYPE", BasicDataType("short"), "Source type of a distinct type or " +
      "user-generated Ref type, SQL type from java.sql.Types (null if DATA_TYPE isn't " +
      "DISTINCT or user-generated REF)"),
    Field("IS_AUTO_INCREMENT", BasicDataType("string"), "Indicates whether this column is " +
      "auto incremented."))
}
