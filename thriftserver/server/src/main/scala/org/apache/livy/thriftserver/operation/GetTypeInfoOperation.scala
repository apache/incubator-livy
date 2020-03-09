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

import java.sql.{DatabaseMetaData, Types}

import org.apache.hive.service.cli.{HiveSQLException, OperationState, OperationType, SessionHandle}

import org.apache.livy.Logging
import org.apache.livy.thriftserver.serde.ThriftResultSet
import org.apache.livy.thriftserver.types.{BasicDataType, Field, Schema}

sealed case class TypeInfo(name: String, sqlType: Int, precision: Option[Int],
  caseSensitive: Boolean, searchable: Short, unsignedAttribute: Boolean, numPrecRadix: Option[Int])

class GetTypeInfoOperation(sessionHandle: SessionHandle)
  extends MetadataOperation(sessionHandle, OperationType.GET_TYPE_INFO) with Logging {

  info("Starting GetTypeInfoOperation")

  protected val rowSet = ThriftResultSet.apply(GetTypeInfoOperation.SCHEMA, protocolVersion)

  @throws(classOf[HiveSQLException])
  override def runInternal(): Unit = {
    setState(OperationState.RUNNING)
    info("Fetching type info metadata")
    try {
      GetTypeInfoOperation.TYPES.foreach { t =>
        val data = Array[Any](
          t.name,
          t.sqlType,
          t.precision.orNull,
          null, // LITERAL_PREFIX
          null, // LITERAL_SUFFIX
          null, // CREATE_PARAMS
          DatabaseMetaData.typeNullable.toShort, // All types are nullable
          t.caseSensitive,
          t.searchable,
          t.unsignedAttribute,
          false, // FIXED_PREC_SCALE
          false, // AUTO_INCREMENT
          null, // LOCAL_TYPE_NAME
          0.toShort, // MINIMUM_SCALE
          0.toShort, // MAXIMUM_SCALE
          null, // SQL_DATA_TYPE
          null, // SQL_DATETIME_SUB
          t.numPrecRadix.orNull)
        rowSet.addRow(data)
      }
      setState(OperationState.FINISHED)
      info("Fetching type info metadata has been successfully finished")
    } catch {
      case e: Throwable =>
        setState(OperationState.ERROR)
        throw new HiveSQLException(e)
    }
  }

  @throws(classOf[HiveSQLException])
  override def getResultSetSchema: Schema = {
    assertState(Seq(OperationState.FINISHED))
    GetTypeInfoOperation.SCHEMA
  }
}

object GetTypeInfoOperation {
  val SCHEMA = Schema(
    Field("TYPE_NAME", BasicDataType("string"), "Type name"),
    Field("DATA_TYPE", BasicDataType("integer"), "SQL data type from java.sql.Types"),
    Field("PRECISION", BasicDataType("integer"), "Maximum precision"),
    Field("LITERAL_PREFIX", BasicDataType("string"),
      "Prefix used to quote a literal (may be null)"),
    Field("LITERAL_SUFFIX", BasicDataType("string"),
      "Suffix used to quote a literal (may be null)"),
    Field("CREATE_PARAMS", BasicDataType("string"),
      "Parameters used in creating the type (may be null)"),
    Field("NULLABLE", BasicDataType("short"), "Can you use NULL for this type"),
    Field("CASE_SENSITIVE", BasicDataType("boolean"), "Is it case sensitive"),
    Field("SEARCHABLE", BasicDataType("short"), "Can you use \"WHERE\" based on this type"),
    Field("UNSIGNED_ATTRIBUTE", BasicDataType("boolean"), "Is it unsigned"),
    Field("FIXED_PREC_SCALE", BasicDataType("boolean"), "Can it be a money value"),
    Field("AUTO_INCREMENT", BasicDataType("boolean"),
      "Can it be used for an auto-increment value"),
    Field("LOCAL_TYPE_NAME", BasicDataType("string"),
      "Localized version of type name (may be null)"),
    Field("MINIMUM_SCALE", BasicDataType("short"), "Minimum scale supported"),
    Field("MAXIMUM_SCALE", BasicDataType("short"), "Maximum scale supported"),
    Field("SQL_DATA_TYPE", BasicDataType("integer"), "Unused"),
    Field("SQL_DATETIME_SUB", BasicDataType("integer"), "Unused"),
    Field("NUM_PREC_RADIX", BasicDataType("integer"), "Usually 2 or 10"))

  import DatabaseMetaData._

  val TYPES = Seq(
    TypeInfo("void", Types.NULL, None, false, typePredNone.toShort, true, None),
    TypeInfo("boolean", Types.BOOLEAN, None, false, typePredBasic.toShort, true, None),
    TypeInfo("byte", Types.TINYINT, Some(3), false, typePredBasic.toShort, false, Some(10)),
    TypeInfo("short", Types.SMALLINT, Some(5), false, typePredBasic.toShort, false, Some(10)),
    TypeInfo("integer", Types.INTEGER, Some(10), false, typePredBasic.toShort, false, Some(10)),
    TypeInfo("long", Types.BIGINT, Some(19), false, typePredBasic.toShort, false, Some(10)),
    TypeInfo("float", Types.FLOAT, Some(7), false, typePredBasic.toShort, false, Some(10)),
    TypeInfo("double", Types.DOUBLE, Some(15), false, typePredBasic.toShort, false, Some(10)),
    TypeInfo("date", Types.DATE, None, false, typePredBasic.toShort, true, None),
    TypeInfo("timestamp", Types.TIMESTAMP, None, false, typePredBasic.toShort, true, None),
    TypeInfo("string", Types.VARCHAR, None, true, typeSearchable.toShort, true, None),
    TypeInfo("binary", Types.BINARY, None, false, typePredBasic.toShort, true, None),
    TypeInfo("decimal", Types.DECIMAL, Some(38), false, typePredBasic.toShort, false, Some(10)),
    TypeInfo("array", Types.ARRAY, None, false, typePredBasic.toShort, true, None),
    TypeInfo("map", Types.OTHER, None, false, typePredNone.toShort, true, None),
    TypeInfo("struct", Types.STRUCT, None, false, typePredBasic.toShort, true, None),
    TypeInfo("udt", Types.OTHER, None, false, typePredNone.toShort, true, None))
}
