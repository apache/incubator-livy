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

package org.apache.livy.test

import java.math.BigDecimal
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Arrays

import org.apache.livy.test.framework.BaseThriftIntegrationTestSuite

class SchemaIT extends BaseThriftIntegrationTestSuite {
  test("basic Schema test") {
    val tinyintVal = 1
    val smallintVal = 2
    val intVal = 3
    val bigintVal = 4
    val floatVal = 5.5F
    val doubleVal = 6.6
    val decimalVal = 7.7
    val booleanVal = true
    val binaryVal = "0101010000101"
    val stringVal = "string_col"
    val varcharVal = "varchar_col"
    val charVal = "char_col"
    val timestampVal = Timestamp.valueOf("2019-10-22 11:49:45")
    val dateVal = "2019-10-22"

    val createTableSql = "create table primary_type (" +
      "tinyint_col tinyint," +
      "smallint_col smallint," +
      "int_col int," +
      "bigint_col bigint," +
      "float_col float," +
      "double_col double," +
      "decimal_col decimal(22, 2)," +
      "boolean_col boolean," +
      "binary_col binary," +
      "string_col string," +
      "varchar_col varchar(10)," +
      "char_col char(10)," +
      "timestamp_col timestamp, " +
      "date_col date) using json"

    val insertSql = "insert into primary_type select " +
      tinyintVal + "," +
      smallintVal + "," +
      intVal + "," +
      bigintVal + "," +
      floatVal + "," +
      doubleVal + "," +
      decimalVal + "," +
      booleanVal + "," +
      "'" + binaryVal + "'," +
      "'" + stringVal + "'," +
      "'" + varcharVal + "'," +
      "'" + charVal + "'," +
      "'" + timestampVal + "'," +
      "'" + dateVal + "'"

    withConnection { c =>
      executeQuery(c, createTableSql)
      executeQuery(c, insertSql)
      checkQuery(
        c, "select * from primary_type") { resultSet =>
        val rsMetaData = resultSet.getMetaData()
        val columnsNum = rsMetaData.getColumnCount()
        assert(columnsNum == 14)

        assert(rsMetaData.getColumnName(1) == "tinyint_col")
        assert(rsMetaData.getColumnTypeName(1) == "tinyint")

        assert(rsMetaData.getColumnName(2) == "smallint_col")
        assert(rsMetaData.getColumnTypeName(2) == "smallint")

        assert(rsMetaData.getColumnName(3) == "int_col")
        assert(rsMetaData.getColumnTypeName(3) == "int")

        assert(rsMetaData.getColumnName(4) == "bigint_col")
        assert(rsMetaData.getColumnTypeName(4) == "bigint")

        assert(rsMetaData.getColumnName(5) == "float_col")
        assert(rsMetaData.getColumnTypeName(5) == "float")

        assert(rsMetaData.getColumnName(6) == "double_col")
        assert(rsMetaData.getColumnTypeName(6) == "double")

        assert(rsMetaData.getColumnName(7) == "decimal_col")
        assert(rsMetaData.getColumnTypeName(7) == "decimal")

        assert(rsMetaData.getColumnName(8) == "boolean_col")
        assert(rsMetaData.getColumnTypeName(8) == "boolean")

        assert(rsMetaData.getColumnName(9) == "binary_col")
        assert(rsMetaData.getColumnTypeName(9) == "binary")

        assert(rsMetaData.getColumnName(10) == "string_col")
        assert(rsMetaData.getColumnTypeName(10) == "string")

        assert(rsMetaData.getColumnName(11) == "varchar_col")
        assert(rsMetaData.getColumnTypeName(11) == "string")

        assert(rsMetaData.getColumnName(12) == "char_col")
        assert(rsMetaData.getColumnTypeName(12) == "string")

        assert(rsMetaData.getColumnName(13) == "timestamp_col")
        assert(rsMetaData.getColumnTypeName(13) == "timestamp")

        assert(rsMetaData.getColumnName(14) == "date_col")
        assert(rsMetaData.getColumnTypeName(14) == "date")

        resultSet.next()
        assert(resultSet.getByte(1) == tinyintVal)
        assert(resultSet.getShort(2) == smallintVal)
        assert(resultSet.getInt(3) == intVal)
        assert(resultSet.getLong(4) == bigintVal)
        assert(resultSet.getFloat(5) == floatVal)
        assert(resultSet.getDouble(6) == doubleVal)
        assert(resultSet.getBigDecimal(7).doubleValue() == decimalVal)
        assert(resultSet.getBoolean(8) == booleanVal)
        // getBytes throw SQLFeatureNotSupportedException: Method not supported,
        // which was threw by HiveBaseResultSet.scala in hive jdbc.
        // So getString instead of getBytes.
        assert(resultSet.getString(9) == binaryVal)
        assert(resultSet.getString(10) == stringVal)
        assert(resultSet.getString(11) == varcharVal)
        assert(resultSet.getString(12) == charVal)
        assert(resultSet.getTimestamp(13).compareTo(timestampVal) == 0)
        assert(resultSet.getDate(14).toString == dateVal)
        assert(!resultSet.next())
      }

      executeQuery(c, "drop table primary_type")
    }
  }
}
