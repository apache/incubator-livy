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

import java.net.URL
import java.sql.{Connection, Date, SQLException, Statement, Types}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

import org.apache.hive.jdbc.HiveStatement

import org.apache.livy.LivyConf


trait CommonThriftTests {
  def hiveSupportEnabled(sparkMajorVersion: Int, livyConf: LivyConf): Boolean = {
    sparkMajorVersion > 1 || livyConf.getBoolean(LivyConf.ENABLE_HIVE_CONTEXT)
  }

  def dataTypesTest(statement: Statement, mapSupported: Boolean): Unit = {
    val resultSet = statement.executeQuery(
      "select 1, 'a', cast(null as int), 1.2345, CAST('2018-08-06' as date), " +
        "CAST('123' as BINARY)")
    resultSet.next()
    assert(resultSet.getInt(1) == 1)
    assert(resultSet.getString(2) == "a")
    assert(resultSet.getInt(3) == 0)
    assert(resultSet.wasNull())
    assert(resultSet.getDouble(4) == 1.2345)
    assert(resultSet.getDate(5) == Date.valueOf("2018-08-06"))
    val resultBytes = Source.fromInputStream(resultSet.getBinaryStream(6))
      .map(_.toByte).toArray
    assert("123".getBytes.sameElements(resultBytes))
    assert(!resultSet.next())

    val resultSetWithNulls = statement.executeQuery("select cast(null as string), " +
      "cast(null as decimal), cast(null as double), cast(null as date), null")
    resultSetWithNulls.next()
    assert(resultSetWithNulls.getString(1) == null)
    assert(resultSetWithNulls.wasNull())
    assert(resultSetWithNulls.getBigDecimal(2) == null)
    assert(resultSetWithNulls.wasNull())
    assert(resultSetWithNulls.getDouble(3) == 0.0)
    assert(resultSetWithNulls.wasNull())
    assert(resultSetWithNulls.getDate(4) == null)
    assert(resultSetWithNulls.wasNull())
    assert(resultSetWithNulls.getString(5) == null)
    assert(resultSetWithNulls.wasNull())
    assert(!resultSetWithNulls.next())

    val complexTypesQuery = if (mapSupported) {
      "select array(1.5, 2.4, 1.3), struct('a', 1, 1.5), map(1, 'a', 2, 'b')"
    } else {
      "select array(1.5, 2.4, 1.3), struct('a', 1, 1.5)"
    }

    val resultSetComplex = statement.executeQuery(complexTypesQuery)
    resultSetComplex.next()
    assert(resultSetComplex.getString(1) == "[1.5,2.4,1.3]")
    assert(resultSetComplex.getString(2) == "{\"col1\":\"a\",\"col2\":1,\"col3\":1.5}")
    if (mapSupported) {
      assert(resultSetComplex.getString(3) == "{1:\"a\",2:\"b\"}")
    }
    assert(!resultSetComplex.next())
  }

  def getSchemasTest(connection: Connection): Unit = {
    val metadata = connection.getMetaData
    val schemaResultSet = metadata.getSchemas()
    assert(schemaResultSet.getMetaData.getColumnCount == 2)
    assert(schemaResultSet.getMetaData.getColumnName(1) == "TABLE_SCHEM")
    assert(schemaResultSet.getMetaData.getColumnName(2) == "TABLE_CATALOG")
    schemaResultSet.next()
    assert(schemaResultSet.getString(1) == "default")
    assert(!schemaResultSet.next())
  }

  def getFunctionsTest(connection: Connection): Unit = {
    val metadata = connection.getMetaData

    val functionResultSet = metadata.getFunctions("", "default", "unix_timestamp")
    assert(functionResultSet.getMetaData.getColumnCount == 6)
    assert(functionResultSet.getMetaData.getColumnName(1) == "FUNCTION_CAT")
    assert(functionResultSet.getMetaData.getColumnName(2) == "FUNCTION_SCHEM")
    assert(functionResultSet.getMetaData.getColumnName(3) == "FUNCTION_NAME")
    assert(functionResultSet.getMetaData.getColumnName(4) == "REMARKS")
    assert(functionResultSet.getMetaData.getColumnName(5) == "FUNCTION_TYPE")
    assert(functionResultSet.getMetaData.getColumnName(6) == "SPECIFIC_NAME")
    functionResultSet.next()
    assert(functionResultSet.getString(3) == "unix_timestamp")
    assert(functionResultSet.getString(6) ==
      "org.apache.spark.sql.catalyst.expressions.UnixTimestamp")
    assert(!functionResultSet.next())
  }

  def getTablesTest(connection: Connection): Unit = {
    val statement = connection.createStatement()
    try {
      statement.execute("CREATE TABLE test_get_tables (id integer, desc string) USING json")
      val metadata = connection.getMetaData
      val tablesResultSet = metadata.getTables("", "default", "*", Array("TABLE"))
      assert(tablesResultSet.getMetaData.getColumnCount == 5)
      assert(tablesResultSet.getMetaData.getColumnName(1) == "TABLE_CAT")
      assert(tablesResultSet.getMetaData.getColumnName(2) == "TABLE_SCHEM")
      assert(tablesResultSet.getMetaData.getColumnName(3) == "TABLE_NAME")
      assert(tablesResultSet.getMetaData.getColumnName(4) == "TABLE_TYPE")
      assert(tablesResultSet.getMetaData.getColumnName(5) == "REMARKS")

      tablesResultSet.next()
      assert(tablesResultSet.getString(3) == "test_get_tables")
      assert(tablesResultSet.getString(4) == "TABLE")
      assert(!tablesResultSet.next())
    } finally {
      statement.execute("DROP TABLE IF EXISTS test_get_tables")
      statement.close()
    }

  }

  def getColumnsTest(connection: Connection): Unit = {
    val metadata = connection.getMetaData
    val statement = connection.createStatement()
    try {
      statement.execute("CREATE TABLE test_get_columns (id integer, desc string) USING json")

      val columnsResultSet = metadata.getColumns("", "default", "test_get_columns", ".*")
      assert(columnsResultSet.getMetaData.getColumnCount == 23)
      columnsResultSet.next()
      assert(columnsResultSet.getString(1) == "")
      assert(columnsResultSet.getString(2) == "default")
      assert(columnsResultSet.getString(3) == "test_get_columns")
      assert(columnsResultSet.getString(4) == "id")
      assert(columnsResultSet.getInt(5) == 4)
      assert(columnsResultSet.getString(6) == "integer")
      assert(columnsResultSet.getInt(7) == 10)
      assert(columnsResultSet.getString(8) == null)
      assert(columnsResultSet.getInt(9) == 0)
      assert(columnsResultSet.getInt(10) == 10)
      assert(columnsResultSet.getInt(11) == 1)
      assert(columnsResultSet.getString(12) == "")
      assert(columnsResultSet.getString(13) == null)
      assert(columnsResultSet.getString(14) == null)
      assert(columnsResultSet.getString(15) == null)
      assert(columnsResultSet.getString(15) == null)
      assert(columnsResultSet.getInt(17) == 0)
      assert(columnsResultSet.getString(18) == "YES")
      assert(columnsResultSet.getString(19) == null)
      assert(columnsResultSet.getString(20) == null)
      assert(columnsResultSet.getString(21) == null)
      assert(columnsResultSet.getString(22) == null)
      assert(columnsResultSet.getString(23) == "NO")
      columnsResultSet.next()
      assert(columnsResultSet.getString(1) == "")
      assert(columnsResultSet.getString(2) == "default")
      assert(columnsResultSet.getString(3) == "test_get_columns")
      assert(columnsResultSet.getString(4) == "desc")
      assert(columnsResultSet.getInt(5) == 12)
      assert(columnsResultSet.getString(6) == "string")
      assert(columnsResultSet.getInt(7) == Integer.MAX_VALUE)
      assert(columnsResultSet.getString(8) == null)
      assert(columnsResultSet.getString(9) == null)
      assert(columnsResultSet.getString(10) == null)
      assert(columnsResultSet.getInt(11) == 1)
      assert(columnsResultSet.getString(12) == "")
      assert(columnsResultSet.getString(13) == null)
      assert(columnsResultSet.getString(14) == null)
      assert(columnsResultSet.getString(15) == null)
      assert(columnsResultSet.getString(16) == null)
      assert(columnsResultSet.getInt(17) == 1)
      assert(columnsResultSet.getString(18) == "YES")
      assert(columnsResultSet.getString(19) == null)
      assert(columnsResultSet.getString(20) == null)
      assert(columnsResultSet.getString(21) == null)
      assert(columnsResultSet.getString(22) == null)
      assert(columnsResultSet.getString(23) == "NO")
      assert(!columnsResultSet.next())
    } finally {
      statement.execute("DROP TABLE IF EXISTS test_get_columns")
      statement.close()
    }

  }

  def operationLogRetrievalTest(statement: Statement): Unit = {
    statement.execute("select 1")
    val logIterator = statement.asInstanceOf[HiveStatement].getQueryLog().iterator()
    statement.close()

    // Only execute statement support operation log retrieval and it only produce one log
    assert(logIterator.next() ==
      "Livy session has not yet started. Please wait for it to be ready...")
    assert(!logIterator.hasNext)
  }

  def getCatalogTest(connection: Connection): Unit = {
    val metadata = connection.getMetaData
    val catalogResultSet = metadata.getCatalogs()
    // Spark doesn't support getCatalog. In current implementation, it's a no-op and does not return
    // any data
    assert(!catalogResultSet.next())
  }

  def getTableTypeTest(connection: Connection): Unit = {
    val metadata = connection.getMetaData
    val tableTypesResultSet = metadata.getTableTypes()
    tableTypesResultSet.next()
    assert(tableTypesResultSet.getString(1) == "TABLE")
    tableTypesResultSet.next()
    assert(tableTypesResultSet.getString(1) == "VIEW")
    assert(!tableTypesResultSet.next())
  }

  def getTypeInfoTest(connection: Connection): Unit = {
    val metadata = connection.getMetaData
    val typeInfoResultSet = metadata.getTypeInfo()
    val expectResults = Array(
      ("void", Types.NULL, 0, 1, false, 0, true, false, false, null, 0, 0, 0, 0, 0),
      ("boolean", Types.BOOLEAN, 0, 1, false, 2, true, false, false, null, 0, 0, 0, 0, 0),
      ("byte", Types.TINYINT, 3, 1, false, 2, false, false, false, null, 0, 0, 0, 0, 10),
      ("short", Types.SMALLINT, 5, 1, false, 2, false, false, false, null, 0, 0, 0, 0, 10),
      ("integer", Types.INTEGER, 10, 1, false, 2, false, false, false, null, 0, 0, 0, 0, 10),
      ("long", Types.BIGINT, 19, 1, false, 2, false, false, false, null, 0, 0, 0, 0, 10),
      ("float", Types.FLOAT, 7, 1, false, 2, false, false, false, null, 0, 0, 0, 0, 10),
      ("double", Types.DOUBLE, 15, 1, false, 2, false, false, false, null, 0, 0, 0, 0, 10),
      ("date", Types.DATE, 0, 1, false, 2, true, false, false, null, 0, 0, 0, 0, 0),
      ("timestamp", Types.TIMESTAMP, 0, 1, false, 2, true, false, false, null, 0, 0, 0, 0, 0),
      ("string", Types.VARCHAR, 0, 1, true, 3, true, false, false, null, 0, 0, 0, 0, 0),
      ("binary", Types.BINARY, 0, 1, false, 2, true, false, false, null, 0, 0, 0, 0, 0),
      ("decimal", Types.DECIMAL, 38, 1, false, 2, false, false, false, null, 0, 0, 0, 0, 10),
      ("array", Types.ARRAY, 0, 1, false, 2, true, false, false, null, 0, 0, 0, 0, 0),
      ("map", Types.OTHER, 0, 1, false, 0, true, false, false, null, 0, 0, 0, 0, 0),
      ("struct", Types.STRUCT, 0, 1, false, 2, true, false, false, null, 0, 0, 0, 0, 0),
      ("udt", Types.OTHER, 0, 1, false, 0, true, false, false, null, 0, 0, 0, 0, 0)
    )
    for (expect <- expectResults) {
      typeInfoResultSet.next()
      assert(typeInfoResultSet.getString(1) == expect._1)
      assert(typeInfoResultSet.getInt(2) == expect._2)
      assert(typeInfoResultSet.getInt(3) == expect._3)
      assert(typeInfoResultSet.getString(4) == null)
      assert(typeInfoResultSet.getString(5) == null)
      assert(typeInfoResultSet.getString(6) == null)
      assert(typeInfoResultSet.getShort(7) == expect._4)
      assert(typeInfoResultSet.getBoolean(8) == expect._5)
      assert(typeInfoResultSet.getShort(9) == expect._6)
      assert(typeInfoResultSet.getBoolean(10) == expect._7)
      assert(typeInfoResultSet.getBoolean(11) == expect._8)
      assert(typeInfoResultSet.getBoolean(12) == expect._9)
      assert(typeInfoResultSet.getString(13) == expect._10)
      assert(typeInfoResultSet.getShort(14) == expect._11)
      assert(typeInfoResultSet.getShort(15) == expect._12)
      assert(typeInfoResultSet.getInt(16) == expect._13)
      assert(typeInfoResultSet.getInt(17) == expect._14)
      assert(typeInfoResultSet.getInt(18) == expect._15)
    }
    assert(!typeInfoResultSet.next())
  }
}

class BinaryThriftServerSuite extends ThriftServerBaseTest with CommonThriftTests {
  override def mode: ServerMode.Value = ServerMode.binary
  override def port: Int = 20000

  test("test multiple session") {
    var defaultV1: String = null
    var defaultV2: String = null
    var data: ArrayBuffer[Int] = null

    // first session, we get the default value of the session status
    withJdbcStatement { statement =>
      val rs1 = statement.executeQuery("SET spark.sql.shuffle.partitions")
      rs1.next()
      assert("spark.sql.shuffle.partitions" === rs1.getString(1))
      defaultV1 = rs1.getString(2)
      rs1.close()

      val rs2 = statement.executeQuery("SET hive.cli.print.header")
      rs2.next()

      assert("hive.cli.print.header" === rs2.getString(1))
      defaultV2 = rs2.getString(2)
      rs2.close()
    }

    // second session, we update the session status
    withJdbcStatement { statement =>
      val queries = Seq(
        "SET spark.sql.shuffle.partitions=291",
        "SET hive.cli.print.header=true"
      )

      queries.map(statement.execute)
      val rs1 = statement.executeQuery("SET spark.sql.shuffle.partitions")
      rs1.next()
      assert("spark.sql.shuffle.partitions" === rs1.getString(1))
      assert("291" === rs1.getString(2))
      rs1.close()

      val rs2 = statement.executeQuery("SET hive.cli.print.header")
      rs2.next()
      assert("hive.cli.print.header" === rs2.getString(1))
      assert("true" === rs2.getString(2))
      rs2.close()
    }

    // third session, we get the latest session status, supposed to be the
    // default value
    withJdbcStatement { statement =>
      val rs1 = statement.executeQuery("SET spark.sql.shuffle.partitions")
      rs1.next()
      assert("spark.sql.shuffle.partitions" === rs1.getString(1))
      assert(defaultV1 === rs1.getString(2))
      rs1.close()

      val rs2 = statement.executeQuery("SET hive.cli.print.header")
      rs2.next()
      assert("hive.cli.print.header" === rs2.getString(1))
      assert(defaultV2 === rs2.getString(2))
      rs2.close()
    }

  }

  test("Reuse existing session") {
    withJdbcConnection { _ =>
      val sessionManager = LivyThriftServer.getInstance.get.getSessionManager
      val sessionHandle = sessionManager.getSessions.head
      // Blocks until the session is ready
      val session = sessionManager.getLivySession(sessionHandle)
      withJdbcConnection("default", Seq(s"livy.server.sessionId=${session.id}")) { _ =>
        val it = sessionManager.getSessions.iterator
        // Blocks until all the sessions are ready
        while (it.hasNext) {
          sessionManager.getLivySession(it.next())
        }
        assert(LivyThriftServer.getInstance.get.livySessionManager.size() == 1)
      }
    }
  }

  test("fetch different data types") {
    val supportMap = hiveSupportEnabled(formattedSparkVersion._1, livyConf)
    withJdbcStatement { statement =>
      dataTypesTest(statement, supportMap)
    }
  }

  test("support default database in connection URIs") {
    assume(hiveSupportEnabled(formattedSparkVersion._1, livyConf))
    val db = "new_db"
    withJdbcConnection { c =>
      val s1 = c.createStatement()
      s1.execute(s"create database $db")
      s1.close()
      val sessionManager = LivyThriftServer.getInstance.get.getSessionManager
      val sessionHandle = sessionManager.getSessions.head
      // Blocks until the session is ready
      val session = sessionManager.getLivySession(sessionHandle)
      withJdbcConnection(db, Seq(s"livy.server.sessionId=${session.id}")) { c =>
        val statement = c.createStatement()
        val resultSet = statement.executeQuery("select current_database()")
        resultSet.next()
        assert(resultSet.getString(1) === db)
        statement.close()
      }
      val s2 = c.createStatement()
      s2.execute(s"drop database $db")
      s2.close()
    }
  }

  test("support hivevar") {
    assume(hiveSupportEnabled(formattedSparkVersion._1, livyConf))
    withJdbcConnection(jdbcUri("default") + "#myVar1=val1;myVar2=val2") { c =>
      val statement = c.createStatement()
      val selectRes = statement.executeQuery("select \"${myVar1}\", \"${myVar2}\"")
      selectRes.next()
      assert(selectRes.getString(1) === "val1")
      assert(selectRes.getString(2) === "val2")
      statement.close()
    }
  }

  test("LIVY-571: returns a meaningful exception when database doesn't exist") {
    assume(hiveSupportEnabled(formattedSparkVersion._1, livyConf))
    withJdbcConnection(jdbcUri("default")) { c =>
      val caught = intercept[SQLException] {
        val statement = c.createStatement()
        try {
          statement.executeQuery("use invalid_database")
        } finally {
          statement.close()
        }
      }
      assert(caught.getMessage.contains("Database 'invalid_database' not found"))
    }
  }

  test("LIVY-571: returns a meaningful exception when global_temp table doesn't exist") {
    withJdbcConnection { c =>
      val caught = intercept[SQLException] {
        val statement = c.createStatement()
        try {
          statement.executeQuery("select * from global_temp.invalid_table")
        } finally {
          statement.close()
        }
      }
      assert(caught.getMessage.contains("Table or view not found: `global_temp`.`invalid_table`"))
    }
  }

  test("fetch schemas") {
    withJdbcConnection { connection =>
      getSchemasTest(connection)
    }
  }

  test("fetch functions") {
    withJdbcConnection { connection =>
      getFunctionsTest(connection)
    }
  }

  test("fetch tables") {
    withJdbcConnection { connection =>
      getTablesTest(connection)
    }
  }

  test("fetch column") {
    withJdbcConnection { connection =>
      getColumnsTest(connection)
    }
  }

  test("operation log retrieval test") {
    withJdbcStatement { statement =>
      operationLogRetrievalTest(statement)
    }
  }

  test("fetch catalog test") {
    withJdbcConnection { c =>
      getCatalogTest(c)
    }
  }

  test("get table types test") {
    withJdbcConnection { c =>
      getTableTypeTest(c)
    }
  }

  test("get types info test") {
    withJdbcConnection { c =>
      getTypeInfoTest(c)
    }
  }
}

class HttpThriftServerSuite extends ThriftServerBaseTest with CommonThriftTests {
  override def mode: ServerMode.Value = ServerMode.http
  override def port: Int = 20001

  test("fetch different data types") {
    val supportMap = hiveSupportEnabled(formattedSparkVersion._1, livyConf)
    withJdbcStatement { statement =>
      dataTypesTest(statement, supportMap)
    }
  }

  test("fetch schemas") {
    withJdbcConnection { connection =>
      getSchemasTest(connection)
    }
  }

  test("fetch functions") {
    withJdbcConnection { connection =>
      getFunctionsTest(connection)
    }
  }

  test("fetch tables") {
    withJdbcConnection { connection =>
      getTablesTest(connection)
    }
  }

  test("fetch column") {
    withJdbcConnection { connection =>
      getColumnsTest(connection)
    }
  }

  test("operation log retrieval test") {
    withJdbcStatement { statement =>
      operationLogRetrievalTest(statement)
    }
  }

  test("fetch catalog test") {
    withJdbcConnection { c =>
      getCatalogTest(c)
    }
  }

  test("get table types test") {
    withJdbcConnection { c =>
      getTableTypeTest(c)
    }
  }

  test("get types info test") {
    withJdbcConnection { c =>
      getTypeInfoTest(c)
    }
  }
}
