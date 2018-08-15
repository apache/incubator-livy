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

import java.sql.{Date, Statement}

import org.apache.livy.LivyConf


trait CommonThriftTests {
  def dataTypesTest(statement: Statement, mapSupported: Boolean): Unit = {
    val resultSet = statement.executeQuery(
      "select 1, 'a', cast(null as int), 1.2345, CAST('2018-08-06' as date)")
    resultSet.next()
    assert(resultSet.getInt(1) == 1)
    assert(resultSet.getString(2) == "a")
    assert(resultSet.getInt(3) == 0)
    assert(resultSet.wasNull())
    assert(resultSet.getDouble(4) == 1.2345)
    assert(resultSet.getDate(5) == Date.valueOf("2018-08-06"))
    assert(!resultSet.next())

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
}

class BinaryThriftServerSuite extends ThriftServerBaseTest with CommonThriftTests {
  override def mode: ServerMode.Value = ServerMode.binary
  override def port: Int = 10001

  test("Reuse existing session") {
    withJdbcConnection { _ =>
      val sessionManager = LivyThriftServer.getInstance.get.getSessionManager()
      val sessionHandle = sessionManager.getSessions.iterator().next().getSessionHandle
      // Blocks until the session is ready
      val session = sessionManager.getLivySession(sessionHandle)
      withJdbcConnection("default", Seq(s"livy.server.sessionId=${session.id}")) { _ =>
        val it = sessionManager.getSessions.iterator()
        // Blocks until all the sessions are ready
        while (it.hasNext) {
          sessionManager.getLivySession(it.next().getSessionHandle)
        }
        assert(LivyThriftServer.getInstance.get.livySessionManager.size() == 1)
      }
    }
  }

  test("fetch different data types") {
    val supportMap = formattedSparkVersion._1 > 1 ||
      livyConf.getBoolean(LivyConf.ENABLE_HIVE_CONTEXT)
    withJdbcStatement { statement =>
      dataTypesTest(statement, supportMap)
    }
  }

  test("support default database in connection URIs") {
    assume(formattedSparkVersion._1 > 1 || livyConf.getBoolean(LivyConf.ENABLE_HIVE_CONTEXT))
    val db = "new_db"
    withJdbcConnection { c =>
      val s1 = c.createStatement()
      s1.execute(s"create database $db")
      s1.close()
      val sessionManager = LivyThriftServer.getInstance.get.getSessionManager()
      val sessionHandle = sessionManager.getSessions.iterator().next().getSessionHandle
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
}

class HttpThriftServerSuite extends ThriftServerBaseTest with CommonThriftTests {
  override def mode: ServerMode.Value = ServerMode.http
  override def port: Int = 20000

  test("fetch different data types") {
    val supportMap = formattedSparkVersion._1 > 1 ||
      livyConf.getBoolean(LivyConf.ENABLE_HIVE_CONTEXT)
    withJdbcStatement { statement =>
      dataTypesTest(statement, supportMap)
    }
  }
}
