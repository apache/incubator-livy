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

import java.sql.Date

import org.apache.livy.test.framework.BaseThriftIntegrationTestSuite

class JdbcIT extends BaseThriftIntegrationTestSuite {
  test("basic JDBC test") {
    withConnection { c =>
      checkQuery(
        c, "select 1, 'a', cast(null as int), 1.2345, CAST('2018-08-06' as date)") { resultSet =>
          resultSet.next()
          assert(resultSet.getInt(1) == 1)
          assert(resultSet.getString(2) == "a")
          assert(resultSet.getInt(3) == 0)
          assert(resultSet.wasNull())
          assert(resultSet.getDouble(4) == 1.2345)
          assert(resultSet.getDate(5) == Date.valueOf("2018-08-06"))
          assert(!resultSet.next())
      }

      checkQuery(
        c, "select cast(null as string), cast(null as decimal), cast(null as double), " +
        "cast(null as date), null") { resultSetWithNulls =>
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
      }

      checkQuery(
        c, "select array(1.5, 2.4, 1.3), struct('a', 1, 1.5), map(1, 'a', 2, 'b')") { resultSet =>
          resultSet.next()
          assert(resultSet.getString(1) == "[1.5,2.4,1.3]")
          assert(resultSet.getString(2) == "{\"col1\":\"a\",\"col2\":1,\"col3\":1.5}")
          assert(resultSet.getString(3) == "{1:\"a\",2:\"b\"}")
          assert(!resultSet.next())
      }
    }
  }
}
