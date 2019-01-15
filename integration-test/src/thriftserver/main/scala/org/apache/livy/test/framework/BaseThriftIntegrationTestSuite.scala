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

package org.apache.livy.test.framework

import java.sql.{Connection, DriverManager, ResultSet}

class BaseThriftIntegrationTestSuite extends BaseIntegrationTestSuite {
  private var jdbcUri: String = _

  override def beforeAll(): Unit = {
    cluster = Cluster.get()
    // The JDBC endpoint must contain a valid value
    assert(cluster.jdbcEndpoint.isDefined)
    jdbcUri = cluster.jdbcEndpoint.get
  }

  def checkQuery(connection: Connection, query: String)(validate: ResultSet => Unit): Unit = {
    val ps = connection.prepareStatement(query)
    try {
      val rs = ps.executeQuery()
      try {
        validate(rs)
      } finally {
        rs.close()
      }
    } finally {
      ps.close()
    }
  }

  def withConnection[T](f: Connection => T): T = {
    val connection = DriverManager.getConnection(jdbcUri)
    try {
      f(connection)
    } finally {
      connection.close()
    }
  }
}
