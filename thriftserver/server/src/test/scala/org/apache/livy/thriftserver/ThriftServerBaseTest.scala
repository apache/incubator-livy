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

import java.sql.{Connection, DriverManager, Statement}

import org.apache.hive.jdbc.HiveDriver
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.livy.LivyConf
import org.apache.livy.LivyConf.{LIVY_SPARK_SCALA_VERSION, LIVY_SPARK_VERSION}
import org.apache.livy.server.AccessManager
import org.apache.livy.server.recovery.{SessionStore, StateStore}
import org.apache.livy.sessions.InteractiveSessionManager
import org.apache.livy.utils.LivySparkUtils.{formatSparkVersion, sparkScalaVersion, sparkSubmitVersion}

object ServerMode extends Enumeration {
  val binary, http = Value
}

abstract class ThriftServerBaseTest extends FunSuite with BeforeAndAfterAll {
  def mode: ServerMode.Value
  def port: Int

  val THRIFT_SERVER_STARTUP_TIMEOUT = 30000 // ms

  val livyConf = new LivyConf()
  val (sparkVersion, scalaVersionFromSparkSubmit) = sparkSubmitVersion(livyConf)
  val formattedSparkVersion: (Int, Int) = {
    formatSparkVersion(sparkVersion)
  }

  def jdbcUri(defaultDb: String, sessionConf: String*): String = if (mode == ServerMode.http) {
    s"jdbc:hive2://localhost:$port/$defaultDb?hive.server2.transport.mode=http;" +
      s"hive.server2.thrift.http.path=cliservice;${sessionConf.mkString(";")}"
  } else {
    s"jdbc:hive2://localhost:$port/$defaultDb?${sessionConf.mkString(";")}"
  }

  override def beforeAll(): Unit = {
    Class.forName(classOf[HiveDriver].getCanonicalName)
    livyConf.set(LivyConf.THRIFT_TRANSPORT_MODE, mode.toString)
    livyConf.set(LivyConf.THRIFT_SERVER_PORT, port)

    // Set formatted Spark and Scala version into livy configuration, this will be used by
    // session creation.
    livyConf.set(LIVY_SPARK_VERSION.key, formattedSparkVersion.productIterator.mkString("."))
    livyConf.set(LIVY_SPARK_SCALA_VERSION.key,
      sparkScalaVersion(formattedSparkVersion, scalaVersionFromSparkSubmit, livyConf))
    StateStore.init(livyConf)

    val ss = new SessionStore(livyConf)
    val sessionManager = new InteractiveSessionManager(livyConf, ss)
    val accessManager = new AccessManager(livyConf)
    LivyThriftServer.start(livyConf, sessionManager, ss, accessManager)
    LivyThriftServer.thriftServerThread.join(THRIFT_SERVER_STARTUP_TIMEOUT)
    assert(LivyThriftServer.getInstance.isDefined)
    assert(LivyThriftServer.getInstance.get.getServiceState == STATE.STARTED)
  }

  override def afterAll(): Unit = {
    LivyThriftServer.stopServer()
  }

  def withJdbcConnection(f: (Connection => Unit)): Unit = {
    withJdbcConnection("default", Seq.empty)(f)
  }

  def withJdbcConnection(db: String, sessionConf: Seq[String])(f: (Connection => Unit)): Unit = {
    withJdbcConnection(jdbcUri(db, sessionConf: _*))(f)
  }

  def withJdbcConnection(uri: String)(f: (Connection => Unit)): Unit = {
    val user = System.getProperty("user.name")
    val connection = DriverManager.getConnection(uri, user, "")
    try {
      f(connection)
    } finally {
      connection.close()
    }
  }

  def withJdbcStatement(f: (Statement => Unit)): Unit = {
    withJdbcConnection { connection =>
      val s = connection.createStatement()
      try {
        f(s)
      } finally {
        s.close()
      }
    }
  }
}
