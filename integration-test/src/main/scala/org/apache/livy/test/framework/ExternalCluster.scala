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

import java.io._

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path

import org.apache.livy.{LivyConf, Logging}
import org.apache.livy.client.common.TestUtils
import org.apache.livy.server.LivyServer

/**
 * Used to run tests on an real external cluster
 * In order to utilize test against an external cluster, you need to create
 * a configuration called cluster.spec and run the test suite with the option
 * -Dcluster.spec=<yourcluster.spec>
 *
 * Also, make sure to disable the following line InteractiveIT.scala
 * s.run("""sc.getConf.get("spark.executor.instances")""").verifyResult("res1: String = 1\n")
 *
 * This is because your external cluster may not have the same configuration as the MiniCluster
 *
 * See the cluster.spec.template file for an example cluster.spec
 */
class ExternalCluster(config: Map[String, String]) extends Cluster with Logging {
  private var _configDir: File = _

  private var _livyEndpoint: String = _
  private var _livyThriftJdbcUrl: Option[String] = _
  private var _hdfsScrathDir: Path = _

  private var _authScheme: String = _
  private var _user: String = _
  private var _password: String = _
  private var _sslCertPath: String = _

  private var _principal: String = _
  private var _keytabPath: String = _

  // Livy rest url endpoint
  override def livyEndpoint: String = _livyEndpoint

  // Livy jdbc url endpoint
  override def jdbcEndpoint: Option[String] = _livyThriftJdbcUrl

  // Temp directory in hdfs
  override def hdfsScratchDir(): Path = _hdfsScrathDir

  // Working directory that store core-site.xml, yarn-site.xml
  override def configDir(): File = _configDir

  // Security details
  override def authScheme: String = _authScheme
  override def user: String = _user
  override def password: String = _password
  override def sslCertPath: String = _sslCertPath

  override def principal: String = _principal
  override def keytabPath: String = _keytabPath

  override def doAsClusterUser[T](task: => T): T = task

  override def deploy(): Unit = {
    _configDir = new File(config.getOrElse("configDir", "hadoop-conf"))
    _livyEndpoint = config.getOrElse("livyEndpoint", "")

    _authScheme = config.getOrElse("authScheme", "")
    _user = config.getOrElse("user", "")
    _password = config.getOrElse("password", "")
    _sslCertPath = config.getOrElse("sslCertPath", "")
    _principal = config.getOrElse("principal", "")
    _keytabPath = config.getOrElse("keytabPath", "")

    // Needs to be set after all the other fields are filled in properly
    _hdfsScrathDir = fs.makeQualified(new Path(config.getOrElse("hdfsScratchDir", "/")))
  }

  override def cleanUp(): Unit = {
  }

  def runLivy(): Unit = {
  }

  def stopLivy(): Unit = {
  }
}
