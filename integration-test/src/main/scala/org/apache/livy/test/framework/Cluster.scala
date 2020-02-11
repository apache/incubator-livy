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
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Properties

import scala.collection.JavaConverters._
import scala.util.Try

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration

import org.apache.livy.Logging

/**
 * An common interface to run test on real cluster and mini cluster.
 */
trait Cluster {
  def deploy(): Unit
  def cleanUp(): Unit
  def configDir(): File

  def runLivy(): Unit
  def stopLivy(): Unit
  def livyEndpoint: String
  def jdbcEndpoint: Option[String]
  def hdfsScratchDir(): Path

  // The potential values for authScheme are kerberos for kerberos auth,
  // basic for basic auth, or nothing for no authentication
  def authScheme: String
  def user: String
  def password: String
  def sslCertPath: String

  def principal: String
  def keytabPath: String

  def doAsClusterUser[T](task: => T): T

  def initKerberosConf(): Configuration = {
    val conf = new Configuration(false)
    configDir().listFiles().foreach { f =>
      if (f.getName().endsWith(".xml")) {
        conf.addResource(new Path(f.toURI()))
      }
    }
    UserGroupInformation.setConfiguration(conf)
    UserGroupInformation.loginUserFromKeytab(principal, keytabPath)
    conf
  }

  lazy val hadoopConf = {
    var conf = new Configuration(false)

    if (authScheme == "kerberos"){
      conf = initKerberosConf()
    }
    configDir().listFiles().foreach { f =>
      if (f.getName().endsWith(".xml")) {
        conf.addResource(new Path(f.toURI()))
      }
    }
    conf
  }

  lazy val yarnConf = {
    var conf = new Configuration(false)

    if (authScheme == "kerberos"){
      conf = initKerberosConf()
    }
    conf.addResource(new Path(s"${configDir().getCanonicalPath}/yarn-site.xml"))
    conf
  }

  lazy val fs = doAsClusterUser {
    FileSystem.get(hadoopConf)
  }

  lazy val yarnClient = doAsClusterUser {
    val c = YarnClient.createYarnClient()
    c.init(yarnConf)
    c.start()
    c
  }
}

object Cluster extends Logging {
  private val CLUSTER_TYPE = "cluster.type"

  private lazy val config = {
    sys.props.get("cluster.spec")
      .filter { path => path.nonEmpty && path != "default" }
      .map { path =>
        val in = Option(getClass.getClassLoader.getResourceAsStream(path))
          .getOrElse(new FileInputStream(path))
        val p = new Properties()
        val reader = new InputStreamReader(in, UTF_8)
        try {
          p.load(reader)
        } finally {
          reader.close()
        }
        p.asScala.toMap
      }
      .getOrElse(Map.empty)
  }

  private lazy val cluster = {
    var _cluster: Cluster = null
    try {
      _cluster = config.get(CLUSTER_TYPE) match {
        case Some("mini") => new MiniCluster(config)
        case Some("external") => new ExternalCluster(config)
        case t => new MiniCluster(config)
      }
      Runtime.getRuntime.addShutdownHook(new Thread {
        override def run(): Unit = {
          info("Shutting down cluster pool.")
          _cluster.cleanUp()
        }
      })
      _cluster.deploy()
    } catch {
      case e: Throwable =>
        error("Failed to initialize cluster.", e)
        Option(_cluster).foreach { c =>
          Try(c.cleanUp()).recover { case e =>
            error("Furthermore, failed to clean up cluster after failure.", e)
          }
        }
        throw e
    }
    _cluster
  }

  def get(): Cluster = cluster

  def isRunningOnTravis: Boolean = sys.env.contains("TRAVIS")
}
