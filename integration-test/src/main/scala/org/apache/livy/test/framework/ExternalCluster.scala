package org.apache.livy.test.framework

import java.io._

import org.apache.hadoop.fs.Path
import org.apache.commons.io.FileUtils

import org.apache.livy.{LivyConf, Logging}
import org.apache.livy.client.common.TestUtils
import org.apache.livy.server.LivyServer

class ExternalCluster(config: Map[String, String]) extends Cluster with Logging {
  private var _livyEndpoint: String = _
  private var _livyThriftJdbcUrl: Option[String] = _
  private var _hdfsScrathDir: Path = _

  private var _configDir: File = _

  private var _authScheme: String = _
  private var _user: String = _
  private var _password: String = _
  private var _sslCertPath: String = _

  // Livy rest url endpoint
  override def livyEndpoint: String = _livyEndpoint

  // Livy jdbc url endpoint
  override def jdbcEndpoint: Option[String] = _livyThriftJdbcUrl

  // Temp directory in hdfs
  override def hdfsScratchDir(): Path = _hdfsScrathDir

  // Working directory that store core-site.xml, yarn-site.xml
  override def configDir(): File = _configDir

  // Knox rest url endpoint details
  override def authScheme: String = _authScheme
  override def user: String = _user
  override def password: String = _password
  override def sslCertPath: String = _sslCertPath


  override def doAsClusterUser[T](task: => T): T = task

  override def deploy(): Unit = {
    _livyEndpoint = config.getOrElse("livyEndpoint", "")
    _configDir = new File(config.getOrElse("configDir", "hadoop-conf"))
    _hdfsScrathDir = fs.makeQualified(new Path(config.getOrElse("hdfsScratchDir", "/")))
    _authScheme = config.getOrElse("authScheme", "")
    _user = config.getOrElse("user", "")
    _password = config.getOrElse("password", "")
    _sslCertPath = config.getOrElse("sslCertPath", "")
  }

  override def cleanUp(): Unit = {
  }

  def runLivy(): Unit = {
    // Do nothing. Livy is already started in aris cluster.
  }

  def stopLivy(): Unit = {
    // Do nothing. We will not stop livy in aris cluster.
  }
}
