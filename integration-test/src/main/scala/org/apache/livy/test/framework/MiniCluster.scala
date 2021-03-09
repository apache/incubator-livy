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
import java.sql.DriverManager
import javax.servlet.http.HttpServletResponse

import scala.concurrent.duration._
import scala.language.postfixOps

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.server.MiniYARNCluster
import org.apache.spark.launcher.SparkLauncher
import org.asynchttpclient.DefaultAsyncHttpClient
import org.scalatest.concurrent.Eventually._

import org.apache.livy.{LivyConf, Logging}
import org.apache.livy.client.common.TestUtils
import org.apache.livy.server.LivyServer

private class MiniClusterConfig(val config: Map[String, String]) {

  val nmCount = getInt("yarn.nm-count", 1)
  val localDirCount = getInt("yarn.local-dir-count", 1)
  val logDirCount = getInt("yarn.log-dir-count", 1)
  val dnCount = getInt("hdfs.dn-count", 1)

  private def getInt(key: String, default: Int): Int = {
    config.get(key).map(_.toInt).getOrElse(default)
  }

}

sealed abstract class MiniClusterBase extends MiniClusterUtils with Logging {

  def main(args: Array[String]): Unit = {
    val klass = getClass().getSimpleName()

    info(s"$klass is starting up.")

    val Array(configPath) = args
    val config = {
      val file = new File(s"$configPath/cluster.conf")
      val props = loadProperties(file)
      new MiniClusterConfig(props)
    }
    start(config, configPath)

    info(s"$klass running.")

    while (true) synchronized {
      wait()
    }
  }

  protected def start(config: MiniClusterConfig, configPath: String): Unit

}

object MiniHdfsMain extends MiniClusterBase {

  override protected def start(config: MiniClusterConfig, configPath: String): Unit = {
    val hadoopConf = new Configuration()
    val hdfsCluster = new MiniDFSCluster.Builder(hadoopConf)
      .numDataNodes(config.dnCount)
      .format(true)
      .waitSafeMode(true)
      .build()

    hdfsCluster.waitActive()

    saveConfig(hadoopConf, new File(configPath + "/core-site.xml"))
  }

}

object MiniYarnMain extends MiniClusterBase {

  override protected def start(config: MiniClusterConfig, configPath: String): Unit = {
    val baseConfig = new YarnConfiguration()
    val yarnCluster = new MiniYARNCluster(getClass().getName(), config.nmCount,
      config.localDirCount, config.logDirCount)
    yarnCluster.init(baseConfig)

    // This allows applications run by YARN during the integration tests to find PIP modules
    // installed in the user's home directory (instead of just the global ones).
    baseConfig.set(YarnConfiguration.NM_USER_HOME_DIR, sys.env("HOME"))

    // Install a shutdown hook for stop the service and kill all running applications.
    Runtime.getRuntime().addShutdownHook(new Thread() {
      override def run(): Unit = yarnCluster.stop()
    })

    yarnCluster.start()

    // Workaround for YARN-2642.
    val yarnConfig = yarnCluster.getConfig()
    eventually(timeout(30 seconds), interval(100 millis)) {
      assert(yarnConfig.get(YarnConfiguration.RM_ADDRESS).split(":")(1) != "0",
        "RM not up yes.")
    }

    info(s"RM address in configuration is ${yarnConfig.get(YarnConfiguration.RM_ADDRESS)}")
    saveConfig(yarnConfig, new File(configPath + "/yarn-site.xml"))
  }

}

object MiniLivyMain extends MiniClusterBase {
  protected def baseLivyConf(configPath: String): Map[String, String] = {
    val baseConf = Map(
      LivyConf.LIVY_SPARK_MASTER.key -> "yarn",
      LivyConf.LIVY_SPARK_DEPLOY_MODE.key -> "cluster",
      LivyConf.HEARTBEAT_WATCHDOG_INTERVAL.key -> "1s",
      LivyConf.YARN_POLL_INTERVAL.key -> "500ms",
      LivyConf.RECOVERY_MODE.key -> "recovery",
      LivyConf.RECOVERY_STATE_STORE.key -> "filesystem",
      LivyConf.RECOVERY_STATE_STORE_URL.key -> s"file://$configPath/state-store")
    val thriftEnabled = sys.env.get("LIVY_TEST_THRIFT_ENABLED")
    if (thriftEnabled.nonEmpty && thriftEnabled.forall(_.toBoolean)) {
      baseConf + (LivyConf.THRIFT_SERVER_ENABLED.key -> "true")
    } else {
      baseConf
    }
  }

  def start(config: MiniClusterConfig, configPath: String): Unit = {
    var livyConf = baseLivyConf(configPath)

    if (Cluster.isRunningOnTravis) {
      livyConf ++= Map("livy.server.yarn.app-lookup-timeout" -> "2m")
    }

    saveProperties(livyConf, new File(configPath + "/livy.conf"))

    val server = new LivyServer()
    server.start()
    server.livyConf.set(LivyConf.ENABLE_HIVE_CONTEXT, false)
    // Write a serverUrl.conf file to the conf directory with the location of the Livy
    // server. Do it atomically since it's used by MiniCluster to detect when the Livy server
    // is up and ready.
    eventually(timeout(30 seconds), interval(1 second)) {
      var serverUrlConf = Map("livy.server.server-url" -> server.serverUrl())
      server.getJdbcUrl.foreach { url =>
        serverUrlConf += ("livy.server.thrift.jdbc-url" -> url)
      }
      saveProperties(serverUrlConf, new File(configPath + "/serverUrl.conf"))
    }
  }
}

private case class ProcessInfo(process: Process, logFile: File)

/**
 * Cluster implementation that uses HDFS / YARN mini clusters running as sub-processes locally.
 * Launching Livy through this mini cluster results in three child processes:
 *
 * - A HDFS mini cluster
 * - A YARN mini cluster
 * - The Livy server
 *
 * Each service will write its client configuration to a temporary directory managed by the
 * framework, so that applications can connect to the services.
 *
 * TODO: add support for MiniKdc.
 */
class MiniCluster(config: Map[String, String]) extends Cluster with MiniClusterUtils with Logging {
  private var _livyEndpoint: String = _
  private var _livyThriftJdbcUrl: Option[String] = None
  private var _hdfsScrathDir: Path = _

  private val _tempDir = new File(s"${sys.props("java.io.tmpdir")}/livy-int-test")
  private var _sparkConfigDir: File = _
  private var _configDir: File = _

  private var hdfs: Option[ProcessInfo] = None
  private var yarn: Option[ProcessInfo] = None
  private var livy: Option[ProcessInfo] = None

  private var _authScheme: String = _
  private var _user: String = _
  private var _password: String = _
  private var _sslCertPath: String = _

  private var _principal: String = _
  private var _keytabPath: String = _

  override def configDir(): File = _configDir

  override def hdfsScratchDir(): Path = _hdfsScrathDir

  override def doAsClusterUser[T](task: => T): T = task

  // Explicitly remove the "test-lib" dependency from the classpath of child processes. We
  // want tests to explicitly upload this jar when necessary, to test those code paths.
  private val childClasspath = {
    val cp = sys.props("java.class.path").split(File.pathSeparator)
    val filtered = cp.filter { path => !new File(path).getName().startsWith("livy-test-lib-") }
    assert(cp.size != filtered.size, "livy-test-lib jar not found in classpath!")
    filtered.mkString(File.pathSeparator)
  }

  override def deploy(): Unit = {
    if (_tempDir.exists()) {
      FileUtils.deleteQuietly(_tempDir)
    }
    assert(_tempDir.mkdir(), "Cannot create temp test dir.")
    _sparkConfigDir = mkdir("spark-conf")

    val sparkConf = Map(
      "spark.executor.instances" -> "1",
      "spark.scheduler.minRegisteredResourcesRatio" -> "0.0",
      "spark.ui.enabled" -> "false",
      SparkLauncher.DRIVER_MEMORY -> "512m",
      SparkLauncher.EXECUTOR_MEMORY -> "512m",
      SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS -> "-Dtest.appender=console",
      SparkLauncher.EXECUTOR_EXTRA_JAVA_OPTIONS -> "-Dtest.appender=console"
    )
    saveProperties(sparkConf, new File(_sparkConfigDir, "spark-defaults.conf"))

    _configDir = mkdir("hadoop-conf")
    saveProperties(config, new File(configDir, "cluster.conf"))
    hdfs = Some(start(MiniHdfsMain.getClass, new File(configDir, "core-site.xml")))
    yarn = Some(start(MiniYarnMain.getClass, new File(configDir, "yarn-site.xml")))
    runLivy()

    _hdfsScrathDir = fs.makeQualified(new Path("/"))
  }

  override def cleanUp(): Unit = {
    Seq(hdfs, yarn, livy).flatten.foreach(stop)
    hdfs = None
    yarn = None
    livy = None
  }

  def runLivy(): Unit = {
    assert(!livy.isDefined)
    val confFile = new File(configDir, "serverUrl.conf")
    val jacocoArgs = Option(TestUtils.getJacocoArgs())
      .map { args =>
        Seq(args, s"-Djacoco.args=$args")
      }.getOrElse(Nil)
    val localLivy = start(MiniLivyMain.getClass, confFile, extraJavaArgs = jacocoArgs)

    val props = loadProperties(confFile)
    _livyEndpoint = config.getOrElse("livyEndpoint", props("livy.server.server-url"))
    _livyThriftJdbcUrl = props.get("livy.server.thrift.jdbc-url")

    // Wait until Livy server responds.
    val httpClient = new DefaultAsyncHttpClient()
    eventually(timeout(30 seconds), interval(1 second)) {
      val res = httpClient.prepareGet(_livyEndpoint + "/metrics").execute().get()
      assert(res.getStatusCode() == HttpServletResponse.SC_OK)
    }

    livy = Some(localLivy)
  }

  def stopLivy(): Unit = {
    assert(livy.isDefined)
    livy.foreach(stop)
    _livyEndpoint = null
    _livyThriftJdbcUrl = None
    livy = None
  }

  def livyEndpoint: String = _livyEndpoint
  def jdbcEndpoint: Option[String] = _livyThriftJdbcUrl

  def authScheme: String = _authScheme
  def user: String = _user
  def password: String = _password
  def sslCertPath: String = _sslCertPath

  def principal: String = _principal
  def keytabPath: String = _keytabPath

  private def mkdir(name: String, parent: File = _tempDir): File = {
    val dir = new File(parent, name)
    if (!dir.exists()) {
      assert(dir.mkdir(), s"Failed to create directory $name.")
    }
    dir
  }

  private def start(
      klass: Class[_],
      configFile: File,
      extraJavaArgs: Seq[String] = Nil): ProcessInfo = {
    val simpleName = klass.getSimpleName().stripSuffix("$")
    val procDir = mkdir(simpleName)
    val procTmp = mkdir("tmp", parent = procDir)

    // Before starting anything, clean up previous running sessions.
    sys.process.Process(s"pkill -f $simpleName") !

    val cmd =
      Seq(
        sys.props("java.home") + "/bin/java",
        "-Dtest.appender=console",
        "-Djava.io.tmpdir=" + procTmp.getAbsolutePath(),
        "-cp", childClasspath + File.pathSeparator + configDir.getAbsolutePath()) ++
      extraJavaArgs ++
      Seq(
        klass.getName().stripSuffix("$"),
        configDir.getAbsolutePath())

    val logFile = new File(procDir, "output.log")
    val pb = new ProcessBuilder(cmd.toArray: _*)
      .directory(procDir)
      .redirectErrorStream(true)
      .redirectOutput(ProcessBuilder.Redirect.appendTo(logFile))

    pb.environment().put("LIVY_CONF_DIR", configDir.getAbsolutePath())
    pb.environment().put("HADOOP_CONF_DIR", configDir.getAbsolutePath())
    pb.environment().put("SPARK_CONF_DIR", _sparkConfigDir.getAbsolutePath())
    pb.environment().put("SPARK_LOCAL_IP", "127.0.0.1")

    val child = pb.start()

    // Wait for the config file to show up before returning, so that dependent services
    // can see the configuration. Exit early if process dies.
    eventually(timeout(30 seconds), interval(100 millis)) {
      assert(configFile.isFile(), s"$simpleName hasn't started yet.")

      try {
        val exitCode = child.exitValue()
        throw new IOException(s"Child process exited unexpectedly (exit code $exitCode)")
      } catch {
        case _: IllegalThreadStateException => // Try again.
      }
    }

    ProcessInfo(child, logFile)
  }

  private def stop(svc: ProcessInfo): Unit = {
    svc.process.destroy()
    svc.process.waitFor()
  }
}
