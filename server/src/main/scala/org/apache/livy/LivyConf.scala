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

package org.apache.livy

import java.io.File
import java.lang.{Boolean => JBoolean, Long => JLong}
import java.util.{Map => JMap}

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration

import org.apache.livy.client.common.ClientConf
import org.apache.livy.client.common.ClientConf.ConfEntry
import org.apache.livy.client.common.ClientConf.DeprecatedConf

object LivyConf {

  case class Entry(override val key: String, override val dflt: AnyRef) extends ConfEntry

  object Entry {
    def apply(key: String, dflt: Boolean): Entry = Entry(key, dflt: JBoolean)
    def apply(key: String, dflt: Int): Entry = Entry(key, dflt: Integer)
    def apply(key: String, dflt: Long): Entry = Entry(key, dflt: JLong)
  }

  val TEST_MODE = ClientConf.TEST_MODE

  val SPARK_HOME = Entry("livy.server.spark-home", null)
  val LIVY_SPARK_MASTER = Entry("livy.spark.master", "local")
  val LIVY_SPARK_DEPLOY_MODE = Entry("livy.spark.deploy-mode", null)

  // Two configurations to specify Spark and related Scala version. These are internal
  // configurations will be set by LivyServer and used in session creation. It is not required to
  // set usually unless running with unofficial Spark + Scala combinations.
  val LIVY_SPARK_SCALA_VERSION = Entry("livy.spark.scala-version", null)
  val LIVY_SPARK_VERSION = Entry("livy.spark.version", null)

  val SESSION_STAGING_DIR = Entry("livy.session.staging-dir", null)
  val FILE_UPLOAD_MAX_SIZE = Entry("livy.file.upload.max.size", 100L * 1024 * 1024)
  val LOCAL_FS_WHITELIST = Entry("livy.file.local-dir-whitelist", null)
  val ENABLE_HIVE_CONTEXT = Entry("livy.repl.enable-hive-context", false)

  val ENVIRONMENT = Entry("livy.environment", "production")

  val SERVER_HOST = Entry("livy.server.host", "0.0.0.0")
  val SERVER_PORT = Entry("livy.server.port", 8998)
  val SERVER_BASE_PATH = Entry("livy.ui.basePath", "")

  val UI_ENABLED = Entry("livy.ui.enabled", true)

  val REQUEST_HEADER_SIZE = Entry("livy.server.request-header.size", 131072)
  val RESPONSE_HEADER_SIZE = Entry("livy.server.response-header.size", 131072)

  val CSRF_PROTECTION = Entry("livy.server.csrf-protection.enabled", false)

  val IMPERSONATION_ENABLED = Entry("livy.impersonation.enabled", false)
  val SUPERUSERS = Entry("livy.superusers", null)

  val ACCESS_CONTROL_ENABLED = Entry("livy.server.access-control.enabled", false)
  // Allowed users to access Livy, by default any user is allowed to access Livy. If user want to
  // limit who could access Livy, user should list all the permitted users with comma
  // separated.
  val ACCESS_CONTROL_ALLOWED_USERS = Entry("livy.server.access-control.allowed-users", "*")
  val ACCESS_CONTROL_MODIFY_USERS = Entry("livy.server.access-control.modify-users", null)
  val ACCESS_CONTROL_VIEW_USERS = Entry("livy.server.access-control.view-users", null)

  val SSL_KEYSTORE = Entry("livy.keystore", null)
  val SSL_KEYSTORE_PASSWORD = Entry("livy.keystore.password", null)
  val SSL_KEY_PASSWORD = Entry("livy.key-password", null)

  val HADOOP_CREDENTIAL_PROVIDER_PATH = Entry("livy.hadoop.security.credential.provider.path", null)

  val AUTH_TYPE = Entry("livy.server.auth.type", null)
  val AUTH_KERBEROS_PRINCIPAL = Entry("livy.server.auth.kerberos.principal", null)
  val AUTH_KERBEROS_KEYTAB = Entry("livy.server.auth.kerberos.keytab", null)
  val AUTH_KERBEROS_NAME_RULES = Entry("livy.server.auth.kerberos.name-rules", "DEFAULT")

  val HEARTBEAT_WATCHDOG_INTERVAL = Entry("livy.server.heartbeat-watchdog.interval", "1m")

  val LAUNCH_KERBEROS_PRINCIPAL = Entry("livy.server.launch.kerberos.principal", null)
  val LAUNCH_KERBEROS_KEYTAB = Entry("livy.server.launch.kerberos.keytab", null)
  val LAUNCH_KERBEROS_REFRESH_INTERVAL = Entry("livy.server.launch.kerberos.refresh-interval", "1h")
  val KINIT_FAIL_THRESHOLD = Entry("livy.server.launch.kerberos.kinit-fail-threshold", 5)

  // Thrift configurations
  val THRIFT_SERVER_ENABLED = Entry("livy.server.thrift.enabled", false)
  val THRIFT_INCR_COLLECT_ENABLED = Entry("livy.server.thrift.incrementalCollect", false)
  val THRIFT_SESSION_CREATION_TIMEOUT = Entry("livy.server.thrift.session.creationTimeout", "10m")
  // The following configs are the same present in Hive
  val THRIFT_RESULTSET_DEFAULT_FETCH_SIZE =
    Entry("livy.server.thrift.resultset.default.fetch.size", 1000)
  val THRIFT_TRANSPORT_MODE = Entry("livy.server.thrift.transport.mode", "binary")
  val THRIFT_SERVER_PORT = Entry("livy.server.thrift.port", 10090)
  val THRIFT_LONG_POLLING_TIMEOUT = Entry("livy.server.thrift.long.polling.timeout", "5000ms")
  val THRIFT_LIMIT_CONNECTIONS_PER_USER = Entry("livy.server.thrift.limit.connections.per.user", 0)
  val THRIFT_LIMIT_CONNECTIONS_PER_IPADDRESS =
    Entry("livy.server.thrift.limit.connections.per.ipaddress", 0)
  val THRIFT_LIMIT_CONNECTIONS_PER_USER_IPADDRESS =
    Entry("livy.server.thrift.limit.connections.per.user.ipaddress", 0)
  val THRIFT_SESSION_CHECK_INTERVAL = Entry("livy.server.thrift.session.check.interval", "6h")
  val THRIFT_CLOSE_SESSION_ON_DISCONNECT =
    Entry("livy.server.thrift.close.session.on.disconnect", true)
  val THRIFT_IDLE_SESSION_TIMEOUT = Entry("livy.server.thrift.idle.session.timeout", "7d")
  val THRIFT_IDLE_OPERATION_TIMEOUT = Entry("livy.server.thrift.idle.operation.timeout", "5d")
  val THRIFT_IDLE_SESSION_CHECK_OPERATION =
    Entry("livy.server.thrift.idle.session.check.operation", true)
  val THRIFT_LOG_OPERATION_ENABLED = Entry("livy.server.thrift.logging.operation.enabled", true)
  val THRIFT_OPERATION_LOG_MAX_SIZE = Entry("livy.server.thrift.operation.log.max.size", 100L)
  val THRIFT_ASYNC_EXEC_THREADS = Entry("livy.server.thrift.async.exec.threads", 100)
  val THRIFT_ASYNC_EXEC_SHUTDOWN_TIMEOUT =
    Entry("livy.server.thrift.async.exec.shutdown.timeout", "10s")
  val THRIFT_ASYNC_EXEC_WAIT_QUEUE_SIZE =
    Entry("livy.server.thrift.async.exec.wait.queue.size", 100)
  val THRIFT_ASYNC_EXEC_KEEPALIVE_TIME =
    Entry("livy.server.thrift.async.exec.keepalive.time", "10s")
  val THRIFT_BIND_HOST = Entry("livy.server.thrift.bind.host", null)
  val THRIFT_WORKER_KEEPALIVE_TIME = Entry("livy.server.thrift.worker.keepalive.time", "60s")
  val THRIFT_MIN_WORKER_THREADS = Entry("livy.server.thrift.min.worker.threads", 5)
  val THRIFT_MAX_WORKER_THREADS = Entry("livy.server.thrift.max.worker.threads", 500)
  val THRIFT_RESULTSET_MAX_FETCH_SIZE = Entry("livy.server.thrift.resultset.max.fetch.size", 10000)
  val THRIFT_ALLOW_USER_SUBSTITUTION = Entry("livy.server.thrift.allow.user.substitution", true)
  val THRIFT_AUTHENTICATION = Entry("livy.server.thrift.authentication", "NONE")
  val THRIFT_ENABLE_DOAS = Entry("livy.server.thrift.enable.doAs", true)
  val THRIFT_SSL_PROTOCOL_BLACKLIST =
    Entry("livy.server.thrift.ssl.protocol.blacklist", "SSLv2,SSLv3")
  val THRIFT_USE_SSL = Entry("livy.server.thrift.use.SSL", false)
  val THRIFT_MAX_MESSAGE_SIZE = Entry("livy.server.thrift.max.message.size", 100 * 1024 * 1024)
  val THRIFT_LOGIN_TIMEOUT = Entry("livy.server.thrift.login.timeout", "20s")
  val THRIFT_LOGIN_BEBACKOFF_SLOT_LENGTH =
    Entry("livy.server.thrift.exponential.backoff.slot.length", "100ms")
  val THRIFT_HTTP_REQUEST_HEADER_SIZE =
    Entry("livy.server.thrift.http.request.header.size", 6*1024)
  val THRIFT_HTTP_RESPONSE_HEADER_SIZE =
    Entry("livy.server.thrift.http.response.header.size", 6*1024)
  val THRIFT_HTTP_MAX_IDLE_TIME = Entry("livy.server.thrift.http.max.idle.time", "1800s")
  val THRIFT_XSRF_FILTER_ENABLED = Entry("livy.server.thrift.xsrf.filter.enabled", false)
  val THRIFT_HTTP_PATH = Entry("livy.server.thrift.http.path", "cliservice")
  val THRIFT_HTTP_COMPRESSION_ENABLED = Entry("livy.server.thrift.http.compression.enabled", true)
  val THRIFT_HTTP_COOKIE_AUTH_ENABLED = Entry("livy.server.thrift.http.cookie.auth.enabled", true)
  val THRIFT_HTTP_COOKIE_MAX_AGE = Entry("livy.server.thrift.http.cookie.max.age", "86400s")
  val THRIFT_HTTP_COOKIE_DOMAIN = Entry("livy.server.thrift.http.cookie.domain", null)
  val THRIFT_HTTP_COOKIE_PATH = Entry("livy.server.thrift.http.cookie.path", null)
  val THRIFT_HTTP_COOKIE_IS_HTTPONLY = Entry("livy.server.thrift.http.cookie.is.httponly", true)
  val THRIFT_CUSTOM_AUTHENTICATION_CLASS =
    Entry("livy.server.thrift.custom.authentication.class", null)
  val THRIFT_SASL_QOP = Entry("livy.server.thrift.sasl.qop", "auth")
  val THRIFT_DELEGATION_KEY_UPDATE_INTERVAL =
    Entry("livy.server.thrift.delegation.key.update-interval", "1d")
  val THRIFT_DELEGATION_TOKEN_GC_INTERVAL =
    Entry("livy.server.thrift.delegation.token.gc-interval", "1h")
  val THRIFT_DELEGATION_TOKEN_MAX_LIFETIME =
    Entry("livy.server.thrift.delegation.token.max-lifetime", "7d")
  val THRIFT_DELEGATION_TOKEN_RENEW_INTERVAL =
    Entry("livy.server.thrift.delegation.token.renew-interval", "1d")

  /**
   * Recovery mode of Livy. Possible values:
   * off: Default. Turn off recovery. Every time Livy shuts down, it stops and forgets all sessions.
   * recovery: Livy persists session info to the state store. When Livy restarts, it recovers
   *   previous sessions from the state store.
   * Must set livy.server.recovery.state-store and livy.server.recovery.state-store.url to
   * configure the state store.
   */
  val RECOVERY_MODE = Entry("livy.server.recovery.mode", "off")
  /**
   * Where Livy should store state to for recovery. Possible values:
   * <empty>: Default. State store disabled.
   * filesystem: Store state on a file system.
   * zookeeper: Store state in a Zookeeper instance.
   */
  val RECOVERY_STATE_STORE = Entry("livy.server.recovery.state-store", null)
  /**
   * For filesystem state store, the path of the state store directory. Please don't use a
   * filesystem that doesn't support atomic rename (e.g. S3). e.g. file:///tmp/livy or hdfs:///.
   * For zookeeper, the address to the Zookeeper servers. e.g. host1:port1,host2:port2
   */
  val RECOVERY_STATE_STORE_URL = Entry("livy.server.recovery.state-store.url", "")

  // Livy will cache the max no of logs specified. 0 means don't cache the logs.
  val SPARK_LOGS_SIZE = Entry("livy.cache-log.size", 200)

  // If Livy can't find the yarn app within this time, consider it lost.
  val YARN_APP_LOOKUP_TIMEOUT = Entry("livy.server.yarn.app-lookup-timeout", "120s")

  // How often Livy polls YARN to refresh YARN app state.
  val YARN_POLL_INTERVAL = Entry("livy.server.yarn.poll-interval", "5s")

  // Days to keep Livy server request logs.
  val REQUEST_LOG_RETAIN_DAYS = Entry("livy.server.request-log-retain.days", 5)

  // REPL related jars separated with comma.
  val REPL_JARS = Entry("livy.repl.jars", null)
  // RSC related jars separated with comma.
  val RSC_JARS = Entry("livy.rsc.jars", null)

  // How long to check livy session leakage
  val YARN_APP_LEAKAGE_CHECK_TIMEOUT = Entry("livy.server.yarn.app-leakage.check-timeout", "600s")
  // how often to check livy session leakage
  val YARN_APP_LEAKAGE_CHECK_INTERVAL = Entry("livy.server.yarn.app-leakage.check-interval", "60s")

  // Whether session timeout should be checked, by default it will be checked, which means inactive
  // session will be stopped after "livy.server.session.timeout"
  val SESSION_TIMEOUT_CHECK = Entry("livy.server.session.timeout-check", true)
  // Whether session timeout check should skip busy sessions, if set to true, then busy sessions
  // that have jobs running will never timeout.
  val SESSION_TIMEOUT_CHECK_SKIP_BUSY = Entry("livy.server.session.timeout-check.skip-busy", false)
  // How long will an inactive session be gc-ed.
  val SESSION_TIMEOUT = Entry("livy.server.session.timeout", "1h")
  // How long a finished session state will be kept in memory
  val SESSION_STATE_RETAIN_TIME = Entry("livy.server.session.state-retain.sec", "600s")
  // Max creating session in livyServer
  val SESSION_MAX_CREATION = Entry("livy.server.session.max-creation", 100)

  val SPARK_MASTER = "spark.master"
  val SPARK_DEPLOY_MODE = "spark.submit.deployMode"
  val SPARK_JARS = "spark.jars"
  val SPARK_FILES = "spark.files"
  val SPARK_ARCHIVES = "spark.yarn.dist.archives"
  val SPARK_PY_FILES = "spark.submit.pyFiles"

  /**
   * These are Spark configurations that contain lists of files that the user can add to
   * their jobs in one way or another. Livy needs to pre-process these to make sure the
   * user can read them (in case they reference local files), and to provide correct URIs
   * to Spark based on the Livy config.
   *
   * The configuration allows adding new configurations in case we either forget something in
   * the hardcoded list, or new versions of Spark add new configs.
   */
  val SPARK_FILE_LISTS = Entry("livy.spark.file-list-configs", null)

  private val HARDCODED_SPARK_FILE_LISTS = Seq(
    SPARK_JARS,
    SPARK_FILES,
    SPARK_ARCHIVES,
    SPARK_PY_FILES,
    "spark.yarn.archive",
    "spark.yarn.dist.files",
    "spark.yarn.dist.jars",
    "spark.yarn.jar",
    "spark.yarn.jars"
  )

  case class DepConf(
      override val key: String,
      override val version: String,
      override val deprecationMessage: String = "")
    extends DeprecatedConf

  private val configsWithAlternatives: Map[String, DeprecatedConf] = Map[String, DepConf](
    LIVY_SPARK_DEPLOY_MODE.key -> DepConf("livy.spark.deployMode", "0.4"),
    LIVY_SPARK_SCALA_VERSION.key -> DepConf("livy.spark.scalaVersion", "0.4"),
    ENABLE_HIVE_CONTEXT.key -> DepConf("livy.repl.enableHiveContext", "0.4"),
    CSRF_PROTECTION.key -> DepConf("livy.server.csrf_protection.enabled", "0.4"),
    ACCESS_CONTROL_ENABLED.key -> DepConf("livy.server.access_control.enabled", "0.4"),
    AUTH_KERBEROS_NAME_RULES.key -> DepConf("livy.server.auth.kerberos.name_rules", "0.4"),
    LAUNCH_KERBEROS_REFRESH_INTERVAL.key ->
      DepConf("livy.server.launch.kerberos.refresh_interval", "0.4"),
    KINIT_FAIL_THRESHOLD.key -> DepConf("livy.server.launch.kerberos.kinit_fail_threshold", "0.4"),
    YARN_APP_LEAKAGE_CHECK_TIMEOUT.key ->
      DepConf("livy.server.yarn.app-leakage.check_timeout", "0.4"),
    YARN_APP_LEAKAGE_CHECK_INTERVAL.key ->
      DepConf("livy.server.yarn.app-leakage.check_interval", "0.4")
  )

  private val deprecatedConfigs: Map[String, DeprecatedConf] = {
    val configs: Seq[DepConf] = Seq(
      DepConf("livy.server.access_control.users", "0.4")
    )

    Map(configs.map { cfg => (cfg.key -> cfg) }: _*)
  }

}

/**
 *
 * @param loadDefaults whether to also load values from the Java system properties
 */
class LivyConf(loadDefaults: Boolean) extends ClientConf[LivyConf](null) {

  import LivyConf._

  lazy val hadoopConf = new Configuration()
  lazy val localFsWhitelist = configToSeq(LOCAL_FS_WHITELIST).map { path =>
    // Make sure the path ends with a single separator.
    path.stripSuffix("/") + "/"
  }

  lazy val sparkFileLists = HARDCODED_SPARK_FILE_LISTS ++ configToSeq(SPARK_FILE_LISTS)

  /**
   * Create a LivyConf that loads defaults from the system properties and the classpath.
   * @return
   */
  def this() = this(true)

  if (loadDefaults) {
    loadFromMap(sys.props)
  }

  def loadFromFile(name: String): LivyConf = {
    getConfigFile(name)
      .map(Utils.getPropertiesFromFile)
      .foreach(loadFromMap)
    this
  }

  /** Return true if spark master starts with yarn. */
  def isRunningOnYarn(): Boolean = sparkMaster().startsWith("yarn")

  /** Return the spark deploy mode Livy sessions should use. */
  def sparkDeployMode(): Option[String] = Option(get(LIVY_SPARK_DEPLOY_MODE)).filterNot(_.isEmpty)

  /** Return the location of the spark home directory */
  def sparkHome(): Option[String] = Option(get(SPARK_HOME)).orElse(sys.env.get("SPARK_HOME"))

  /** Return the spark master Livy sessions should use. */
  def sparkMaster(): String = get(LIVY_SPARK_MASTER)

  /** Return the path to the spark-submit executable. */
  def sparkSubmit(): String = {
    sparkHome().map { _ + File.separator + "bin" + File.separator + "spark-submit" }.get
  }

  private val configDir: Option[File] = {
    sys.env.get("LIVY_CONF_DIR")
      .orElse(sys.env.get("LIVY_HOME").map(path => s"$path${File.separator}conf"))
      .map(new File(_))
      .filter(_.exists())
  }

  private def getConfigFile(name: String): Option[File] = {
    configDir.map(new File(_, name)).filter(_.exists())
  }

  private def loadFromMap(map: Iterable[(String, String)]): Unit = {
    map.foreach { case (k, v) =>
      if (k.startsWith("livy.")) {
        set(k, v)
      }
    }
  }

  def configToSeq(entry: LivyConf.Entry): Seq[String] = {
    Option(get(entry)).map(_.split("[, ]+").toSeq).getOrElse(Nil)
  }

  override def getConfigsWithAlternatives: JMap[String, DeprecatedConf] = {
    configsWithAlternatives.asJava
  }

  override def getDeprecatedConfigs: JMap[String, DeprecatedConf] = {
    deprecatedConfigs.asJava
  }

}
