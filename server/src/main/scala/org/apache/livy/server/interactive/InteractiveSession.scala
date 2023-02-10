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

package org.apache.livy.server.interactive

import java.io.{File, InputStream}
import java.net.URI
import java.nio.ByteBuffer
import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.{Random, Try}

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import org.apache.hadoop.fs.Path
import org.apache.spark.launcher.SparkLauncher

import org.apache.livy._
import org.apache.livy.client.common.HttpMessages._
import org.apache.livy.rsc.{PingJob, RSCClient, RSCConf}
import org.apache.livy.rsc.driver.Statement
import org.apache.livy.server.AccessManager
import org.apache.livy.server.recovery.SessionStore
import org.apache.livy.sessions._
import org.apache.livy.sessions.Session._
import org.apache.livy.sessions.SessionState.Dead
import org.apache.livy.utils._

@JsonIgnoreProperties(ignoreUnknown = true)
case class InteractiveRecoveryMetadata(
    id: Int,
    name: Option[String],
    appId: Option[String],
    appTag: String,
    kind: Kind,
    heartbeatTimeoutS: Int,
    owner: String,
    ttl: Option[String],
    proxyUser: Option[String],
    rscDriverUri: Option[URI],
    version: Int = 1)
  extends RecoveryMetadata

object InteractiveSession extends Logging {
  private[interactive] val SPARK_YARN_IS_PYTHON = "spark.yarn.isPython"

  val RECOVERY_SESSION_TYPE = "interactive"

  def create(
      id: Int,
      name: Option[String],
      owner: String,
      proxyUser: Option[String],
      livyConf: LivyConf,
      accessManager: AccessManager,
      request: CreateInteractiveRequest,
      sessionStore: SessionStore,
      ttl: Option[String],
      mockApp: Option[SparkApp] = None,
      mockClient: Option[RSCClient] = None): InteractiveSession = {
    val appTag = s"livy-session-$id-${Random.alphanumeric.take(8).mkString}"
    val impersonatedUser = accessManager.checkImpersonation(proxyUser, owner)

    val client = mockClient.orElse {
      val conf = SparkApp.prepareSparkConf(appTag, livyConf, prepareConf(
        request.conf, request.jars, request.files, request.archives, request.pyFiles, livyConf))

      val builderProperties = prepareBuilderProp(conf, request.kind, livyConf)

      val userOpts: Map[String, Option[String]] = Map(
        "spark.driver.cores" -> request.driverCores.map(_.toString),
        SparkLauncher.DRIVER_MEMORY -> request.driverMemory.map(_.toString),
        SparkLauncher.EXECUTOR_CORES -> request.executorCores.map(_.toString),
        SparkLauncher.EXECUTOR_MEMORY -> request.executorMemory.map(_.toString),
        "spark.executor.instances" -> request.numExecutors.map(_.toString),
        "spark.app.name" -> request.name.map(_.toString),
        "spark.yarn.queue" -> request.queue
      )

      userOpts.foreach { case (key, opt) =>
        opt.foreach { value => builderProperties.put(key, value) }
      }

      builderProperties.getOrElseUpdate("spark.app.name", s"livy-session-$id")

      info(s"Creating Interactive session $id: [owner: $owner, request: $request]")
      val builder = new LivyClientBuilder()
        .setAll(builderProperties.asJava)
        .setConf("livy.client.session-id", id.toString)
        .setConf(RSCConf.Entry.DRIVER_CLASS.key(), "org.apache.livy.repl.ReplDriver")
        .setConf(RSCConf.Entry.PROXY_USER.key(), impersonatedUser.orNull)
        .setURI(new URI("rsc:/"))

      Option(builder.build().asInstanceOf[RSCClient])
    }

    new InteractiveSession(
      id,
      name,
      None,
      appTag,
      client,
      SessionState.Starting,
      request.kind,
      request.heartbeatTimeoutInSecond,
      livyConf,
      owner,
      impersonatedUser,
      ttl,
      sessionStore,
      mockApp)
  }

  def recover(
      metadata: InteractiveRecoveryMetadata,
      livyConf: LivyConf,
      sessionStore: SessionStore,
      mockApp: Option[SparkApp] = None,
      mockClient: Option[RSCClient] = None): InteractiveSession = {
    val client = mockClient.orElse(metadata.rscDriverUri.map { uri =>
      val builder = new LivyClientBuilder().setURI(uri)
      builder.build().asInstanceOf[RSCClient]
    })

    new InteractiveSession(
      metadata.id,
      metadata.name,
      metadata.appId,
      metadata.appTag,
      client,
      SessionState.Recovering,
      metadata.kind,
      metadata.heartbeatTimeoutS,
      livyConf,
      metadata.owner,
      metadata.proxyUser,
      metadata.ttl,
      sessionStore,
      mockApp)
  }

  private[interactive] def prepareBuilderProp(
    conf: Map[String, String],
    kind: Kind,
    livyConf: LivyConf): mutable.Map[String, String] = {

    val builderProperties = mutable.Map[String, String]()
    builderProperties ++= conf

    def livyJars(livyConf: LivyConf, scalaVersion: String): List[String] = {
      Option(livyConf.get(LivyConf.REPL_JARS)).map { jars =>
        val regex = """[\w-]+_(\d\.\d\d).*\.jar""".r
        jars.split(",").filter { name => new Path(name).getName match {
            // Filter out unmatched scala jars
            case regex(ver) => ver == scalaVersion
            // Keep all the java jars end with ".jar"
            case _ => name.endsWith(".jar")
          }
        }.toList
      }.getOrElse {
        val home = sys.env("LIVY_HOME")
        val jars = Option(new File(home, s"repl_$scalaVersion-jars"))
          .filter(_.isDirectory())
          .getOrElse(new File(home, s"repl/scala-$scalaVersion/target/jars"))
        require(jars.isDirectory(), "Cannot find Livy REPL jars.")
        jars.listFiles().map(_.getAbsolutePath()).toList
      }
    }

    def findSparkRArchive(): Option[String] = {
      Option(livyConf.get(RSCConf.Entry.SPARKR_PACKAGE.key())).orElse {
        sys.env.get("SPARK_HOME").flatMap { case sparkHome =>
          val path = Seq(sparkHome, "R", "lib", "sparkr.zip").mkString(File.separator)
          val rArchivesFile = new File(path)
          if (rArchivesFile.exists()) {
            Some(rArchivesFile.getAbsolutePath)
          } else {
            warn("sparkr.zip not found; cannot start R interpreter.")
            None
          }
        }
      }
    }

    def datanucleusJars(livyConf: LivyConf, sparkMajorVersion: Int): Seq[String] = {
      if (sys.env.getOrElse("LIVY_INTEGRATION_TEST", "false").toBoolean) {
        // datanucleus jars has already been in classpath in integration test
        Seq.empty
      } else {
        val sparkHome = livyConf.sparkHome().get
        val libdir = sparkMajorVersion match {
          case 2 | 3 =>
            if (new File(sparkHome, "RELEASE").isFile) {
              new File(sparkHome, "jars")
            } else if (new File(sparkHome, "assembly/target/scala-2.11/jars").isDirectory) {
              new File(sparkHome, "assembly/target/scala-2.11/jars")
            } else {
              new File(sparkHome, "assembly/target/scala-2.12/jars")
            }
          case v =>
            throw new RuntimeException(s"Unsupported Spark major version: $sparkMajorVersion")
        }
        val jars = if (!libdir.isDirectory) {
          Seq.empty[String]
        } else {
          libdir.listFiles().filter(_.getName.startsWith("datanucleus-"))
            .map(_.getAbsolutePath).toSeq
        }
        if (jars.isEmpty) {
          warn("datanucleus jars can not be found")
        }
        jars
      }
    }

    /**
     * Look for hive-site.xml (for now just ignore spark.files defined in spark-defaults.conf)
     * 1. First look for hive-site.xml in user request
     * 2. Then look for that under classpath
     * @param livyConf
     * @return  (hive-site.xml path, whether it is provided by user)
     */
    def hiveSiteFile(sparkFiles: Array[String], livyConf: LivyConf): (Option[File], Boolean) = {
      if (sparkFiles.exists(_.split("/").last == "hive-site.xml")) {
        (None, true)
      } else {
        val hiveSiteURL = getClass.getResource("/hive-site.xml")
        if (hiveSiteURL != null && hiveSiteURL.getProtocol == "file") {
          (Some(new File(hiveSiteURL.toURI)), false)
        } else {
          (None, false)
        }
      }
    }

    def findPySparkArchives(): Seq[String] = {
      Option(livyConf.get(RSCConf.Entry.PYSPARK_ARCHIVES))
        .map(_.split(",").toSeq)
        .getOrElse {
          sys.env.get("SPARK_HOME") .map { case sparkHome =>
            val pyLibPath = Seq(sparkHome, "python", "lib").mkString(File.separator)
            val pyArchivesFile = new File(pyLibPath, "pyspark.zip")
            val py4jFile = Try {
              Files.newDirectoryStream(Paths.get(pyLibPath), "py4j-*-src.zip")
                .iterator()
                .next()
                .toFile
            }.toOption

            if (!pyArchivesFile.exists()) {
              warn("pyspark.zip not found; cannot start pyspark interpreter.")
              Seq.empty
            } else if (py4jFile.isEmpty || !py4jFile.get.exists()) {
              warn("py4j-*-src.zip not found; can start pyspark interpreter.")
              Seq.empty
            } else {
              Seq(pyArchivesFile.getAbsolutePath, py4jFile.get.getAbsolutePath)
            }
          }.getOrElse(Seq())
        }
    }

    def mergeConfList(list: Seq[String], key: String): Unit = {
      if (list.nonEmpty) {
        builderProperties.get(key) match {
          case None =>
            builderProperties.put(key, list.mkString(","))
          case Some(oldList) =>
            val newList = (oldList :: list.toList).mkString(",")
            builderProperties.put(key, newList)
        }
      }
    }

    def mergeHiveSiteAndHiveDeps(sparkMajorVersion: Int): Unit = {
      val sparkFiles = conf.get("spark.files").map(_.split(",")).getOrElse(Array.empty[String])
      hiveSiteFile(sparkFiles, livyConf) match {
        case (_, true) =>
          debug("Enable HiveContext because hive-site.xml is found in user request.")
          mergeConfList(datanucleusJars(livyConf, sparkMajorVersion), LivyConf.SPARK_JARS)
        case (Some(file), false) =>
          debug("Enable HiveContext because hive-site.xml is found under classpath, "
            + file.getAbsolutePath)
          mergeConfList(List(file.getAbsolutePath), LivyConf.SPARK_FILES)
          mergeConfList(datanucleusJars(livyConf, sparkMajorVersion), LivyConf.SPARK_JARS)
        case (None, false) =>
          warn("Enable HiveContext but no hive-site.xml found under" +
            " classpath or user request.")
      }
    }

    val pySparkFiles = if (!LivyConf.TEST_MODE) {
      findPySparkArchives()
    } else {
      Nil
    }

    if (pySparkFiles.nonEmpty) {
      builderProperties.put(SPARK_YARN_IS_PYTHON, "true")
    }

    mergeConfList(pySparkFiles, LivyConf.SPARK_PY_FILES)

    val sparkRArchive = if (!LivyConf.TEST_MODE) findSparkRArchive() else None
    sparkRArchive.foreach { archive =>
      builderProperties.put(RSCConf.Entry.SPARKR_PACKAGE.key(), archive + "#sparkr")
    }

    builderProperties.put(RSCConf.Entry.SESSION_KIND.key, kind.toString)

    // Set Livy.rsc.jars from livy conf to rsc conf, RSC conf will take precedence if both are set.
    Option(livyConf.get(LivyConf.RSC_JARS)).foreach(
      builderProperties.getOrElseUpdate(RSCConf.Entry.LIVY_JARS.key(), _))

    require(livyConf.get(LivyConf.LIVY_SPARK_VERSION) != null)
    require(livyConf.get(LivyConf.LIVY_SPARK_SCALA_VERSION) != null)

    val (sparkMajorVersion, _) =
      LivySparkUtils.formatSparkVersion(livyConf.get(LivyConf.LIVY_SPARK_VERSION))
    val scalaVersion = livyConf.get(LivyConf.LIVY_SPARK_SCALA_VERSION)

    mergeConfList(livyJars(livyConf, scalaVersion), LivyConf.SPARK_JARS)
    val enableHiveContext = livyConf.getBoolean(LivyConf.ENABLE_HIVE_CONTEXT)
    // pass spark.livy.spark_major_version to driver
    builderProperties.put("spark.livy.spark_major_version", sparkMajorVersion.toString)

    val confVal = if (enableHiveContext) "hive" else "in-memory"
    builderProperties.put("spark.sql.catalogImplementation", confVal)

    if (enableHiveContext) {
      mergeHiveSiteAndHiveDeps(sparkMajorVersion)
    }

    // Pick all the RSC-specific configs that have not been explicitly set otherwise, and
    // put them in the resulting properties, so that the remote driver can use them.
    livyConf.iterator().asScala.foreach { e =>
      val (key, value) = (e.getKey(), e.getValue())
      if (key.startsWith(RSCConf.RSC_CONF_PREFIX) && !builderProperties.contains(key)) {
        builderProperties(key) = value
      }
    }

    builderProperties
  }
}

class InteractiveSession(
    id: Int,
    name: Option[String],
    appIdHint: Option[String],
    appTag: String,
    val client: Option[RSCClient],
    initialState: SessionState,
    val kind: Kind,
    heartbeatTimeoutS: Int,
    livyConf: LivyConf,
    owner: String,
    override val proxyUser: Option[String],
    ttl: Option[String],
    sessionStore: SessionStore,
    mockApp: Option[SparkApp]) // For unit test.
  extends Session(id, name, owner, ttl, livyConf)
  with SessionHeartbeat
  with SparkAppListener {

  import InteractiveSession._

  private var serverSideState: SessionState = initialState

  override protected val heartbeatTimeout: FiniteDuration = {
    val heartbeatTimeoutInSecond = heartbeatTimeoutS
    Duration(heartbeatTimeoutInSecond, TimeUnit.SECONDS)
  }
  private val operations = mutable.Map[Long, String]()
  private val operationCounter = new AtomicLong(0)
  private var rscDriverUri: Option[URI] = None
  private var sessionLog: IndexedSeq[String] = IndexedSeq.empty
  private val sessionSaveLock = new Object()

  _appId = appIdHint

  private var app: Option[SparkApp] = None

  override def start(): Unit = {
    sessionStore.save(RECOVERY_SESSION_TYPE, recoveryMetadata)
    heartbeat()
    app = mockApp.orElse {
      val driverProcess = client.flatMap { c => Option(c.getDriverProcess) }
        .map(new LineBufferedProcess(_, livyConf.getInt(LivyConf.SPARK_LOGS_SIZE)))

      if (livyConf.isRunningOnYarn() || driverProcess.isDefined) {
        Some(SparkApp.create(appTag, appId, driverProcess, livyConf, Some(this)))
      } else {
        None
      }
    }

    if (client.isEmpty) {
      transition(Dead())
      val msg = s"Cannot recover interactive session $id because its RSCDriver URI is unknown."
      info(msg)
      sessionLog = IndexedSeq(msg)
    } else {
      val uriFuture = Future { client.get.getServerUri.get() }

      uriFuture.onSuccess { case url =>
        rscDriverUri = Option(url)
        sessionSaveLock.synchronized {
          sessionStore.save(RECOVERY_SESSION_TYPE, recoveryMetadata)
        }
      }
      uriFuture.onFailure { case e => warn("Fail to get rsc uri", e) }

      // Send a dummy job that will return once the client is ready to be used, and set the
      // state to "idle" at that point.
      client.get.submit(new PingJob()).addListener(new JobHandle.Listener[Void]() {
      override def onJobQueued(job: JobHandle[Void]): Unit = { }
      override def onJobStarted(job: JobHandle[Void]): Unit = { }

        override def onJobCancelled(job: JobHandle[Void]): Unit = errorOut()

        override def onJobFailed(job: JobHandle[Void], cause: Throwable): Unit = errorOut()

        override def onJobSucceeded(job: JobHandle[Void], result: Void): Unit = {
          transition(SessionState.Running)
          info(s"Interactive session $id created [appid: ${appId.orNull}, " +
            s"owner: $owner, proxyUser:" +
            s" $proxyUser, state: ${state.toString}, kind: ${kind.toString}, " +
            s"info: ${appInfo.asJavaMap}]")
        }

        private def errorOut(): Unit = {
          // Other code might call stop() to close the RPC channel. When RPC channel is closing,
          // this callback might be triggered. Check and don't call stop() to avoid nested called
          // if the session is already shutting down.
          if (serverSideState != SessionState.ShuttingDown) {
            transition(SessionState.Error())
            stop()
            app.foreach { a =>
              info(s"Failed to ping RSC driver for session $id. Killing application.")
              a.kill()
            }
          }
        }
      })
    }
  }

  override def logLines(): IndexedSeq[String] = app.map(_.log()).getOrElse(sessionLog)

  override def recoveryMetadata: RecoveryMetadata =
    InteractiveRecoveryMetadata(id, name, appId, appTag, kind,
      heartbeatTimeout.toSeconds.toInt, owner, None, proxyUser, rscDriverUri)

  override def state: SessionState = {
    if (serverSideState == SessionState.Running) {
      // If session is in running state, return the repl state from RSCClient.
      client
        .flatMap(s => Option(s.getReplState))
        .map(SessionState(_))
        .getOrElse(SessionState.Busy) // If repl state is unknown, assume repl is busy.
    } else serverSideState
  }

  override def stopSession(): Unit = {
    try {
      transition(SessionState.ShuttingDown)
      sessionStore.remove(RECOVERY_SESSION_TYPE, id)
      client.foreach { _.stop(true) }
    } catch {
      case _: Exception =>
        app.foreach {
          warn(s"Failed to stop RSCDriver. Killing it...")
          _.kill()
        }
    } finally {
      transition(SessionState.Dead())
    }
  }

  def statements: IndexedSeq[Statement] = {
    ensureActive()
    val r = client.get.getReplJobResults().get()
    r.statements.toIndexedSeq
  }

  def getStatement(stmtId: Int): Option[Statement] = {
    ensureActive()
    val r = client.get.getReplJobResults(stmtId, 1).get()
    if (r.statements.length < 1) {
      None
    } else {
      Option(r.statements(0))
    }
  }

  def interrupt(): Future[Unit] = {
    stop()
  }

  def executeStatement(content: ExecuteRequest): Statement = {
    ensureRunning()
    recordActivity()

    val id = client.get.submitReplCode(content.code, content.kind.orNull).get
    client.get.getReplJobResults(id, 1).get().statements(0)
  }

  def cancelStatement(statementId: Int): Unit = {
    ensureRunning()
    recordActivity()
    client.get.cancelReplCode(statementId)
  }

  def completion(content: CompletionRequest): CompletionResponse = {
    ensureRunning()
    recordActivity()

    val proposals = client.get.completeReplCode(content.code, content.kind,
        content.cursor).get
    CompletionResponse(proposals.toList)
  }

  def runJob(job: Array[Byte], jobType: String): Long = {
    performOperation(job, jobType, true)
  }

  def submitJob(job: Array[Byte], jobType: String): Long = {
    performOperation(job, jobType, false)
  }

  def addFile(fileStream: InputStream, fileName: String): Unit = {
    addFile(copyResourceToHDFS(fileStream, fileName))
  }

  def addJar(jarStream: InputStream, jarName: String): Unit = {
    addJar(copyResourceToHDFS(jarStream, jarName))
  }

  def addFile(uri: URI): Unit = {
    ensureActive()
    recordActivity()
    client.get.addFile(resolveURI(uri, livyConf)).get()
  }

  def addJar(uri: URI): Unit = {
    ensureActive()
    recordActivity()
    client.get.addJar(resolveURI(uri, livyConf)).get()
  }

  def jobStatus(id: Long): Any = {
    ensureActive()
    val clientJobId = operations(id)
    recordActivity()
    // TODO: don't block indefinitely?
    val status = client.get.getBypassJobStatus(clientJobId).get()
    new JobStatus(id, status.state, status.result, status.error)
  }

  def cancelJob(id: Long): Unit = {
    ensureActive()
    recordActivity()
    operations.remove(id).foreach { client.get.cancel }
  }

  private def transition(newState: SessionState) = synchronized {
    // When a statement returns an error, the session should transit to error state.
    // If the session crashed because of the error, the session should instead go to dead state.
    // Since these 2 transitions are triggered by different threads, there's a race condition.
    // Make sure we won't transit from dead to error state.
    val areSameStates = serverSideState.getClass() == newState.getClass()

    if (!areSameStates) {
      newState match {
        case _: SessionState.Killed | _: SessionState.Dead =>
          sessionStore.remove(RECOVERY_SESSION_TYPE, id)
        case SessionState.ShuttingDown =>
          sessionStore.remove(RECOVERY_SESSION_TYPE, id)
        case _ =>
      }
    }

    val transitFromInactiveToActive = !serverSideState.isActive && newState.isActive
    if (!areSameStates && !transitFromInactiveToActive) {
      debug(s"$this session state change from ${serverSideState} to $newState")
      serverSideState = newState
    }
  }

  private def ensureActive(): Unit = synchronized {
    require(serverSideState.isActive, "Session isn't active.")
    require(client.isDefined, "Session is active but client hasn't been created.")
  }

  private def ensureRunning(): Unit = synchronized {
    serverSideState match {
      case SessionState.Running =>
      case _ =>
        throw new IllegalStateException("Session is in state %s" format serverSideState)
    }
  }

  private def performOperation(job: Array[Byte], jobType: String, sync: Boolean): Long = {
    ensureActive()
    recordActivity()
    val future = client.get.bypass(ByteBuffer.wrap(job), jobType, sync)
    val opId = operationCounter.incrementAndGet()
    operations(opId) = future
    opId
   }

  override def appIdKnown(appId: String): Unit = {
    _appId = Option(appId)
    sessionSaveLock.synchronized {
      sessionStore.save(RECOVERY_SESSION_TYPE, recoveryMetadata)
    }
  }

  override def stateChanged(oldState: SparkApp.State, newState: SparkApp.State): Unit = {
    synchronized {
      debug(s"$this app state changed from $oldState to $newState")
      newState match {
        case SparkApp.State.FINISHED | SparkApp.State.FAILED =>
          transition(SessionState.Dead())
        case SparkApp.State.KILLED => transition(SessionState.Killed())
        case _ =>
      }
    }
  }

  override def infoChanged(appInfo: AppInfo): Unit = { this.appInfo = appInfo }

  override def lastActivity: Long = {
    val serverSideLastActivity = super.lastActivity
    if (serverSideState == SessionState.Running) {
      // If the rsc client is running, we compare the lastActivity of the session and the repl,
      // and return the more latest one
      client.flatMap { s => Option(s.getReplLastActivity) }.filter(_ > serverSideLastActivity)
        .getOrElse(serverSideLastActivity)
    } else {
      serverSideLastActivity
    }
  }
}
