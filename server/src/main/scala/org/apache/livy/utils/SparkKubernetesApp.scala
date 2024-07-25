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
package org.apache.livy.utils

import java.net.URLEncoder
import java.util.Collections
import java.util.concurrent._

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.api.model.networking.v1.{Ingress, IngressBuilder}
import io.fabric8.kubernetes.client.{Config, ConfigBuilder, _}
import org.apache.commons.lang.StringUtils

import org.apache.livy.{LivyConf, Logging}

object SparkKubernetesApp extends Logging {

  private val leakedAppTags = new java.util.concurrent.ConcurrentHashMap[String, Long]()

  private val monitorAppThreadMap = new java.util.concurrent.ConcurrentHashMap[Thread, Long]()

  private val appQueue = new ConcurrentLinkedQueue[SparkKubernetesApp]()

  private val leakedAppsGCThread = new Thread() {
    override def run(): Unit = {
      import KubernetesExtensions._
      while (true) {
        if (!leakedAppTags.isEmpty) {
          // kill the app if found it and remove it if exceeding a threshold
          val iter = leakedAppTags.entrySet().iterator()
          var isRemoved = false
          val now = System.currentTimeMillis()
          val apps = withRetry(kubernetesClient.getApplications())
          while (iter.hasNext) {
            val entry = iter.next()
            apps.find(_.getApplicationTag.contains(entry.getKey))
              .foreach({
                app =>
                  info(s"Kill leaked app ${app.getApplicationId}")
                  withRetry(kubernetesClient.killApplication(app))
                  iter.remove()
                  isRemoved = true
              })
            if (!isRemoved) {
              if ((entry.getValue - now) > sessionLeakageCheckTimeout) {
                iter.remove()
                info(s"Remove leaked Kubernetes app tag ${entry.getKey}")
              }
            }
          }
        }
        Thread.sleep(sessionLeakageCheckInterval)
      }
    }
  }

  val RefreshServiceAccountTokenThread = new Thread() {
    override def run(): Unit = {
      while (true) {
        var currentContext = new Context()
        var currentContextName = new String
        val config = kubernetesClient.getConfiguration
        if (config.getCurrentContext != null) {
          currentContext = config.getCurrentContext.getContext
          currentContextName = config.getCurrentContext.getName
        }

        var newAccessToken = new String
        val newestConfig = Config.autoConfigure(currentContextName)
        newAccessToken = newestConfig.getOauthToken
        info(s"Refresh a new token ${newAccessToken}")

        config.setOauthToken(newAccessToken)
        kubernetesClient = new DefaultKubernetesClient(config)

        // Token will expire 1 hour default, community recommend to update every 5 minutes
        Thread.sleep(300000)
      }
    }
  }

  private val checkMonitorAppTimeoutThread = new Thread() {
    override def run(): Unit = {
      while (true) {
        try {
          val iter = monitorAppThreadMap.entrySet().iterator()
          val now = System.currentTimeMillis()

          while (iter.hasNext) {
            val entry = iter.next()
            val thread = entry.getKey
            val updatedTime = entry.getValue

            val remaining: Long = now - updatedTime - pollInterval.toMillis
            if (remaining > appLookupTimeout.toMillis) {
              thread.interrupt()
            }
          }

          Thread.sleep(pollInterval.toMillis)
        } catch {
          case e: InterruptedException =>
            error("Apps timeout monitoring thread was interrupted.", e)
        }
      }
    }
  }

  private var livyConf: LivyConf = _

  private var cacheLogSize: Int = _
  private var appLookupTimeout: FiniteDuration = _
  private var pollInterval: FiniteDuration = _

  private var sessionLeakageCheckTimeout: Long = _
  private var sessionLeakageCheckInterval: Long = _

  var kubernetesClient: DefaultKubernetesClient = _

  private var appLookupThreadPoolSize: Long = _
  private var appLookupMaxFailedTimes: Long = _

  def init(livyConf: LivyConf, client: Option[KubernetesClient] = None): Unit = {
    this.livyConf = livyConf

    // KubernetesClient is thread safe. Create once, share it across threads.
    kubernetesClient =
      KubernetesClientFactory.createKubernetesClient(livyConf)

    cacheLogSize = livyConf.getInt(LivyConf.SPARK_LOGS_SIZE)
    appLookupTimeout = livyConf.getTimeAsMs(LivyConf.KUBERNETES_APP_LOOKUP_TIMEOUT).milliseconds
    pollInterval = livyConf.getTimeAsMs(LivyConf.KUBERNETES_POLL_INTERVAL).milliseconds

    appLookupThreadPoolSize = livyConf.getInt(LivyConf.KUBERNETES_APP_LOOKUP_THREAD_POOL_SIZE)
    appLookupMaxFailedTimes = livyConf.getInt(LivyConf.KUBERNETES_APP_LOOKUP_MAX_FAILED_TIMES)

    sessionLeakageCheckInterval =
      livyConf.getTimeAsMs(LivyConf.KUBERNETES_APP_LEAKAGE_CHECK_INTERVAL)
    sessionLeakageCheckTimeout = livyConf.getTimeAsMs(LivyConf.KUBERNETES_APP_LEAKAGE_CHECK_TIMEOUT)

    leakedAppsGCThread.setDaemon(true)
    leakedAppsGCThread.setName("LeakedAppsGCThread")
    leakedAppsGCThread.start()

    RefreshServiceAccountTokenThread.
      setName("RefreshServiceAccountTokenThread")
    RefreshServiceAccountTokenThread.setDaemon(true)
    RefreshServiceAccountTokenThread.start()

    checkMonitorAppTimeoutThread.setDaemon(true)
    checkMonitorAppTimeoutThread.setName("CheckMonitorAppTimeoutThread")
    checkMonitorAppTimeoutThread.start()

    initKubernetesAppMonitorThreadPool(livyConf)
  }

  // Returning T, throwing the exception on failure
  // When istio-proxy restarts, the access to K8s API from livy could be down
  // until envoy comes back, which could take upto 30 seconds
  @tailrec
  private def withRetry[T](fn: => T, n: Int = 10, retryBackoff: Long = 3000): T = {
    Try { fn } match {
      case Success(x) => x
      case _ if n > 1 =>
        Thread.sleep(Math.max(retryBackoff, 3000))
        withRetry(fn, n - 1)
      case Failure(e) => throw e
    }
  }

  class KubernetesAppMonitorRunnable extends Runnable {
    override def run(): Unit = {
      while (true) {
        try {
          val poolSize = livyConf.getInt(LivyConf.KUBERNETES_APP_LOOKUP_THREAD_POOL_SIZE)
          var numberOfAppsToProcess = appQueue.size() / poolSize
          if (numberOfAppsToProcess < 1) {
            numberOfAppsToProcess = 1
          } else if (numberOfAppsToProcess > 20) {
            numberOfAppsToProcess = 20
          }
          for (_ <- 0 until numberOfAppsToProcess) {
            // update time when monitor app so that
            // checkMonitorAppTimeoutThread can check whether the thread was blocked on monitoring
            monitorAppThreadMap.put(Thread.currentThread(), System.currentTimeMillis())
            val app = appQueue.poll()
            if (app != null) {
              app.monitorSparkKubernetesApp()
              if (app.isRunning) {
                appQueue.add(app)
              }
            }
          }
          Thread.sleep(pollInterval.toMillis)
        } catch {
          case e: InterruptedException =>
            error(s"Kubernetes app monitoring was interrupted.", e)
        }
      }
    }
  }

  private def initKubernetesAppMonitorThreadPool(livyConf: LivyConf): Unit = {
    val poolSize = livyConf.getInt(LivyConf.KUBERNETES_APP_LOOKUP_THREAD_POOL_SIZE)
    val KubernetesAppMonitorThreadPool: ExecutorService =
      Executors.newFixedThreadPool(poolSize)

    val runnable = new KubernetesAppMonitorRunnable()

    for (_ <- 0 until poolSize) {
      KubernetesAppMonitorThreadPool.execute(runnable)
    }
  }

  def getAppSize: Int = appQueue.size()

  def clearApps(): Unit = appQueue.clear()
}

class SparkKubernetesApp private[utils] (
  appTag: String,
  appIdOption: Option[String],
  process: Option[LineBufferedProcess],
  listener: Option[SparkAppListener],
  livyConf: LivyConf,
  kubernetesClient: => KubernetesClient = SparkKubernetesApp.kubernetesClient) // For unit test.
  extends SparkApp
    with Logging {

  import KubernetesExtensions._
  import SparkKubernetesApp._

  appQueue.add(this)
  private var killed = false
  private val appPromise: Promise[KubernetesApplication] = Promise()
  private[utils] var state: SparkApp.State = SparkApp.State.STARTING
  private var kubernetesDiagnostics: IndexedSeq[String] = IndexedSeq.empty[String]
  private var kubernetesAppLog: IndexedSeq[String] = IndexedSeq.empty[String]

  private var kubernetesTagToAppIdFailedTimes: Int = _
  private var kubernetesAppMonitorFailedTimes: Int = _

  private def failToMonitor(): Unit = {
    changeState(SparkApp.State.FAILED)
    process.foreach(_.destroy())
    leakedAppTags.put(appTag, System.currentTimeMillis())
  }

  private def failToGetAppId(): Unit = {
    kubernetesTagToAppIdFailedTimes += 1
    if (kubernetesTagToAppIdFailedTimes > appLookupMaxFailedTimes) {
      val msg = "No KUBERNETES application is found with tag " +
        s"${appTag.toLowerCase}. This may be because " +
        "1) spark-submit fail to submit application to KUBERNETES; " +
        "or 2) KUBERNETES cluster doesn't have enough resource to start the application in time. " +
        "Please check Livy log and KUBERNETES log to know the details."

      error(s"Failed monitoring the app $appTag: $msg")
      kubernetesDiagnostics = ArrayBuffer(msg)
      failToMonitor()
    }
  }

  private def monitorSparkKubernetesApp(): Unit = {
    try {
      if (killed) {
        changeState(SparkApp.State.KILLED)
      } else if (isProcessErrExit) {
        changeState(SparkApp.State.FAILED)
      }
      // Get KubernetesApplication by appTag.
      val appOption: Option[KubernetesApplication] = try {
        getAppFromTag(appTag, pollInterval, appLookupTimeout.fromNow)
      } catch {
        case e: Exception =>
          failToGetAppId()
          appPromise.failure(e)
          return
      }
      if (appOption.isEmpty) {
        failToGetAppId()
        return
      }
      val app: KubernetesApplication = appOption.get
      appPromise.trySuccess(app)
      val appId = app.getApplicationId

      Thread.currentThread().setName(s"kubernetesAppMonitorThread-$appId")
      listener.foreach(_.appIdKnown(appId))

      if (livyConf.getBoolean(LivyConf.KUBERNETES_INGRESS_CREATE)) {
        withRetry(kubernetesClient.createSparkUIIngress(app, livyConf))
      }

      var appInfo = AppInfo()

      // while loop is replaced with "if" condition so that another thread can process and continue
      if (isRunning) {
        try {
          Clock.sleep(pollInterval.toMillis)

          // Refresh application state
          val appReport = withRetry {
            debug(s"getApplicationReport, applicationId: ${app.getApplicationId}, " +
              s"namespace: ${app.getApplicationNamespace} " +
              s"applicationTag: ${app.getApplicationTag}")
            val report = kubernetesClient.getApplicationReport(livyConf, app,
              cacheLogSize = cacheLogSize)
            report
          }

          kubernetesAppLog = appReport.getApplicationLog
          kubernetesDiagnostics = appReport.getApplicationDiagnostics
          changeState(mapKubernetesState(appReport.getApplicationState, appTag))

          val latestAppInfo = AppInfo(
            appReport.getDriverLogUrl,
            appReport.getTrackingUrl,
            appReport.getExecutorsLogUrls
          )
          if (appInfo != latestAppInfo) {
            listener.foreach(_.infoChanged(latestAppInfo))
            appInfo = latestAppInfo
          }
        } catch {
          // TODO analyse available exceptions
          case e: Throwable =>
            error(s"Failed to refresh application state for $appTag.", e)
        }
      }

      kubernetesTagToAppIdFailedTimes = 0
      kubernetesAppMonitorFailedTimes = 0
      debug(s"$appId $state ${kubernetesDiagnostics.mkString(" ")}")
      Thread.currentThread().setName(s"appMonitorCommonThreadPool")
    } catch {
      case e: InterruptedException =>
        kubernetesAppMonitorFailedTimes += 1
        if (kubernetesAppMonitorFailedTimes > appLookupMaxFailedTimes) {
          error(s"Monitoring of the app $appTag was interrupted.", e)
          kubernetesDiagnostics = ArrayBuffer(e.getMessage)
          failToMonitor()
        }
      case NonFatal(e) =>
        error(s"Error while refreshing Kubernetes state", e)
        kubernetesDiagnostics = ArrayBuffer(e.getMessage)
        changeState(SparkApp.State.FAILED)
    } finally {
      if (!isRunning) {
        listener.foreach(_.infoChanged(AppInfo(sparkUiUrl = Option(buildHistoryServerUiUrl(
          livyConf, Try(appPromise.future.value.get.get.getApplicationId).getOrElse("unknown")
        )))))
      }
    }
  }

  override def log(): IndexedSeq[String] =
    ("stdout: " +: kubernetesAppLog) ++
      ("\nstderr: " +: (process.map(_.inputLines).getOrElse(ArrayBuffer.empty[String]) ++
        process.map(_.errorLines).getOrElse(ArrayBuffer.empty[String]))) ++
      ("\nKubernetes Diagnostics: " +: kubernetesDiagnostics)

  override def kill(): Unit = synchronized {
    killed = true

    if (!isRunning) {
      return
    }

    process.foreach(_.destroy())

    def applicationDetails: Option[Try[KubernetesApplication]] = appPromise.future.value
    if (applicationDetails.isEmpty) {
      leakedAppTags.put(appTag, System.currentTimeMillis())
      return
    }
    def kubernetesApplication: KubernetesApplication = applicationDetails.get.get
    if (kubernetesApplication != null && kubernetesApplication.getApplicationId != null) {
      try {
        withRetry(kubernetesClient.killApplication(
          Await.result(appPromise.future, appLookupTimeout)))
      } catch {
        // We cannot kill the Kubernetes app without the appTag.
        // There's a chance the Kubernetes app hasn't been submitted during a livy-server failure.
        // We don't want a stuck session that can't be deleted. Emit a warning and move on.
        case _: TimeoutException | _: InterruptedException =>
          warn("Deleting a session while its Kubernetes application is not found.")
      }
    } else {
      leakedAppTags.put(appTag, System.currentTimeMillis())
    }
  }

  private def isProcessErrExit: Boolean = {
    process.isDefined && !process.get.isAlive && process.get.exitValue() != 0
  }

  private def isRunning: Boolean = {
    state != SparkApp.State.FAILED &&
      state != SparkApp.State.FINISHED &&
      state != SparkApp.State.KILLED
  }

  private def changeState(newState: SparkApp.State.Value): Unit = {
    if (state != newState) {
      listener.foreach(_.stateChanged(state, newState))
      state = newState
    }
  }

  /**
    * Find the corresponding KubernetesApplication from an application tag.
    *
    * @param appTag The application tag tagged on the target application.
    *               If the tag is not unique, it returns the first application it found.
    * @return Option[KubernetesApplication] or the failure.
    */
  private def getAppFromTag(
    appTag: String,
    pollInterval: duration.Duration,
    deadline: Deadline): Option[KubernetesApplication] = {
    import KubernetesExtensions._

    withRetry(kubernetesClient.getApplications().find(_.getApplicationTag.contains(appTag)))
    match {
      case Some(app) => Some(app)
      case None =>
        if (deadline.isOverdue) {
          process.foreach(_.destroy())
          leakedAppTags.put(appTag, System.currentTimeMillis())
          throw new IllegalStateException(s"No Kubernetes application is found with tag" +
            s" $appTag in ${livyConf.getTimeAsMs(LivyConf.KUBERNETES_APP_LOOKUP_TIMEOUT) / 1000}" +
            " seconds. This may be because 1) spark-submit failed to submit application to " +
            "Kubernetes; or 2) Kubernetes cluster doesn't have enough resources to start the " +
            "application in time. Please check Livy log and Kubernetes log to know the details.")
        } else if (process.exists(p => !p.isAlive && p.exitValue() != 0)) {
          throw new IllegalStateException(s"Failed to submit Kubernetes application with tag" +
            s" $appTag. 'spark-submit' exited with non-zero status. " +
            s"Please check Livy log and Kubernetes log to know the details.")
        } else None
    }
  }

  // Exposed for unit test.
  private[utils] def mapKubernetesState(
    kubernetesAppState: String,
    appTag: String
  ): SparkApp.State.Value = {
    import KubernetesApplicationState._
    kubernetesAppState.toLowerCase match {
      case PENDING | CONTAINER_CREATING =>
        SparkApp.State.STARTING
      case RUNNING =>
        SparkApp.State.RUNNING
      case COMPLETED | SUCCEEDED =>
        SparkApp.State.FINISHED
      case FAILED | ERROR =>
        SparkApp.State.FAILED
      case other => // any other combination is invalid, so FAIL the application.
        error(s"Unknown Kubernetes state $other for app with tag $appTag.")
        SparkApp.State.FAILED
    }
  }

  private def buildHistoryServerUiUrl(livyConf: LivyConf, appId: String): String =
    s"${livyConf.get(LivyConf.UI_HISTORY_SERVER_URL)}/history/$appId/jobs/"

}

object KubernetesApplicationState {
  val PENDING = "pending"
  val CONTAINER_CREATING = "containercreating"
  val RUNNING = "running"
  val COMPLETED = "completed"
  val SUCCEEDED = "succeeded"
  val FAILED = "failed"
  val ERROR = "error"
}

object KubernetesConstants {
  val SPARK_APP_ID_LABEL = "spark-app-selector"
  val SPARK_APP_TAG_LABEL = "spark-app-tag"
  val SPARK_ROLE_LABEL = "spark-role"
  val SPARK_EXEC_ID_LABEL = "spark-exec-id"
  val SPARK_ROLE_DRIVER = "driver"
  val SPARK_ROLE_EXECUTOR = "executor"
  val SPARK_UI_PORT_NAME = "spark-ui"
  val CREATED_BY_LIVY_LABEL = Map("created-by" -> "livy")
}

class KubernetesApplication(driverPod: Pod) {

  import KubernetesConstants._

  private val appTag = driverPod.getMetadata.getLabels.get(SPARK_APP_TAG_LABEL)
  private val appId = driverPod.getMetadata.getLabels.get(SPARK_APP_ID_LABEL)
  private val namespace = driverPod.getMetadata.getNamespace

  def getApplicationTag: String = appTag

  def getApplicationId: String = appId

  def getApplicationNamespace: String = namespace

  def getApplicationPod: Pod = driverPod
}

private[utils] case class KubernetesAppReport(driver: Option[Pod], executors: Seq[Pod],
  appLog: IndexedSeq[String], ingress: Option[Ingress], livyConf: LivyConf) {

  import KubernetesConstants._

  private val grafanaUrl = livyConf.get(LivyConf.KUBERNETES_GRAFANA_URL)
  private val timeRange = livyConf.get(LivyConf.KUBERNETES_GRAFANA_TIME_RANGE)
  private val lokiDatasource = livyConf.get(LivyConf.KUBERNETES_GRAFANA_LOKI_DATASOURCE)
  private val sparkAppTagLogLabel = SPARK_APP_TAG_LABEL.replaceAll("-", "_")
  private val sparkRoleLogLabel = SPARK_ROLE_LABEL.replaceAll("-", "_")
  private val sparkExecIdLogLabel = SPARK_EXEC_ID_LABEL.replaceAll("-", "_")

  def getApplicationState: String =
    driver.map(getDriverState).getOrElse("unknown")

  // if 'KUBERNETES_SPARK_SIDECAR_ENABLED' is set
  // inspect the spark container status to figure out the termination status
  // if spark container cannot be detected, default to pod phase.
  def getDriverState(driverPod: Pod): String = {
    val podStatus = driverPod.getStatus
    val phase = podStatus.getPhase.toLowerCase
    // if not running with sidecars, just return the pod phase
    if (!livyConf.getBoolean(LivyConf.KUBERNETES_SPARK_SIDECAR_ENABLED)) {
      return phase
    }
    if (phase != KubernetesApplicationState.RUNNING) {
      return phase
    }
    // if the POD is still running, check spark container termination status
    // default to pod phase if container state is indeterminate.
    getTerminalState(podStatus).getOrElse(phase)
  }

  // if the spark container has terminated
  // try to figure out status based on termination status
  def getTerminalState(podStatus: PodStatus): Option[String] = {
    import scala.collection.JavaConverters._
    val sparkContainerName = livyConf.get(LivyConf.KUBERNETES_SPARK_CONTAINER_NAME)
    for (c <- podStatus.getContainerStatuses.asScala) {
      if (c.getName ==  sparkContainerName && c.getState.getTerminated != null) {
        val exitCode = c.getState.getTerminated.getExitCode
        if (exitCode == 0) {
          return Some(KubernetesApplicationState.SUCCEEDED)
        } else {
          return Some(KubernetesApplicationState.FAILED)
        }
      }
    }
    None
  }

  def getApplicationLog: IndexedSeq[String] = appLog

  def getDriverLogUrl: Option[String] = {
    if (livyConf.getBoolean(LivyConf.KUBERNETES_GRAFANA_LOKI_ENABLED)) {
      val appTag = driver.map(_.getMetadata.getLabels.get(SPARK_APP_TAG_LABEL))
      if (appTag.isDefined && appTag.get != null) {
        return Some(
          s"""$grafanaUrl/explore?left=""" + URLEncoder.encode(
            s"""["now-$timeRange","now","$lokiDatasource",""" +
              s"""{"expr":"{$sparkAppTagLogLabel=\\"${appTag.get}\\",""" +
              s"""$sparkRoleLogLabel=\\"$SPARK_ROLE_DRIVER\\"}"},""" +
              s"""{"ui":[true,true,true,"exact"]}]""", "UTF-8")
        )
      }
    }
    None
  }

  def getExecutorsLogUrls: Option[String] = {
    if (livyConf.getBoolean(LivyConf.KUBERNETES_GRAFANA_LOKI_ENABLED)) {
      val urls = executors.map(_.getMetadata.getLabels).flatMap(labels => {
        val sparkAppTag = labels.get(SPARK_APP_TAG_LABEL)
        val sparkExecId = labels.get(SPARK_EXEC_ID_LABEL)
        if (sparkAppTag != null && sparkExecId != null) {
          val sparkRole = labels.getOrDefault(SPARK_ROLE_LABEL, SPARK_ROLE_EXECUTOR)
          Some(s"executor-$sparkExecId#$grafanaUrl/explore?left=" + URLEncoder.encode(
            s"""["now-$timeRange","now","$lokiDatasource",""" +
              s"""{"expr":"{$sparkAppTagLogLabel=\\"$sparkAppTag\\",""" +
              s"""$sparkRoleLogLabel=\\"$sparkRole\\",""" +
              s"""$sparkExecIdLogLabel=\\"$sparkExecId\\"}"},""" +
              s"""{"ui":[true,true,true,"exact"]}]""", "UTF-8"))
        } else {
          None
        }
      })
      if (urls.nonEmpty) return Some(urls.mkString(";"))
    }
    None
  }

  def getTrackingUrl: Option[String] = {
    val host = ingress.flatMap(i => Try(i.getSpec.getRules.get(0).getHost).toOption)
    val path = driver
      .map(_.getMetadata.getLabels.getOrDefault(SPARK_APP_TAG_LABEL, "unknown"))
    val protocol = livyConf.get(LivyConf.KUBERNETES_INGRESS_PROTOCOL)
    if (host.isDefined && path.isDefined) Some(s"$protocol://${host.get}/${path.get}")
    else None
  }

  def getApplicationDiagnostics: IndexedSeq[String] = {
    (Seq(driver) ++ executors.sortBy(_.getMetadata.getName).map(Some(_)))
      .filter(_.nonEmpty)
      .map(opt => buildSparkPodDiagnosticsPrettyString(opt.get))
      .flatMap(_.split("\n")).toIndexedSeq
  }

  private def buildSparkPodDiagnosticsPrettyString(pod: Pod): String = {
    import scala.collection.JavaConverters._
    def printMap(map: Map[_, _]): String = map.map {
      case (key, value) => s"$key=$value"
    }.mkString(", ")

    if (pod == null) return "unknown"

    s"${pod.getMetadata.getName}.${pod.getMetadata.getNamespace}:" +
      s"\n\tnode: ${pod.getSpec.getNodeName}" +
      s"\n\thostname: ${pod.getSpec.getHostname}" +
      s"\n\tpodIp: ${pod.getStatus.getPodIP}" +
      s"\n\tstartTime: ${pod.getStatus.getStartTime}" +
      s"\n\tphase: ${pod.getStatus.getPhase}" +
      s"\n\treason: ${pod.getStatus.getReason}" +
      s"\n\tmessage: ${pod.getStatus.getMessage}" +
      s"\n\tlabels: ${printMap(pod.getMetadata.getLabels.asScala.toMap)}" +
      s"\n\tcontainers:" +
      s"\n\t\t${
        pod.getSpec.getContainers.asScala.map(container =>
          s"${container.getName}:" +
            s"\n\t\t\timage: ${container.getImage}" +
            s"\n\t\t\trequests: ${printMap(container.getResources.getRequests.asScala.toMap)}" +
            s"\n\t\t\tlimits: ${printMap(container.getResources.getLimits.asScala.toMap)}" +
            s"\n\t\t\tcommand: ${container.getCommand} ${container.getArgs}"
        ).mkString("\n\t\t")
      }" +
      s"\n\tconditions:" +
      s"\n\t\t${pod.getStatus.getConditions.asScala.mkString("\n\t\t")}"
  }

}

private[utils] object KubernetesExtensions {
  import KubernetesConstants._

  implicit class KubernetesClientExtensions(client: KubernetesClient) {
    import scala.collection.JavaConverters._

    private val NGINX_CONFIG_SNIPPET: String =
      """
        |proxy_set_header Accept-Encoding "";
        |sub_filter_last_modified off;
        |sub_filter_once off;
        |sub_filter_types text/html text/css text/javascript application/javascript;
      """.stripMargin

    def getApplications(
      labels: Map[String, String] = Map(SPARK_ROLE_LABEL -> SPARK_ROLE_DRIVER),
      appTagLabel: String = SPARK_APP_TAG_LABEL,
      appIdLabel: String = SPARK_APP_ID_LABEL
    ): Seq[KubernetesApplication] = {
      client.pods.inAnyNamespace
        .withLabels(labels.asJava)
        .withLabel(appTagLabel)
        .withLabel(appIdLabel)
        .list.getItems.asScala.map(new KubernetesApplication(_))
    }

    def killApplication(app: KubernetesApplication): Boolean = {
      client.pods.inAnyNamespace.delete(app.getApplicationPod)
    }

    def getApplicationReport(
      livyConf: LivyConf,
      app: KubernetesApplication,
      cacheLogSize: Int,
      appTagLabel: String = SPARK_APP_TAG_LABEL
    ): KubernetesAppReport = {
      val pods = client.pods.inNamespace(app.getApplicationNamespace)
        .withLabels(Map(appTagLabel -> app.getApplicationTag).asJava)
        .list.getItems.asScala
      val driver = pods.find(_.getMetadata.getLabels.get(SPARK_ROLE_LABEL) == SPARK_ROLE_DRIVER)
      val executors =
        pods.filter(_.getMetadata.getLabels.get(SPARK_ROLE_LABEL) == SPARK_ROLE_EXECUTOR)
      val appLog = Try(
        client.pods.inNamespace(app.getApplicationNamespace)
          .withName(app.getApplicationPod.getMetadata.getName)
          .tailingLines(cacheLogSize).getLog.split("\n").toIndexedSeq
      ).getOrElse(IndexedSeq.empty)
      val ingress = client.network.v1.ingresses.inNamespace(app.getApplicationNamespace)
        .withLabel(SPARK_APP_TAG_LABEL, app.getApplicationTag)
        .list.getItems.asScala.headOption
      KubernetesAppReport(driver, executors, appLog, ingress, livyConf)
    }

    def createSparkUIIngress(app: KubernetesApplication, livyConf: LivyConf): Unit = {
      val annotationsString = livyConf.get(LivyConf.KUBERNETES_INGRESS_ADDITIONAL_ANNOTATIONS)
      var annotations: Seq[(String, String)] = Seq.empty
      if (annotationsString != null && annotationsString.trim.nonEmpty) {
        annotations = annotationsString
          .split(";").map(_.split("="))
          .map(array => array.head -> array.tail.mkString("=")).toSeq
      }

      val sparkUIIngress = buildSparkUIIngress(
        app,
        livyConf.get(LivyConf.KUBERNETES_INGRESS_CLASS_NAME),
        livyConf.get(LivyConf.KUBERNETES_INGRESS_PROTOCOL),
        livyConf.get(LivyConf.KUBERNETES_INGRESS_HOST),
        livyConf.get(LivyConf.KUBERNETES_INGRESS_TLS_SECRET_NAME),
        livyConf.get(LivyConf.KUBERNETES_INGRESS_ADDITIONAL_CONF_SNIPPET),
        annotations: _*
      )
      val resources: Seq[HasMetadata] = Seq(sparkUIIngress)
      addOwnerReference(app.getApplicationPod, resources: _*)
      client.network.v1.ingresses.inNamespace(app.getApplicationNamespace).
        createOrReplace(sparkUIIngress)
    }

    private[utils] def buildSparkUIIngress(
      app: KubernetesApplication, className: String, protocol: String, host: String,
      tlsSecretName: String, additionalConfSnippet: String, additionalAnnotations: (String, String)*
    ): Ingress = {
      val appTag = app.getApplicationTag
      val serviceHost = s"${getServiceName(app)}.${app.getApplicationNamespace}.svc.cluster.local"

      // Common annotations
      val annotations = Map(
        "nginx.ingress.kubernetes.io/rewrite-target" -> "/$1",
        "nginx.ingress.kubernetes.io/proxy-redirect-to" -> s"/$appTag/",
        "nginx.ingress.kubernetes.io/proxy-redirect-from" -> s"http://$serviceHost/",
        "nginx.ingress.kubernetes.io/upstream-vhost" -> s"$serviceHost",
        "nginx.ingress.kubernetes.io/service-upstream" -> "true",
        "nginx.ingress.kubernetes.io/x-forwarded-prefix" -> s"/$appTag",
        "nginx.ingress.kubernetes.io/configuration-snippet" ->
          NGINX_CONFIG_SNIPPET.concat(additionalConfSnippet)
      ) ++ additionalAnnotations

      val builder = new IngressBuilder()
        .withApiVersion("networking.k8s.io/v1")
        .withNewMetadata()
        .withName(getServiceName(app))
        .withNamespace(app.getApplicationNamespace)
        .addToAnnotations(annotations.asJava)
        .addToLabels(SPARK_APP_TAG_LABEL, appTag)
        .addToLabels(CREATED_BY_LIVY_LABEL.asJava)
        .endMetadata()
        .withNewSpec()
        .withIngressClassName(className)
        .addNewRule()
        .withHost(host)
        .withNewHttp()
        .addNewPath()
        .withPath(s"/$appTag/?(.*)")
        .withPathType("ImplementationSpecific")
        .withNewBackend()
        .withNewService()
        .withName(getServiceName(app))
        .withNewPort()
        .withName(SPARK_UI_PORT_NAME).endPort()
        .endService()
        .endBackend()
        .endPath()
        .endHttp()
        .endRule()
      if (protocol.endsWith("s") && tlsSecretName != null && tlsSecretName.nonEmpty) {
        builder.addNewTl().withSecretName(tlsSecretName).addToHosts(host).endTl()
      }
      builder.endSpec().build()
    }

    private def getServiceName(app: KubernetesApplication): String =
      StringUtils.stripEnd(
        StringUtils.left(s"${app.getApplicationPod.getMetadata.getName}-svc", 63), "-"
      ).toLowerCase

    // Add a OwnerReference to the given resources making the driver pod an owner of them so when
    // the driver pod is deleted, the resources are garbage collected.
    private def addOwnerReference(owner: Pod, resources: HasMetadata*): Unit = {
      val driverPodOwnerReference = new OwnerReferenceBuilder()
        .withName(owner.getMetadata.getName)
        .withApiVersion(owner.getApiVersion)
        .withUid(owner.getMetadata.getUid)
        .withKind(owner.getKind)
        .withController(true)
        .build()
      resources.foreach {
        resource =>
          val originalMetadata = resource.getMetadata
          originalMetadata.setOwnerReferences(Collections.singletonList(driverPodOwnerReference))
      }
    }

  }

}

private[utils] object KubernetesClientFactory {
  import java.io.File
  import com.google.common.base.Charsets
  import com.google.common.io.Files

  private implicit class OptionString(val string: String) extends AnyVal {
    def toOption: Option[String] = if (string == null || string.isEmpty) None else Option(string)
  }

  def createKubernetesClient(livyConf: LivyConf): DefaultKubernetesClient = {
    val masterUrl = sparkMasterToKubernetesApi(livyConf.sparkMaster())

    val oauthTokenFile = livyConf.get(LivyConf.KUBERNETES_OAUTH_TOKEN_FILE).toOption
    val oauthTokenValue = livyConf.get(LivyConf.KUBERNETES_OAUTH_TOKEN_VALUE).toOption
    require(oauthTokenFile.isEmpty || oauthTokenValue.isEmpty,
      s"Cannot specify OAuth token through both " +
        s"a file $oauthTokenFile and a value $oauthTokenValue.")

    val caCertFile = livyConf.get(LivyConf.KUBERNETES_CA_CERT_FILE).toOption
    val clientKeyFile = livyConf.get(LivyConf.KUBERNETES_CLIENT_KEY_FILE).toOption
    val clientCertFile = livyConf.get(LivyConf.KUBERNETES_CLIENT_CERT_FILE).toOption

    val config = new ConfigBuilder()
      .withApiVersion("v1")
      .withMasterUrl(masterUrl)
      .withOption(oauthTokenValue) {
        (token, configBuilder) => configBuilder.withOauthToken(token)
      }
      .withOption(oauthTokenFile) {
        (file, configBuilder) =>
          configBuilder
            .withOauthToken(Files.toString(new File(file), Charsets.UTF_8))
      }
      .withOption(caCertFile) {
        (file, configBuilder) => configBuilder.withCaCertFile(file)
      }
      .withOption(clientKeyFile) {
        (file, configBuilder) => configBuilder.withClientKeyFile(file)
      }
      .withOption(clientCertFile) {
        (file, configBuilder) => configBuilder.withClientCertFile(file)
      }
      .build()
    new DefaultKubernetesClient(config)
  }

  def sparkMasterToKubernetesApi(sparkMaster: String): String = {
    val replaced = sparkMaster.replaceFirst("k8s://", "")
    if (!replaced.startsWith("http")) s"https://$replaced"
    else replaced
  }

  private implicit class OptionConfigurableConfigBuilder(
    val configBuilder: ConfigBuilder) extends AnyVal {
    def withOption[T]
    (option: Option[T])
      (configurator: (T, ConfigBuilder) => ConfigBuilder): ConfigBuilder = {
      option.map {
        opt => configurator(opt, configBuilder)
      }.getOrElse(configBuilder)
    }
  }

}
