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

import java.util.concurrent.TimeoutException

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.client._

import org.apache.livy.{LivyConf, Logging, Utils}

object SparkKubernetesApp extends Logging {

  lazy val kubernetesClient: LivyKubernetesClient =
    KubernetesClientFactory.createKubernetesClient(livyConf)

  private val RETRY_BACKOFF_MILLIS = 1000
  private val leakedAppTags = new java.util.concurrent.ConcurrentHashMap[String, Long]()

  private val leakedAppsGCThread = new Thread() {
    override def run(): Unit = {
      while (true) {
        if (!leakedAppTags.isEmpty) {
          // kill the app if found it or remove it if exceeding a threshold
          val leakedApps = leakedAppTags.entrySet().iterator()
          val now = System.currentTimeMillis()
          try {
            val apps = withRetry(kubernetesClient.getApplications()).groupBy(_.getApplicationTag)
            while (leakedApps.hasNext) {
              val leakedApp = leakedApps.next()
              apps.get(leakedApp.getKey) match {
                case Some(seq) =>
                  seq.foreach(app =>
                    if (withRetry(kubernetesClient.killApplication(app))) {
                      leakedApps.remove()
                      info(s"Killed leaked app with tag ${leakedApp.getKey}")
                    } else {
                      warn(s"Leaked app with tag ${leakedApp.getKey} haven't been killed")
                    }
                  )
                case None if (leakedApp.getValue - now) > sessionLeakageCheckTimeout =>
                  leakedApps.remove()
                  warn(s"Leaked app with tag ${leakedApp.getKey} doesn't exist")
              }
            }
          } catch {
            case e: KubernetesClientException =>
              error("Kubernetes client failure", e)
            case NonFatal(e) =>
              error("Failed to remove leaked apps", e)
          }
        }
        Thread.sleep(sessionLeakageCheckInterval)
      }
    }
  }

  private var livyConf: LivyConf = _
  private var sessionLeakageCheckTimeout: Long = _
  private var sessionLeakageCheckInterval: Long = _

  def init(livyConf: LivyConf): Unit = {
    this.livyConf = livyConf
    sessionLeakageCheckInterval =
      livyConf.getTimeAsMs(LivyConf.KUBERNETES_APP_LEAKAGE_CHECK_INTERVAL)
    sessionLeakageCheckTimeout = livyConf.getTimeAsMs(LivyConf.KUBERNETES_APP_LEAKAGE_CHECK_TIMEOUT)
    leakedAppsGCThread.setDaemon(true)
    leakedAppsGCThread.setName("LeakedAppsGCThread")
    leakedAppsGCThread.start()
  }

  // Returning T, throwing the exception on failure
  @tailrec
  private def withRetry[T](fn: => T, retries: Int = 3): T = {
    Try { fn } match {
      case Success(x) => x
      case _ if retries > 1 =>
        Thread.sleep(RETRY_BACKOFF_MILLIS)
        withRetry(fn, retries - 1)
      case Failure(e) => throw e
    }
  }

  private[utils] def mapKubernetesState(
      kubernetesAppState: String,
      appTag: String): SparkApp.State.Value = {
    import KubernetesApplicationState._
    kubernetesAppState.toLowerCase match {
      case PENDING =>
        SparkApp.State.STARTING
      case RUNNING =>
        SparkApp.State.RUNNING
      case SUCCEEDED =>
        SparkApp.State.FINISHED
      case FAILED =>
        SparkApp.State.FAILED
      case other => // any other combination is invalid, so FAIL the application.
        error(s"Unknown Kubernetes state $other for app with tag $appTag")
        SparkApp.State.FAILED
    }
  }

  private[utils] object KubernetesApplicationState {
    val PENDING = "pending"
    val RUNNING = "running"
    val SUCCEEDED = "succeeded"
    val FAILED = "failed"
  }
}

class SparkKubernetesApp private[utils](
    appTag: String,
    appIdOption: Option[String],
    process: Option[LineBufferedProcess],
    listener: Option[SparkAppListener],
    livyConf: LivyConf,
    // For unit tests
    kubernetesClient: => LivyKubernetesClient = SparkKubernetesApp.kubernetesClient)
  extends SparkApp with Logging {

  import SparkKubernetesApp._

  private val appLookupTimeout =
    livyConf.getTimeAsMs(LivyConf.KUBERNETES_APP_LOOKUP_TIMEOUT).milliseconds
  private val cacheLogSize = livyConf.getInt(LivyConf.SPARK_LOGS_SIZE)
  private val pollInterval = livyConf.getTimeAsMs(LivyConf.KUBERNETES_POLL_INTERVAL).milliseconds

  private var kubernetesAppLog: IndexedSeq[String] = IndexedSeq.empty[String]
  private var kubernetesDiagnostics: IndexedSeq[String] = IndexedSeq.empty[String]

  private var state: SparkApp.State = SparkApp.State.STARTING
  private val appPromise: Promise[KubernetesApplication] = Promise()

  private[utils] val kubernetesAppMonitorThread = Utils
    .startDaemonThread(s"kubernetesAppMonitorThread-$this") {
      try {
        val app = try {
          getAppFromTag(appTag, pollInterval, appLookupTimeout.fromNow)
        } catch {
          case e: Exception =>
            appPromise.failure(e)
            throw e
        }
        appPromise.success(app)
        val appId = app.getApplicationId

        Thread.currentThread().setName(s"kubernetesAppMonitorThread-$appTag")
        listener.foreach(_.appIdKnown(appId))

        while (isRunning) {
          val appReport = withRetry(kubernetesClient.getApplicationReport(app, cacheLogSize))
          kubernetesAppLog = appReport.getApplicationLog
          kubernetesDiagnostics = appReport.getApplicationDiagnostics
          changeState(mapKubernetesState(appReport.getApplicationState, appTag))

          Clock.sleep(pollInterval.toMillis)
        }
        debug(s"Application $appId is in state $state\nDiagnostics:" +
          s"\n${kubernetesDiagnostics.mkString("\n")}")
      } catch {
        case _: InterruptedException =>
          kubernetesDiagnostics = ArrayBuffer("Application stopped by user.")
          changeState(SparkApp.State.KILLED)
        case NonFatal(e) =>
          error("Couldn't refresh Kubernetes state", e)
          kubernetesDiagnostics = ArrayBuffer(e.getMessage)
          changeState(SparkApp.State.FAILED)
      } finally {
        info(s"Finished monitoring application $appTag with state $state")
      }
    }

  override def log(): IndexedSeq[String] = {
    ("stdout: " +: kubernetesAppLog) ++
      ("\nstderr: " +: (process.map(_.inputLines).getOrElse(ArrayBuffer.empty[String]) ++
        process.map(_.errorLines).getOrElse(ArrayBuffer.empty[String]))) ++
      ("\nKubernetes Diagnostics: " +: kubernetesDiagnostics)
  }

  override def kill(): Unit = {
    try {
      withRetry {
        kubernetesClient.killApplication(Await.result(appPromise.future, appLookupTimeout))
      }
    } catch {
      // We cannot kill the Kubernetes app without the appTag.
      // There's a chance the Kubernetes app hasn't been submitted during a livy-server failure.
      // We don't want a stuck session that can't be deleted. Emit a warning and move on.
      case _: TimeoutException | _: InterruptedException =>
        warn("Attempted to delete a session while its Kubernetes application is not found.")
        kubernetesAppMonitorThread.interrupt()
    } finally {
      process.foreach(_.destroy())
    }
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
   * @return KubernetesApplication or the failure.
   */
  @tailrec
  private def getAppFromTag(
      appTag: String,
      pollInterval: Duration,
      deadline: Deadline): KubernetesApplication = {
    withRetry(kubernetesClient.getApplications().find(_.getApplicationTag.contains(appTag)))
    match {
      case Some(app) => app
      case None =>
        if (deadline.isOverdue) {
          process.foreach(_.destroy())
          leakedAppTags.put(appTag, System.currentTimeMillis())
          throw new IllegalStateException("No Kubernetes application is found with tag" +
            s" $appTag in ${livyConf.getTimeAsMs(LivyConf.KUBERNETES_APP_LOOKUP_TIMEOUT) / 1000}" +
            " seconds. This may be because 1) spark-submit fail to submit application to " +
            "Kubernetes; or 2) Kubernetes cluster doesn't have enough resources to start the " +
            "application in time. Please check Livy log and Kubernetes log to know the details.")
        } else {
          Clock.sleep(pollInterval.toMillis)
          getAppFromTag(appTag, pollInterval, deadline)
        }
    }
  }
}

object KubernetesConstants {
  val CREATED_BY_ANNOTATION = "created-by"

  val SPARK_APP_ID_LABEL = "spark-app-selector"
  val SPARK_APP_TAG_LABEL = "spark-app-tag"
  val SPARK_ROLE_LABEL = "spark-role"
  val SPARK_EXEC_ID_LABEL = "spark-exec-id"

  val SPARK_ROLE_DRIVER = "driver"
  val SPARK_ROLE_EXECUTOR = "executor"
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

private[utils] case class KubernetesAppReport(
    driver: Option[Pod],
    executors: Seq[Pod],
    appLog: IndexedSeq[String]) {

  def getApplicationState: String = {
    driver.map(_.getStatus.getPhase.toLowerCase).getOrElse("unknown")
  }

  def getApplicationLog: IndexedSeq[String] = appLog

  def getApplicationDiagnostics: IndexedSeq[String] = {
    (Seq(driver) ++ executors.sortBy(_.getMetadata.getName).map(Some(_)))
      .filter(_.nonEmpty)
      .map(buildSparkPodDiagnosticsPrettyString)
      .flatMap(_.split("\n")).toIndexedSeq
  }

  private def buildSparkPodDiagnosticsPrettyString(podOption: Option[Pod]): String = {
    import scala.collection.JavaConverters._
    def printMap(map: Map[_, _]): String = {
      map.map {
        case (key, value) => s"$key=$value"
      }.mkString(", ")
    }

    if (podOption.isEmpty) return "unknown"
    val pod = podOption.get

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

private[utils] class LivyKubernetesClient(
    client: DefaultKubernetesClient, namespaces: Set[String] = Set.empty) {

  import KubernetesConstants._
  import scala.collection.JavaConverters._

  def getApplications(
      labels: Map[String, String] = Map(SPARK_ROLE_LABEL -> SPARK_ROLE_DRIVER),
      appTagLabel: String = SPARK_APP_TAG_LABEL,
      appIdLabel: String = SPARK_APP_ID_LABEL): Seq[KubernetesApplication] = {
    Option(namespaces).filter(_.nonEmpty)
      .map(_.map(client.inNamespace))
      .getOrElse(Seq(client.inAnyNamespace()))
      .map(_.pods
        .withLabels(labels.asJava)
        .withLabel(appTagLabel)
        .withLabel(appIdLabel)
        .list.getItems.asScala.map(new KubernetesApplication(_)))
      .reduce(_ ++ _)
  }

  def killApplication(app: KubernetesApplication): Boolean = {
    client.inNamespace(app.getApplicationNamespace).pods.delete(app.getApplicationPod)
  }

  def getApplicationReport(
      app: KubernetesApplication,
      cacheLogSize: Int,
      appTagLabel: String = SPARK_APP_TAG_LABEL): KubernetesAppReport = {
    val pods = client.inNamespace(app.getApplicationNamespace).pods
      .withLabels(Map(appTagLabel -> app.getApplicationTag).asJava)
      .list.getItems.asScala
    val driver = pods.find(isDriver)
    val executors = pods.filter(isExecutor)
    val appLog = getApplicationLog(app, cacheLogSize)
    KubernetesAppReport(driver, executors, appLog)
  }

  private def getApplicationLog(
      app: KubernetesApplication, cacheLogSize: Int): IndexedSeq[String] = {
    Try(
      client.inNamespace(app.getApplicationNamespace).pods
        .withName(app.getApplicationPod.getMetadata.getName)
        .tailingLines(cacheLogSize).getLog.split("\n").toIndexedSeq
    ).getOrElse(IndexedSeq.empty)
  }

  private def isDriver: Pod => Boolean = {
    _.getMetadata.getLabels.get(SPARK_ROLE_LABEL) == SPARK_ROLE_DRIVER
  }

  private def isExecutor: Pod => Boolean = {
    _.getMetadata.getLabels.get(SPARK_ROLE_LABEL) == SPARK_ROLE_EXECUTOR
  }

  def getDefaultNamespace: String = client.getNamespace
}

private[utils] object KubernetesClientFactory {

  import java.io.File

  import com.google.common.base.Charsets
  import com.google.common.io.Files
  import io.fabric8.kubernetes.client.ConfigBuilder

  def createKubernetesClient(livyConf: LivyConf): LivyKubernetesClient = {
    val masterUrl = sparkMasterToKubernetesApi(livyConf.sparkMaster())

    val oauthTokenFile = livyConf.get(LivyConf.KUBERNETES_OAUTH_TOKEN_FILE).toOption
    val oauthTokenValue = livyConf.get(LivyConf.KUBERNETES_OAUTH_TOKEN_VALUE).toOption
    require(oauthTokenFile.isEmpty || oauthTokenValue.isEmpty,
      "Cannot specify OAuth token through both " +
        s"a file $oauthTokenFile and a value $oauthTokenValue.")

    val caCertFile = livyConf.get(LivyConf.KUBERNETES_CA_CERT_FILE).toOption
    val clientKeyFile = livyConf.get(LivyConf.KUBERNETES_CLIENT_KEY_FILE).toOption
    val clientCertFile = livyConf.get(LivyConf.KUBERNETES_CLIENT_CERT_FILE).toOption
    val clientNamespace = livyConf.get(LivyConf.KUBERNETES_DEFAULT_NAMESPACE).toOption

    val config = new ConfigBuilder()
      .withApiVersion("v1")
      .withMasterUrl(masterUrl)
      .withOption(oauthTokenValue) {
        (token, builder) => builder.withOauthToken(token)
      }
      .withOption(oauthTokenFile) {
        (file, builder) => builder.withOauthToken(Files.toString(new File(file), Charsets.UTF_8))
      }
      .withOption(caCertFile) {
        (file, builder) => builder.withCaCertFile(file)
      }
      .withOption(clientKeyFile) {
        (file, builder) => builder.withClientKeyFile(file)
      }
      .withOption(clientCertFile) {
        (file, builder) => builder.withClientCertFile(file)
      }
      .withOption(clientNamespace) {
        (namespace, builder) => builder.withNamespace(namespace)
      }
      .build()
    new LivyKubernetesClient(
      new DefaultKubernetesClient(config), livyConf.getKubernetesNamespaces())
  }

  private[utils] def sparkMasterToKubernetesApi(sparkMaster: String): String = {
    val replaced = sparkMaster.replaceFirst("k8s://", "")
    if (!replaced.startsWith("http")) s"https://$replaced"
    else replaced
  }

  implicit class OptionString(val string: String) extends AnyVal {
    def toOption: Option[String] = {
      if (string == null || string.trim.isEmpty) None
      else Some(string)
    }
  }

  private implicit class OptionConfigurableConfigBuilder(val configBuilder: ConfigBuilder)
    extends AnyVal {

    def withOption[T](option: Option[T])
      (configurator: (T, ConfigBuilder) => ConfigBuilder): ConfigBuilder = {
      option.map(configurator(_, configBuilder)).getOrElse(configBuilder)
    }
  }
}
