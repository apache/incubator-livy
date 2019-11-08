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

import java.util.concurrent.Executors

import scala.collection.concurrent.TrieMap
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.language.postfixOps
import scala.util.Try
import scala.util.control.NonFatal

import org.apache.hadoop.yarn.api.records.{ApplicationId, ApplicationReport, FinalApplicationStatus, YarnApplicationState}
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException
import org.apache.hadoop.yarn.util.ConverterUtils

import org.apache.livy.{LivyConf, Logging}

object SparkYarnApp extends Logging {

  def init(livyConf: LivyConf, client: Option[YarnClient] = None): Unit = {
    mockYarnClient = client
    sessionLeakageCheckInterval = livyConf.getTimeAsMs(LivyConf.YARN_APP_LEAKAGE_CHECK_INTERVAL)
    sessionLeakageCheckTimeout = livyConf.getTimeAsMs(LivyConf.YARN_APP_LEAKAGE_CHECK_TIMEOUT)

    yarnPollInterval = livyConf.getTimeAsMs(LivyConf.YARN_POLL_INTERVAL)
    yarnAppMonitorTimeout = livyConf.getTimeAsMs(LivyConf.YARN_APP_MONITOR_TIMEOUT)
    yarnAppMonitorMaxFailedTimes = livyConf.getTimeAsMs(LivyConf.YARN_APP_MONITOR_MAX_FAILED_TIMES)
    yarnAppLookUpInterval = livyConf.getTimeAsMs(LivyConf.YARN_APP_LOOKUP_INTERVAL)

    leakedAppsGCThread.setDaemon(true)
    leakedAppsGCThread.setName("LeakedAppsGCThread")
    leakedAppsGCThread.start()

    yarnAppMonitorThread.setDaemon(true)
    yarnAppMonitorThread.setName("YarnAppMonitorThread")
    yarnAppMonitorThread.start()
  }

  private var mockYarnClient: Option[YarnClient] = None

  // YarnClient is thread safe. Create once, share it across threads.
  lazy val yarnClient = {
    val c = YarnClient.createYarnClient()
    c.init(new YarnConfiguration())
    c.start()
    c
  }

  private[utils] val appType = Set("SPARK").asJava

  private[utils] val leakedAppTags = new java.util.concurrent.ConcurrentHashMap[String, Long]()

  private[utils] val appMap = new TrieMap[SparkYarnApp, String]()

  private var sessionLeakageCheckTimeout: Long = _

  private var sessionLeakageCheckInterval: Long = _

  private var yarnPollInterval: Long = _

  private var yarnAppMonitorTimeout: Long = _

  private var yarnAppMonitorMaxFailedTimes: Long = _

  private var yarnAppLookUpInterval: Long = _

  private val leakedAppsGCThread = new Thread() {
    override def run(): Unit = {
      val client = {
        mockYarnClient match {
          case Some(client) => client
          case None => yarnClient
        }
      }

      while (true) {
        if (!leakedAppTags.isEmpty) {
          // kill the app if found it and remove it if exceeding a threshold
          val iter = leakedAppTags.entrySet().iterator()
          val now = System.currentTimeMillis()
          val apps = client.getApplications(appType).asScala

          while(iter.hasNext) {
            var isRemoved = false
            val entry = iter.next()

            apps.find(_.getApplicationTags.contains(entry.getKey))
              .foreach({ e =>
                info(s"Kill leaked app ${e.getApplicationId}")
                client.killApplication(e.getApplicationId)
                iter.remove()
                isRemoved = true
              })

            if (!isRemoved) {
              if ((now - entry.getValue) > sessionLeakageCheckTimeout) {
                iter.remove()
                info(s"Remove leaked yarn app tag ${entry.getKey}")
              }
            }
          }
        }
        Thread.sleep(sessionLeakageCheckInterval)
      }
    }
  }

  private val yarnAppMonitorThread = new Thread() {
    override def run() : Unit = {
      val executor = Executors.newSingleThreadExecutor
      while (true) {
        for ((app, appTag) <- appMap) {
          Future {
            app.monitorLock.synchronized {
              val handler = executor.submit(new Runnable {
                override def run(): Unit = {
                  app.monitorSparkYarnApp()
                }
              })
              try {
                // prevent the rpc block of one app from blocking all apps
                handler.get(yarnAppMonitorTimeout, MILLISECONDS)
              } catch {
                case e: Exception => {
                  handler.cancel(true)
                  error(s"monitor app: ${appTag} exception:", e)
                }
              }
              if (!app.isRunning) {
                appMap -= app
              }
            }
          }
        }
        Thread.sleep(yarnPollInterval)
      }
    }
  }
}

/**
 * Provide a class to control a Spark application using YARN API.
 *
 * @param appTag An app tag that can unique identify the YARN app.
 * @param appIdOption The appId of the YARN app. If this's None, SparkYarnApp will find it
 *                    using appTag.
 * @param process The spark-submit process launched the YARN application. This is optional.
 *                If it's provided, SparkYarnApp.log() will include its log.
 * @param listener Optional listener for notification of appId discovery and app state changes.
 */
class SparkYarnApp private[utils] (
    appTag: String,
    appIdOption: Option[String],
    process: Option[LineBufferedProcess],
    listener: Option[SparkAppListener],
    livyConf: LivyConf,
    yarnClient: => YarnClient = SparkYarnApp.yarnClient) // For unit test.
  extends SparkApp
  with Logging {
  import SparkYarnApp._

  appMap.put(this, appTag)

  private var killed = false
  private[utils] var state: SparkApp.State = SparkApp.State.STARTING
  private var yarnDiagnostics: IndexedSeq[String] = IndexedSeq.empty[String]
  private var appInfo = AppInfo()
  private var appId: Option[ApplicationId] = None

  override def log(): IndexedSeq[String] =
    ("stdout: " +: process.map(_.inputLines).getOrElse(ArrayBuffer.empty[String])) ++
    ("\nstderr: " +: process.map(_.errorLines).getOrElse(ArrayBuffer.empty[String])) ++
    ("\nYARN Diagnostics: " +: yarnDiagnostics)

  override def kill(): Unit = synchronized {
    killed = true

    if (!isRunning) {
      return
    }

    process.foreach(_.destroy())

    if (appId.isDefined) {
      yarnClient.killApplication(appId.get)
    } else {
      Future {
        val deadline = getYarnTagToAppIdTimeout(livyConf).fromNow
        while (!deadline.isOverdue()) {
          try {
            appId = Some(getAppId())
            yarnClient.killApplication(appId.get)
            return
          } catch {
            case e: Exception =>
              if (!deadline.isOverdue()) {
                Thread.sleep(yarnAppLookUpInterval)
              } else {
                // We cannot kill the YARN app without the app id.
                // There's a chance the YARN app hasn't been submitted during a livy-server failure.
                // We don't want a stuck session that can't be deleted. Emit a warning and move on.
                warn("Deleting a session while its YARN application is not found.")
              }
          }
        }
      }
    }
  }

  private def isProcessErrExit(): Boolean = {
    process.isDefined && !process.get.isAlive && process.get.exitValue() != 0
  }

  private def changeState(newState: SparkApp.State.Value): Unit = {
    if (state != newState) {
      listener.foreach(_.stateChanged(state, newState))
      state = newState
    }
  }

  /**
   * Find the corresponding YARN application id from an application tag.
   *
   * @return ApplicationId or the failure.
   */
  private def getAppId(): ApplicationId = {
    appIdOption.map(ConverterUtils.toApplicationId).getOrElse {
      val appTagLowerCase = appTag.toLowerCase()
      // FIXME Should not loop thru all YARN applications but YarnClient doesn't offer an API.
      // Consider calling rmClient in YarnClient directly.
      yarnClient.getApplications(appType).asScala.
        find(_.getApplicationTags.contains(appTagLowerCase))
      match {
        case Some(app) => app.getApplicationId
        case None =>
          throw new IllegalStateException(s"No YARN application is found with tag" +
            s" $appTagLowerCase in ${livyConf.getTimeAsMs(LivyConf.YARN_APP_LOOKUP_TIMEOUT)/1000}" +
            " seconds. This may be because 1) spark-submit fail to submit application to YARN; " +
            "or 2) YARN cluster doesn't have enough resources to start the application in time. " +
            "Please check Livy log and YARN log to know the details.")
      }
    }
  }

  private def getYarnDiagnostics(appReport: ApplicationReport): IndexedSeq[String] = {
    Option(appReport.getDiagnostics)
      .filter(_.nonEmpty)
      .map[IndexedSeq[String]](_.split("\n"))
      .getOrElse(IndexedSeq.empty)
  }

  // Exposed for unit test.
  private[utils] def isRunning: Boolean = {
    state != SparkApp.State.FAILED && state != SparkApp.State.FINISHED &&
      state != SparkApp.State.KILLED
  }

  // Exposed for unit test.
  private[utils] def mapYarnState(
      appId: ApplicationId,
      yarnAppState: YarnApplicationState,
      finalAppStatus: FinalApplicationStatus): SparkApp.State.Value = {
    (yarnAppState, finalAppStatus) match {
      case (YarnApplicationState.NEW, FinalApplicationStatus.UNDEFINED) |
           (YarnApplicationState.NEW_SAVING, FinalApplicationStatus.UNDEFINED) |
           (YarnApplicationState.SUBMITTED, FinalApplicationStatus.UNDEFINED) |
           (YarnApplicationState.ACCEPTED, FinalApplicationStatus.UNDEFINED) =>
        SparkApp.State.STARTING
      case (YarnApplicationState.RUNNING, FinalApplicationStatus.UNDEFINED) |
           (YarnApplicationState.RUNNING, FinalApplicationStatus.SUCCEEDED) =>
        SparkApp.State.RUNNING
      case (YarnApplicationState.FINISHED, FinalApplicationStatus.SUCCEEDED) =>
        SparkApp.State.FINISHED
      case (YarnApplicationState.FAILED, FinalApplicationStatus.FAILED) =>
        SparkApp.State.FAILED
      case (YarnApplicationState.KILLED, FinalApplicationStatus.KILLED) =>
        SparkApp.State.KILLED
      case (state, finalStatus) => // any other combination is invalid, so FAIL the application.
        error(s"Unknown YARN state $state for app $appId with final status $finalStatus.")
        SparkApp.State.FAILED
    }
  }

  private var yarnAppMonitorFailedTimes: Int = _
  private var yarnTagToAppIdFailedTimes: Int = _

  private def failToMonitor(): Unit = {
    changeState(SparkApp.State.FAILED)
    process.foreach(_.destroy())
    leakedAppTags.put(appTag, System.currentTimeMillis())
  }

  private val monitorLock: Int = 0
  private def monitorSparkYarnApp(): Unit = {
    try {
      if (killed) {
        changeState(SparkApp.State.KILLED)
      } else if (isProcessErrExit()) {
        changeState(SparkApp.State.FAILED)
      }
      // If appId is not known, query YARN by appTag to get it.
      if (appId.isEmpty) {
        appId = Some(getAppId())
        listener.foreach(_.appIdKnown(appId.get.toString))
      }

      if (isRunning) {
        try {
          // Refresh application state
          val appReport = yarnClient.getApplicationReport(appId.get)
          yarnDiagnostics = getYarnDiagnostics(appReport)
          changeState(mapYarnState(
            appReport.getApplicationId,
            appReport.getYarnApplicationState,
            appReport.getFinalApplicationStatus))

          val latestAppInfo = {
            val attempt =
              yarnClient.getApplicationAttemptReport(appReport.getCurrentApplicationAttemptId)
            val driverLogUrl =
              Try(yarnClient.getContainerReport(attempt.getAMContainerId).getLogUrl)
                .toOption
            AppInfo(driverLogUrl, Option(appReport.getTrackingUrl))
          }

          if (appInfo != latestAppInfo) {
            listener.foreach(_.infoChanged(latestAppInfo))
            appInfo = latestAppInfo
          }
        } catch {
          // This exception might be thrown during app is starting up. It's transient.
          case e: ApplicationAttemptNotFoundException =>
          // Workaround YARN-4411: No enum constant FINAL_SAVING from getApplicationAttemptReport()
          case e: IllegalArgumentException =>
            if (e.getMessage.contains("FINAL_SAVING")) {
              debug("Encountered YARN-4411.")
            } else {
              throw e
            }
        }
      }

      yarnAppMonitorFailedTimes = 0
      yarnTagToAppIdFailedTimes = 0

      debug(s"$appId $state ${yarnDiagnostics.mkString(" ")}")
    } catch {
      // throw InterruptedException when monitor timeout
      case e: InterruptedException =>
        yarnAppMonitorFailedTimes += 1
        if (yarnAppMonitorFailedTimes > yarnAppMonitorMaxFailedTimes) {
          error(s"Exception whiling monitor $appTag", e)
          yarnDiagnostics = ArrayBuffer(e.getMessage)
          failToMonitor()
        }
      // throw IllegalStateException when getAppId failed
      case e: IllegalStateException =>
        yarnTagToAppIdFailedTimes += 1
        if (yarnTagToAppIdFailedTimes > getYarnTagToAppIdMaxFailedTimes(livyConf)) {
          error(s"Exception whiling monitor $appTag", e)
          yarnDiagnostics = ArrayBuffer(e.getMessage)
          failToMonitor()
        }
      case NonFatal(e) =>
        error(s"Error whiling monitor $appTag", e)
        yarnDiagnostics = ArrayBuffer(e.getMessage)
        failToMonitor()
    }
  }
}
