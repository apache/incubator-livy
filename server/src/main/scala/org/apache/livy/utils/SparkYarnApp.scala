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

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps

import org.apache.hadoop.yarn.api.records.{ApplicationId, FinalApplicationStatus, YarnApplicationState}
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.ConverterUtils

import org.apache.livy.{LivyConf, Logging, Utils}

object SparkYarnApp extends Logging {

  def init(livyConf: LivyConf): Unit = {
    sessionLeakageCheckInterval = livyConf.getTimeAsMs(LivyConf.YARN_APP_LEAKAGE_CHECK_INTERVAL)
    sessionLeakageCheckTimeout = livyConf.getTimeAsMs(LivyConf.YARN_APP_LEAKAGE_CHECK_TIMEOUT)
    leakedAppsGCThread.setDaemon(true)
    leakedAppsGCThread.setName("LeakedAppsGCThread")
    leakedAppsGCThread.start()
  }

  // YarnClient is thread safe. Create once, share it across threads.
  lazy val yarnClient = {
    val c = YarnClient.createYarnClient()
    c.init(new YarnConfiguration())
    c.start()
    c
  }

  private def getYarnTagToAppIdTimeout(livyConf: LivyConf): FiniteDuration =
    livyConf.getTimeAsMs(LivyConf.YARN_APP_LOOKUP_TIMEOUT) milliseconds

  private def getYarnPollInterval(livyConf: LivyConf): FiniteDuration =
    livyConf.getTimeAsMs(LivyConf.YARN_POLL_INTERVAL) milliseconds

  private val appType = Set("SPARK").asJava

  private val leakedAppTags = new java.util.concurrent.ConcurrentHashMap[String, Long]()

  private var sessionLeakageCheckTimeout: Long = _

  private var sessionLeakageCheckInterval: Long = _

  private val leakedAppsGCThread = new Thread() {
    override def run(): Unit = {
      while (true) {
        if (!leakedAppTags.isEmpty) {
          // kill the app if found it and remove it if exceeding a threashold
          val iter = leakedAppTags.entrySet().iterator()
          var isRemoved = false
          val now = System.currentTimeMillis()
          val apps = yarnClient.getApplications(appType).asScala
          while(iter.hasNext) {
            val entry = iter.next()
            apps.find(_.getApplicationTags.contains(entry.getKey))
              .foreach({ e =>
                info(s"Kill leaked app ${e.getApplicationId}")
                yarnClient.killApplication(e.getApplicationId)
                iter.remove()
                isRemoved = true
              })
            if (!isRemoved) {
              if ((entry.getValue - now) > sessionLeakageCheckTimeout) {
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
    val appTag: String,
    val appIdOption: Option[String],
    val process: Option[LineBufferedProcess],
    val listener: Option[SparkAppListener],
    livyConf: LivyConf,
    yarnInterface: YarnInterface)
  extends SparkApp
  with Logging {

  import scala.concurrent.ExecutionContext.Implicits.global

  val appId: Future[ApplicationId] = Future {
    appIdOption.fold(yarnInterface.getAppIdFromTag(appTag, process))(ConverterUtils.toApplicationId)
  }
  private[utils] var state: SparkApp.State = SparkApp.State.STARTING
  private var yarnDiagnostics: IndexedSeq[String] = IndexedSeq.empty[String]

  override def log(): IndexedSeq[String] =
    ("stdout: " +: process.map(_.inputLines).getOrElse(ArrayBuffer.empty[String])) ++
    ("\nstderr: " +: process.map(_.errorLines).getOrElse(ArrayBuffer.empty[String])) ++
    ("\nYARN Diagnostics: " +: yarnDiagnostics)

  override def kill(): Unit = yarnInterface.kill(this)

  private[utils] def changeState(newState: SparkApp.State.Value): Unit = {
    if (state != newState) {
      listener.foreach(_.stateChanged(state, newState))
      state = newState
    }
  }

  def updateYarnDiagnostics(diag: IndexedSeq[String]): Unit = {
    yarnDiagnostics = diag
  }

  private[utils] def isRunning: Boolean = {
    state != SparkApp.State.FAILED && state != SparkApp.State.FINISHED &&
      state != SparkApp.State.KILLED
  }

  // Exposed for unit test.
  private[utils] def mapYarnState(
      appId: String,
      yarnAppState: String,
      finalAppStatus: String): SparkApp.State.Value = {
    (yarnAppState, finalAppStatus) match {
      case ("NEW", "UNDEFINED") |
           ("NEW_SAVING", "UNDEFINED") |
           ("SUBMITTED", "UNDEFINED") |
           ("ACCEPTED", "UNDEFINED") =>
        SparkApp.State.STARTING
      case ("RUNNING", "UNDEFINED") =>
        SparkApp.State.RUNNING
      case ("FINISHED", "SUCCEEDED") =>
        SparkApp.State.FINISHED
      case ("FAILED", "FAILED") =>
        SparkApp.State.FAILED
      case ("KILLED", "KILLED") =>
        SparkApp.State.KILLED
      case (state, finalStatus) => // any other combination is invalid
        error(s"Unknown YARN state $state for app $appId with final status $finalStatus.")
        SparkApp.State.FAILED
    }
  }

  def needsUpdating(appInfo: AppInfo): Boolean = {
    (isRunning || appInfo.driverLogUrl == None || appInfo.sparkUiUrl == None)
  }
}
