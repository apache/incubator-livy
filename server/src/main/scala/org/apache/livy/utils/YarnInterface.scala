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
import java.util.concurrent.atomic.AtomicBoolean

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.{Map => MutableMap}
import scala.concurrent.blocking
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

import org.apache.hadoop.yarn.api.records.{ApplicationId, ApplicationReport, ContainerReport}
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration

import org.apache.livy.{LivyConf, Logging, Utils}

/**
  * An interface to handle all interactions with Yarn.
  */
class YarnInterface(livyConf: LivyConf, yarnClient: YarnClient) extends Logging {

  import scala.concurrent.ExecutionContext.Implicits.global

  import YarnInterface.appType

  private val TIMEOUT_EXIT_CODE = -1

  val sessionLeakageCheckTimeout = livyConf.getTimeAsMs(LivyConf.YARN_APP_LEAKAGE_CHECK_TIMEOUT)

  val sessionLeakageCheckInterval = livyConf.getTimeAsMs(LivyConf.YARN_APP_LEAKAGE_CHECK_INTERVAL)

  val yarnPollInterval = (livyConf.getTimeAsMs(LivyConf.YARN_POLL_INTERVAL) milliseconds)

  val yarnTagToAppIdTimeout = livyConf.getTimeAsMs(LivyConf.YARN_APP_LOOKUP_TIMEOUT) milliseconds

  private var yarnApplicationReports = Map.empty[ApplicationReport, ContainerReport]

  private var appTagToAppIdMap = Map.empty[String, Seq[ApplicationId]]

  val isRunning: AtomicBoolean = new AtomicBoolean(true)

  val lock = new Object

  val appReportUpdater = Utils.startDaemonThread(s"yarnAppMonitorThread-$this") {
    Try {
      while (isRunning.get) {

        debug(s"cleaning yarnApplicationReports. Currently it is ${
          yarnApplicationReports.map { case (appReport, _) =>
            appReport.getApplicationId
          }.toSeq.sorted.mkString(",")
        }")

        val yarnApps = blocking(yarnClient.getApplications(appType).asScala)
        debug(s"new yarn apps = ${
          yarnApps.map(appReport =>
            appReport.getApplicationId
          ).sorted.mkString(",")
        }")

        val updatedAppTagToAppIdMap = MutableMap.empty[String, Seq[ApplicationId]]
        val newYarnApplicationReports = yarnApps.map { appReport =>
          appReport.getApplicationTags.asScala.map(_.toLowerCase).map { tag =>
            val appIdsForTag = updatedAppTagToAppIdMap.get(tag).getOrElse(Seq[ApplicationId]())
            if (!appIdsForTag.contains(appReport.getApplicationId)) {
              updatedAppTagToAppIdMap.update(tag, appIdsForTag :+ appReport.getApplicationId)
            }
          }
          val containerReport = Try {
            appReport.getCurrentApplicationAttemptId
          }.toOption.flatMap { attemptId =>
            Try(yarnClient.getApplicationAttemptReport(attemptId)).toOption
          }.flatMap { attempt =>
            Try(attempt.getAMContainerId).toOption
          }.flatMap { containerId =>
            Try(yarnClient.getContainerReport(containerId)).toOption
          }
          (appReport, containerReport.getOrElse(null))
        }.toMap
        blocking {
          lock.synchronized {
            yarnApplicationReports = newYarnApplicationReports
            appTagToAppIdMap = updatedAppTagToAppIdMap.toMap // to immutable map
          }
        }
        debug(s"yarnAppMonitorThread is going to sleep for $yarnPollInterval")
        Clock.sleep(yarnPollInterval.toMillis)
      }
    } match {
      case Success(_) =>
        debug("Yarn App Monitor thread executed successfully! It is shutting down now!")
      case Failure(ex) =>
        warn("Unhandled Exception in Yarn App Monitor:${ex.getMessage}!")
        warn(ex.getStackTrace.mkString("\n"))
    }
  }

  def getApplicationReport(appId: ApplicationId): Option[ApplicationReport] = {
    blocking {
      lock.synchronized {
        yarnApplicationReports.find { case (appReport, _) =>
          appReport.getApplicationId == appId
        }.map { case (appReport, _) =>
          appReport
        }
      }
    }
  }

  /**
    * Find the corresponding YARN application id from an application tag.
    *
    * @param appTag The application tag tagged on the target application.
    *               If the tag is not unique, it returns the first application it found.
    *               It will be converted to lower case to match YARN's behaviour.
    * @return ApplicationId or the failure.
    */
  def getAppIdFromTag(
                       appTag: String,
                       process: Option[LineBufferedProcess]
                     ): ApplicationId = {

    val appTagLowerCase = appTag.toLowerCase()
    val deadline = yarnTagToAppIdTimeout.fromNow
    debug(s"Going to find the application id for tag $appTag")

    @tailrec
    def go(appTagLowerCase: String, deadLine: Deadline): ApplicationId = {
      debug(s"recursively finding appId for tag $appTagLowerCase. Deadline is $deadline")


      val taggedApps = blocking {
        lock.synchronized {
          appTagToAppIdMap.get(appTagLowerCase)
        }
      }

      debug(s"all apps with tag: $appTagLowerCase = ${taggedApps.mkString(",")}")

      taggedApps match {
        case Some(Seq(applicationId)) =>
          info(s"Found $applicationId for tag $appTagLowerCase.")
          applicationId
        case _ =>
          debug(s"didn't find the any app with tag $appTagLowerCase... Trying again")
          if (deadline.isOverdue) {
            process.foreach(_.destroy())
            leakedAppTags.put(appTag, System.currentTimeMillis())
            val timeOut = yarnTagToAppIdTimeout / 1000
            val errorMsg =
              s"""No YARN application was found with tag $appTag in $timeOut seconds.
                 |Please make sure you submitted your application correctly.
                 |Also check your cluster status, it is may be very busy.""".stripMargin
            throw new Exception(errorMsg)
          } else {
            debug(s"going to sleep for ${yarnPollInterval.toMillis} ms... before retry")
            Clock.sleep(yarnPollInterval.toMillis)
            go(appTagLowerCase, deadline)
          }
      }
    }

    go(appTagLowerCase, deadline)
  }

  @tailrec
  private def waitFor(process: LineBufferedProcess, deadline: Deadline): Int = {

    debug(s"waiting for process $process to exit within deadline $deadline")
    if (!deadline.isOverdue()) {
      Try {
        process.exitValue()
      } match {
        case Success(exitValue) =>
          info(s"process $process exited with exit  code $exitValue")
          exitValue
        case Failure(ex) =>
          debug(s"process $process did not exit!... trying one more time...: ${ex.getMessage}")
          Clock.sleep(yarnPollInterval.toMillis)
          waitFor(process, deadline)
      }
    } else {
      info(s"process $process did not exit withing the deadline ($deadline)... Giving up!")

      TIMEOUT_EXIT_CODE
    }
  }

  def onExit(process: LineBufferedProcess,
             onSuccess: Int => Unit,
             onFailure: Throwable => Unit): Future[Int] = {
    val exitCodeFuture = Future {
      val deadline = yarnTagToAppIdTimeout.fromNow
      debug(s"Going to wait for process $process to exit")
      waitFor(process, deadline)
    }

    exitCodeFuture.onComplete {
      case Success(exitCode) if exitCode == 0 =>
        info(s"process $process exit successfully")
        onSuccess(exitCode)
      case Success(exitCode) =>
        info(s"process $process exit with exit code $exitCode")
        val exception = new Exception("Failed to submit the job to YARN.")
        onFailure(exception)
      case Failure(ex) =>
        info(s"process $process FAILED :${ex.getMessage}")
        onFailure(ex)
    }

    exitCodeFuture
  }

  def killApplication(appId: ApplicationId): Unit = yarnClient.killApplication(appId)

  private val leakedAppTags = new java.util.concurrent.ConcurrentHashMap[String, Long]()
  private val leakedAppsGCThread = new Thread() {
    override def run(): Unit = {
      while (true) {
        if (!leakedAppTags.isEmpty) {
          // kill the app if found it and remove it if exceeding a threashold
          val iter = leakedAppTags.entrySet().iterator()
          var isRemoved = false
          val now = System.currentTimeMillis()
          val apps = yarnClient.getApplications(appType).asScala
          while (iter.hasNext) {
            val entry = iter.next()
            apps.find(_.getApplicationTags.contains(entry.getKey))
              .foreach { applicationReport: ApplicationReport =>
                info(s"Kill leaked app ${applicationReport.getApplicationId}")
                killApplication(applicationReport.getApplicationId)
                iter.remove()
                isRemoved = true
              }
            if (!isRemoved) {
              if ((entry.getValue - now) > sessionLeakageCheckTimeout) {
                iter.remove()
                info(s"Remove leaked yarn app tag ${entry.getKey}")
              }
            }
          }
        }
        Clock.sleep(sessionLeakageCheckInterval)
      }
    }
  }
  leakedAppsGCThread.setDaemon(true)
  leakedAppsGCThread.setName("LeakedAppsGCThread")
  leakedAppsGCThread.start()

  def kill(app: SparkYarnApp): Unit = synchronized {
    info(s"Going to kill app with tag ${app.appTag} and id ${app.appIdOption}")
    if (app.isRunning) {
      try {
        info(s"App with tag ${app.appTag} and id ${app.appIdOption} is running. Go kill it...")
        val killYarnAppFuture = app.appId.map { appIdToKill =>
          Try {
            blocking {
              info(s"Calling kill on $app with tag ${app.appTag} and id ${app.appIdOption}..")
              killApplication(appIdToKill)
              info(s"Called kill on $app with tag ${app.appTag} and id ${app.appIdOption}!")
              true
            }
          } match {
            case Success(_) =>
              true
            case Failure(ex) =>
              warn(s"Failed to kill app with appTag = ${app.appTag} appId = ${app.appId}.")
              warn(ex.getStackTrace.mkString("\n"))
              false
          }
        }

        @tailrec
        def checkState: Boolean = {
          if (!app.isRunning) {
            true
          } else {
            Clock.sleep(yarnPollInterval.toMillis)
            checkState
          }
        }

        val appNotRunningFuture = Future[Boolean] {
          checkState
        }

        Future.firstCompletedOf(Seq(appNotRunningFuture, killYarnAppFuture)).onComplete {
          case Success(successCode) => info(s"Successfully killed app? $successCode")
          case Failure(ex) => warn(s"Failed to kill the application: ${ex.getMessage}")
        }

      }
      catch {
        // We cannot kill the YARN app without the app id.
        // There's a chance the YARN app hasn't been submitted during a livy-server failure.
        // We don't want a stuck session that can't be deleted. Emit a warning and move on.
        case _: TimeoutException | _: InterruptedException =>
          error("Deleting a session while its YARN application is not found.")
        // app.yarnAppMonitorThread.interrupt()
      }
      finally {
        app.process.foreach(_.destroy())
      }
      //    } else if (app.yarnAppMonitorThread.isAlive) {
      //      debug("Interrupting yarnAppMonitorThread...!!!")
      //      app.yarnAppMonitorThread.interrupt()
    }
  }

  /**
    * Check the status of the give application on YARN periodically and updates information about it
    * locally on this instance of Livy server.
    *
    * @param app
    * The given `SparkYarnApp`
    * @return
    * returns a future that can be waited on
    */
  def checkStatus(app: SparkYarnApp): Future[Unit] = {
    info(s"Checking status for app = $app tag ${app.appTag} and id ${app.appIdOption} ")
    val process = app.process

    val processExitCodeFuture = process.map { p =>
      onExit(p, _ =>
        debug(s"exist code for ${p} is ready"),
        ex => {
          warn(s"process $process failed: ${ex.getMessage}")
          warn(process.get.inputLines.mkString("\n"))
          app.changeState(SparkApp.State.FAILED)
        }
      )
    }.getOrElse(Future.successful(0)) // return 0 if there is no process to get its exit code.

    val appIdFuture = processExitCodeFuture.zip(app.appId).map {
      case (0, appId) =>
        Some(appId)
      case _ =>
        None
    }

    appIdFuture.onComplete {
      case Success(Some(applicationId)) =>
        app.listener.foreach(_.appIdKnown(applicationId.toString))
      case Success(None) =>
        warn(s"No application ID for to get appId for $process:$app")
      case Failure(ex) =>
        warn(s"Failed to get appId for $process:$app: ${ex.getMessage}")
    }

    appIdFuture.map {
      case None =>
        warn(s"Not going to check status of yarn application ${app.appTag}")
      case Some(appId) =>
        var appInfo = AppInfo()
        do {
          try {
            Clock.sleep(yarnPollInterval.toMillis / 2)

            // Refresh application state
            val appReport = getApplicationReport(appId)
            appReport.fold {
              warn(s"No application report found for $appId")
            } { appReport =>
              app.updateYarnDiagnostics(getYarnDiagnostics(appReport))
              app.changeState(app.mapYarnState(
                appReport.getApplicationId,
                appReport.getYarnApplicationState,
                appReport.getFinalApplicationStatus))

              val driverLogUrl = blocking {
                lock.synchronized {
//                  lock.wait
                  yarnApplicationReports.get(appReport).flatMap { containerReport =>
                    Try(containerReport.getLogUrl).toOption
                  }
                }
              }
              val latestAppInfo = AppInfo(driverLogUrl, Option(appReport.getTrackingUrl))
              if (appInfo != latestAppInfo) {
                app.listener.map{ appListener =>
                  Try(appListener.infoChanged(latestAppInfo))
                }
                appInfo = latestAppInfo
              }
            }
          } catch {
            case ex: Throwable =>
              warn(s"Error when checking status of $app: ${ex.getMessage}")
              throw ex
          }
        } while (this.isRunning.get &&
          (app.isRunning || appInfo.driverLogUrl == None || appInfo.sparkUiUrl == None))
    }
  }

  def getStartTime(appId: ApplicationId): Long = {
    yarnClient.getApplicationReport(appId).getStartTime
  }

  def getFinishTime(appId: ApplicationId): Long = {
    yarnClient.getApplicationReport(appId).getFinishTime
  }

  private def toLogUrl(appId: Option[ApplicationId]): Option[String] = {
    None
  }

  private def getYarnDiagnostics(appReport: ApplicationReport): IndexedSeq[String] = {
    Option(appReport.getDiagnostics)
      .filter(_.nonEmpty)
      .map[IndexedSeq[String]]("YARN Diagnostics:" +: _.split("\n"))
      .getOrElse(IndexedSeq.empty)
  }

  def shutdown: Unit = {
    info("Shutting down the YARN interface...")
    isRunning.set(false)
    lock.synchronized {
//      lock.notifyAll()
    }
  }
}

object YarnInterface {

  val appType = Set("SPARK").asJava

  // YarnClient is thread safe. Create once, share it across threads.
  val yarnClient = {
    val c = YarnClient.createYarnClient()
    c.init(new YarnConfiguration())
    c.start()
    c
  }
}
