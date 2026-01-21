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

import java.io.{File, FileInputStream}
import java.util.Properties

import scala.collection.JavaConverters._

import org.apache.livy.LivyConf

object AppInfo {
  val DRIVER_LOG_URL_NAME = "driverLogUrl"
  val SPARK_UI_URL_NAME = "sparkUiUrl"
  val EXECUTORS_LOG_URLS_NAME = "executorLogUrls"
}

case class AppInfo(
                    var driverLogUrl: Option[String] = None,
                    var sparkUiUrl: Option[String] = None,
                    var executorLogUrls: Option[String] = None) {

  import AppInfo._

  def asJavaMap: java.util.Map[String, String] =
    Map(
      DRIVER_LOG_URL_NAME -> driverLogUrl.orNull,
      SPARK_UI_URL_NAME -> sparkUiUrl.orNull,
      EXECUTORS_LOG_URLS_NAME -> executorLogUrls.orNull
    ).asJava
}

trait SparkAppListener {
  /** Fired when appId is known, even during recovery. */
  def appIdKnown(appId: String): Unit = {}

  /** Fired when the app state in the cluster changes. */
  def stateChanged(oldState: SparkApp.State, newState: SparkApp.State): Unit = {}

  /** Fired when the app info is changed. */
  def infoChanged(appInfo: AppInfo): Unit = {}
}

/**
 * Provide factory methods for SparkApp.
 */
object SparkApp {
  private val SPARK_YARN_TAG_KEY = "spark.yarn.tags"
  val SPARK_KUBERNETES_NAMESPACE_KEY = "spark.kubernetes.namespace"

  object State extends Enumeration {
    val STARTING, RUNNING, FINISHED, FAILED, KILLED = Value
  }

  type State = State.Value

  def getNamespace(conf: Map[String, String], livyConf: LivyConf): String = {
    var namespace: String = conf.getOrElse(SPARK_KUBERNETES_NAMESPACE_KEY, "")
    if (namespace == "") {
      val sparkHome = livyConf.sparkHome().get // SPARK_HOME is mandatory for Livy
      val sparkDefaultsPath =
        sparkHome + File.separator + "conf" + File.separator + "spark-defaults.conf"
      val properties = new Properties()
      properties.load(new FileInputStream(sparkDefaultsPath))
      namespace = properties.getProperty(SPARK_KUBERNETES_NAMESPACE_KEY, "default")
    }
    namespace
  }

  /**
   * Return cluster manager dependent SparkConf.
   *
   * @param uniqueAppTag A tag that can uniquely identify the application.
   * @param livyConf
   * @param sparkConf
   */
  def prepareSparkConf(
                        uniqueAppTag: String,
                        livyConf: LivyConf,
                        sparkConf: Map[String, String]): Map[String, String] = {
    if (livyConf.isRunningOnYarn()) {
      val userYarnTags = sparkConf.get(SPARK_YARN_TAG_KEY).map("," + _).getOrElse("")
      val mergedYarnTags = uniqueAppTag + userYarnTags
      sparkConf ++ Map(
        SPARK_YARN_TAG_KEY -> mergedYarnTags,
        "spark.yarn.submit.waitAppCompletion" -> "false")
    } else if (livyConf.isRunningOnKubernetes()) {
      import KubernetesConstants._
      sparkConf ++ Map(
        s"spark.kubernetes.driver.label.$SPARK_APP_TAG_LABEL" -> uniqueAppTag,
        s"spark.kubernetes.executor.label.$SPARK_APP_TAG_LABEL" -> uniqueAppTag,
        "spark.kubernetes.submission.waitAppCompletion" -> "false",
        "spark.ui.proxyBase" -> s"/$uniqueAppTag")
    } else {
      sparkConf
    }
  }

  /**
   * Return a SparkApp object to control the underlying Spark application via YARN, Kubernetes
   * or spark-submit.
   *
   * @param uniqueAppTag A tag that can uniquely identify the application.
   */
  def create(
              uniqueAppTag: String,
              appId: Option[String],
              process: Option[LineBufferedProcess],
              livyConf: LivyConf,
              listener: Option[SparkAppListener],
              extrasMap: Map[String, String]): SparkApp = {
    if (livyConf.isRunningOnYarn()) {
      new SparkYarnApp(uniqueAppTag, appId, process, listener, livyConf)
    } else if (livyConf.isRunningOnKubernetes()) {
      new SparkKubernetesApp(uniqueAppTag, appId, process, listener, livyConf, extrasMap)
    } else {
      require(process.isDefined, "process must not be None when Livy master is not YARN or" +
        "Kubernetes.")
      new SparkProcApp(process.get, listener)
    }
  }
}

/**
 * Encapsulate a Spark application.
 */
abstract class SparkApp {
  def kill(): Unit

  def log(): IndexedSeq[String]
}
