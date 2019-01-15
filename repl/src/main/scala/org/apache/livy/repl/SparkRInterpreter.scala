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

package org.apache.livy.repl

import java.io.File
import java.nio.file.Files
import java.util.concurrent.{CountDownLatch, Semaphore, TimeUnit}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.reflect.runtime.universe
import scala.util.Try
import scala.util.control.NonFatal

import org.apache.commons.codec.binary.Base64
import org.apache.commons.lang.StringEscapeUtils
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SQLContext
import org.json4s._
import org.json4s.JsonDSL._

import org.apache.livy.Logging
import org.apache.livy.client.common.ClientConf
import org.apache.livy.rsc.driver.SparkEntries

private case class RequestResponse(content: String, error: Boolean)

// scalastyle:off println
object SparkRInterpreter extends Logging {
  private val LIVY_END_MARKER = "----LIVY_END_OF_COMMAND----"
  private val LIVY_ERROR_MARKER = "----LIVY_END_OF_ERROR----"
  private val PRINT_MARKER = f"""print("$LIVY_END_MARKER")"""
  private val EXPECTED_OUTPUT = f"""[1] "$LIVY_END_MARKER""""
  private var sparkEntries: SparkEntries = null

  private val PLOT_REGEX = (
    "(" +
      "(?:bagplot)|" +
      "(?:barplot)|" +
      "(?:boxplot)|" +
      "(?:dotchart)|" +
      "(?:hist)|" +
      "(?:lines)|" +
      "(?:pie)|" +
      "(?:pie3D)|" +
      "(?:plot)|" +
      "(?:qqline)|" +
      "(?:qqnorm)|" +
      "(?:scatterplot)|" +
      "(?:scatterplot3d)|" +
      "(?:scatterplot\\.matrix)|" +
      "(?:splom)|" +
      "(?:stripchart)|" +
      "(?:vioplot)" +
    ")"
    ).r.unanchored

  def apply(conf: SparkConf, entries: SparkEntries): SparkRInterpreter = {
    sparkEntries = entries
    val backendTimeout = sys.env.getOrElse("SPARKR_BACKEND_TIMEOUT", "120").toInt
    val mirror = universe.runtimeMirror(getClass.getClassLoader)
    val sparkRBackendClass = mirror.classLoader.loadClass("org.apache.spark.api.r.RBackend")
    val backendInstance = sparkRBackendClass.getDeclaredConstructor().newInstance()

    var sparkRBackendPort = 0
    var sparkRBackendSecret: String = null
    val initialized = new Semaphore(0)
    // Launch a SparkR backend server for the R process to connect to
    val backendThread = new Thread("SparkR backend") {
      override def run(): Unit = {
        try {
          sparkRBackendPort = sparkRBackendClass.getMethod("init").invoke(backendInstance)
            .asInstanceOf[Int]
        } catch {
          case NonFatal(e) =>
            warn("Fail to init Spark RBackend, using different method signature", e)
            val retTuple = sparkRBackendClass.getMethod("init").invoke(backendInstance)
              .asInstanceOf[(Int, Object)]
            sparkRBackendPort = retTuple._1
            sparkRBackendSecret = Try {
              val rAuthHelper = retTuple._2
              rAuthHelper.getClass.getMethod("secret").invoke(rAuthHelper).asInstanceOf[String]
            }.getOrElse(null)
        }

        initialized.release()
        sparkRBackendClass.getMethod("run").invoke(backendInstance)
      }
    }

    backendThread.setDaemon(true)
    backendThread.start()
    try {
      // Wait for RBackend initialization to finish
      initialized.tryAcquire(backendTimeout, TimeUnit.SECONDS)
      val rExec = conf.getOption("spark.r.shell.command")
        .orElse(sys.env.get("SPARKR_DRIVER_R"))
        .getOrElse("R")

      var packageDir = ""
      if (sys.env.getOrElse("SPARK_YARN_MODE", "") == "true" ||
        (conf.get("spark.master", "").toLowerCase == "yarn" &&
          conf.get("spark.submit.deployMode", "").toLowerCase == "cluster")) {
        packageDir = "./sparkr"
      } else {
        // local mode
        val rLibPath = new File(sys.env.getOrElse("SPARKR_PACKAGE_DIR",
          Seq(sys.env.getOrElse("SPARK_HOME", "."), "R", "lib").mkString(File.separator)))
        if (!ClientConf.TEST_MODE) {
          require(rLibPath.exists(), "Cannot find sparkr package directory.")
          packageDir = rLibPath.getAbsolutePath()
        }
      }

      val builder = new ProcessBuilder(Seq(rExec, "--slave @").asJava)
      val env = builder.environment()
      env.put("SPARK_HOME", sys.env.getOrElse("SPARK_HOME", "."))
      env.put("EXISTING_SPARKR_BACKEND_PORT", sparkRBackendPort.toString)
      if (sparkRBackendSecret != null) {
        env.put("SPARKR_BACKEND_AUTH_SECRET", sparkRBackendSecret)
      }
      env.put("SPARKR_PACKAGE_DIR", packageDir)
      env.put("R_PROFILE_USER",
        Seq(packageDir, "SparkR", "profile", "general.R").mkString(File.separator))

      builder.redirectErrorStream(true)
      val process = builder.start()
      new SparkRInterpreter(process, backendInstance, backendThread,
        conf.getInt("spark.livy.spark_major_version", 1), sparkRBackendSecret != null)
    } catch {
      case e: Exception =>
        if (backendThread != null) {
          backendThread.interrupt()
        }
        throw e
    }
  }

  def getSparkContext(): JavaSparkContext = {
    require(sparkEntries != null)
    sparkEntries.sc()
  }

  def getSparkSession(): Object = {
    require(sparkEntries != null)
    sparkEntries.sparkSession()
  }

  def getSQLContext(): SQLContext = {
    require(sparkEntries != null)
    if (sparkEntries.hivectx() != null) sparkEntries.hivectx() else sparkEntries.sqlctx()
  }
}

class SparkRInterpreter(
    process: Process,
    backendInstance: Any,
    backendThread: Thread,
    val sparkMajorVersion: Int,
    authProvided: Boolean)
  extends ProcessInterpreter(process) {
  import SparkRInterpreter._

  implicit val formats = DefaultFormats

  private[this] var executionCount = 0
  override def kind: String = "sparkr"
  private[this] val isStarted = new CountDownLatch(1)

  final override protected def waitUntilReady(): Unit = {
    // Set the option to catch and ignore errors instead of halting.
    sendRequest("options(error = dump.frames)")
    if (!ClientConf.TEST_MODE) {
      // scalastyle:off line.size.limit
      sendRequest("library(SparkR)")
      sendRequest("""port <- Sys.getenv("EXISTING_SPARKR_BACKEND_PORT", "")""")
      if (authProvided) {
        sendRequest("""authSecret <- Sys.getenv("SPARKR_BACKEND_AUTH_SECRET", "")""")
        sendRequest("""SparkR:::connectBackend("localhost", port, 6000, authSecret)""")
      } else {
        sendRequest("""SparkR:::connectBackend("localhost", port, 6000)""")
      }
      sendRequest("""assign(".scStartTime", as.integer(Sys.time()), envir = SparkR:::.sparkREnv)""")

      sendRequest("""assign(".sc", SparkR:::callJStatic("org.apache.livy.repl.SparkRInterpreter", "getSparkContext"), envir = SparkR:::.sparkREnv)""")
      sendRequest("""assign("sc", get(".sc", envir = SparkR:::.sparkREnv), envir=.GlobalEnv)""")

      if (sparkMajorVersion >= 2) {
        sendRequest("""assign(".sparkRsession", SparkR:::callJStatic("org.apache.livy.repl.SparkRInterpreter", "getSparkSession"), envir = SparkR:::.sparkREnv)""")
        sendRequest("""assign("spark", get(".sparkRsession", envir = SparkR:::.sparkREnv), envir=.GlobalEnv)""")
      }

      sendRequest("""assign(".sqlc", SparkR:::callJStatic("org.apache.livy.repl.SparkRInterpreter", "getSQLContext"), envir = SparkR:::.sparkREnv)""")
      sendRequest("""assign("sqlContext", get(".sqlc", envir = SparkR:::.sparkREnv), envir = .GlobalEnv)""")
      // scalastyle:on line.size.limit
    }

    isStarted.countDown()
    executionCount = 0
  }

  override protected def sendExecuteRequest(command: String): Interpreter.ExecuteResponse = {
    isStarted.await()
    var code = command

    // Create a image file if this command is trying to plot.
    val tempFile = PLOT_REGEX.findFirstIn(code).map { case _ =>
      val tempFile = Files.createTempFile("", ".png")
      val tempFileString = tempFile.toAbsolutePath

      code = f"""png("$tempFileString")\n$code\ndev.off()"""

      tempFile
    }

    try {
      val response = sendRequest(code)

      if (response.error) {
        Interpreter.ExecuteError("Error", response.content)
      } else {
        var content: JObject = TEXT_PLAIN -> response.content

        // If we rendered anything, pass along the last image.
        tempFile.foreach { case file =>
          val bytes = Files.readAllBytes(file)
          if (bytes.nonEmpty) {
            val image = Base64.encodeBase64String(bytes)
            content = content ~ (IMAGE_PNG -> image)
          }
        }

        Interpreter.ExecuteSuccess(content)
      }

    } catch {
      case e: Error =>
        Interpreter.ExecuteError("Error", e.output)
      case e: Exited =>
        Interpreter.ExecuteAborted(e.getMessage)
    } finally {
      tempFile.foreach(Files.delete)
    }

  }

  private def sendRequest(code: String): RequestResponse = {
    stdin.println(s"""tryCatch(eval(parse(text="${StringEscapeUtils.escapeJava(code)}"))
                     |,error = function(e) sprintf("%s%s", e, "${LIVY_ERROR_MARKER}"))
                  """.stripMargin)
    stdin.flush()

    stdin.println(PRINT_MARKER)
    stdin.flush()

    readTo(EXPECTED_OUTPUT, LIVY_ERROR_MARKER)
  }

  override protected def sendShutdownRequest() = {
    stdin.println("q()")
    stdin.flush()

    while (stdout.readLine() != null) {}
  }

  override def close(): Unit = {
    try {
      val closeMethod = backendInstance.getClass().getMethod("close")
      closeMethod.setAccessible(true)
      closeMethod.invoke(backendInstance)

      backendThread.interrupt()
      backendThread.join()
    } finally {
      super.close()
    }
  }

  @tailrec
  private def readTo(
      marker: String,
      errorMarker: String,
      output: StringBuilder = StringBuilder.newBuilder): RequestResponse = {
    var char = readChar(output)

    // Remove any ANSI color codes which match the pattern "\u001b\\[[0-9;]*[mG]".
    // It would be easier to do this with a regex, but unfortunately I don't see an easy way to do
    // without copying the StringBuilder into a string for each character.
    if (char == '\u001b') {
      if (readChar(output) == '[') {
        char = readDigits(output)

        if (char == 'm' || char == 'G') {
          output.delete(output.lastIndexOf('\u001b'), output.length)
        }
      }
    }

    if (output.endsWith(marker)) {
      var result = stripMarker(output.toString(), marker)

      if (result.endsWith(errorMarker + "\"")) {
        result = stripMarker(result, "\\n" + errorMarker)
        RequestResponse(result, error = true)
      } else {
        RequestResponse(result, error = false)
      }
    } else {
      readTo(marker, errorMarker, output)
    }
  }

  private def stripMarker(result: String, marker: String): String = {
    result.replace(marker, "")
      .stripPrefix("\n")
      .stripSuffix("\n")
  }

  private def readChar(output: StringBuilder): Char = {
    val byte = stdout.read()
    if (byte == -1) {
      throw new Exited(output.toString())
    } else {
      val char = byte.toChar
      output.append(char)
      char
    }
  }

  @tailrec
  private def readDigits(output: StringBuilder): Char = {
    val byte = stdout.read()
    if (byte == -1) {
      throw new Exited(output.toString())
    }

    val char = byte.toChar

    if (('0' to '9').contains(char)) {
      output.append(char)
      readDigits(output)
    } else {
      char
    }
  }

  private class Exited(val output: String) extends Exception {}
  private class Error(val output: String) extends Exception {}
}
// scalastyle:on println
