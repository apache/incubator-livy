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
import java.net.{URL, URLClassLoader}
import java.nio.file.{Files, Paths}

import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.Completion
import scala.tools.nsc.interpreter.IMain
import scala.tools.nsc.interpreter.JPrintWriter
import scala.tools.nsc.interpreter.NoCompletion
import scala.tools.nsc.interpreter.Results.Result

import org.apache.spark.SparkConf
import org.apache.spark.repl.SparkILoop

/**
 * This represents a Spark interpreter. It is not thread safe.
 */
class SparkInterpreter(protected override val conf: SparkConf) extends AbstractSparkInterpreter {

  private var sparkILoop: SparkILoop = _

  override def start(): Unit = {
    require(sparkILoop == null)

    val rootDir = conf.get("spark.repl.classdir", System.getProperty("java.io.tmpdir"))
    val outputDir = Files.createTempDirectory(Paths.get(rootDir), "spark").toFile
    outputDir.deleteOnExit()
    conf.set("spark.repl.class.outputDir", outputDir.getAbsolutePath)

    val settings = new Settings()
    settings.processArguments(List("-Yrepl-class-based",
      "-Yrepl-outdir", s"${outputDir.getAbsolutePath}"), true)
    settings.usejavacp.value = true
    settings.embeddedDefaults(Thread.currentThread().getContextClassLoader())

    sparkILoop = new SparkILoop(None, new JPrintWriter(outputStream, true))
    sparkILoop.settings = settings
    sparkILoop.createInterpreter()
    sparkILoop.initializeSynchronous()

    restoreContextClassLoader {
      sparkILoop.compilerClasspath
      sparkILoop.ensureClassLoader
      var classLoader = Thread.currentThread().getContextClassLoader
      while (classLoader != null) {
        if (classLoader.getClass.getCanonicalName ==
          "org.apache.spark.util.MutableURLClassLoader") {
          val extraJarPath = classLoader.asInstanceOf[URLClassLoader].getURLs()
            // Check if the file exists. Otherwise an exception will be thrown.
            .filter { u => u.getProtocol == "file" && new File(u.getPath).isFile }
            // Livy rsc and repl are also in the extra jars list. Filter them out.
            .filterNot { u => Paths.get(u.toURI).getFileName.toString.startsWith("livy-") }
            // Some bad spark packages depend on the wrong version of scala-reflect. Blacklist it.
            .filterNot { u =>
              Paths.get(u.toURI).getFileName.toString.contains("org.scala-lang_scala-reflect")
            }

          extraJarPath.foreach { p => debug(s"Adding $p to Scala interpreter's class path...") }
          sparkILoop.addUrlsToClassPath(extraJarPath: _*)
          classLoader = null
        } else {
          classLoader = classLoader.getParent
        }
      }

      postStart()
    }
  }

  override def close(): Unit = synchronized {
    super.close()

    if (sparkILoop != null) {
      sparkILoop.closeInterpreter()
      sparkILoop = null
    }
  }

  override def addJar(jar: String): Unit = {
    sparkILoop.addUrlsToClassPath(new URL(jar))
  }

  override protected def isStarted(): Boolean = {
    sparkILoop != null
  }

  override protected def interpret(code: String): Result = {
    sparkILoop.interpret(code)
  }

  override protected def completeCandidates(code: String, cursor: Int) : Array[String] = {
    val completer : Completion = {
      try {
        val cls = Class.forName("scala.tools.nsc.interpreter.PresentationCompilerCompleter")
        cls.getDeclaredConstructor(classOf[IMain]).newInstance(sparkILoop.intp)
          .asInstanceOf[Completion]
      } catch {
        case e : ClassNotFoundException => NoCompletion
      }
    }
    completer.complete(code, cursor).candidates.toArray
  }

  override protected def valueOfTerm(name: String): Option[Any] = {
    // IMain#valueOfTerm will always return None, so use other way instead.
    Option(sparkILoop.lastRequest.lineRep.call("$result"))
  }

  override protected def bind(name: String,
      tpe: String,
      value: Object,
      modifier: List[String]): Unit = {
    sparkILoop.beQuietDuring {
      sparkILoop.bind(name, tpe, value, modifier)
    }
  }
}
