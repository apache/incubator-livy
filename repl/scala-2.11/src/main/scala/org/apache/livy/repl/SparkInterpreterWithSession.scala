package org.apache.livy.repl

import java.io.File
import java.net.URLClassLoader
import java.nio.file.{Files, Paths}

import org.apache.livy.rsc.driver.StatementState
import org.apache.spark.SparkContext
import org.apache.spark.repl.SparkILoop
import org.apache.spark.sql.SparkSession

import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.Results.Result
import scala.tools.nsc.interpreter._
import org.apache.livy.Logging


/**
  * @author naruto.zwb
  */
class SparkInterpreterWithSession(spark: SparkSession, val multSparkInterpreter: ConcurrentSparkInterpreter)
  extends AbstractSparkInterpreter with Runnable with Logging {
  private var sc: SparkContext = spark.sparkContext
  private var sparkILoop: SparkILoop = _

  override def start(): SparkContext = {
    require(sparkILoop == null)
    info("starting SparkInterpreterWithSession")

    val rootDir = spark.sparkContext.getConf.get("spark.repl.classdir", System.getProperty("java.io.tmpdir"))
    val outputDir = Files.createTempDirectory(Paths.get(rootDir), "spark").toFile
    outputDir.deleteOnExit()

    val settings = new Settings()
    settings.processArguments(List("-Yrepl-class-based",
      "-Yrepl-outdir", s"${outputDir.getAbsolutePath}"), true)
    settings.usejavacp.value = true
    settings.embeddedDefaults(Thread.currentThread().getContextClassLoader())

    sparkILoop = new SparkILoop(None, new JPrintWriter(outputStream, true))
    info(s"created sparkILoop ${sparkILoop.toString} in ${this.toString}")

    sparkILoop.settings = settings
    sparkILoop.createInterpreter()
    sparkILoop.initializeSynchronous()

    restoreContextClassLoader {
      info("starting restoreContextClassLoader")
      sparkILoop.setContextClassLoader()

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

          extraJarPath.foreach { p => info(s"Adding $p to Scala interpreter's class path...") }
          sparkILoop.addUrlsToClassPath(extraJarPath: _*)
          classLoader = null
        } else {
          classLoader = classLoader.getParent
        }
      }
    }

    info("starting bindAndImport")
    bindAndImport()

    sc
  }

  private def bindAndImport(): Unit = {
    bind("spark", "org.apache.spark.sql.SparkSession", spark, List("""@transient"""))
    bind("sc", "org.apache.spark.SparkContext", sc, List("""@transient"""))

    execute("import org.apache.spark.SparkContext._")
    execute("import spark.implicits._")
    execute("import spark.sql")
    execute("import org.apache.spark.sql.functions._")
  }

  override def close(): Unit = synchronized {
    info("close SparkInterpreterWithSession")
    if (sc != null) {
      sc.stop()
      sc = null
    }

    if (sparkILoop != null) {
      sparkILoop.closeInterpreter()
      sparkILoop = null
    }
  }

  override protected def isStarted(): Boolean = {
    sc != null && sparkILoop != null
  }

  override protected def interpret(code: String): Result = {
    info("execute " + code)
    sparkILoop.interpret(code)
  }

  override protected def valueOfTerm(name: String): Option[Any] = {
    // IMain#valueOfTerm will always return None, so use other way instead.
    Option(sparkILoop.lastRequest.lineRep.call("$result"))
  }

  protected def bind(name: String, tpe: String, value: Object, modifier: List[String]): Unit = {
    sparkILoop.beQuietDuring {
      sparkILoop.bind(name, tpe, value, modifier)
    }
  }

  override def run(): Unit = {
    start()

    var codeSnippet: CodeSnippet = null
    var response: Interpreter.ExecuteResponse = null
    var id: Int = 0
    while (true) {
      codeSnippet = multSparkInterpreter.getCodeSnippetToExecute()
      id = codeSnippet.id
      info(s"get code snippet, id=$id")

      multSparkInterpreter.setCodeSnippetState(id, StatementState.Running)
      response = execute(codeSnippet.code)

      multSparkInterpreter.setCodeSnippetResponse(id, response)
      // TODO: 要判断结果是否为成功来确定状态为 avaliable 或 error
      multSparkInterpreter.setCodeSnippetState(id, StatementState.Available)
      info(s"finish execute code snippet, id=$id")

      Thread.sleep(10)
    }
  }
}
