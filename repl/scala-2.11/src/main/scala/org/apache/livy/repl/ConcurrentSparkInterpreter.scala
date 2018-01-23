package org.apache.livy.repl

import java.nio.file.{Files, Paths}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, Executors, LinkedBlockingQueue}

import org.apache.livy.rsc.driver.StatementState
import org.apache.livy.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.concurrent.ExecutionContext

case class CodeSnippet(id: Int, code: String)

/**
  * this kind of interpreter contains mult sparkILoops to support execute code concorrently in mult threads
  *
  * @author naruto.zwb
  * @date 2018/01/11
  * @note Not available for spark 1.x
  */
class  ConcurrentSparkInterpreter(interpretersCount: Int, conf: SparkConf)
  extends Interpreter with Logging {
  require(interpretersCount > 0 )

  protected var sparkContext: SparkContext = _
  private val executorService = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(interpretersCount))
  private val codeSnippetQueue = new LinkedBlockingQueue[CodeSnippet]()
  private val codeIdToState = new ConcurrentHashMap[Int, StatementState]()
  private val codeIdToResponse = new ConcurrentHashMap[Int, Interpreter.ExecuteResponse]()

  private val newCodeId = new AtomicInteger(0)

  def getCodeSnippetToExecute(): CodeSnippet = {
    codeSnippetQueue.take()
  }

  def setCodeSnippetState(id: Int, state: StatementState): Unit = {
    codeIdToState.put(id, state)
  }

  def setCodeSnippetResponse(id: Int, response: Interpreter.ExecuteResponse): Unit = {
    codeIdToResponse.put(id, response)
  }

  override def start(): SparkContext = {
    info("start concurrentspark session")

    // step1: create sparkSession
    val rootDir = conf.get("spark.repl.classdir", System.getProperty("java.io.tmpdir"))
    val outputDir = Files.createTempDirectory(Paths.get(rootDir), "spark").toFile
    outputDir.deleteOnExit()
    conf.set("spark.repl.class.outputDir", outputDir.getAbsolutePath)
    val spark = createSparkSession()
    info("created sparkSession")

    // step2: create interpretersCount SparkInterpreterWithSC and to execute in executorService
    (0 to interpretersCount).foreach { i =>
      val interpreter = new SparkInterpreterWithSession(spark, this)
      executorService.execute(interpreter)
      info(s"starting interpreter $i")
    }

    // step3: return sparkContext
    info("finish start concurrentspark session, interpreters are still starting")
    spark.sparkContext
  }

  override def execute(code: String): Interpreter.ExecuteResponse = {
    val codeSnippet = new CodeSnippet(newCodeId.incrementAndGet(), code)
    codeSnippetQueue.put(codeSnippet)
    setCodeSnippetState(codeSnippet.id, StatementState.Waiting)
    info(s"starting execute code snippet, id=${codeSnippet.id}")

    var response: Interpreter.ExecuteResponse = null
    var continue = true
    while (continue) {
      response = codeIdToResponse.get(codeSnippet.id)
      if (null != response) {
        response = codeIdToResponse.get(codeSnippet.id)
        continue = false
      }
      info(s"waiting code snippet ${codeSnippet.id} finish")
      Thread.sleep(10)
    }
    response
  }

  override def close(): Unit = {
    // shutdown sparkContext
  }

  private def createSparkSession(): SparkSession = {
    val sparkClz = Class.forName("org.apache.spark.sql.SparkSession$")
    val sparkObj = sparkClz.getField("MODULE$").get(null)

    val builder = SparkSession.builder()
    val spark: SparkSession = if (conf.get("spark.sql.catalogImplementation", "in-memory").toLowerCase == "hive") {
      if (sparkClz.getMethod("hiveClassesArePresent").invoke(sparkObj).asInstanceOf[Boolean]) {
        val loader = Option(Thread.currentThread().getContextClassLoader)
          .getOrElse(getClass.getClassLoader)
        if (loader.getResource("hive-site.xml") == null) {
          warn("livy.repl.enable-hive-context is true but no hive-site.xml found on classpath.")
        }

        builder.enableHiveSupport()
        info("Created Spark session (with Hive support).")
        builder.getOrCreate()
      } else {
        info("Created Spark session.")
        builder.config(conf).getOrCreate()
      }
    } else {
      // builder.config(conf).getOrCreate()
      builder.getOrCreate()
    }
    spark
  }

  override def kind: String = "concurrentspark"
}
