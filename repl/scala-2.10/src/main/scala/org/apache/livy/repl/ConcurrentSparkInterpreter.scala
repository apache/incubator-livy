package org.apache.livy.repl

import org.apache.livy.Logging
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author naruto.zwb
  */
class ConcurrentSparkInterpreter(interpretersCount: Int, conf: SparkConf) extends Interpreter with Logging {
  throw new RuntimeException("concurrentspark session not support scala-2.10, please use scala-2.11")

  override def kind: String = {
    null
  }

  /**
    * Start the Interpreter.
    *
    * @return A SparkContext
    */
  override def start(): SparkContext = {
    null
  }

  /**
    * Execute the code and return the result, it may
    * take some time to execute.
    */
  override def execute(code: String): Interpreter.ExecuteResponse = {
    null
  }

  /** Shut down the interpreter. */
  override def close(): Unit = {

  }
}
