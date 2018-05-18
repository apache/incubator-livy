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

import org.apache.livy.{Logging, Utils}

/**
 * Provide a class to control a Spark application using spark-submit.
 *
 * @param process The spark-submit process launched the Spark application.
 */
class SparkProcApp (
    process: LineBufferedProcess,
    listener: Option[SparkAppListener])
  extends SparkApp with Logging {

  private var state = SparkApp.State.STARTING

  private var killed = false

  override def kill(): Unit = {
    killed = true
    if (process.isAlive) {
      process.destroy()
      waitThread.join()
    }
  }

  override def log(): IndexedSeq[String] =
    ("stdout: " +: process.inputLines) ++ ("\nstderr: " +: process.errorLines)

  private def changeState(newState: SparkApp.State.Value) = {
    if (state != newState) {
      listener.foreach(_.stateChanged(state, newState))
      state = newState
    }
  }

  private val waitThread = Utils.startDaemonThread(s"SparProcApp_$this") {
    changeState(SparkApp.State.RUNNING)
    process.waitFor() match {
      case 0 => changeState(SparkApp.State.FINISHED)
      case exitCode =>
        val message = if (killed) {
          changeState(SparkApp.State.KILLED)
          "job was killed by user"
        } else {
          changeState(SparkApp.State.FAILED)
          s"spark-submit exited with code $exitCode"
        }
        error(message)
    }
  }
}
