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

import java.util.{LinkedHashMap => JLinkedHashMap}
import java.util.Map.Entry
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.control.NonFatal

import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.jackson.JsonMethods.{compact, render}
import org.json4s.DefaultFormats
import org.json4s.JsonDSL._

import org.apache.livy.Logging
import org.apache.livy.rsc.RSCConf
import org.apache.livy.rsc.driver.{SparkEntries, Statement, StatementState}
import org.apache.livy.sessions._

object Session {
  val STATUS = "status"
  val OK = "ok"
  val ERROR = "error"
  val EXECUTION_COUNT = "execution_count"
  val DATA = "data"
  val ENAME = "ename"
  val EVALUE = "evalue"
  val TRACEBACK = "traceback"
}

class Session(
    livyConf: RSCConf,
    sparkConf: SparkConf,
    mockSparkInterpreter: Option[SparkInterpreter] = None,
    stateChangedCallback: SessionState => Unit = { _ => })
  extends Logging {
  import Session._

  private val interpreterExecutor = ExecutionContext.fromExecutorService(
    Executors.newSingleThreadExecutor())

  private val cancelExecutor = ExecutionContext.fromExecutorService(
    Executors.newSingleThreadExecutor())

  private implicit val formats = DefaultFormats

  private var _state: SessionState = SessionState.NotStarted

  // Number of statements kept in driver's memory
  private val numRetainedStatements = livyConf.getInt(RSCConf.Entry.RETAINED_STATEMENTS)

  private val _statements = new JLinkedHashMap[Int, Statement] {
    protected override def removeEldestEntry(eldest: Entry[Int, Statement]): Boolean = {
      size() > numRetainedStatements
    }
  }.asScala

  private val newStatementId = new AtomicInteger(0)

  private val defaultInterpKind = Kind(livyConf.get(RSCConf.Entry.SESSION_KIND))

  private val interpGroup = new mutable.HashMap[Kind, Interpreter]()

  @volatile private var entries: SparkEntries = _

  stateChangedCallback(_state)

  private def sc: SparkContext = {
    require(entries != null)
    entries.sc().sc
  }

  private[repl] def interpreter(kind: Kind): Option[Interpreter] = interpGroup.synchronized {
    if (interpGroup.contains(kind)) {
      Some(interpGroup(kind))
    } else {
      try {
        require(entries != null,
          "SparkEntries should not be null when lazily initialize other interpreters.")

        val interp = kind match {
          case Spark =>
            // This should never be touched here.
            throw new IllegalStateException("SparkInterpreter should not be lazily created.")
          case PySpark => PythonInterpreter(sparkConf, entries)
          case SparkR => SparkRInterpreter(sparkConf, entries)
          case SQL => new SQLInterpreter(sparkConf, livyConf, entries)
        }
        interp.start()
        interpGroup(kind) = interp
        Some(interp)
      } catch {
        case NonFatal(e) =>
          warn(s"Fail to start interpreter $kind", e)
          None
      }
    }
  }

  def start(): Future[SparkEntries] = {
    val future = Future {
      changeState(SessionState.Starting)

      // Always start SparkInterpreter after beginning, because we rely on SparkInterpreter to
      // initialize SparkContext and create SparkEntries.
      val sparkInterp = mockSparkInterpreter.getOrElse(new SparkInterpreter(sparkConf))
      sparkInterp.start()

      entries = sparkInterp.sparkEntries()
      require(entries != null, "SparkEntries object should not be null in Spark Interpreter.")
      interpGroup.synchronized {
        interpGroup.put(Spark, sparkInterp)
      }

      changeState(SessionState.Idle)
      entries
    }(interpreterExecutor)

    future.onFailure { case _ => changeState(SessionState.Error()) }(interpreterExecutor)
    future
  }

  def state: SessionState = _state

  def statements: collection.Map[Int, Statement] = _statements.synchronized {
    _statements.toMap
  }

  def execute(code: String, codeType: String = null): Int = {
    val tpe = if (codeType != null) {
      Kind(codeType)
    } else if (defaultInterpKind != Shared) {
      defaultInterpKind
    } else {
      throw new IllegalArgumentException(s"Code type should be specified if session kind is shared")
    }

    val statementId = newStatementId.getAndIncrement()
    val statement = new Statement(statementId, code, StatementState.Waiting, null)
    _statements.synchronized { _statements(statementId) = statement }

    Future {
      setJobGroup(tpe, statementId)
      statement.compareAndTransit(StatementState.Waiting, StatementState.Running)

      if (statement.state.get() == StatementState.Running) {
        statement.started = System.currentTimeMillis()
        statement.output = executeCode(interpreter(tpe), statementId, code)
      }

      statement.compareAndTransit(StatementState.Running, StatementState.Available)
      statement.compareAndTransit(StatementState.Cancelling, StatementState.Cancelled)
      statement.updateProgress(1.0)
      statement.completed = System.currentTimeMillis()
    }(interpreterExecutor)

    statementId
  }

  def complete(code: String, codeType: String, cursor: Int): Array[String] = {
    val tpe = Kind(codeType)
    interpreter(tpe).map { _.complete(code, cursor) }.getOrElse(Array.empty)
  }

  def cancel(statementId: Int): Unit = {
    val statementOpt = _statements.synchronized { _statements.get(statementId) }
    if (statementOpt.isEmpty) {
      return
    }

    val statement = statementOpt.get
    if (statement.state.get().isOneOf(
      StatementState.Available, StatementState.Cancelled, StatementState.Cancelling)) {
      return
    } else {
      // statement 1 is running and statement 2 is waiting. User cancels
      // statement 2 then cancels statement 1. The 2nd cancel call will loop and block the 1st
      // cancel call since cancelExecutor is single threaded. To avoid this, set the statement
      // state to cancelled when cancelling a waiting statement.
      statement.compareAndTransit(StatementState.Waiting, StatementState.Cancelled)
      statement.compareAndTransit(StatementState.Running, StatementState.Cancelling)
    }

    info(s"Cancelling statement $statementId...")

    Future {
      val deadline = livyConf.getTimeAsMs(RSCConf.Entry.JOB_CANCEL_TIMEOUT).millis.fromNow

      while (statement.state.get() == StatementState.Cancelling) {
        if (deadline.isOverdue()) {
          info(s"Failed to cancel statement $statementId.")
          statement.compareAndTransit(StatementState.Cancelling, StatementState.Cancelled)
        } else {
          sc.cancelJobGroup(statementId.toString)
          if (statement.state.get() == StatementState.Cancelling) {
            Thread.sleep(livyConf.getTimeAsMs(RSCConf.Entry.JOB_CANCEL_TRIGGER_INTERVAL))
          }
        }
      }

      if (statement.state.get() == StatementState.Cancelled) {
        statement.completed = System.currentTimeMillis()
        info(s"Statement $statementId cancelled.")
      }
    }(cancelExecutor)
  }

  def close(): Unit = {
    interpreterExecutor.shutdown()
    cancelExecutor.shutdown()
    interpGroup.values.foreach(_.close())
  }

  /**
   * Get the current progress of given statement id.
   */
  def progressOfStatement(stmtId: Int): Double = {
    val jobGroup = statementIdToJobGroup(stmtId)

    val jobIds = sc.statusTracker.getJobIdsForGroup(jobGroup)
    val jobs = jobIds.flatMap { id => sc.statusTracker.getJobInfo(id) }
    val stages = jobs.flatMap { job =>
      job.stageIds().flatMap(sc.statusTracker.getStageInfo)
    }

    val taskCount = stages.map(_.numTasks).sum
    val completedTaskCount = stages.map(_.numCompletedTasks).sum
    if (taskCount == 0) {
      0.0
    } else {
      completedTaskCount.toDouble / taskCount
    }
  }

  private def changeState(newState: SessionState): Unit = {
    synchronized {
      _state = newState
    }
    stateChangedCallback(newState)
  }

  private def executeCode(interp: Option[Interpreter],
     executionCount: Int,
     code: String): String = {
    changeState(SessionState.Busy)

    def transitToIdle() = {
      val executingLastStatement = executionCount == newStatementId.intValue() - 1
      if (_statements.isEmpty || executingLastStatement) {
        changeState(SessionState.Idle)
      }
    }

    val resultInJson = interp.map { i =>
      try {
        i.execute(code) match {
          case Interpreter.ExecuteSuccess(data) =>
            transitToIdle()

            (STATUS -> OK) ~
              (EXECUTION_COUNT -> executionCount) ~
              (DATA -> data)

          case Interpreter.ExecuteIncomplete() =>
            transitToIdle()

            (STATUS -> ERROR) ~
              (EXECUTION_COUNT -> executionCount) ~
              (ENAME -> "Error") ~
              (EVALUE -> "incomplete statement") ~
              (TRACEBACK -> Seq.empty[String])

          case Interpreter.ExecuteError(ename, evalue, traceback) =>
            transitToIdle()

            (STATUS -> ERROR) ~
              (EXECUTION_COUNT -> executionCount) ~
              (ENAME -> ename) ~
              (EVALUE -> evalue) ~
              (TRACEBACK -> traceback)

          case Interpreter.ExecuteAborted(message) =>
            changeState(SessionState.Error())

            (STATUS -> ERROR) ~
              (EXECUTION_COUNT -> executionCount) ~
              (ENAME -> "Error") ~
              (EVALUE -> f"Interpreter died:\n$message") ~
              (TRACEBACK -> Seq.empty[String])
        }
      } catch {
        case e: Throwable =>
          error("Exception when executing code", e)

          transitToIdle()

          (STATUS -> ERROR) ~
            (EXECUTION_COUNT -> executionCount) ~
            (ENAME -> f"Internal Error: ${e.getClass.getName}") ~
            (EVALUE -> e.getMessage) ~
            (TRACEBACK -> Seq.empty[String])
      }
    }.getOrElse {
      transitToIdle()
      (STATUS -> ERROR) ~
        (EXECUTION_COUNT -> executionCount) ~
        (ENAME -> "InterpreterError") ~
        (EVALUE -> "Fail to start interpreter") ~
        (TRACEBACK -> Seq.empty[String])
    }

    compact(render(resultInJson))
  }

  private def setJobGroup(codeType: Kind, statementId: Int): String = {
    val jobGroup = statementIdToJobGroup(statementId)
    val (cmd, tpe) = codeType match {
      case Spark | SQL =>
        // A dummy value to avoid automatic value binding in scala REPL.
        (s"""val _livyJobGroup$jobGroup = sc.setJobGroup("$jobGroup",""" +
          s""""Job group for statement $jobGroup")""",
         Spark)
      case PySpark =>
        (s"""sc.setJobGroup("$jobGroup", "Job group for statement $jobGroup")""", PySpark)
      case SparkR =>
        sc.getConf.get("spark.livy.spark_major_version", "1") match {
          case "1" =>
            (s"""setJobGroup(sc, "$jobGroup", "Job group for statement $jobGroup", FALSE)""",
             SparkR)
          case "2" =>
            (s"""setJobGroup("$jobGroup", "Job group for statement $jobGroup", FALSE)""", SparkR)
        }
    }
    // Set the job group
    executeCode(interpreter(tpe), statementId, cmd)
  }

  private def statementIdToJobGroup(statementId: Int): String = {
    statementId.toString
  }
}
