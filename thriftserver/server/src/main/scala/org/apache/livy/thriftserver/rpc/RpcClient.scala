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

package org.apache.livy.thriftserver.rpc

import java.lang.reflect.InvocationTargetException

import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

import org.apache.hive.service.cli.SessionHandle
import org.apache.spark.sql.{Row, SparkSession}

import org.apache.livy._
import org.apache.livy.server.interactive.InteractiveSession
import org.apache.livy.thriftserver.serde.ColumnOrientedResultSet
import org.apache.livy.thriftserver.types.DataType
import org.apache.livy.utils.LivySparkUtils

class RpcClient(livySession: InteractiveSession) extends Logging {
  import RpcClient._

  private val defaultIncrementalCollect =
    livySession.livyConf.getBoolean(LivyConf.THRIFT_INCR_COLLECT_ENABLED).toString

  private val rscClient = livySession.client.get

  def isValid: Boolean = rscClient.isAlive

  private def sessionId(sessionHandle: SessionHandle): String = {
    sessionHandle.getSessionId.toString
  }

  @throws[Exception]
  def executeSql(
      sessionHandle: SessionHandle,
      statementId: String,
      statement: String): JobHandle[_] = {
    info(s"RSC client is executing SQL query: $statement, statementId = $statementId, session = " +
      sessionHandle)
    require(null != statementId, s"Invalid statementId specified. StatementId = $statementId")
    require(null != statement, s"Invalid statement specified. StatementId = $statement")
    livySession.recordActivity()
    rscClient.submit(executeSqlJob(sessionId(sessionHandle),
      statementId,
      statement,
      defaultIncrementalCollect,
      s"spark.${LivyConf.THRIFT_INCR_COLLECT_ENABLED}"))
  }

  @throws[Exception]
  def fetchResult(statementId: String,
      types: Array[DataType],
      maxRows: Int): JobHandle[ColumnOrientedResultSet] = {
    info(s"RSC client is fetching result for statementId $statementId with $maxRows maxRows.")
    require(null != statementId, s"Invalid statementId specified. StatementId = $statementId")
    livySession.recordActivity()
    rscClient.submit(fetchResultJob(statementId, types, maxRows))
  }

  @throws[Exception]
  def fetchResultSchema(statementId: String): JobHandle[String] = {
    info(s"RSC client is fetching result schema for statementId = $statementId")
    require(null != statementId, s"Invalid statementId specified. statementId = $statementId")
    livySession.recordActivity()
    rscClient.submit(fetchResultSchemaJob(statementId))
  }

  @throws[Exception]
  def cleanupStatement(statementId: String, cancelJob: Boolean = false): JobHandle[_] = {
    info(s"Cleaning up remote session for statementId = $statementId")
    require(null != statementId, s"Invalid statementId specified. statementId = $statementId")
    livySession.recordActivity()
    rscClient.submit(cleanupStatementJob(statementId))
  }

  /**
   * Creates a new Spark context for the specified session and stores it in a shared variable so
   * that any incoming session uses a different one: it is needed in order to avoid interactions
   * between different users working on the same remote Livy session (eg. setting a property,
   * changing database, etc.).
   */
  @throws[Exception]
  def executeRegisterSession(sessionHandle: SessionHandle): JobHandle[_] = {
    info(s"RSC client is executing register session $sessionHandle")
    livySession.recordActivity()
    rscClient.submit(registerSessionJob(sessionId(sessionHandle)))
  }

  /**
   * Removes the Spark session created for the specified session from the shared variable.
   */
  @throws[Exception]
  def executeUnregisterSession(sessionHandle: SessionHandle): JobHandle[_] = {
    info(s"RSC client is executing unregister session $sessionHandle")
    livySession.recordActivity()
    rscClient.submit(unregisterSessionJob(sessionId(sessionHandle)))
  }
}

/**
 * As remotely we don't have any class instance, all the job definitions are placed here in
 * order to enforce that we are not accessing any class attribute
 */
object RpcClient {
  // Maps a session ID to its SparkSession.
  val SESSION_SPARK_ENTRY_MAP = "livy.thriftserver.rpc_sessionIdToSparkSQLSession"
  val STATEMENT_RESULT_ITER_MAP = "livy.thriftserver.rpc_statementIdToResultIter"
  val STATEMENT_SCHEMA_MAP = "livy.thriftserver.rpc_statementIdToSchema"

  private def registerSessionJob(sessionId: String): Job[_] = new Job[Boolean] {
    override def call(jc: JobContext): Boolean = {
      val spark = jc.sparkSession[SparkSession]()
      val sessionSpecificSpark = spark.newSession()
      jc.sc().synchronized {
        val existingMap =
          Try(jc.getSharedObject[HashMap[String, SparkSession]](SESSION_SPARK_ENTRY_MAP))
            .getOrElse(new HashMap[String, AnyRef]())
        jc.setSharedObject(SESSION_SPARK_ENTRY_MAP,
          existingMap + ((sessionId, sessionSpecificSpark)))
        Try(jc.getSharedObject[HashMap[String, String]](STATEMENT_SCHEMA_MAP))
          .failed.foreach { _ =>
          jc.setSharedObject(STATEMENT_SCHEMA_MAP, new HashMap[String, String]())
        }
        Try(jc.getSharedObject[HashMap[String, Iterator[Row]]](STATEMENT_RESULT_ITER_MAP))
          .failed.foreach { _ =>
          jc.setSharedObject(STATEMENT_RESULT_ITER_MAP, new HashMap[String, Iterator[Row]]())
        }
      }
      true
    }
  }

  private def unregisterSessionJob(sessionId: String): Job[_] = new Job[Boolean] {
    override def call(jobContext: JobContext): Boolean = {
      jobContext.sc().synchronized {
        val existingMap =
          jobContext.getSharedObject[HashMap[String, SparkSession]](SESSION_SPARK_ENTRY_MAP)
        jobContext.setSharedObject(SESSION_SPARK_ENTRY_MAP, existingMap - sessionId)
      }
      true
    }
  }

  private def cleanupStatementJob(statementId: String): Job[_] = new Job[Boolean] {
    override def call(jc: JobContext): Boolean = {
      val sparkContext = jc.sc()
      sparkContext.cancelJobGroup(statementId)
      sparkContext.synchronized {
        // Clear job group only if current job group is same as expected job group.
        if (sparkContext.getLocalProperty("spark.jobGroup.id") == statementId) {
          sparkContext.clearJobGroup()
        }
        val iterMap = jc.getSharedObject[HashMap[String, Iterator[Row]]](STATEMENT_RESULT_ITER_MAP)
        jc.setSharedObject(STATEMENT_RESULT_ITER_MAP, iterMap - statementId)
        val schemaMap = jc.getSharedObject[HashMap[String, String]](STATEMENT_SCHEMA_MAP)
        jc.setSharedObject(STATEMENT_SCHEMA_MAP, schemaMap - statementId)
      }
      true
    }
  }

  private def fetchResultSchemaJob(statementId: String): Job[String] = new Job[String] {
    override def call(jobContext: JobContext): String = {
      jobContext.getSharedObject[HashMap[String, String]](STATEMENT_SCHEMA_MAP)(statementId)
    }
  }

  private def fetchResultJob(statementId: String,
      types: Array[DataType],
      maxRows: Int): Job[ColumnOrientedResultSet] = new Job[ColumnOrientedResultSet] {
    override def call(jobContext: JobContext): ColumnOrientedResultSet = {
      val statementIterMap =
        jobContext.getSharedObject[HashMap[String, Iterator[Row]]](STATEMENT_RESULT_ITER_MAP)
      val iter = statementIterMap(statementId)

      if (null == iter) {
        // Previous query execution failed.
        throw new NoSuchElementException("No successful query executed for output")
      }

      val resultSet = new ColumnOrientedResultSet(types)
      val numOfColumns = types.length
      if (!iter.hasNext) {
        resultSet
      } else {
        var curRow = 0
        while (curRow < maxRows && iter.hasNext) {
          val sparkRow = iter.next()
          val row = ArrayBuffer[Object]()
          var curCol: Integer = 0
          while (curCol < numOfColumns) {
            row += sparkRow.get(curCol).asInstanceOf[Object]
            curCol += 1
          }
          resultSet.addRow(row.toArray)
          curRow += 1
        }
        resultSet
      }
    }
  }

  private def executeSqlJob(sessionId: String,
      statementId: String,
      statement: String,
      defaultIncrementalCollect: String,
      incrementalCollectEnabledProp: String): Job[_] = new Job[Boolean] {
    override def call(jc: JobContext): Boolean = {
      val sparkContext = jc.sc()
      sparkContext.synchronized {
        sparkContext.setJobGroup(statementId, statement)
      }
      val spark =
        jc.getSharedObject[HashMap[String, SparkSession]](SESSION_SPARK_ENTRY_MAP)(sessionId)
      try {
        val result = spark.sql(statement)
        val jsonSchema = result.schema.json

        // Set the schema in the shared map
        sparkContext.synchronized {
          val existingMap = jc.getSharedObject[HashMap[String, String]](STATEMENT_SCHEMA_MAP)
          jc.setSharedObject(STATEMENT_SCHEMA_MAP, existingMap + ((statementId, jsonSchema)))
        }

        val incrementalCollect = spark.conf.get(incrementalCollectEnabledProp,
          defaultIncrementalCollect).toBoolean

        val iter = if (incrementalCollect) {
          result.rdd.toLocalIterator
        } else {
          result.collect().iterator
        }

        // Set the iterator in the shared map
        sparkContext.synchronized {
          val existingMap =
            jc.getSharedObject[HashMap[String, Iterator[Row]]](STATEMENT_RESULT_ITER_MAP)
          jc.setSharedObject(STATEMENT_RESULT_ITER_MAP, existingMap + ((statementId, iter)))
        }
      } catch {
        case e: InvocationTargetException => throw e.getCause
      }

      true
    }
  }
}
