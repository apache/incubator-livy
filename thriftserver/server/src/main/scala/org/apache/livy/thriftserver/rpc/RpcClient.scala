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

import org.apache.livy._
import org.apache.livy.server.interactive.InteractiveSession
import org.apache.livy.thriftserver.serde.ColumnOrientedResultSet
import org.apache.livy.thriftserver.types.DataType
import org.apache.livy.utils.LivySparkUtils

class RpcClient(livySession: InteractiveSession) extends Logging {
  import RpcClient._

  private val isSpark1 = {
    val (sparkMajorVersion, _) =
      LivySparkUtils.formatSparkVersion(livySession.livyConf.get(LivyConf.LIVY_SPARK_VERSION))
    sparkMajorVersion == 1
  }
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
      isSpark1,
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
    rscClient.submit(registerSessionJob(sessionId(sessionHandle), isSpark1))
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

object RpcClient {
  val SPARK_CONTEXT_MAP = "sparkContextMap"
  val STATEMENT_RESULT_ITER_MAP = "statementIdToResultIter"
  val STATEMENT_SCHEMA_MAP = "statementIdToSchema"

  // As remotely we don't have any class instance, all the job definitions are placed here in
  // order to enforce that we are not accessing any class attribute
  private def registerSessionJob(sessionId: String, isSpark1: Boolean): Job[_] = new Job[Boolean] {
    override def call(jc: JobContext): Boolean = {
      val spark: Any = if (isSpark1) {
        Option(jc.hivectx()).getOrElse(jc.sqlctx())
      } else {
        jc.sparkSession()
      }
      val sessionSpecificSpark = spark.getClass.getMethod("newSession").invoke(spark)
      jc.sc().synchronized {
        val existingMap =
          Try(jc.getSharedObject[HashMap[String, AnyRef]](SPARK_CONTEXT_MAP))
            .getOrElse(new HashMap[String, AnyRef]())
        jc.setSharedObject(SPARK_CONTEXT_MAP,
          existingMap + ((sessionId, sessionSpecificSpark)))
        Try(jc.getSharedObject[HashMap[String, String]](STATEMENT_SCHEMA_MAP))
          .failed.foreach { _ =>
          jc.setSharedObject(STATEMENT_SCHEMA_MAP, new HashMap[String, String]())
        }
        Try(jc.getSharedObject[HashMap[String, Iterator[_]]](STATEMENT_RESULT_ITER_MAP))
          .failed.foreach { _ =>
          jc.setSharedObject(STATEMENT_RESULT_ITER_MAP, new HashMap[String, Iterator[_]]())
        }
      }
      true
    }
  }

  private def unregisterSessionJob(sessionId: String): Job[_] = new Job[Boolean] {
    override def call(jobContext: JobContext): Boolean = {
      jobContext.sc().synchronized {
        val existingMap =
          jobContext.getSharedObject[HashMap[String, AnyRef]](SPARK_CONTEXT_MAP)
        jobContext.setSharedObject(SPARK_CONTEXT_MAP, existingMap - sessionId)
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
        val iterMap = jc.getSharedObject[HashMap[String, Iterator[_]]](STATEMENT_RESULT_ITER_MAP)
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
        jobContext.getSharedObject[HashMap[String, Iterator[_]]](STATEMENT_RESULT_ITER_MAP)
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
          val row = ArrayBuffer[Any]()
          var curCol: Integer = 0
          while (curCol < numOfColumns) {
            row += sparkRow.getClass.getMethod("get", classOf[Int]).invoke(sparkRow, curCol)
            curCol += 1
          }
          resultSet.addRow(row.toArray.asInstanceOf[Array[Object]])
          curRow += 1
        }
        resultSet
      }
    }
  }

  private def executeSqlJob(sessionId: String,
      statementId: String,
      statement: String,
      isSpark1: Boolean,
      defaultIncrementalCollect: String,
      incrementalCollectEnabledProp: String): Job[_] = new Job[Boolean] {
    override def call(jc: JobContext): Boolean = {
      val sparkContext = jc.sc()
      sparkContext.synchronized {
        sparkContext.setJobGroup(statementId, statement)
      }
      val spark = jc.getSharedObject[HashMap[String, AnyRef]](SPARK_CONTEXT_MAP)(sessionId)
      try {
        val result = spark.getClass.getMethod("sql", classOf[String]).invoke(spark, statement)
        val schema = result.getClass.getMethod("schema").invoke(result)
        val jsonString = schema.getClass.getMethod("json").invoke(schema).asInstanceOf[String]

        // Set the schema in the shared map
        sparkContext.synchronized {
          val existingMap = jc.getSharedObject[HashMap[String, String]](STATEMENT_SCHEMA_MAP)
          jc.setSharedObject(STATEMENT_SCHEMA_MAP, existingMap + ((statementId, jsonString)))
        }

        val incrementalCollect = {
          if (isSpark1) {
            spark.getClass.getMethod("getConf", classOf[String], classOf[String])
              .invoke(spark,
                incrementalCollectEnabledProp,
                defaultIncrementalCollect)
              .asInstanceOf[String].toBoolean
          } else {
            val conf = spark.getClass.getMethod("conf").invoke(spark)
            conf.getClass.getMethod("get", classOf[String], classOf[String])
              .invoke(conf,
                incrementalCollectEnabledProp,
                defaultIncrementalCollect)
              .asInstanceOf[String].toBoolean
          }
        }

        val iter = if (incrementalCollect) {
          val rdd = result.getClass.getMethod("rdd").invoke(result)
          rdd.getClass.getMethod("toLocalIterator").invoke(rdd).asInstanceOf[Iterator[_]]
        } else {
          result.getClass.getMethod("collect").invoke(result).asInstanceOf[Array[_]].iterator
        }

        // Set the iterator in the shared map
        sparkContext.synchronized {
          val existingMap =
            jc.getSharedObject[HashMap[String, Iterator[_]]](STATEMENT_RESULT_ITER_MAP)
          jc.setSharedObject(STATEMENT_RESULT_ITER_MAP, existingMap + ((statementId, iter)))
        }
      } catch {
        case e: InvocationTargetException => throw e.getCause
      }

      true
    }
  }
}
