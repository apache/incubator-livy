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

import org.apache.hive.service.cli.SessionHandle

import org.apache.livy._
import org.apache.livy.server.interactive.InteractiveSession
import org.apache.livy.thriftserver.session._

class RpcClient(livySession: InteractiveSession) extends Logging {
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
    rscClient.submit(new SqlJob(
      sessionId(sessionHandle),
      statementId,
      statement,
      defaultIncrementalCollect,
      s"spark.${LivyConf.THRIFT_INCR_COLLECT_ENABLED}"))
  }

  @throws[Exception]
  def fetchResult(
      sessionHandle: SessionHandle,
      statementId: String,
      maxRows: Int): JobHandle[ResultSet] = {
    info(s"RSC client is fetching result for statementId $statementId with $maxRows maxRows.")
    require(null != statementId, s"Invalid statementId specified. StatementId = $statementId")
    livySession.recordActivity()
    rscClient.submit(new FetchResultJob(sessionId(sessionHandle), statementId, maxRows))
  }

  @throws[Exception]
  def fetchResultSchema(sessionHandle: SessionHandle, statementId: String): JobHandle[String] = {
    info(s"RSC client is fetching result schema for statementId = $statementId")
    require(null != statementId, s"Invalid statementId specified. statementId = $statementId")
    livySession.recordActivity()
    rscClient.submit(new FetchResultSchemaJob(sessionId(sessionHandle), statementId))
  }

  @throws[Exception]
  def cleanupStatement(
      sessionHandle: SessionHandle,
      statementId: String,
      cancelJob: Boolean = false): JobHandle[java.lang.Boolean] = {
    info(s"Cleaning up remote session for statementId = $statementId")
    require(null != statementId, s"Invalid statementId specified. statementId = $statementId")
    livySession.recordActivity()
    rscClient.submit(new CleanupStatementJob(sessionId(sessionHandle), statementId))
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
    rscClient.submit(new RegisterSessionJob(sessionId(sessionHandle)))
  }

  /**
   * Removes the Spark session created for the specified session from the shared variable.
   */
  @throws[Exception]
  def executeUnregisterSession(sessionHandle: SessionHandle): JobHandle[_] = {
    info(s"RSC client is executing unregister session $sessionHandle")
    livySession.recordActivity()
    rscClient.submit(new UnregisterSessionJob(sessionId(sessionHandle)))
  }
}
