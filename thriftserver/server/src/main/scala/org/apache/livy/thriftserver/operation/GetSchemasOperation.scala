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

package org.apache.livy.thriftserver.operation

import org.apache.hive.service.cli._

import org.apache.livy.Logging
import org.apache.livy.thriftserver.types.{BasicDataType, Field, Schema}
import org.apache.livy.thriftserver.LivyThriftSessionManager
import org.apache.livy.thriftserver.session.{GetSchemasJob, GetTablesJob}

class GetSchemasOperation(
    sessionHandle: SessionHandle,
    schemaPattern: String,
    sessionManager: LivyThriftSessionManager)
  extends SparkCatalogOperation(
    sessionHandle, OperationType.GET_SCHEMAS, sessionManager) with Logging {

  @throws(classOf[HiveSQLException])
  override protected def runInternal(): Unit = {
    setState(OperationState.RUNNING)
    try {
      rscClient.submit(new GetSchemasJob(
        schemaPattern,
        sessionId,
        jobId,
        GetSchemasOperation.SCHEMA.fields.map(_.fieldType.dataType))).get()
      setState(OperationState.FINISHED)
    } catch {
      case e: Throwable =>
        error("Remote job meet an exception: ", e)
        setState(OperationState.ERROR)
        throw new HiveSQLException(e)
    }
  }

  @throws(classOf[HiveSQLException])
  override def getResultSetSchema: Schema = {
    assertState(Seq(OperationState.FINISHED))
    GetSchemasOperation.SCHEMA
  }
}

object GetSchemasOperation {
  val SCHEMA = Schema(
    Field("TABLE_SCHEM", BasicDataType("string"), "Schema name."),
    Field("TABLE_CATALOG", BasicDataType("string"), "Catalog name."))
}
