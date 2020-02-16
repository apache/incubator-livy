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

import org.apache.hive.service.cli.{HiveSQLException, OperationState, OperationType, SessionHandle}

import org.apache.livy.Logging
import org.apache.livy.thriftserver.session.GetFunctionsJob
import org.apache.livy.thriftserver.types.{BasicDataType, Field, Schema}
import org.apache.livy.thriftserver.LivyThriftSessionManager

class GetFunctionsOperation(
    sessionHandle: SessionHandle,
    schemaPattern: String,
    functionSQLSearch: String,
    sessionManager: LivyThriftSessionManager)
  extends SparkCatalogOperation(
    sessionHandle, OperationType.GET_FUNCTIONS, sessionManager) with Logging {

  @throws(classOf[HiveSQLException])
  override protected def runInternal(): Unit = {
    setState(OperationState.RUNNING)
    try {
      rscClient.submit(new GetFunctionsJob(
        schemaPattern,
        functionSQLSearch,
        sessionId,
        jobId,
        GetFunctionsOperation.SCHEMA.fields.map(_.fieldType.dataType))).get()

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
    GetFunctionsOperation.SCHEMA
  }
}

object GetFunctionsOperation {
  val SCHEMA = Schema(
    Field("FUNCTION_CAT", BasicDataType("string"), "Function catalog (may be null)"),
    Field("FUNCTION_SCHEM", BasicDataType("string"), "Function schema (may be null)"),
    Field("FUNCTION_NAME", BasicDataType("string"),
      "Function name. This is the name used to invoke the function"),
    Field("REMARKS", BasicDataType("string"), "Explanatory comment on the function"),
    Field("FUNCTION_TYPE", BasicDataType("integer"),
      "Kind of function."),
    Field("SPECIFIC_NAME", BasicDataType("string"),
      "The name which uniquely identifies this function within its schema"))
}
