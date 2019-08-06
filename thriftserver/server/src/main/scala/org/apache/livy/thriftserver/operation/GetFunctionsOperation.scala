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
import org.apache.livy.thriftserver.serde.ThriftResultSet
import org.apache.livy.thriftserver.session.GetFunctionsJob
import org.apache.livy.thriftserver.types.{BasicDataType, Field, Schema}
import org.apache.livy.thriftserver.LivyThriftSessionManager

class GetFunctionsOperation(
    sessionHandle: SessionHandle,
    catalogName: String,
    schemaName: String,
    functionName: String,
    sessionManager: LivyThriftSessionManager)
  extends MetadataOperation(sessionHandle, OperationType.GET_FUNCTIONS) with Logging {

  info("Starting GetFunctionsOperation")

  // The initialization need to be lazy in order not to block when the instance is created
  private lazy val rscClient = {
    // This call is blocking, we are waiting for the session to be ready.
    sessionManager.getLivySession(sessionHandle).client.get
  }

  protected val rowSet = ThriftResultSet.apply(GetFunctionsOperation.SCHEMA, protocolVersion)

  @throws(classOf[HiveSQLException])
  override protected def runInternal(): Unit = {
    setState(OperationState.RUNNING)
    info("Fetching function list")
    try {
      val functions = rscClient.submit(new GetFunctionsJob(
        convertSchemaPattern(schemaName),
        convertFunctionName(functionName)
      )).get()

      import scala.collection.JavaConverters._
      functions.asScala.foreach(f => rowSet.addRow(f.asInstanceOf[Array[Any]]))
      setState(OperationState.FINISHED)
      info("Fetching function list has been successfully finished")
    } catch {
      case e: Throwable =>
        setState(OperationState.ERROR)
        throw new HiveSQLException(e)
    }
  }

  @throws(classOf[HiveSQLException])
  override def getResultSetSchema: Schema = {
    assertState(Seq(OperationState.FINISHED))
    GetFunctionsOperation.SCHEMA
  }

  private def convertFunctionName(name: String): String = {
    if (name == null) {
      ".*"
    } else {
      var escape = false
      name.flatMap {
        case c if escape =>
          if (c != '\\') escape = false
          c.toString
        case '\\' =>
          escape = true
          ""
        case '%' => ".*"
        case '_' => "."
        case c => Character.toLowerCase(c).toString
      }
    }
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
      "The name which uniquely identifies this function within its schema")
  )
}
