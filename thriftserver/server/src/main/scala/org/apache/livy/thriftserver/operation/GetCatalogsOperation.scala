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
import org.apache.livy.thriftserver.serde.ThriftResultSet
import org.apache.livy.thriftserver.types.{BasicDataType, Field, Schema}

class GetCatalogsOperation(sessionHandle: SessionHandle)
  extends MetadataOperation(sessionHandle, OperationType.GET_TABLE_TYPES) with Logging {

  protected val rowSet = ThriftResultSet.apply(GetCatalogsOperation.SCHEMA, protocolVersion)

  info("Starting GetCatalogsOperation")

  @throws(classOf[HiveSQLException])
  override def runInternal(): Unit = {
    setState(OperationState.RUNNING)
    info("Fetching catalogs metadata")
    try {
      // catalogs are actually not supported in spark, so this is a no-op
      setState(OperationState.FINISHED)
      info("Fetching catalogs has been successfully finished")
    } catch {
      case e: Throwable =>
        setState(OperationState.ERROR)
        throw new HiveSQLException(e)
    }
  }

  @throws(classOf[HiveSQLException])
  override def getResultSetSchema: Schema = {
    assertState(Seq(OperationState.FINISHED))
    GetCatalogsOperation.SCHEMA
  }
}

object GetCatalogsOperation {
  val SCHEMA = Schema(Field("TABLE_CAT", BasicDataType("string"),
    "Catalog name. NULL if not applicable."))
}
