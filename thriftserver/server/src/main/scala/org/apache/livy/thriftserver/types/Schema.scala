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

package org.apache.livy.thriftserver.types

import org.apache.hive.service.rpc.thrift.{TColumnDesc, TTableSchema}

case class Field(name: String, dataType: DataType, comment: String)

class Schema(val fields: Array[Field]) {

  def toTTableSchema: TTableSchema = {
    val tTableSchema = new TTableSchema()
    fields.zipWithIndex.foreach { case (f, idx) =>
      tTableSchema.addToColumns(Schema.columnDescriptor(f, idx))
    }
    tTableSchema
  }
}

object Schema {
  def apply(fields: Field*): Schema = new Schema(fields.toArray)

  def apply(fields: Array[Field]): Schema = new Schema(fields)

  def apply(names: Array[String], types: Array[DataType]): Schema = {
    assert(names.length == types.length)
    val fields = names.zip(types).map { case (n, dt) => Field(n, dt, "") }
    apply(fields)
  }

  private def columnDescriptor(field: Field, index: Int): TColumnDesc = {
    val tColumnDesc = new TColumnDesc
    tColumnDesc.setColumnName(field.name)
    tColumnDesc.setComment(field.comment)
    tColumnDesc.setTypeDesc(DataTypeUtils.toTTypeDesc(field.dataType))
    tColumnDesc.setPosition(index)
    tColumnDesc
  }
}
