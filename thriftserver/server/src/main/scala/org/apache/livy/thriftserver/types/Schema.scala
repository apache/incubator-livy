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

import org.apache.hive.service.rpc.thrift.{TColumnDesc, TPrimitiveTypeEntry, TTableSchema, TTypeDesc, TTypeEntry, TTypeId}

import org.apache.livy.thriftserver.session.DataType

private[thriftserver] trait FieldType {
  def name: String
  def dataType: DataType
}

sealed trait ComplexFieldType extends FieldType {
  override val dataType = DataType.STRING
}

case class BasicDataType(name: String) extends FieldType {

  override val dataType: DataType = name match {
    case "boolean" => DataType.BOOLEAN
    case "byte" => DataType.BYTE
    case "short" => DataType.SHORT
    case "integer" => DataType.INTEGER
    case "long" => DataType.LONG
    case "float" => DataType.FLOAT
    case "double" => DataType.DOUBLE
    case "binary" => DataType.BINARY
    case _ if name.contains("decimal") => DataType.DECIMAL
    case "timestamp" => DataType.TIMESTAMP
    case "date" => DataType.DATE
    case _ => DataType.STRING
  }
}

case class StructType(fields: Array[Field]) extends ComplexFieldType {
  override val name = "struct"
}

case class ArrayType(elementsType: FieldType) extends ComplexFieldType {
  val name = "array"
}

case class MapType(keyType: FieldType, valueType: FieldType) extends ComplexFieldType {
  val name = "map"
}

case class Field(name: String, fieldType: FieldType, comment: String)

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

  def apply(names: Array[String], types: Array[FieldType]): Schema = {
    assert(names.length == types.length)
    val fields = names.zip(types).map { case (n, dt) => Field(n, dt, "") }
    apply(fields)
  }

  private def columnDescriptor(field: Field, index: Int): TColumnDesc = {
    val tColumnDesc = new TColumnDesc
    tColumnDesc.setColumnName(field.name)
    tColumnDesc.setComment(field.comment)
    tColumnDesc.setTypeDesc(toTTypeDesc(field.fieldType.dataType))
    tColumnDesc.setPosition(index)
    tColumnDesc
  }

  private def toTTypeDesc(dt: DataType): TTypeDesc = {
    val typeId = dt match {
      case DataType.BOOLEAN => TTypeId.BOOLEAN_TYPE
      case DataType.BYTE => TTypeId.TINYINT_TYPE
      case DataType.SHORT => TTypeId.SMALLINT_TYPE
      case DataType.INTEGER => TTypeId.INT_TYPE
      case DataType.LONG => TTypeId.BIGINT_TYPE
      case DataType.FLOAT => TTypeId.FLOAT_TYPE
      case DataType.DOUBLE => TTypeId.DOUBLE_TYPE
      case DataType.BINARY => TTypeId.BINARY_TYPE
      case DataType.DECIMAL => TTypeId.DECIMAL_TYPE
      case DataType.TIMESTAMP => TTypeId.TIMESTAMP_TYPE
      case DataType.DATE => TTypeId.DATE_TYPE
      case _ => TTypeId.STRING_TYPE
    }
    val primitiveEntry = new TPrimitiveTypeEntry(typeId)
    val entry = TTypeEntry.primitiveEntry(primitiveEntry)
    val desc = new TTypeDesc
    desc.addToTypes(entry)
    desc
  }
}
