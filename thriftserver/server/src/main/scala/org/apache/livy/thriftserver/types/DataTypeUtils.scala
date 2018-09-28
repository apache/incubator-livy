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

import org.apache.hive.service.rpc.thrift.{TPrimitiveTypeEntry, TTypeDesc, TTypeEntry, TTypeId}
import org.json4s.{DefaultFormats, JValue}
import org.json4s.JsonAST.{JObject, JString}
import org.json4s.jackson.JsonMethods.parse

import org.apache.livy.Logging

/**
 * Utility class for converting and representing Spark and Hive data types.
 */
object DataTypeUtils extends Logging {
  // Used for JSON conversion
  private implicit val formats = DefaultFormats

  private def toDataType(jValue: JValue): DataType = {
    jValue match {
      case JString(t) => BasicDataType(t)
      case o: JObject => complexToDataType(o)
      case _ => throw new IllegalArgumentException(
        s"Spark type was neither a string nor a object. It was: $jValue.")
    }
  }

  private def complexToDataType(sparkType: JObject): DataType = {
    (sparkType \ "type").extract[String] match {
      case "array" => ArrayType(toDataType(sparkType \ "elementType"))
      case "struct" =>
        val fields = (sparkType \ "fields").children.map { f =>
          // TODO: get comment from metadata
          Field((f \ "name").extract[String], toDataType(f \ "type"), "")
        }
        StructType(fields.toArray)
      case "map" =>
        MapType(toDataType(sparkType \ "keyType"), toDataType(sparkType \ "valueType"))
      case "udt" => toDataType(sparkType \ "sqlType")
    }
  }

  /**
   * Converts a JSON representing the Spark schema (the one returned by `df.schema.json`) into
   * a [[Schema]] instance.
   *
   * @param sparkJson a [[String]] containing the JSON representation of a Spark Dataframe schema
   * @return a [[Schema]] representing the schema provided as input
   */
  def schemaFromSparkJson(sparkJson: String): Schema = {
    val schema = parse(sparkJson) \ "fields"
    val fields = schema.children.map { field =>
      val name = (field \ "name").extract[String]
      val hiveType = toDataType(field \ "type")
      // TODO: retrieve comment from metadata
      Field(name, hiveType, "")
    }
    Schema(fields.toArray)
  }

  /**
   * Extracts the main type of each column contained in the JSON. This means that complex types
   * are not returned in their full representation with the nested types: eg. for an array of any
   * kind of data it returns `"array"`.
   *
   * @param sparkJson a [[String]] containing the JSON representation of a Spark Dataframe schema
   * @return an [[Array]] of the principal type of the columns is the schema.
   */
  def getInternalTypes(sparkJson: String): Array[DataType] = {
    val schema = parse(sparkJson) \ "fields"
    schema.children.map { field =>
      toDataType(field \ "type")
    }.toArray
  }

  /**
   * Converts {@param value} of type {@param dt} into its corresponding string representation
   * as it is returned by Hive.x
   */
  def toHiveString(value: Any, dt: DataType): String = (value, dt) match {
    case (null, _) => "NULL"
    case (struct: Any, StructType(fields)) =>
      val values = struct.getClass.getMethod("toSeq").invoke(struct).asInstanceOf[Seq[Any]]
      values.zip(fields).map {
        case (v, t) => s""""${t.name}":${toHiveComplexTypeFieldString((v, t.dataType))}"""
      }.mkString("{", ",", "}")
    case (seq: Seq[_], ArrayType(t)) =>
      seq.map(v => (v, t)).map(toHiveComplexTypeFieldString).mkString("[", ",", "]")
    case (map: Map[_, _], MapType(kType, vType)) =>
      map.map { case (k, v) =>
        s"${toHiveComplexTypeFieldString((k, kType))}:${toHiveComplexTypeFieldString((v, vType))}"
      }.toSeq.sorted.mkString("{", ",", "}")
    case (decimal: java.math.BigDecimal, t) if t.name.startsWith("decimal") =>
      decimal.stripTrailingZeros.toString
    case (other, _) => other.toString
  }

  private def toHiveComplexTypeFieldString(a: (Any, DataType)): String = a match {
    case (null, _) => "null"
    case (struct: Any, StructType(fields)) =>
      val values = struct.getClass.getMethod("toSeq").invoke(struct).asInstanceOf[Seq[Any]]
      values.zip(fields).map {
        case (v, t) => s""""${t.name}":${toHiveComplexTypeFieldString((v, t.dataType))}"""
      }.mkString("{", ",", "}")
    case (seq: Seq[_], ArrayType(t)) =>
      seq.map(v => (v, t)).map(toHiveComplexTypeFieldString).mkString("[", ",", "]")
    case (map: Map[_, _], MapType(kType, vType)) =>
      map.map { case (k, v) =>
        s"${toHiveComplexTypeFieldString((k, kType))}:${toHiveComplexTypeFieldString((v, vType))}"
      }.toSeq.sorted.mkString("{", ",", "}")
    case (s: String, t) if t.name == "string" => s""""$s""""
    case (other, _) => other.toString
  }

  def toTTypeDesc(dt: DataType): TTypeDesc = {
    val typeId = dt.name match {
      case "boolean" => TTypeId.BOOLEAN_TYPE
      case "byte" => TTypeId.TINYINT_TYPE
      case "short" => TTypeId.SMALLINT_TYPE
      case "integer" => TTypeId.INT_TYPE
      case "long" => TTypeId.BIGINT_TYPE
      case "float" => TTypeId.FLOAT_TYPE
      case "double" => TTypeId.DOUBLE_TYPE
      case "binary" => TTypeId.BINARY_TYPE
      case _ => TTypeId.STRING_TYPE
    }
    val primitiveEntry = new TPrimitiveTypeEntry(typeId)
    val entry = TTypeEntry.primitiveEntry(primitiveEntry)
    val desc = new TTypeDesc
    desc.addToTypes(entry)
    desc
  }
}
