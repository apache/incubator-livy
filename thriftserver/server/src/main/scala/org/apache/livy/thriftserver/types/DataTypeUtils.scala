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

import scala.collection.JavaConverters._

import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.serde2.thrift.Type
import org.apache.hive.service.cli.TableSchema
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

  private def toHive(jValue: JValue): String = {
    jValue match {
      case JString(t) => primitiveToHive(t)
      case o: JObject => complexToHive(o)
      case _ => throw new IllegalArgumentException(
        s"Spark type was neither a string nor a object. It was: $jValue.")
    }
  }

  private def getInternalType(jValue: JValue): DataType = {
    jValue match {
      case JString(t) => BasicDataType(t)
      case o: JObject => complexToInternal(o)
      case _ => throw new IllegalArgumentException(
        s"Spark type was neither a string nor a object. It was: $jValue.")
    }
  }

  private def primitiveToHive(sparkType: String): String = {
    sparkType match {
      case "integer" => "int"
      case "long" => "bigint"
      case "short" => "smallint"
      case "byte" => "tinyint"
      case "null" => "void"
      // boolean, string, float, double, decimal, date, timestamp are the same
      case other => other
    }
  }

  private def complexToHive(sparkType: JObject): String = {
    (sparkType \ "type").extract[String] match {
      case "array" => s"array<${toHive(sparkType \ "elementType")}>"
      case "struct" =>
        val fields = (sparkType \ "fields").children.map { f =>
          s"${(f \ "name").extract[String]}:${toHive(f \ "type")}"
        }
        s"struct<${fields.mkString(",")}>"
      case "map" => s"map<${toHive(sparkType \ "keyType")}, ${toHive(sparkType \ "valueType")}>"
      case "udt" => toHive(sparkType \ "sqlType")
    }
  }

  private def complexToInternal(sparkType: JObject): DataType = {
    (sparkType \ "type").extract[String] match {
      case "array" => ArrayType(getInternalType(sparkType \ "elementType"))
      case "struct" =>
        val fields = (sparkType \ "fields").children.map { f =>
          StructField((f \ "name").extract[String], getInternalType(f \ "type"))
        }
        StructType(fields.toArray)
      case "map" =>
        MapType(getInternalType(sparkType \ "keyType"), getInternalType(sparkType \ "valueType"))
      case "udt" => getInternalType(sparkType \ "sqlType")
    }
  }

  /**
   * Converts a JSON representing the Spark schema (the one returned by `df.schema.json`) into
   * a Hive [[TableSchema]] instance.
   *
   * @param sparkJson a [[String]] containing the JSON representation of a Spark Dataframe schema
   * @return a [[TableSchema]] representing the schema provided as input
   */
  def tableSchemaFromSparkJson(sparkJson: String): TableSchema = {
    val schema = parse(sparkJson) \ "fields"
    val fields = schema.children.map { field =>
      val name = (field \ "name").extract[String]
      val hiveType = toHive(field \ "type")
      new FieldSchema(name, hiveType, "")
    }
    new TableSchema(fields.asJava)
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
      getInternalType(field \ "type")
    }.toArray
  }

  /**
   * Returns the Hive [[Type]] used in the thrift communications for {@param thriftDt}.
   */
  def toHiveThriftType(thriftDt: DataType): Type = {
    thriftDt.name match {
      case "boolean" => Type.BOOLEAN_TYPE
      case "byte" => Type.TINYINT_TYPE
      case "short" => Type.SMALLINT_TYPE
      case "integer" => Type.INT_TYPE
      case "long" => Type.BIGINT_TYPE
      case "float" => Type.FLOAT_TYPE
      case "double" => Type.DOUBLE_TYPE
      case "binary" => Type.BINARY_TYPE
      case _ => Type.STRING_TYPE
    }
  }

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

  def toHiveComplexTypeFieldString(a: (Any, DataType)): String = a match {
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
}
