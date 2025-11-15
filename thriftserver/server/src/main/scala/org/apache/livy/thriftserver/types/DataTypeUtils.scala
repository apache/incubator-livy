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

import org.json4s.{DefaultFormats, JValue, StringInput}
import org.json4s.JsonAST.{JObject, JString}
import org.json4s.jackson.JsonMethods.parse

/**
 * Utility class for converting and representing Spark and Hive data types.
 */
object DataTypeUtils {
  // Used for JSON conversion
  private implicit val formats = DefaultFormats

  private def toFieldType(jValue: JValue): FieldType = {
    jValue match {
      case JString(t) => BasicDataType(t)
      case o: JObject => complexToDataType(o)
      case _ => throw new IllegalArgumentException(
        s"Spark type was neither a string nor a object. It was: $jValue.")
    }
  }

  private def complexToDataType(sparkType: JObject): FieldType = {
    (sparkType \ "type").extract[String] match {
      case "array" => ArrayType(toFieldType(sparkType \ "elementType"))
      case "struct" =>
        val fields = (sparkType \ "fields").children.map { f =>
          val comment = extractComment(f \ "metadata")
          Field((f \ "name").extract[String], toFieldType(f \ "type"), comment)
        }
        StructType(fields.toArray)
      case "map" =>
        MapType(toFieldType(sparkType \ "keyType"), toFieldType(sparkType \ "valueType"))
      case "udt" => toFieldType(sparkType \ "sqlType")
    }
  }

  /**
   * Extracts comment from Spark field metadata JSON.
   * Spark stores comments in metadata under the "comment" key.
   */
  private def extractComment(metadata: JValue): String = {
    metadata match {
      case JObject(fields) =>
        fields.find(_._1 == "comment") match {
          case Some((_, JString(comment))) => comment
          case _ => ""
        }
      case _ => ""
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
    val schema = parse(StringInput(sparkJson), false) \ "fields"
    val fields = schema.children.map { field =>
      val name = (field \ "name").extract[String]
      val hiveType = toFieldType(field \ "type")
      val comment = extractComment(field \ "metadata")
      Field(name, hiveType, comment)
    }
    Schema(fields.toArray)
  }
}
