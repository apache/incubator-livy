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

package org.apache.livy.sessions

import com.fasterxml.jackson.core.{JsonGenerator, JsonParser, JsonToken}
import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.databind.module.SimpleModule

sealed abstract class Kind(val name: String) {
  override def toString: String = name
}

object Spark extends Kind("spark")

object PySpark extends Kind("pyspark")

object SparkR extends Kind("sparkr")

object Shared extends Kind("shared")

object SQL extends Kind("sql")

object Kind {

  def apply(kind: String): Kind = kind match {
    case "spark" | "scala" => Spark
    case "pyspark" | "python" => PySpark
    case "sparkr" | "r" => SparkR
    case "shared" => Shared
    case "sql" => SQL
    case other => throw new IllegalArgumentException(s"Invalid kind: $other")
  }
}

class SessionKindModule extends SimpleModule("SessionKind") {

  addSerializer(classOf[Kind], new JsonSerializer[Kind]() {
    override def serialize(value: Kind, jgen: JsonGenerator, provider: SerializerProvider): Unit = {
      jgen.writeString(value.toString)
    }
  })

  addDeserializer(classOf[Kind], new JsonDeserializer[Kind]() {
    override def deserialize(jp: JsonParser, ctxt: DeserializationContext): Kind = {
      require(jp.getCurrentToken() == JsonToken.VALUE_STRING, "Kind should be a string.")
      Kind(jp.getText())
    }
  })

}
