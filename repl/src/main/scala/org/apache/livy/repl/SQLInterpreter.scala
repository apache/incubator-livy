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

package org.apache.livy.repl

import java.lang.reflect.InvocationTargetException
import java.sql.Date

import scala.util.control.NonFatal

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.JsonAST.{JNull, JString}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.livy.Logging
import org.apache.livy.rsc.RSCConf
import org.apache.livy.rsc.driver.SparkEntries

/**
 * A SQL interpreter which only accepts SQL query, execute the query and return the result. The
 * format of result is JSON format:
 *  {
 *    application/json: {
 *        "schema": {
 *          "type": "struct",
 *          "fields": [
 *            {
 *              "name": "column_name",
 *              "type": "column_type",
 *              "nullable": true|false,
 *              "metadata": {}
 *            },
 *            ...
 *          ]
 *        },
 *        "data":[
 *          [column_a, column_b, ...],
 *          [column_a, column_b, ...],
 *          ...
 *        ]
 *      }
 *    }
 *  }
 *
 * The size of ouput data is controlled by "spark.livy.sql.max-result", default maximum output
 * rows is 1000.
 */
class SQLInterpreter(
    sparkConf: SparkConf,
    rscConf: RSCConf,
    sparkEntries: SparkEntries) extends Interpreter with Logging {

  case object DateSerializer extends CustomSerializer[Date](_ => ( {
    case JString(s) => Date.valueOf(s)
    case JNull => null
  }, {
    case d: Date => JString(d.toString)
  }))

  private implicit def formats: Formats = DefaultFormats + DateSerializer

  private var spark: SparkSession = null

  private val maxResult = rscConf.getInt(RSCConf.Entry.SQL_NUM_ROWS)

  override def kind: String = "sql"

  override def start(): Unit = {
    require(!sparkEntries.sc().sc.isStopped)
    spark = sparkEntries.sparkSession()
  }

  override protected[repl] def execute(code: String): Interpreter.ExecuteResponse = {
    try {
      val result = spark.sql(code)
      val schema = parse(result.schema.json)

      // Get the row data
      val rows = result.take(maxResult)
        .map {
          _.toSeq.map {
            // Convert java BigDecimal type to Scala BigDecimal, because current version of
            // Json4s doesn't support java BigDecimal as a primitive type (LIVY-455).
            case i: java.math.BigDecimal => BigDecimal(i)
            case e => e
          }
        }

      val jRows = Extraction.decompose(rows)

      Interpreter.ExecuteSuccess(
        APPLICATION_JSON -> (("schema" -> schema) ~ ("data" -> jRows)))
    } catch {
      case e: InvocationTargetException =>
        warn(s"Fail to execute query $code", e.getTargetException)
        val cause = e.getTargetException
        Interpreter.ExecuteError("Error", cause.getMessage, cause.getStackTrace.map(_.toString))

      case NonFatal(f) =>
        warn(s"Fail to execute query $code", f)
        Interpreter.ExecuteError("Error", f.getMessage, f.getStackTrace.map(_.toString))
    }
  }

  override def close(): Unit = { }
}
