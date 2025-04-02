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

import java.sql.Date

import scala.util.Try

import org.apache.spark.SparkConf
import org.json4s.{DefaultFormats, JValue}
import org.json4s.JsonAST.{JArray, JNull}
import org.json4s.JsonDSL._

import org.apache.livy.rsc.RSCConf
import org.apache.livy.rsc.driver.SparkEntries

case class People(name: String, age: Int)
case class Person(name: String, birthday: Date)

class SQLInterpreterSpec extends BaseInterpreterSpec {

  implicit val formats = DefaultFormats

  private var sparkEntries: SparkEntries = _

  override def createInterpreter(): Interpreter = {
    val conf = new SparkConf()
    if (sparkEntries == null) {
      sparkEntries = new SparkEntries(conf)
    }
    new SQLInterpreter(conf, new RSCConf(), sparkEntries)
  }

  it should "handle java.sql.Date tpye" in withInterpreter { interpreter =>
    val personList = Seq(Person("Jerry", Date.valueOf("2019-07-24")),
      Person("Michael", Date.valueOf("2019-07-23")))

    val rdd = sparkEntries.sc().parallelize(personList)
    val df = sparkEntries.sqlctx().createDataFrame(rdd)
    df.createOrReplaceTempView("person")

    // Test normal behavior
    val resp1 = interpreter.execute("SELECT * FROM person")

    val expectedResult = Interpreter.ExecuteSuccess(
      APPLICATION_JSON -> (("schema" ->
        (("type" -> "struct") ~
          ("fields" -> List(
            ("name" -> "name") ~ ("type" -> "string") ~ ("nullable" -> true) ~
              ("metadata" -> List()),
            ("name" -> "birthday") ~ ("type" -> "date") ~ ("nullable" -> true) ~
              ("metadata" -> List())
          )))) ~
        ("data" -> List(
          List[JValue]("Jerry", "2019-07-24"),
          List[JValue]("Michael", "2019-07-23")
        )))
    )

    val result = Try { resp1 should equal(expectedResult)}
    if (result.isFailure) {
      fail(s"$resp1 doesn't equal to expected result")
    }
  }

  it should "test java.sql.Date null" in withInterpreter { interpreter =>
    val personList = Seq(Person("Jerry", null),
      Person("Michael", Date.valueOf("2019-07-23")))

    val rdd = sparkEntries.sc().parallelize(personList)
    val df = sparkEntries.sqlctx().createDataFrame(rdd)
    df.createOrReplaceTempView("person")

    // Test normal behavior
    val resp1 = interpreter.execute("SELECT * FROM person")

    val expectedResult = Interpreter.ExecuteSuccess(
      APPLICATION_JSON -> (("schema" ->
        (("type" -> "struct") ~
          ("fields" -> List(
            ("name" -> "name") ~ ("type" -> "string") ~ ("nullable" -> true) ~
              ("metadata" -> List()),
            ("name" -> "birthday") ~ ("type" -> "date") ~ ("nullable" -> true) ~
              ("metadata" -> List())
          )))) ~
        ("data" -> List(
          List[JValue]("Jerry", JNull),
          List[JValue]("Michael", "2019-07-23")
        )))
    )

    val result = Try { resp1 should equal(expectedResult)}
    if (result.isFailure) {
      fail(s"$resp1 doesn't equal to expected result")
    }
  }

  it should "execute sql queries" in withInterpreter { interpreter =>
    val rdd = sparkEntries.sc().parallelize(Seq(People("Jerry", 20), People("Michael", 21)))
    val df = sparkEntries.sqlctx().createDataFrame(rdd)
    df.createOrReplaceTempView("people")

    // Test normal behavior
    val resp1 = interpreter.execute(
      """
        |SELECT * FROM people
      """.stripMargin)

    resp1 should equal(
      Interpreter.ExecuteSuccess(
        APPLICATION_JSON -> (("schema" ->
          (("type" -> "struct") ~
            ("fields" -> List(
              ("name" -> "name") ~ ("type" -> "string") ~ ("nullable" -> true) ~
                ("metadata" -> List()),
              ("name" -> "age") ~ ("type" -> "integer") ~ ("nullable" -> false) ~
                ("metadata" -> List())
            )))) ~
          ("data" -> List(
            List[JValue]("Jerry", 20),
            List[JValue]("Michael", 21)
          )))
      )
    )

    // Test empty result
     val resp2 = interpreter.execute(
      """
        |SELECT name FROM people WHERE age > 22
      """.stripMargin)
    resp2 should equal(Interpreter.ExecuteSuccess(
      APPLICATION_JSON -> (("schema" ->
        (("type" -> "struct") ~
          ("fields" -> List(
            ("name" -> "name") ~ ("type" -> "string") ~ ("nullable" -> true) ~
              ("metadata" -> List())
          )))) ~
        ("data" -> JArray(List())))
    ))
  }

  it should "handle java BigDecimal" in withInterpreter { interpreter =>
    val rdd = sparkEntries.sc().parallelize(Seq(
      ("1", new java.math.BigDecimal(1.0)),
      ("2", new java.math.BigDecimal(2.0))))
    val df = sparkEntries.sqlctx().createDataFrame(rdd).selectExpr("_1 as col1", "_2 as col2")
    df.createOrReplaceTempView("test")

    val resp1 = interpreter.execute(
      """
        |SELECT * FROM test
      """.stripMargin)

    resp1 should equal(
      Interpreter.ExecuteSuccess(
        APPLICATION_JSON -> (("schema" ->
          (("type" -> "struct") ~
            ("fields" -> List(
              ("name" -> "col1") ~ ("type" -> "string") ~ ("nullable" -> true) ~
                ("metadata" -> List()),
              ("name" -> "col2") ~ ("type" -> "decimal(38,18)") ~ ("nullable" -> true) ~
                ("metadata" -> List())
            )))) ~
          ("data" -> List(
            List[JValue]("1", 1.0d),
            List[JValue]("2", 2.0d)
          )))
      )
    )
  }

  it should "throw exception for illegal query" in withInterpreter { interpreter =>
    val resp = interpreter.execute(
      """
        |SELECT * FROM people1
      """.stripMargin)

    assert(resp.isInstanceOf[Interpreter.ExecuteError])
    val error = resp.asInstanceOf[Interpreter.ExecuteError]
    error.ename should be ("Error")
    assert(error.evalue.contains("TABLE_OR_VIEW_NOT_FOUND"))
  }

  it should "fail if submitting multiple queries" in withInterpreter { interpreter =>
    val resp = interpreter.execute(
      """
        |SELECT name FROM people;
        |SELECT age FROM people
      """.stripMargin)

    assert(resp.isInstanceOf[Interpreter.ExecuteError])
    resp.asInstanceOf[Interpreter.ExecuteError].ename should be ("Error")
  }
}
