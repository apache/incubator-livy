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

import scala.concurrent.duration._
import scala.language.postfixOps

import org.json4s.Extraction
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods.parse
import org.scalatest.concurrent.Eventually.{eventually, interval, timeout}

import org.apache.livy.rsc.driver.{Statement, StatementState}
import org.apache.livy.sessions._

class SharedSessionSpec extends BaseSessionSpec(Shared) {

  private def execute(session: Session, code: String, codeType: String): Statement = {
    val id = session.execute(code, codeType)
    eventually(timeout(30 seconds), interval(100 millis)) {
      val s = session.statements(id)
      s.state.get() shouldBe StatementState.Available
      s
    }
  }

  it should "execute `1 + 2` == 3" in withSession { session =>
    val statement = execute(session, "1 + 2", "spark")
    statement.id should equal (0)

    val result = parse(statement.output)
    val expectedResult = Extraction.decompose(Map(
      "status" -> "ok",
      "execution_count" -> 0,
      "data" -> Map(
        "text/plain" -> "res0: Int = 3\n"
      )
    ))

    result should equal (expectedResult)
  }

  it should "access the spark context" in withSession { session =>
    val statement = execute(session, """sc""", "spark")
    statement.id should equal (0)

    val result = parse(statement.output)
    val resultMap = result.extract[Map[String, JValue]]

    // Manually extract the values since the line numbers in the exception could change.
    resultMap("status").extract[String] should equal ("ok")
    resultMap("execution_count").extract[Int] should equal (0)

    val data = resultMap("data").extract[Map[String, JValue]]
    data("text/plain").extract[String] should include (
      "res0: org.apache.spark.SparkContext = org.apache.spark.SparkContext")
  }

  it should "execute spark commands" in withSession { session =>
    val statement = execute(session,
      """sc.parallelize(0 to 1).map{i => i+1}.collect""".stripMargin, "spark")
    statement.id should equal (0)

    val result = parse(statement.output)

    val expectedResult = Extraction.decompose(Map(
      "status" -> "ok",
      "execution_count" -> 0,
      "data" -> Map(
        "text/plain" -> "res0: Array[Int] = Array(1, 2)\n"
      )
    ))

    result should equal (expectedResult)
  }

  it should "throw exception if code type is not specified in shared session" in withSession {
    session =>
      intercept[IllegalArgumentException](session.execute("1 + 2"))
  }

  it should "execute `1 + 2 = 3` in Python" in withSession { session =>
    val statement = execute(session, "1 + 2", "pyspark")
    statement.id should equal (0)

    val result = parse(statement.output)

    val expectedResult = Extraction.decompose(Map(
      "status" -> "ok",
      "execution_count" -> 0,
      "data" -> Map("text/plain" -> "3")
    ))

    result should equal (expectedResult)
  }

  it should "execute `1 + 2 = 3` in R" in withSession { session =>
    val statement = execute(session, "1 + 2", "sparkr")
    statement.id should be (0)

    val result = parse(statement.output)

    val expectedResult = Extraction.decompose(Map(
      "status" -> "ok",
      "execution_count" -> 0,
      "data" -> Map("text/plain" -> "[1] 3")
    ))

    result should be (expectedResult)
  }
}
