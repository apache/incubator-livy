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

package org.apache.livy.utils

import org.scalatest.FunSuite
import org.scalatest.Matchers

import org.apache.livy.{LivyBaseUnitTestSuite, LivyConf}
import org.apache.livy.LivyConf._
import org.apache.livy.server.LivyServer

class LivySparkUtilsSuite extends FunSuite with Matchers with LivyBaseUnitTestSuite {

  import LivySparkUtils._

  private val livyConf = new LivyConf()
  private val livyConf210 = new LivyConf()
  livyConf210.set(LIVY_SPARK_SCALA_VERSION, "2.10.6")

  private val livyConf211 = new LivyConf()
  livyConf211.set(LIVY_SPARK_SCALA_VERSION, "2.11.1")

  test("check for SPARK_HOME") {
    testSparkHome(livyConf)
  }

  test("check spark-submit version") {
    testSparkSubmit(livyConf)
  }

  test("should recognize supported Spark versions") {
    testSparkVersion("2.2.0")
    testSparkVersion("2.3.0")
    testSparkVersion("2.4.0")
    testSparkVersion("3.0.0")
    testSparkVersion("3.1.1")
    testSparkVersion("3.2.0")
  }

  test("should complain about unsupported Spark versions") {
    intercept[IllegalArgumentException] { testSparkVersion("1.4.0") }
    intercept[IllegalArgumentException] { testSparkVersion("1.5.0") }
    intercept[IllegalArgumentException] { testSparkVersion("1.5.1") }
    intercept[IllegalArgumentException] { testSparkVersion("1.5.2") }
    intercept[IllegalArgumentException] { testSparkVersion("1.5.0-cdh5.6.1") }
    intercept[IllegalArgumentException] { testSparkVersion("1.6.0") }
    intercept[IllegalArgumentException] { testSparkVersion("2.0.1") }
    intercept[IllegalArgumentException] { testSparkVersion("2.1.3") }
  }

  test("should fail on bad version") {
    intercept[IllegalArgumentException] { testSparkVersion("not a version") }
  }

  test("should error out if recovery is turned on but master isn't yarn") {
    val livyConf = new LivyConf()
    livyConf.set(LivyConf.LIVY_SPARK_MASTER, "local")
    livyConf.set(LivyConf.RECOVERY_MODE, "recovery")
    val s = new LivyServer()
    intercept[IllegalArgumentException] { s.testRecovery(livyConf) }
  }

  test("formatScalaVersion() should format Scala version") {
    formatScalaVersion("2.10.8") shouldBe "2.10"
    formatScalaVersion("2.11.4") shouldBe "2.11"
    formatScalaVersion("2.10") shouldBe "2.10"
    formatScalaVersion("2.10.x.x.x.x") shouldBe "2.10"

    // Throw exception for bad Scala version.
    intercept[IllegalArgumentException] { formatScalaVersion("") }
    intercept[IllegalArgumentException] { formatScalaVersion("xxx") }
  }

  test("defaultSparkScalaVersion() should return default Scala version") {
    defaultSparkScalaVersion(formatSparkVersion("2.2.1")) shouldBe "2.11"
    defaultSparkScalaVersion(formatSparkVersion("2.3.0")) shouldBe "2.11"
    defaultSparkScalaVersion(formatSparkVersion("2.4.0")) shouldBe "2.11"
    defaultSparkScalaVersion(formatSparkVersion("3.0.0")) shouldBe "2.12"
  }

  test("sparkScalaVersion() should use spark-submit detected Scala version.") {
    sparkScalaVersion(formatSparkVersion("2.0.1"), Some("2.10"), livyConf) shouldBe "2.10"
    sparkScalaVersion(formatSparkVersion("1.6.0"), Some("2.11"), livyConf) shouldBe "2.11"
  }

  test("sparkScalaVersion() should throw if configured and detected Scala version mismatch.") {
    intercept[IllegalArgumentException] {
      sparkScalaVersion(formatSparkVersion("2.0.1"), Some("2.11"), livyConf210)
    }
    intercept[IllegalArgumentException] {
      sparkScalaVersion(formatSparkVersion("1.6.1"), Some("2.10"), livyConf211)
    }
  }

  test("sparkScalaVersion() should use default Spark Scala version.") {
    sparkScalaVersion(formatSparkVersion("2.2.0"), None, livyConf) shouldBe "2.11"
    sparkScalaVersion(formatSparkVersion("2.3.1"), None, livyConf) shouldBe "2.11"
    sparkScalaVersion(formatSparkVersion("3.0.0"), None, livyConf) shouldBe "2.12"
  }
}
