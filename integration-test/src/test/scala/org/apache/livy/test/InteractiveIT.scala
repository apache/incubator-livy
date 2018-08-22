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

package org.apache.livy.test

import java.util.concurrent.atomic.AtomicInteger
import java.util.regex.Pattern

import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.scalatest.concurrent.Eventually._
import org.scalatest.OptionValues._
import scala.concurrent.duration._
import scala.language.postfixOps

import org.apache.livy.rsc.RSCConf
import org.apache.livy.sessions._
import org.apache.livy.test.framework.{BaseIntegrationTestSuite, LivyRestClient}

class InteractiveIT extends BaseIntegrationTestSuite {
  test("basic interactive session") {
    withNewSession(Spark) { s =>
      s.run("val sparkVersion = sc.version").result().left.foreach(info(_))
      s.run("val scalaVersion = util.Properties.versionString").result().left.foreach(info(_))
      s.run("1+1").verifyResult("res0: Int = 2\n")
      s.run("""sc.getConf.get("spark.executor.instances")""").verifyResult("res1: String = 1\n")
      s.run("val sql = new org.apache.spark.sql.SQLContext(sc)").verifyResult(
        ".*" + Pattern.quote(
        "sql: org.apache.spark.sql.SQLContext = org.apache.spark.sql.SQLContext") + ".*")
      s.run("abcde").verifyError(evalue = ".*?:[0-9]+: error: not found: value abcde.*")
      s.run("throw new IllegalStateException()")
        .verifyError(evalue = ".*java\\.lang\\.IllegalStateException.*")

      // Check if we're running with Spark1 or Spark2, in Spark1 we will use SQLContext, whereas
      // for Spark2 we will use SparkSession.
      val entry = if (s.run("spark").result().isLeft) {
        "spark"
      } else {
        "sqlContext"
      }
      // Verify query submission
      s.run(s"""val df = $entry.createDataFrame(Seq(("jerry", 20), ("michael", 21)))""")
        .verifyResult(".*" + Pattern.quote("df: org.apache.spark.sql.DataFrame") + ".*")
      s.run("df.registerTempTable(\"people\")").result()
      s.run("SELECT * FROM people", Some(SQL)).verifyResult(".*\"jerry\",20.*\"michael\",21.*")

      // Verify Livy internal configurations are not exposed.
      // TODO separate all these checks to different sub tests after merging new IT code.
      s.run("""sc.getConf.getAll.exists(_._1.startsWith("spark.__livy__."))""")
        .verifyResult(".*false\n")
      s.run("""sys.props.exists(_._1.startsWith("spark.__livy__."))""").verifyResult(".*false\n")
      s.run("""val str = "str"""")
      s.complete("str.", "scala", 4).verifyContaining(List("compare", "contains"))
      s.complete("str2.", "scala", 5).verifyNone()

      // Make sure appInfo is reported correctly.
      val state = s.snapshot()
      state.appInfo.driverLogUrl.value should include ("containerlogs")
      state.appInfo.sparkUiUrl.value should startWith ("http")

      // Stop session and verify the YARN app state is finished.
      // This is important because if YARN app state is killed, Spark history is not archived.
      val appId = s.appId()
      s.stop()
      eventually(timeout(5 seconds), interval(1 seconds)) {
        val appReport = cluster.yarnClient.getApplicationReport(appId)
        appReport.getYarnApplicationState() shouldEqual YarnApplicationState.FINISHED
      }
    }
  }

  test("pyspark interactive session") {
    withNewSession(PySpark) { s =>
      s.run("1+1").verifyResult("2")
      s.run("sc.parallelize(range(100)).map(lambda x: x * 2).reduce(lambda x, y: x + y)")
        .verifyResult("9900")
      s.run("from pyspark.sql.types import Row").verifyResult("")
      s.run("x = [Row(age=1, name=u'a'), Row(age=2, name=u'b'), Row(age=3, name=u'c')]")
        .verifyResult("")
      // Check if we're running with Spark2.
      if (s.run("spark").result().isLeft) {
        s.run("sqlContext.sparkSession").verifyResult(".*pyspark\\.sql\\.session\\.SparkSession.*")
      }
      s.run("%table x").verifyResult(".*headers.*type.*name.*data.*")
      s.run("abcde").verifyError(ename = "NameError", evalue = "name 'abcde' is not defined")
      s.run("raise KeyError, 'foo'").verifyError(ename = "KeyError", evalue = "'foo'")
      s.run("print(1)\r\nprint(1)").verifyResult("1\n1")
    }
  }

  test("R interactive session") {
    withNewSession(SparkR) { s =>
      // R's output sometimes includes the count of statements, which makes it annoying to test
      // things. This helps a bit.
      val curr = new AtomicInteger()
      def count: Int = curr.incrementAndGet()

      s.run("1+1").verifyResult(startsWith(s"[$count] 2"))
      s.run("""localDF <- data.frame(name=c("John", "Smith", "Sarah"), age=c(19, 23, 18))""")
        .verifyResult(null)
      s.run("df <- createDataFrame(sqlContext, localDF)").verifyResult(null)
      s.run("printSchema(df)").verifyResult(literal(
        """|root
          | |-- name: string (nullable = true)
          | |-- age: double (nullable = true)""".stripMargin))
      s.run("print(1)\r\nprint(1)").verifyResult(".*1\n.*1")
    }
  }

  test("application kills session") {
    withNewSession(Spark) { s =>
      s.runFatalStatement("System.exit(0)")
    }
  }

  test("should kill RSCDriver if it doesn't respond to end session") {
    val testConfName = s"${RSCConf.LIVY_SPARK_PREFIX}${RSCConf.Entry.TEST_STUCK_END_SESSION.key()}"
    withNewSession(Spark, Map(testConfName -> "true")) { s =>
      val appId = s.appId()
      s.stop()
      val appReport = cluster.yarnClient.getApplicationReport(appId)
      appReport.getYarnApplicationState() shouldBe YarnApplicationState.KILLED
    }
  }

  test("should kill RSCDriver if it didn't register itself in time") {
    val testConfName =
      s"${RSCConf.LIVY_SPARK_PREFIX}${RSCConf.Entry.TEST_STUCK_START_DRIVER.key()}"
    withNewSession(Spark, Map(testConfName -> "true"), false) { s =>
      eventually(timeout(2 minutes), interval(5 seconds)) {
        val appId = s.appId()
        appId should not be null
        val appReport = cluster.yarnClient.getApplicationReport(appId)
        appReport.getYarnApplicationState() shouldBe YarnApplicationState.KILLED
      }
    }
  }

  test("user jars are properly imported in Scala interactive sessions") {
    // Include a popular Java library to test importing user jars.
    val sparkConf = Map("spark.jars.packages" -> "org.codehaus.plexus:plexus-utils:3.0.24")
    withNewSession(Spark, sparkConf) { s =>
      // Check is the library loaded in JVM in the proper class loader.
      s.run("Thread.currentThread.getContextClassLoader.loadClass" +
          """("org.codehaus.plexus.util.FileUtils")""")
        .verifyResult(".*Class\\[_\\] = class org.codehaus.plexus.util.FileUtils\n")

      // Check does Scala interpreter see the library.
      s.run("import org.codehaus.plexus.util._").verifyResult("import org.codehaus.plexus.util._\n")

      // Check does SparkContext see classes defined by Scala interpreter.
      s.run("case class Item(i: Int)").verifyResult("defined class Item\n")
      s.run("val rdd = sc.parallelize(Array.fill(10){new Item(scala.util.Random.nextInt(1000))})")
        .verifyResult("rdd.*")
      s.run("rdd.count()").verifyResult(".*= 10\n")
    }
  }

  test("heartbeat should kill expired session") {
    // Set it to 2s because verifySessionIdle() is calling GET every second.
    val heartbeatTimeout = Duration.create("2s")
    withNewSession(Spark, Map.empty, true, heartbeatTimeout.toSeconds.toInt) { s =>
      // If the test reaches here, that means verifySessionIdle() is successfully keeping the
      // session alive. Now verify heartbeat is killing expired session.
      Thread.sleep(heartbeatTimeout.toMillis * 2)
      s.verifySessionDoesNotExist()
    }
  }

  test("recover interactive session") {
    withNewSession(Spark) { s =>
      val stmt1 = s.run("1")
      stmt1.verifyResult("res0: Int = 1\n")

      restartLivy()

      // Verify session still exists.
      s.verifySessionIdle()
      s.run("2").verifyResult("res1: Int = 2\n")
      // Verify statement result is preserved.
      stmt1.verifyResult("res0: Int = 1\n")

      s.stop()

      restartLivy()

      // Verify deleted session doesn't show up after recovery.
      s.verifySessionDoesNotExist()

      // Verify new session doesn't reuse old session id.
      withNewSession(Spark, Map.empty, false) { s1 =>
        s1.id should be > s.id
      }
    }
  }

  private def withNewSession[R] (
      kind: Kind,
      sparkConf: Map[String, String] = Map.empty,
      waitForIdle: Boolean = true,
      heartbeatTimeoutInSecond: Int = 0)
    (f: (LivyRestClient#InteractiveSession) => R): R = {
    withSession(livyClient.startSession(kind, sparkConf, heartbeatTimeoutInSecond)) { s =>
      if (waitForIdle) {
        s.verifySessionIdle()
      }
      f(s)
    }
  }

  private def startsWith(result: String): String = Pattern.quote(result) + ".*"

  private def literal(result: String): String = Pattern.quote(result)
}
