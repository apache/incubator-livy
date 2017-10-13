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

import java.util.Properties
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}

import org.apache.spark.SparkConf
import org.scalatest.{BeforeAndAfter, FunSpec}
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually
import org.scalatest.time._

import org.apache.livy.LivyBaseUnitTestSuite
import org.apache.livy.repl.Interpreter.ExecuteResponse
import org.apache.livy.rsc.RSCConf
import org.apache.livy.sessions._

class SessionSpec extends FunSpec with Eventually with LivyBaseUnitTestSuite with BeforeAndAfter {
  override implicit val patienceConfig =
    PatienceConfig(timeout = scaled(Span(30, Seconds)), interval = scaled(Span(100, Millis)))

  private val rscConf = new RSCConf(new Properties()).set(RSCConf.Entry.SESSION_KIND, "spark")

  describe("Session") {
    var session: Session = null

    after {
      if (session != null) {
        session.close()
        session = null
      }
    }

    it("should call state changed callbacks in happy path") {
      val expectedStateTransitions =
        Array("not_started", "starting", "idle", "busy", "idle", "busy", "idle")
      val actualStateTransitions = new ConcurrentLinkedQueue[String]()

      session = new Session(rscConf, new SparkConf(), None,
        { s => actualStateTransitions.add(s.toString) })
      session.start()
      session.execute("")

      eventually {
        actualStateTransitions.toArray shouldBe expectedStateTransitions
      }
    }

    it("should not transit to idle if there're any pending statements.") {
      val expectedStateTransitions =
        Array("not_started", "starting", "idle", "busy", "busy", "busy", "idle", "busy", "idle")
      val actualStateTransitions = new ConcurrentLinkedQueue[String]()

      val blockFirstExecuteCall = new CountDownLatch(1)
      val interpreter = new SparkInterpreter(new SparkConf()) {
        override def execute(code: String): ExecuteResponse = {
          blockFirstExecuteCall.await(10, TimeUnit.SECONDS)
          super.execute(code)
        }
      }
      session = new Session(rscConf, new SparkConf(), Some(interpreter),
        { s => actualStateTransitions.add(s.toString) })
      session.start()

      for (_ <- 1 to 2) {
        session.execute("")
      }

      blockFirstExecuteCall.countDown()
      eventually {
        actualStateTransitions.toArray shouldBe expectedStateTransitions
      }
    }

    it("should remove old statements when reaching threshold") {
      rscConf.set(RSCConf.Entry.RETAINED_STATEMENTS, 2)
      session = new Session(rscConf, new SparkConf())
      session.start()

      session.statements.size should be (0)
      session.execute("")
      session.statements.size should be (1)
      session.statements.map(_._1).toSet should be (Set(0))
      session.execute("")
      session.statements.size should be (2)
      session.statements.map(_._1).toSet should be (Set(0, 1))
      session.execute("")
      eventually {
        session.statements.size should be (2)
        session.statements.map(_._1).toSet should be (Set(1, 2))
      }

      // Continue submitting statements, total statements in memory should be 2.
      session.execute("")
      eventually {
        session.statements.size should be (2)
        session.statements.map(_._1).toSet should be (Set(2, 3))
      }
    }
  }
}
