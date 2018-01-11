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
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

import org.apache.spark.SparkConf
import org.json4s._
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.concurrent.Eventually._

import org.apache.livy.LivyBaseUnitTestSuite
import org.apache.livy.rsc.RSCConf
import org.apache.livy.rsc.driver.{Statement, StatementState}
import org.apache.livy.sessions._

abstract class BaseSessionSpec(kind: Kind)
    extends FlatSpec with Matchers with LivyBaseUnitTestSuite {

  implicit val formats = DefaultFormats

  private val rscConf = new RSCConf(new Properties()).set(RSCConf.Entry.SESSION_KIND, kind.toString)

  private val sparkConf = new SparkConf()

  protected def execute(session: Session)(code: String): Statement = {
    val id = session.execute(code)
    eventually(timeout(30 seconds), interval(100 millis)) {
      val s = session.statements(id)
      s.state.get() shouldBe StatementState.Available
      s
    }
  }

  protected def withSession(testCode: Session => Any): Unit = {
    val stateChangedCalled = new AtomicInteger()
    val session =
      new Session(rscConf, sparkConf, None, { _ => stateChangedCalled.incrementAndGet() })
    try {
      // Session's constructor should fire an initial state change event.
      stateChangedCalled.intValue() shouldBe 1
      Await.ready(session.start(), 30 seconds)
      assert(session.state === SessionState.Idle)
      // There should be at least 1 state change event fired when session transits to idle.
      stateChangedCalled.intValue() should (be > 1)
      testCode(session)
    } finally {
      session.close()
    }
  }

  it should "start in the starting or idle state" in {
    val session = new Session(rscConf, sparkConf)
    val future = session.start()
    try {
      Await.ready(future, 60 seconds)
      session.state should (equal (SessionState.Starting) or equal (SessionState.Idle))
    } finally {
      session.close()
    }
  }

  it should "eventually become the idle state" in withSession { session =>
    session.state should equal (SessionState.Idle)
  }

}
