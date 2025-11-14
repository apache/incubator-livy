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

import java.nio.ByteBuffer

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.math.random

import org.mockito.Mockito.when
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.Waiters.{interval, timeout}
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.livy._
import org.apache.livy.client.common.Serializer
import org.apache.livy.rsc.driver.TaskState
import org.apache.livy.sessions.Spark

class SessionTaskSpec extends BaseSessionSpec(Spark) {

  lazy val serializer = new Serializer()

  val computePi: Job[Double] = new Job[Double]() {
    override def call(jc: JobContext): Double = {
      val slices = 100
      val n = math.min(1000000L * slices, Int.MaxValue).toInt
      val xs = 1 until n
      val rdd = jc.sc.parallelize(xs, slices)
        .setName("'Initial rdd'")
      val sample = rdd.map { _ =>
        val x = random * 2 - 1
        val y = random * 2 - 1
        (x, y)
      }.setName("'Random points sample'")

      val inside = sample.filter { case (x, y) => x * x + y * y < 1 }
        .setName("'Random points inside circle'")

      val count = inside.count()

      4.0 * count / n
    }
  }

  def wrapJob[T](job: Job[T]): Job[Array[Byte]] = new Job[Array[Byte]] {
    override def call(jc: JobContext): Array[Byte] = serializer.serialize(job.call(jc)).array()
  }

  def makeJob(fn: JobContext => Array[Byte]): Job[Array[Byte]] = new Job[Array[Byte]] {
    override def call(jc: JobContext): Array[Byte] = fn(jc)
  }

  val wrappedComputePi: Job[Array[Byte]] = wrapJob(computePi)


  it should "should submit and retrieve a task result after completion" in withSession { session =>
    val context = mock[JobContext]
    when(context.sc()).thenReturn(session.sc)

    val job = makeJob(_ => "result".getBytes)

    val taskId = session.submitTask(job, context)

    eventually(timeout(30 seconds), interval(100 millis)) {
      val task = session.getTask(taskId).get
      assert(task.state.get() == TaskState.Available)
    }

    val task = session.getTask(taskId).get
    task.output should be("result".getBytes)
  }

  it should "should update task state and it's progression" in withSession { session =>
    val context = mock[JobContext]
    when(context.sc()).thenReturn(session.sc)

    val taskId = session.submitTask(wrappedComputePi, context)

    // Wait for the task to start and check that progress is non-zero.
    eventually(timeout(30 seconds), interval(100 millis)) {
      session.tasks(taskId).updateProgress(session.progressOfTask(taskId))
      val task = session.getTask(taskId).get
      assert(task.state.get() == TaskState.Running)
      assert(task.progress > 0.0)
    }
  }

  it should "should be able to cancel a task" in withSession { session =>
    val context = mock[JobContext]
    when(context.sc()).thenReturn(session.sc)

    val taskId = session.submitTask(wrappedComputePi, context)

    // Wait for the task to start and check that progress is non-zero.
    eventually(timeout(30 seconds), interval(100 millis)) {
      session.tasks(taskId).updateProgress(session.progressOfTask(taskId))
      val task = session.getTask(taskId).get
      assert(task.state.get() == TaskState.Running)
      assert(task.progress > 0.0)
    }

    session.cancelTask(taskId)

    // Check that the task is cancelled.
    eventually(timeout(30 seconds), interval(100 millis)) {
      val task = session.getTask(taskId).get
      assert(task.state.get() == TaskState.Cancelled)
    }
  }

  it should "should retrieve any checked exception thrown" in withSession { session =>
    val context = mock[JobContext]
    when(context.sc()).thenReturn(session.sc)

    val job = makeJob(_ => throw new Exception("test"))
    val taskId = session.submitTask(job, context)

    // Wait for the task to start and check that progress is non-zero.
    eventually(timeout(30 seconds), interval(100 millis)) {
      val task = session.getTask(taskId).get
      assert(task.state.get() == TaskState.Failed)
    }

    val task = session.getTask(taskId).get
    val deserializedException = serializer.deserialize(ByteBuffer.wrap(task.serializedException))
    task.output should be(null)
    task.error should be("java.lang.Exception: test")
    deserializedException.toString should be(new Exception("test").toString)
  }

  it should "should retrieve any unchecked exception thrown" in withSession { session =>
    val context = mock[JobContext]
    when(context.sc()).thenReturn(session.sc)

    val job = makeJob(_ => throw new RuntimeException("test"))
    val taskId = session.submitTask(job, context)

    // Wait for the task to start and check that progress is non-zero.
    eventually(timeout(30 seconds), interval(100 millis)) {
      val task = session.getTask(taskId).get
      assert(task.state.get() == TaskState.Failed)
    }

    val task = session.getTask(taskId).get
    val deserializedException = serializer.deserialize(ByteBuffer.wrap(task.serializedException))
    task.output should be(null)
    task.error should be("java.lang.RuntimeException: test")
    deserializedException.toString should be(new RuntimeException("test").toString)
  }
}

