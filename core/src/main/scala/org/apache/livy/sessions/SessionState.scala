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

sealed abstract class SessionState(val state: String, val isActive: Boolean) {
  override def toString: String = state
}

class FinishedSessionState(
  override val state: String,
  override val isActive: Boolean,
  val time: Long
) extends SessionState(state, isActive)

object SessionState {

  def apply(s: String): SessionState = s match {
    case "not_started" => NotStarted
    case "starting" => Starting
    case "recovering" => Recovering
    case "idle" => Idle
    case "running" => Running
    case "busy" => Busy
    case "shutting_down" => ShuttingDown
    case "error" => Error()
    case "dead" => Dead()
    case "success" => Success()
    case _ => throw new IllegalArgumentException(s"Illegal session state: $s")
  }

  object NotStarted extends SessionState("not_started", true)

  object Starting extends SessionState("starting", true)

  object Recovering extends SessionState("recovering", true)

  object Idle extends SessionState("idle", true)

  object Running extends SessionState("running", true)

  object Busy extends SessionState("busy", true)

  object ShuttingDown extends SessionState("shutting_down", false)

  case class Error(override val time: Long = System.nanoTime()) extends
    FinishedSessionState("error", true, time)

  case class Dead(override val time: Long = System.nanoTime()) extends
    FinishedSessionState("dead", false, time)

  case class Success(override val time: Long = System.nanoTime()) extends
    FinishedSessionState("success", false, time)
}
