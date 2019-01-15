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

package org.apache.livy.thriftserver

import scala.collection.mutable

import org.apache.hive.service.ServiceException

import org.apache.livy.{LivyConf, Logging}

/**
 * Service states
 */
object STATE extends Enumeration {
  type STATE = Value
  val

  /** Constructed but not initialized */
  NOTINITED,

  /** Initialized but not started or stopped */
  INITED,

  /** started and not stopped */
  STARTED,

  /** stopped. No further state transitions are permitted */
  STOPPED = Value
}

class ThriftService(val name: String) extends Logging {
  private val serviceList = new mutable.ListBuffer[ThriftService]

  /**
   * Service state: initially {@link STATE#NOTINITED}.
   */
  private var state = STATE.NOTINITED

  /**
   * Service start time. Will be zero until the service is started.
   */
  private var startTime = 0L

  def getServices: Seq[ThriftService] = serviceList.toList

  protected def addService(service: ThriftService): Unit = {
    serviceList += service
  }

  protected def removeService(service: ThriftService): Unit = serviceList -= service

  def init(conf: LivyConf): Unit = {
    serviceList.foreach(_.init(conf))
    ensureCurrentState(STATE.NOTINITED)
    changeState(STATE.INITED)
    info(s"Service:$getName is inited.")
  }

  def start(): Unit = {
    var i = 0
    try {
      val n = serviceList.size
      while (i < n) {
        val service = serviceList(i)
        service.start()
        i += 1
      }
      startTime = System.currentTimeMillis
      ensureCurrentState(STATE.INITED)
      changeState(STATE.STARTED)
      info(s"Service:$getName is started.")
    } catch {
      case e: Throwable =>
        error("Error starting services " + getName, e)
        // Note that the state of the failed service is still INITED and not
        // STARTED. Even though the last service is not started completely, still
        // call stop() on all services including failed service to make sure cleanup
        // happens.
        stop(i)
        throw new ServiceException("Failed to Start " + getName, e)
    }
  }

  def stop(): Unit = {
    if (this.getServiceState == STATE.STOPPED) {
      // The base composite-service is already stopped, don't do anything again.
      return
    }
    if (serviceList.nonEmpty) stop(serviceList.size - 1)
    if ((state == STATE.STOPPED) || (state == STATE.INITED) || (state == STATE.NOTINITED)) {
      // already stopped, or else it was never
      // started (eg another service failing canceled startup)
      return
    }
    ensureCurrentState(STATE.STARTED)
    changeState(STATE.STOPPED)
    info(s"Service:$getName is stopped.")
  }

  private def stop(numOfServicesStarted: Int): Unit = {
    // stop in reverse order of start
    var i = numOfServicesStarted
    while (i >= 0) {
      val service = serviceList(i)
      try {
        service.stop()
      } catch {
        case t: Throwable => info("Error stopping " + service.getName, t)
      }
      i -= 1
    }
  }

  def getServiceState: STATE.Value = state

  def getName: String = name

  def getStartTime: Long = startTime

  /**
   * Verify that that a service is in a given state.
   *
   * @param currentState
   * the desired state
   * @throws IllegalStateException
   * if the service state is different from
   * the desired state
   */
  private def ensureCurrentState(currentState: STATE.Value): Unit = {
    if (state != currentState) {
      throw new IllegalStateException(
        s"For this operation, the current service state must be $currentState instead of $state")
    }
  }

  /**
   * Change to a new state.
   *
   * @param newState new service state
   */
  private def changeState(newState: STATE.Value): Unit = {
    state = newState
  }
}
