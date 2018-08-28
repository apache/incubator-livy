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

import java.util

import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.livy.Logging

case class SessionInfo(username: String,
    ipAddress: String,
    forwardedAddresses: util.List[String],
    protocolVersion: TProtocolVersion) {
  val creationTime: Long = System.currentTimeMillis()
}

/**
 * Mirrors Hive behavior which stores thread local information in its session manager.
 */
object SessionInfo extends Logging {

  private val threadLocalIpAddress = new ThreadLocal[String]

  def setIpAddress(ipAddress: String): Unit = {
    threadLocalIpAddress.set(ipAddress)
  }

  def clearIpAddress(): Unit = {
    threadLocalIpAddress.remove()
  }

  def getIpAddress: String = threadLocalIpAddress.get

  private val threadLocalForwardedAddresses = new ThreadLocal[util.List[String]]

  def setForwardedAddresses(ipAddress: util.List[String]): Unit = {
    threadLocalForwardedAddresses.set(ipAddress)
  }

  def clearForwardedAddresses(): Unit = {
    threadLocalForwardedAddresses.remove()
  }

  def getForwardedAddresses: util.List[String] = threadLocalForwardedAddresses.get

  private val threadLocalUserName = new ThreadLocal[String]() {
    override protected def initialValue: String = null
  }

  def setUserName(userName: String): Unit = {
    threadLocalUserName.set(userName)
  }

  def clearUserName(): Unit = {
    threadLocalUserName.remove()
  }

  def getUserName: String = threadLocalUserName.get

  private val threadLocalProxyUserName = new ThreadLocal[String]() {
    override protected def initialValue: String = null
  }

  def setProxyUserName(userName: String): Unit = {
    debug("setting proxy user name based on query param to: " + userName)
    threadLocalProxyUserName.set(userName)
  }

  def getProxyUserName: String = threadLocalProxyUserName.get

  def clearProxyUserName(): Unit = {
    threadLocalProxyUserName.remove()
  }
}
