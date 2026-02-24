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

import java.util.concurrent.TimeoutException
import java.util.concurrent.TimeUnit

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

import org.apache.hive.service.cli.{HiveSQLException, SessionHandle}
import org.junit.Assert._
import org.junit.Test
import org.mockito.Mockito.{mock, when}

import org.apache.livy.LivyConf
import org.apache.livy.server.interactive.InteractiveSession
import org.apache.livy.server.recovery.SessionStore
import org.apache.livy.server.AccessManager
import org.apache.livy.sessions.{InteractiveSessionManager, SessionState}
import org.apache.livy.utils.Clock.sleep

object ConnectionLimitType extends Enumeration {
  type ConnectionLimitType = Value
  val User, IpAddress, UserIpAddress = Value
}

class TestLivyThriftSessionManager {

  import ConnectionLimitType._

  private def createThriftSessionManager(
      limitTypes: ConnectionLimitType*): (LivyThriftSessionManager, LivyThriftServer) = {
    val conf = new LivyConf()
    conf.set(LivyConf.LIVY_SPARK_VERSION, sys.env("LIVY_SPARK_VERSION"))
    val limit = 3
    limitTypes.foreach { limitType =>
      val entry = limitType match {
        case User => LivyConf.THRIFT_LIMIT_CONNECTIONS_PER_USER
        case IpAddress => LivyConf.THRIFT_LIMIT_CONNECTIONS_PER_IPADDRESS
        case UserIpAddress => LivyConf.THRIFT_LIMIT_CONNECTIONS_PER_USER_IPADDRESS
      }
      conf.set(entry, limit)
    }
    this.createThriftSessionManager(conf)
  }

  private def createThriftSessionManager(
      maxSessionWait: Option[String]): (LivyThriftSessionManager, LivyThriftServer) = {
    val conf = new LivyConf()
    conf.set(LivyConf.LIVY_SPARK_VERSION, sys.env("LIVY_SPARK_VERSION"))
    maxSessionWait.foreach(conf.set(LivyConf.THRIFT_SESSION_CREATION_TIMEOUT, _))
    this.createThriftSessionManager(conf)
  }

  private def createThriftSessionManager(conf: LivyConf): (LivyThriftSessionManager,
    LivyThriftServer) = {
    val server = new LivyThriftServer(
      conf,
      mock(classOf[InteractiveSessionManager]),
      mock(classOf[SessionStore]),
      mock(classOf[AccessManager])
    )
    val sessionManager = new LivyThriftSessionManager(server, conf)
    (sessionManager, server)
  }

  private def testLimit(
      thriftSessionMgr: LivyThriftSessionManager,
      user: String,
      ipAddress: String,
      forwardedAddresses: java.util.List[String],
      msg: String): Unit = {
    val failureMsg = "Should have thrown HiveSQLException"
    try {
      thriftSessionMgr.incrementConnections(user, ipAddress, forwardedAddresses)
      fail(failureMsg)
    } catch {
      case e: HiveSQLException =>
        assertEquals(msg, e.getMessage)
      case _: Throwable =>
        fail(failureMsg)
    }
  }

  @Test
  def testLimitConnectionsByUser(): Unit = {
    val (thriftSessionMgr, _) = createThriftSessionManager(User)
    val user = "alice"
    val forwardedAddresses = new java.util.ArrayList[String]()
    thriftSessionMgr.incrementConnections(user, "10.20.30.40", forwardedAddresses)
    thriftSessionMgr.incrementConnections(user, "10.20.30.41", forwardedAddresses)
    thriftSessionMgr.incrementConnections(user, "10.20.30.42", forwardedAddresses)
    val msg = s"Connection limit per user reached (user: $user limit: 3)"
    testLimit(thriftSessionMgr, user, "10.20.30.43", forwardedAddresses, msg)
  }

  @Test
  def testLimitConnectionsByIpAddress(): Unit = {
    val (thriftSessionMgr, _) = createThriftSessionManager(IpAddress)
    val ipAddress = "10.20.30.40"
    val forwardedAddresses = new java.util.ArrayList[String]()
    thriftSessionMgr.incrementConnections("alice", ipAddress, forwardedAddresses)
    thriftSessionMgr.incrementConnections("bob", ipAddress, forwardedAddresses)
    thriftSessionMgr.incrementConnections("charlie", ipAddress, forwardedAddresses)
    val msg = s"Connection limit per ipaddress reached (ipaddress: $ipAddress limit: 3)"
    testLimit(thriftSessionMgr, "dan", ipAddress, forwardedAddresses, msg)
  }

  @Test
  def testLimitConnectionsByUserAndIpAddress(): Unit = {
    val (thriftSessionMgr, _) = createThriftSessionManager(UserIpAddress)
    val user = "alice"
    val ipAddress = "10.20.30.40"
    val userAndAddress = user + ":" + ipAddress
    val forwardedAddresses = new java.util.ArrayList[String]()
    thriftSessionMgr.incrementConnections(user, ipAddress, forwardedAddresses)

    // more than 3 connections from the same IP Address is ok if users are different
    thriftSessionMgr.incrementConnections("bob", ipAddress, forwardedAddresses)
    thriftSessionMgr.incrementConnections("charlie", ipAddress, forwardedAddresses)
    thriftSessionMgr.incrementConnections("dan", ipAddress, forwardedAddresses)

    // more than 3 connections from the same user is ok if IP addresses are different
    thriftSessionMgr.incrementConnections(user, "10.20.30.41", forwardedAddresses)
    thriftSessionMgr.incrementConnections(user, "10.20.30.42", forwardedAddresses)
    thriftSessionMgr.incrementConnections(user, "10.20.30.43", forwardedAddresses)

    thriftSessionMgr.incrementConnections(user, ipAddress, forwardedAddresses)
    thriftSessionMgr.incrementConnections(user, ipAddress, forwardedAddresses)
    val msg =
      s"Connection limit per user:ipaddress reached (user:ipaddress: $userAndAddress limit: 3)"
    testLimit(thriftSessionMgr, user, ipAddress, forwardedAddresses, msg)
  }

  @Test
  def testMultipleConnectionLimits(): Unit = {
    val (thriftSessionMgr, _) = createThriftSessionManager(User, IpAddress)
    val user = "alice"
    val ipAddress = "10.20.30.40"
    val forwardedAddresses = new java.util.ArrayList[String]()
    thriftSessionMgr.incrementConnections(user, ipAddress, forwardedAddresses)
    thriftSessionMgr.incrementConnections("bob", ipAddress, forwardedAddresses)
    thriftSessionMgr.incrementConnections("charlie", ipAddress, forwardedAddresses)
    thriftSessionMgr.incrementConnections(user, "10.20.30.41", forwardedAddresses)
    thriftSessionMgr.incrementConnections(user, "10.20.30.42", forwardedAddresses)
    // At this point, both user and ipAddress are at their respective limits.
    // If the limit for both are exceeded at the same time, the error message is for user.
    val msg = s"Connection limit per user reached (user: $user limit: 3)"
    testLimit(thriftSessionMgr, user, ipAddress, forwardedAddresses, msg)
  }

  @Test(expected = classOf[TimeoutException])
  def testGetLivySessionWaitForTimeout(): Unit = {
    val (thriftSessionMgr, _) = createThriftSessionManager(Some("10ms"))
    val sessionHandle = mock(classOf[SessionHandle])
    val future = Future[InteractiveSession] {
      sleep(100)
      mock(classOf[InteractiveSession])
    }
    thriftSessionMgr.sessionHandleToLivySession.put(sessionHandle, future)
    thriftSessionMgr.getLivySession(sessionHandle)
  }

  @Test(expected = classOf[TimeoutException])
  def testGetLivySessionWithTimeoutException(): Unit = {
    val (thriftSessionMgr, _) = createThriftSessionManager(None)
    val sessionHandle = mock(classOf[SessionHandle])
    val future = Future[InteractiveSession] {
      throw new TimeoutException("Actively throw TimeoutException in Future.")
    }
    thriftSessionMgr.sessionHandleToLivySession.put(sessionHandle, future)
    Await.ready(future, Duration(30, TimeUnit.SECONDS))
    thriftSessionMgr.getLivySession(sessionHandle)
  }


  @Test
  def testGetOrCreateLivySessionDifferentSessions(): Unit = {
    val (thriftSessionMgr, server) = createThriftSessionManager(User, IpAddress)
    val sessionHandle = mock(classOf[SessionHandle])
    val sessionUser = "testUser"
    val sessionId1 = Some(1)
    val session1 = mock(classOf[InteractiveSession])
    when(session1.state).thenReturn(SessionState.Running)
    when(session1.owner).thenReturn(sessionUser)
    when(server.livySessionManager.get(1)).thenReturn(Some(session1))
    val sessionId2 = Some(2)
    val session2 = mock(classOf[InteractiveSession])
    when(session2.state).thenReturn(SessionState.Running)
    when(session2.owner).thenReturn(sessionUser)
    when(server.livySessionManager.get(2)).thenReturn(Some(session2))
    val result1 = thriftSessionMgr.getOrCreateLivySession(sessionHandle, sessionId1, None,
      sessionUser, () => null)
    val result2 = thriftSessionMgr.getOrCreateLivySession(sessionHandle, sessionId2, None,
      sessionUser, () => null)

    assertNotNull(result1)
    assertNotNull(result2)
    assertNotEquals(result1, result2)
  }

  @Test
  def testGetOrCreateLivySessionExistingSessionByID(): Unit = {
    val (thriftSessionMgr, server) = createThriftSessionManager(User, IpAddress)
    val sessionHandle = mock(classOf[SessionHandle])
    val sessionUser = "testUser"
    val sessionId = Some(1)
    val session1 = mock(classOf[InteractiveSession])
    when(session1.state).thenReturn(SessionState.Running)
    when(session1.owner).thenReturn(sessionUser)
    when(server.livySessionManager.get(1)).thenReturn(Some(session1))
    val result1 = thriftSessionMgr.getOrCreateLivySession(sessionHandle, sessionId, None,
      sessionUser, () => null)
    val result2 = thriftSessionMgr.getOrCreateLivySession(sessionHandle, sessionId, None,
      sessionUser, () => null)

    assertNotNull(result1)
    assertNotNull(result2)
    assertEquals(result1, result2)
  }


  @Test
  def testGetOrCreateLivySessionExistingSessionByName(): Unit = {
    val (thriftSessionMgr, server) = createThriftSessionManager(User, IpAddress)
    val sessionHandle = mock(classOf[SessionHandle])
    val sessionUser = "testUser"
    val sessionName = Some("sessionName")
    val session1 = mock(classOf[InteractiveSession])
    when(session1.state).thenReturn(SessionState.Running)
    when(session1.owner).thenReturn(sessionUser)
    when(server.livySessionManager.get("sessionName")).thenReturn(Some(session1))
    val result1 = thriftSessionMgr.getOrCreateLivySession(sessionHandle, None, sessionName,
      sessionUser, () => null)
    val result2 = thriftSessionMgr.getOrCreateLivySession(sessionHandle, None, sessionName,
      sessionUser, () => null)

    assertNotNull(result1)
    assertNotNull(result2)
    assertEquals(result1, result2)
  }

}
