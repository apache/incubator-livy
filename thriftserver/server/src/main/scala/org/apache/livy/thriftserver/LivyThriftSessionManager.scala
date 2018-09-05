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

import java.lang.reflect.UndeclaredThrowableException
import java.net.URI
import java.security.PrivilegedExceptionAction
import java.util
import java.util.{Date, Map => JMap, UUID}
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success, Try}

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.shims.Utils
import org.apache.hive.service.CompositeService
import org.apache.hive.service.cli.{HiveSQLException, SessionHandle}
import org.apache.hive.service.rpc.thrift.TProtocolVersion
import org.apache.hive.service.server.ThreadFactoryWithGarbageCleanup

import org.apache.livy.LivyConf
import org.apache.livy.Logging
import org.apache.livy.server.interactive.{CreateInteractiveRequest, InteractiveSession}
import org.apache.livy.sessions.Spark
import org.apache.livy.thriftserver.SessionStates._
import org.apache.livy.thriftserver.rpc.RpcClient
import org.apache.livy.utils.LivySparkUtils

class LivyThriftSessionManager(val server: LivyThriftServer, hiveConf: HiveConf)
  extends CompositeService(classOf[LivyThriftSessionManager].getName) with Logging {

  private[thriftserver] val operationManager = new LivyOperationManager(this)
  private val sessionHandleToLivySession =
    new ConcurrentHashMap[SessionHandle, Future[InteractiveSession]]()
  // A map which returns how many incoming connections are open for a Livy session.
  // This map tracks only the sessions created by the Livy thriftserver and not those which have
  // been created through the REST API, as those should not be stopped even though there are no
  // more active connections.
  private val managedLivySessionActiveUsers = new mutable.HashMap[Int, Int]()

  // Contains metadata about a session
  private val sessionInfo = new ConcurrentHashMap[SessionHandle, SessionInfo]()

  // Map the number of incoming connections for IP, user. It is used in order to check
  // that the configured limits are not exceeded.
  private val connectionsCount = new ConcurrentHashMap[String, AtomicLong]

  // Timeout for a Spark session creation
  private val maxSessionWait = Duration(
    server.livyConf.getTimeAsMs(LivyConf.THRIFT_SESSION_CREATION_TIMEOUT),
    scala.concurrent.duration.MILLISECONDS)

  // Flag indicating whether the Spark version being used supports the USE database statement
  val supportUseDatabase: Boolean = {
    val sparkVersion = server.livyConf.get(LivyConf.LIVY_SPARK_VERSION)
    val (sparkMajorVersion, _) = LivySparkUtils.formatSparkVersion(sparkVersion)
    sparkMajorVersion > 1 || server.livyConf.getBoolean(LivyConf.ENABLE_HIVE_CONTEXT)
  }

  // Configs from Hive
  private val userLimit = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_LIMIT_CONNECTIONS_PER_USER)
  private val ipAddressLimit =
    hiveConf.getIntVar(ConfVars.HIVE_SERVER2_LIMIT_CONNECTIONS_PER_IPADDRESS)
  private val userIpAddressLimit =
    hiveConf.getIntVar(ConfVars.HIVE_SERVER2_LIMIT_CONNECTIONS_PER_USER_IPADDRESS)
  private val checkInterval = HiveConf.getTimeVar(
    hiveConf, ConfVars.HIVE_SERVER2_SESSION_CHECK_INTERVAL, TimeUnit.MILLISECONDS)
  private val sessionTimeout = HiveConf.getTimeVar(
    hiveConf, ConfVars.HIVE_SERVER2_IDLE_SESSION_TIMEOUT, TimeUnit.MILLISECONDS)
  private val checkOperation = HiveConf.getBoolVar(
    hiveConf, ConfVars.HIVE_SERVER2_IDLE_SESSION_CHECK_OPERATION)

  private var backgroundOperationPool: ThreadPoolExecutor = _

  def getLivySession(sessionHandle: SessionHandle): InteractiveSession = {
    val future = sessionHandleToLivySession.get(sessionHandle)
    assert(future != null, s"Looking for not existing session: $sessionHandle.")

    if (!future.isCompleted) {
      Try(Await.result(future, maxSessionWait)) match {
        case Success(session) => session
        case Failure(e) => throw e.getCause
      }
    } else {
      future.value match {
        case Some(Success(session)) => session
        case Some(Failure(e)) => throw e.getCause
        case None => throw new RuntimeException("Future cannot be None when it is completed")
      }
    }
  }

  def livySessionId(sessionHandle: SessionHandle): Option[Int] = {
    sessionHandleToLivySession.get(sessionHandle).value.filter(_.isSuccess).map(_.get.id)
  }

  def livySessionState(sessionHandle: SessionHandle): SessionStates = {
    sessionHandleToLivySession.get(sessionHandle).value match {
      case Some(Success(_)) => CREATION_SUCCESS
      case Some(Failure(_)) => CREATION_FAILED
      case None => CREATION_IN_PROGRESS
    }
  }

  def onLivySessionOpened(livySession: InteractiveSession): Unit = {
    server.livySessionManager.register(livySession)
    synchronized {
      managedLivySessionActiveUsers += livySession.id -> 0
    }
  }

  def onUserSessionClosed(sessionHandle: SessionHandle, livySession: InteractiveSession): Unit = {
    val closeSession = synchronized[Boolean] {
      managedLivySessionActiveUsers.get(livySession.id) match {
        case Some(1) =>
          // it was the last user, so we can close the LivySession
          managedLivySessionActiveUsers -= livySession.id
          true
        case Some(activeUsers) =>
          managedLivySessionActiveUsers(livySession.id) = activeUsers - 1
          false
        case None =>
          // This case can happen when we don't track the number of active users because the session
          // has not been created in the thriftserver (ie. it has been created in the REST API).
          false
      }
    }
    if (closeSession) {
      server.livySessionManager.delete(livySession)
    } else {
      // We unregister the session only if we don't close it, as it is unnecessary in that case
      val rpcClient = new RpcClient(livySession)
      try {
        rpcClient.executeUnregisterSession(sessionHandle).get()
      } catch {
        case e: Exception => warn(s"Unable to unregister session $sessionHandle", e)
      }
    }
  }

  /**
   * If the user specified an existing sessionId to use, the corresponding session is returned,
   * otherwise a new session is created and returned.
   */
  private def getOrCreateLivySession(
      sessionHandle: SessionHandle,
      sessionId: Option[Int],
      username: String,
      createLivySession: () => InteractiveSession): InteractiveSession = {
    sessionId match {
      case Some(id) =>
        server.livySessionManager.get(id) match {
          case None =>
            warn(s"Session id $id doesn't exist, so we will ignore it.")
            createLivySession()
          case Some(session) if !server.isAllowedToUse(username, session) =>
            warn(s"Session id $id doesn't belong to $username, so we will ignore it.")
            createLivySession()
          case Some(session) =>
            if (session.state.isActive) {
              info(s"Reusing Session $id for $sessionHandle.")
              session
            } else {
              warn(s"Session id $id is not active anymore, so we will ignore it.")
              createLivySession()
            }
        }
      case None =>
        createLivySession()
    }
  }

  /**
   * Performs the initialization of the new Thriftserver session:
   *  - adds the Livy thrifserver JAR to the Spark application;
   *  - register the new Thriftserver session in the Spark application;
   *  - runs the initialization statements;
   */
  private def initSession(
      sessionHandle: SessionHandle,
      livySession: InteractiveSession,
      initStatements: List[String]): Unit = {
    // Add the thriftserver jar to Spark application as we need to deserialize there the classes
    // which handle the job submission.
    // Note: if this is an already existing session, adding the JARs multiple times is not a
    // problem as Spark ignores JARs which have already been added.
    try {
      livySession.addJar(LivyThriftSessionManager.thriftserverJarLocation(server.livyConf))
    } catch {
      case e: java.util.concurrent.ExecutionException
          if Option(e.getCause).forall(_.getMessage.contains("has already been uploaded")) =>
        // We have already uploaded the jar to this session, we can ignore this error
        debug(e.getMessage, e)
    }

    val rpcClient = new RpcClient(livySession)
    rpcClient.executeRegisterSession(sessionHandle).get()
    initStatements.foreach { statement =>
      val statementId = UUID.randomUUID().toString
      try {
        rpcClient.executeSql(sessionHandle, statementId, statement).get()
      } finally {
        Try(rpcClient.cleanupStatement(statementId).get()).failed.foreach { e =>
          error(s"Failed to close init operation $statementId", e)
        }
      }
    }
  }

  def openSession(
      protocol: TProtocolVersion,
      username: String,
      password: String,
      ipAddress: String,
      sessionConf: JMap[String, String],
      withImpersonation: Boolean,
      delegationToken: String): SessionHandle = {
    val sessionHandle = new SessionHandle(protocol)
    incrementConnections(username, ipAddress, SessionInfo.getForwardedAddresses)
    sessionInfo.put(sessionHandle,
      new SessionInfo(username, ipAddress, SessionInfo.getForwardedAddresses, protocol))
    val (initStatements, createInteractiveRequest, sessionId) =
      LivyThriftSessionManager.processSessionConf(sessionConf, supportUseDatabase)
    val createLivySession = () => {
      createInteractiveRequest.kind = Spark
      val newSession = InteractiveSession.create(
        server.livySessionManager.nextId(),
        username,
        None,
        server.livyConf,
        createInteractiveRequest,
        server.sessionStore)
      onLivySessionOpened(newSession)
      newSession
    }
    val futureLivySession = Future {
      val livyServiceUGI = Utils.getUGI
      var livySession: InteractiveSession = null
      try {
        livyServiceUGI.doAs(new PrivilegedExceptionAction[InteractiveSession] {
          override def run(): InteractiveSession = {
            livySession =
              getOrCreateLivySession(sessionHandle, sessionId, username, createLivySession)
            synchronized {
              managedLivySessionActiveUsers.get(livySession.id).foreach { numUsers =>
                managedLivySessionActiveUsers(livySession.id) = numUsers + 1
              }
            }
            initSession(sessionHandle, livySession, initStatements)
            livySession
          }
        })
      } catch {
        case e: UndeclaredThrowableException =>
          throw new ThriftSessionCreationException(Option(livySession), e.getCause)
        case e: Throwable =>
          throw new ThriftSessionCreationException(Option(livySession), e)
      }
    }
    sessionHandleToLivySession.put(sessionHandle, futureLivySession)
    sessionHandle
  }

  def closeSession(sessionHandle: SessionHandle): Unit = {
    val removedSession = sessionHandleToLivySession.remove(sessionHandle)
    val removedSessionInfo = sessionInfo.remove(sessionHandle)
    try {
      removedSession.value match {
        case Some(Success(interactiveSession)) =>
          onUserSessionClosed(sessionHandle, interactiveSession)
        case Some(Failure(e: ThriftSessionCreationException)) =>
          e.livySession.foreach(onUserSessionClosed(sessionHandle, _))
        case None =>
          removedSession.onComplete {
            case Success(interactiveSession) =>
              onUserSessionClosed(sessionHandle, interactiveSession)
            case Failure(e: ThriftSessionCreationException) =>
              e.livySession.foreach(onUserSessionClosed(sessionHandle, _))
          }
        case _ => // We should never get here
      }
    } finally {
      decrementConnections(removedSessionInfo)
    }
  }

  // Taken from Hive
  override def init(hiveConf: HiveConf): Unit = {
    createBackgroundOperationPool(hiveConf)
    info("Connections limit are user: {} ipaddress: {} user-ipaddress: {}",
      userLimit, ipAddressLimit, userIpAddressLimit)
    super.init(hiveConf)
  }

  // Taken from Hive
  private def createBackgroundOperationPool(hiveConf: HiveConf): Unit = {
    val poolSize = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_ASYNC_EXEC_THREADS)
    info("HiveServer2: Background operation thread pool size: " + poolSize)
    val poolQueueSize = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_ASYNC_EXEC_WAIT_QUEUE_SIZE)
    info("HiveServer2: Background operation thread wait queue size: " + poolQueueSize)
    val keepAliveTime = HiveConf.getTimeVar(
      hiveConf, ConfVars.HIVE_SERVER2_ASYNC_EXEC_KEEPALIVE_TIME, TimeUnit.SECONDS)
    info(s"HiveServer2: Background operation thread keepalive time: $keepAliveTime seconds")
    // Create a thread pool with #poolSize threads
    // Threads terminate when they are idle for more than the keepAliveTime
    // A bounded blocking queue is used to queue incoming operations, if #operations > poolSize
    val threadPoolName = "LivyServer2-Background-Pool"
    val queue = new LinkedBlockingQueue[Runnable](poolQueueSize)
    backgroundOperationPool = new ThreadPoolExecutor(
      poolSize,
      poolSize,
      keepAliveTime,
      TimeUnit.SECONDS,
      queue,
      new ThreadFactoryWithGarbageCleanup(threadPoolName))
    backgroundOperationPool.allowCoreThreadTimeOut(true)
  }

  // Taken from Hive
  override def start(): Unit = {
    super.start()
    if (checkInterval > 0) startTimeoutChecker()
  }

  private val timeoutCheckerLock: Object = new Object
  @volatile private var shutdown: Boolean = false

  // Taken from Hive
  private def startTimeoutChecker(): Unit = {
    val interval: Long = Math.max(checkInterval, 3000L)
    // minimum 3 seconds
    val timeoutChecker: Runnable = new Runnable() {
      override def run(): Unit = {
        sleepFor(interval)
        while (!shutdown) {
          val current: Long = System.currentTimeMillis
          val iterator = sessionHandleToLivySession.entrySet().iterator()
          while (iterator.hasNext && ! shutdown) {
            val entry = iterator.next()
            val sessionHandle = entry.getKey
            entry.getValue.value.flatMap(_.toOption).foreach { livySession =>

              if (sessionTimeout > 0 && livySession.lastActivity + sessionTimeout <= current &&
                 (!checkOperation || getNoOperationTime(sessionHandle) > sessionTimeout)) {
                warn(s"Session $sessionHandle is Timed-out (last access : " +
                  new Date(livySession.lastActivity) + ") and will be closed")
                try {
                  closeSession(sessionHandle)
                } catch {
                  case e: HiveSQLException =>
                    warn(s"Exception is thrown closing session $sessionHandle", e)
                }
              } else {
                val operations = operationManager.getTimedOutOperations(sessionHandle)
                if (operations.nonEmpty) {
                  operations.foreach { op =>
                    try {
                      warn(s"Operation ${op.getHandle} is timed-out and will be closed")
                      operationManager.closeOperation(op.getHandle)
                    } catch {
                      case e: Exception =>
                        warn("Exception is thrown closing timed-out operation: " + op.getHandle, e)
                    }
                  }
                }
              }
            }
          }
          sleepFor(interval)
        }
      }

      private def sleepFor(interval: Long): Unit = {
        timeoutCheckerLock.synchronized {
          try {
            timeoutCheckerLock.wait(interval)
          } catch {
            case e: InterruptedException =>
            // Ignore, and break.
          }
        }
      }
    }
    backgroundOperationPool.execute(timeoutChecker)
  }

  // Taken from Hive
  private def shutdownTimeoutChecker(): Unit = {
    shutdown = true
    timeoutCheckerLock.synchronized { timeoutCheckerLock.notify() }
  }

  // Taken from Hive
  override def stop(): Unit = {
    super.stop()
    shutdownTimeoutChecker()
    if (backgroundOperationPool != null) {
      backgroundOperationPool.shutdown()
      val timeout =
        hiveConf.getTimeVar(ConfVars.HIVE_SERVER2_ASYNC_EXEC_SHUTDOWN_TIMEOUT, TimeUnit.SECONDS)
      try {
        backgroundOperationPool.awaitTermination(timeout, TimeUnit.SECONDS)
      } catch {
        case e: InterruptedException =>
          warn("HIVE_SERVER2_ASYNC_EXEC_SHUTDOWN_TIMEOUT = " + timeout +
            " seconds has been exceeded. RUNNING background operations will be shut down", e)
      }
      backgroundOperationPool = null
    }
  }

  // Taken from Hive
  @throws[HiveSQLException]
  private def incrementConnections(
      username: String,
      ipAddress: String,
      forwardedAddresses: util.List[String]): Unit = {
    val clientIpAddress: String = getOriginClientIpAddress(ipAddress, forwardedAddresses)
    val violation = anyViolations(username, clientIpAddress)
    // increment the counters only when there are no violations
    if (violation.isEmpty) {
      if (trackConnectionsPerUser(username)) incrementConnectionsCount(username)
      if (trackConnectionsPerIpAddress(clientIpAddress)) incrementConnectionsCount(clientIpAddress)
      if (trackConnectionsPerUserIpAddress(username, clientIpAddress)) {
        incrementConnectionsCount(username + ":" + clientIpAddress)
      }
    } else {
      error(violation.get)
      throw new HiveSQLException(violation.get)
    }
  }

  // Taken from Hive
  private def incrementConnectionsCount(key: String): Unit = {
    if (!connectionsCount.containsKey(key)) connectionsCount.get(key).incrementAndGet
    else connectionsCount.put(key, new AtomicLong)
  }

  // Taken from Hive
  private def decrementConnectionsCount(key: String): Unit = {
    if (!connectionsCount.containsKey(key)) connectionsCount.get(key).decrementAndGet
    else connectionsCount.put(key, new AtomicLong)
  }

  // Taken from Hive
  private def getOriginClientIpAddress(ipAddress: String, forwardedAddresses: util.List[String]) = {
    if (forwardedAddresses == null || forwardedAddresses.isEmpty) {
      ipAddress
    } else {
      // order of forwarded ips per X-Forwarded-For http spec (client, proxy1, proxy2)
      forwardedAddresses.get(0)
    }
  }

  // Taken from Hive
  private def anyViolations(username: String, ipAddress: String): Option[String] = {
    val userAndAddress = username + ":" + ipAddress
    if (trackConnectionsPerUser(username) && !withinLimits(username, userLimit)) {
      Some(s"Connection limit per user reached (user: $username limit: $userLimit)")
    } else if (trackConnectionsPerIpAddress(ipAddress) &&
        !withinLimits(ipAddress, ipAddressLimit)) {
      Some(s"Connection limit per ipaddress reached (ipaddress: $ipAddress limit: " +
        s"$ipAddressLimit)")
    } else if (trackConnectionsPerUserIpAddress(username, ipAddress) &&
        !withinLimits(userAndAddress, userIpAddressLimit)) {
      Some(s"Connection limit per user:ipaddress reached (user:ipaddress: $userAndAddress " +
        s"limit: $userIpAddressLimit)")
    } else {
      None
    }
  }

  // Taken from Hive
  private def trackConnectionsPerUserIpAddress(username: String, ipAddress: String): Boolean = {
    userIpAddressLimit > 0 && username != null && !username.isEmpty && ipAddress != null &&
      !ipAddress.isEmpty
  }

  // Taken from Hive
  private def trackConnectionsPerIpAddress(ipAddress: String): Boolean = {
    ipAddressLimit > 0 && ipAddress != null && !ipAddress.isEmpty
  }

  // Taken from Hive
  private def trackConnectionsPerUser(username: String): Boolean = {
    userLimit > 0 && username != null && !username.isEmpty
  }

  // Taken from Hive
  private def withinLimits(track: String, limit: Int): Boolean = {
    !(connectionsCount.containsKey(track) && connectionsCount.get(track).intValue >= limit)
  }

  private def decrementConnections(sessionInfo: SessionInfo): Unit = {
    val username = sessionInfo.username
    val clientIpAddress = getOriginClientIpAddress(
      sessionInfo.ipAddress, sessionInfo.forwardedAddresses)
    if (trackConnectionsPerUser(username)) {
      decrementConnectionsCount(username)
    }
    if (trackConnectionsPerIpAddress(clientIpAddress)) {
      decrementConnectionsCount(clientIpAddress)
    }
    if (trackConnectionsPerUserIpAddress(username, clientIpAddress)) {
      decrementConnectionsCount(username + ":" + clientIpAddress)
    }
  }

  def submitBackgroundOperation(r: Runnable): util.concurrent.Future[_] = {
    backgroundOperationPool.submit(r)
  }

  def getNoOperationTime(sessionHandle: SessionHandle): Long = {
    if (operationManager.getOperations(sessionHandle).isEmpty) {
      System.currentTimeMillis() - getLivySession(sessionHandle).lastActivity
    } else {
      0
    }
  }

  def getSessions: Set[SessionHandle] = {
    sessionInfo.keySet().asScala.toSet
  }

  def getSessionInfo(sessionHandle: SessionHandle): SessionInfo = {
    sessionInfo.get(sessionHandle)
  }
}

object LivyThriftSessionManager extends Logging {
  // Users can explicitly set the Livy connection id they want to connect to using this hiveconf
  // variable
  private val livySessionIdConfigKey = "set:hiveconf:livy.server.sessionId"
  private val livySessionConfRegexp = "set:hiveconf:livy.session.conf.(.*)".r
  private val hiveVarPattern = "set:hivevar:(.*)".r
  private val JAR_LOCATION = getClass.getProtectionDomain.getCodeSource.getLocation.toURI

  def thriftserverJarLocation(livyConf: LivyConf): URI = {
    Option(livyConf.get(LivyConf.THRIFT_SERVER_JAR_LOCATION)).map(new URI(_))
      .getOrElse(JAR_LOCATION)
  }

  private def convertConfValueToInt(key: String, value: String) = {
    val res = Try(value.toInt)
    if (res.isFailure) {
      warn(s"Ignoring $key = $value as it is not a valid integer")
      None
    } else {
      Some(res.get)
    }
  }

  private def processSessionConf(
      sessionConf: JMap[String, String],
      supportUseDatabase: Boolean): (List[String], CreateInteractiveRequest, Option[Int]) = {
    if (null != sessionConf && !sessionConf.isEmpty) {
      val statements = new mutable.ListBuffer[String]
      val extraLivyConf = new mutable.ListBuffer[(String, String)]
      val createInteractiveRequest = new CreateInteractiveRequest
      sessionConf.asScala.foreach {
        case (key, value) =>
          key match {
            case v if v.startsWith("use:") && supportUseDatabase =>
              statements += s"use $value"
            // Process session configs for Livy session creation request
            case "set:hiveconf:livy.session.driverMemory" =>
              createInteractiveRequest.driverMemory = Some(value)
            case "set:hiveconf:livy.session.driverCores" =>
              createInteractiveRequest.driverCores = convertConfValueToInt(key, value)
            case "set:hiveconf:livy.session.executorMemory" =>
              createInteractiveRequest.executorMemory = Some(value)
            case "set:hiveconf:livy.session.executorCores" =>
              createInteractiveRequest.executorCores = convertConfValueToInt(key, value)
            case "set:hiveconf:livy.session.queue" =>
              createInteractiveRequest.queue = Some(value)
            case "set:hiveconf:livy.session.name" =>
              createInteractiveRequest.name = Some(value)
            case "set:hiveconf:livy.session.heartbeatTimeoutInSecond" =>
              convertConfValueToInt(key, value).foreach { heartbeatTimeoutInSecond =>
                createInteractiveRequest.heartbeatTimeoutInSecond = heartbeatTimeoutInSecond
              }
            case livySessionConfRegexp(livyConfKey) => extraLivyConf += (livyConfKey -> value)
            // set the hivevars specified by the user
            case hiveVarPattern(confKey) => statements += s"set hivevar:${confKey.trim}=$value"
            case _ if key == livySessionIdConfigKey => // Ignore it, we handle it later
            case _ =>
              info(s"Ignoring key: $key = '$value'")
          }
      }
      createInteractiveRequest.conf = extraLivyConf.toMap
      val sessionId = Option(sessionConf.get(livySessionIdConfigKey)).flatMap { id =>
        val res = Try(id.toInt)
        if (res.isFailure) {
          warn(s"Ignoring $livySessionIdConfigKey=$id as it is not an int.")
          None
        } else {
          Some(res.get)
        }
      }
      (statements.toList, createInteractiveRequest, sessionId)
    } else {
      (List(), new CreateInteractiveRequest, None)
    }
  }
}

/**
 * Exception which happened during the session creation and/or initialization. It contains the
 * `livySession` (if it was created) where the error occurred and the `cause` of the error.
 */
class ThriftSessionCreationException(val livySession: Option[InteractiveSession], cause: Throwable)
  extends Exception(cause)
