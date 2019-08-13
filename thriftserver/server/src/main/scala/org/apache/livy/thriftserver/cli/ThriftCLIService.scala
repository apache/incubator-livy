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

package org.apache.livy.thriftserver.cli

import java.io.IOException
import java.net.{InetAddress, UnknownHostException}
import java.util
import java.util.Collections
import javax.security.auth.login.LoginException

import scala.collection.JavaConverters._

import com.google.common.base.Preconditions.checkArgument
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.authentication.util.KerberosName
import org.apache.hadoop.security.authorize.ProxyUsers
import org.apache.hadoop.util.StringUtils
import org.apache.hive.service.{ServiceException, ServiceUtils}
import org.apache.hive.service.auth.{HiveAuthConstants, TSetIpAddressProcessor}
import org.apache.hive.service.auth.HiveAuthConstants.AuthTypes
import org.apache.hive.service.cli._
import org.apache.hive.service.rpc.thrift._
import org.apache.thrift.TException
import org.apache.thrift.server.ServerContext

import org.apache.livy.LivyConf
import org.apache.livy.thriftserver.{LivyCLIService, LivyThriftServer, SessionInfo, ThriftService}
import org.apache.livy.thriftserver.auth.AuthFactory

/**
 * This class is ported from Hive. We cannot reuse Hive's one because we need to use the
 * `LivyCLIService`, `LivyConf` and `AuthFacotry` instead of Hive's one.
 */
abstract class ThriftCLIService(val cliService: LivyCLIService, val serviceName: String)
    extends ThriftService(serviceName) with TCLIService.Iface with Runnable {

  def hiveAuthFactory: AuthFactory

  protected val currentServerContext = new ThreadLocal[ServerContext]
  protected var portNum: Int = 0
  protected var serverIPAddress: InetAddress = _
  protected var hiveHost: String = _
  private var isStarted: Boolean = false
  protected var isEmbedded: Boolean = false
  protected var livyConf: LivyConf = _
  protected var minWorkerThreads: Int = 0
  protected var maxWorkerThreads: Int = 0
  protected var workerKeepAliveTime: Long = 0L
  private var serverThread: Thread = _

  override def init(conf: LivyConf): Unit = {
    livyConf = conf
    hiveHost = livyConf.get(LivyConf.THRIFT_BIND_HOST)
    try {
      if (hiveHost == null || hiveHost.isEmpty) {
        serverIPAddress = InetAddress.getLocalHost
      } else {
        serverIPAddress = InetAddress.getByName(hiveHost)
      }
    } catch {
      case e: UnknownHostException =>
        throw new ServiceException(e)
    }
    portNum = livyConf.getInt(LivyConf.THRIFT_SERVER_PORT)
    workerKeepAliveTime = livyConf.getTimeAsMs(LivyConf.THRIFT_WORKER_KEEPALIVE_TIME) / 1000
    minWorkerThreads = livyConf.getInt(LivyConf.THRIFT_MIN_WORKER_THREADS)
    maxWorkerThreads = livyConf.getInt(LivyConf.THRIFT_MAX_WORKER_THREADS)
    super.init(livyConf)
  }

  protected def initServer(): Unit

  override def start(): Unit = {
    super.start()
    if (!isStarted && !isEmbedded) {
      initServer()
      serverThread = new Thread(this)
      serverThread.setName("Thrift Server")
      serverThread.start()
      isStarted = true
    }
  }

  protected def stopServer(): Unit

  override def stop(): Unit = {
    if (isStarted && !isEmbedded) {
      if (serverThread != null) {
        serverThread.interrupt()
        serverThread = null
      }
      stopServer()
      isStarted = false
    }
    super.stop()
  }

  def getPortNumber: Int = portNum

  def getServerIPAddress: InetAddress = serverIPAddress

  @throws[TException]
  override def GetDelegationToken(req: TGetDelegationTokenReq): TGetDelegationTokenResp = {
    val resp: TGetDelegationTokenResp = new TGetDelegationTokenResp
    if (!hiveAuthFactory.isSASLKerberosUser) {
      resp.setStatus(unsecureTokenErrorStatus)
    } else {
      try {
        val token = cliService.getDelegationToken(
          new SessionHandle(req.getSessionHandle), hiveAuthFactory, req.getOwner, req.getRenewer)
        resp.setDelegationToken(token)
        resp.setStatus(ThriftCLIService.OK_STATUS)
      } catch {
        case e: HiveSQLException =>
          error("Error obtaining delegation token", e)
          val tokenErrorStatus = HiveSQLException.toTStatus(e)
          tokenErrorStatus.setSqlState("42000")
          resp.setStatus(tokenErrorStatus)
      }
    }
    resp
  }

  @throws[TException]
  override def CancelDelegationToken(req: TCancelDelegationTokenReq): TCancelDelegationTokenResp = {
    val resp: TCancelDelegationTokenResp = new TCancelDelegationTokenResp
    if (!hiveAuthFactory.isSASLKerberosUser) {
      resp.setStatus(unsecureTokenErrorStatus)
    } else {
      try {
        cliService.cancelDelegationToken(
          new SessionHandle(req.getSessionHandle), hiveAuthFactory, req.getDelegationToken)
        resp.setStatus(ThriftCLIService.OK_STATUS)
      } catch {
        case e: HiveSQLException =>
          error("Error canceling delegation token", e)
          resp.setStatus(HiveSQLException.toTStatus(e))
      }
    }
    resp
  }

  @throws[TException]
  override def RenewDelegationToken(req: TRenewDelegationTokenReq): TRenewDelegationTokenResp = {
    val resp: TRenewDelegationTokenResp = new TRenewDelegationTokenResp
    if (!hiveAuthFactory.isSASLKerberosUser) {
      resp.setStatus(unsecureTokenErrorStatus)
    } else {
      try {
        cliService.renewDelegationToken(
          new SessionHandle(req.getSessionHandle), hiveAuthFactory, req.getDelegationToken)
        resp.setStatus(ThriftCLIService.OK_STATUS)
      } catch {
        case e: HiveSQLException =>
          error("Error obtaining renewing token", e)
          resp.setStatus(HiveSQLException.toTStatus(e))
      }
    }
    resp
  }

  private def unsecureTokenErrorStatus: TStatus = {
    val errorStatus: TStatus = new TStatus(TStatusCode.ERROR_STATUS)
    errorStatus.setErrorMessage(
      "Delegation token only supported over remote client with kerberos authentication")
    errorStatus
  }

  @throws[TException]
  override def OpenSession(req: TOpenSessionReq): TOpenSessionResp = {
    info("Client protocol version: " + req.getClient_protocol)
    val resp: TOpenSessionResp = new TOpenSessionResp
    try {
      val sessionHandle = getSessionHandle(req, resp)
      resp.setSessionHandle(sessionHandle.toTSessionHandle)
      val configurationMap: util.Map[String, String] = new util.HashMap[String, String]
      // Set the updated fetch size from the server into the configuration map for the client
      val defaultFetchSize =
        Integer.toString(livyConf.getInt(LivyConf.THRIFT_RESULTSET_DEFAULT_FETCH_SIZE))
      configurationMap.put(LivyConf.THRIFT_RESULTSET_DEFAULT_FETCH_SIZE.key, defaultFetchSize)
      resp.setConfiguration(configurationMap)
      resp.setStatus(ThriftCLIService.OK_STATUS)
      Option(currentServerContext.get).foreach { context =>
        context.asInstanceOf[ThriftCLIServerContext].setSessionHandle(sessionHandle)
      }
    } catch {
      case e: Exception =>
        warn("Error opening session: ", e)
        resp.setStatus(HiveSQLException.toTStatus(e))
    }
    resp
  }

  @throws[TException]
  override def SetClientInfo(req: TSetClientInfoReq): TSetClientInfoResp = {
    // TODO: We don't do anything for now, just log this for debugging.
    //       We may be able to make use of this later, e.g. for workload management.
    if (req.isSetConfiguration) {
      val sh = new SessionHandle(req.getSessionHandle)
      val sb = new StringBuilder("Client information for ").append(sh).append(": ")

      def processEntry(e: util.Map.Entry[String, String]): Unit = {
        sb.append(e.getKey).append(" = ").append(e.getValue)
        if ("ApplicationName" == e.getKey) {
          cliService.setApplicationName(sh, e.getValue)
        }
      }

      val entries = req.getConfiguration.entrySet.asScala.toSeq
      try {
        entries.headOption.foreach(processEntry)
        entries.tail.foreach { e =>
          sb.append(", ")
          processEntry(e)
        }
      } catch {
        case ex: Exception =>
          warn("Error setting application name", ex)
          return new TSetClientInfoResp(HiveSQLException.toTStatus(ex))
      }
      info(sb.toString())
    }
    new TSetClientInfoResp(ThriftCLIService.OK_STATUS)
  }

  private def getIpAddress: String = {
    // Http transport mode.
    // We set the thread local ip address, in ThriftHttpServlet.
    val clientIpAddress = if (LivyThriftServer.isHTTPTransportMode(livyConf)) {
      SessionInfo.getIpAddress
    } else if (hiveAuthFactory.isSASLWithKerberizedHadoop) {
      hiveAuthFactory.getIpAddress
    } else {
      // NOSASL
      TSetIpAddressProcessor.getUserIpAddress
    }
    debug(s"Client's IP Address: $clientIpAddress")
    clientIpAddress
  }

  /**
   * Returns the effective username.
   * 1. If livy.server.thrift.allow.user.substitution = false: the username of the connecting user
   * 2. If livy.server.thrift.allow.user.substitution = true: the username of the end user,
   * that the connecting user is trying to proxy for.
   * This includes a check whether the connecting user is allowed to proxy for the end user.
   */
  @throws[HiveSQLException]
  @throws[IOException]
  private def getUserName(req: TOpenSessionReq): String = {
    val username = if (LivyThriftServer.isHTTPTransportMode(livyConf)) {
      Option(SessionInfo.getUserName).getOrElse(req.getUsername)
    } else if (hiveAuthFactory.isSASLWithKerberizedHadoop) {
      Option(hiveAuthFactory.getRemoteUser).orElse(Option(TSetIpAddressProcessor.getUserName))
        .getOrElse(req.getUsername)
    } else {
      Option(TSetIpAddressProcessor.getUserName).getOrElse(req.getUsername)
    }
    val effectiveClientUser =
      getProxyUser(getShortName(username), req.getConfiguration, getIpAddress)
    debug(s"Client's username: $effectiveClientUser")
    effectiveClientUser
  }

  @throws[IOException]
  private def getShortName(userName: String): String = {
    Option(userName).map { un =>
      if (hiveAuthFactory.isSASLKerberosUser) {
        // KerberosName.getShorName can only be used for kerberos user
        new KerberosName(un).getShortName
      } else {
        val indexOfDomainMatch = ServiceUtils.indexOfDomainMatch(un)
        if (indexOfDomainMatch <= 0) {
          un
        } else {
          un.substring(0, indexOfDomainMatch)
        }
      }
    }.orNull
  }

  /**
   * Create a session handle
   */
  @throws[HiveSQLException]
  @throws[LoginException]
  @throws[IOException]
  private[thriftserver] def getSessionHandle(
      req: TOpenSessionReq, res: TOpenSessionResp): SessionHandle = {
    val userName = getUserName(req)
    val ipAddress = getIpAddress
    val protocol = getMinVersion(LivyCLIService.SERVER_VERSION, req.getClient_protocol)
    val sessionHandle =
      if (livyConf.getBoolean(LivyConf.THRIFT_ENABLE_DOAS) && (userName != null)) {
        cliService.openSessionWithImpersonation(
          protocol, userName, req.getPassword, ipAddress, req.getConfiguration, null)
      } else {
        cliService.openSession(protocol, userName, req.getPassword, ipAddress, req.getConfiguration)
      }
    res.setServerProtocolVersion(protocol)
    sessionHandle
  }

  @throws[HiveSQLException]
  private def getProgressedPercentage(opHandle: OperationHandle): Double = {
    checkArgument(OperationType.EXECUTE_STATEMENT == opHandle.getOperationType)
    0.0
  }

  private def getMinVersion(versions: TProtocolVersion*): TProtocolVersion = {
    val values = TProtocolVersion.values
    var current = values(values.length - 1).getValue
    versions.foreach { version =>
      if (current > version.getValue) {
        current = version.getValue
      }
    }
    val res = values.find(_.getValue == current)
    assert(res.isDefined)
    res.get
  }

  @throws[TException]
  override def CloseSession(req: TCloseSessionReq): TCloseSessionResp = {
    val resp = new TCloseSessionResp
    try {
      val sessionHandle = new SessionHandle(req.getSessionHandle)
      cliService.closeSession(sessionHandle)
      resp.setStatus(ThriftCLIService.OK_STATUS)
      Option(currentServerContext.get).foreach { ctx =>
        ctx.asInstanceOf[ThriftCLIServerContext].setSessionHandle(null)
      }
    } catch {
      case e: Exception =>
        warn("Error closing session: ", e)
        resp.setStatus(HiveSQLException.toTStatus(e))
    }
    resp
  }

  @throws[TException]
  override def GetInfo(req: TGetInfoReq): TGetInfoResp = {
    val resp = new TGetInfoResp
    try {
      val getInfoValue = cliService.getInfo(
        new SessionHandle(req.getSessionHandle), GetInfoType.getGetInfoType(req.getInfoType))
      resp.setInfoValue(getInfoValue.toTGetInfoValue)
      resp.setStatus(ThriftCLIService.OK_STATUS)
    } catch {
      case e: Exception =>
        warn("Error getting info: ", e)
        resp.setStatus(HiveSQLException.toTStatus(e))
    }
    resp
  }

  @throws[TException]
  override def ExecuteStatement(req: TExecuteStatementReq): TExecuteStatementResp = {
    val resp = new TExecuteStatementResp
    try {
      val sessionHandle = new SessionHandle(req.getSessionHandle)
      val statement = req.getStatement
      val confOverlay = req.getConfOverlay
      val runAsync = req.isRunAsync
      val queryTimeout = req.getQueryTimeout
      val operationHandle = if (runAsync) {
          cliService.executeStatementAsync(sessionHandle, statement, confOverlay, queryTimeout)
        } else {
          cliService.executeStatement(sessionHandle, statement, confOverlay, queryTimeout)
        }
      resp.setOperationHandle(operationHandle.toTOperationHandle)
      resp.setStatus(ThriftCLIService.OK_STATUS)
    } catch {
      case e: Exception =>
        warn("Error executing statement: ", e)
        resp.setStatus(HiveSQLException.toTStatus(e))
    }
    resp
  }

  @throws[TException]
  override def GetTypeInfo(req: TGetTypeInfoReq): TGetTypeInfoResp = {
    val resp = new TGetTypeInfoResp
    try {
      val operationHandle = cliService.getTypeInfo(createSessionHandle(req.getSessionHandle))
      resp.setOperationHandle(operationHandle.toTOperationHandle)
      resp.setStatus(ThriftCLIService.OK_STATUS)
    } catch {
      case e: Exception =>
        warn("Error getting type info: ", e)
        resp.setStatus(HiveSQLException.toTStatus(e))
    }
    resp
  }

  @throws[TException]
  override def GetCatalogs(req: TGetCatalogsReq): TGetCatalogsResp = {
    val resp = new TGetCatalogsResp
    try {
      val opHandle = cliService.getCatalogs(createSessionHandle(req.getSessionHandle))
      resp.setOperationHandle(opHandle.toTOperationHandle)
      resp.setStatus(ThriftCLIService.OK_STATUS)
    } catch {
      case e: Exception =>
        warn("Error getting catalogs: ", e)
        resp.setStatus(HiveSQLException.toTStatus(e))
    }
    resp
  }

  @throws[TException]
  override def GetSchemas(req: TGetSchemasReq): TGetSchemasResp = {
    val resp = new TGetSchemasResp
    try {
      val opHandle = cliService.getSchemas(createSessionHandle(req.getSessionHandle),
        req.getCatalogName, req.getSchemaName)
      resp.setOperationHandle(opHandle.toTOperationHandle)
      resp.setStatus(ThriftCLIService.OK_STATUS)
    } catch {
      case e: Exception =>
        warn("Error getting schemas: ", e)
        resp.setStatus(HiveSQLException.toTStatus(e))
    }
    resp
  }

  @throws[TException]
  override def GetTables(req: TGetTablesReq): TGetTablesResp = {
    val resp = new TGetTablesResp
    try {
      val opHandle = cliService.getTables(
        createSessionHandle(req.getSessionHandle),
        req.getCatalogName,
        req.getSchemaName,
        req.getTableName,
        req.getTableTypes)
      resp.setOperationHandle(opHandle.toTOperationHandle)
      resp.setStatus(ThriftCLIService.OK_STATUS)
    } catch {
      case e: Exception =>
        warn("Error getting tables: ", e)
        resp.setStatus(HiveSQLException.toTStatus(e))
    }
    resp
  }

  @throws[TException]
  override def GetTableTypes(req: TGetTableTypesReq): TGetTableTypesResp = {
    val resp = new TGetTableTypesResp
    try {
      val opHandle = cliService.getTableTypes(createSessionHandle(req.getSessionHandle))
      resp.setOperationHandle(opHandle.toTOperationHandle)
      resp.setStatus(ThriftCLIService.OK_STATUS)
    } catch {
      case e: Exception =>
        warn("Error getting table types: ", e)
        resp.setStatus(HiveSQLException.toTStatus(e))
    }
    resp
  }

  @throws[TException]
  override def GetColumns(req: TGetColumnsReq): TGetColumnsResp = {
    val resp = new TGetColumnsResp
    try {
      val opHandle = cliService.getColumns(
        createSessionHandle(req.getSessionHandle),
        req.getCatalogName,
        req.getSchemaName,
        req.getTableName,
        req.getColumnName)
      resp.setOperationHandle(opHandle.toTOperationHandle)
      resp.setStatus(ThriftCLIService.OK_STATUS)
    } catch {
      case e: Exception =>
        warn("Error getting columns: ", e)
        resp.setStatus(HiveSQLException.toTStatus(e))
    }
    resp
  }

  @throws[TException]
  override def GetFunctions(req: TGetFunctionsReq): TGetFunctionsResp = {
    val resp = new TGetFunctionsResp
    try {
      val opHandle = cliService.getFunctions(
        createSessionHandle(req.getSessionHandle),
        req.getCatalogName,
        req.getSchemaName,
        req.getFunctionName)
      resp.setOperationHandle(opHandle.toTOperationHandle)
      resp.setStatus(ThriftCLIService.OK_STATUS)
    } catch {
      case e: Exception =>
        warn("Error getting functions: ", e)
        resp.setStatus(HiveSQLException.toTStatus(e))
    }
    resp
  }

  @throws[TException]
  override def GetOperationStatus(req: TGetOperationStatusReq): TGetOperationStatusResp = {
    val resp = new TGetOperationStatusResp
    val operationHandle = new OperationHandle(req.getOperationHandle)
    try {
      val operationStatus = cliService.getOperationStatus(operationHandle, req.isGetProgressUpdate)
      resp.setOperationState(operationStatus.state.toTOperationState)
      resp.setErrorMessage(operationStatus.state.getErrorMessage)
      val opException = operationStatus.operationException
      resp.setOperationStarted(operationStatus.operationStarted)
      resp.setOperationCompleted(operationStatus.operationCompleted)
      resp.setHasResultSet(operationStatus.hasResultSet)
      val executionStatus = TJobExecutionStatus.NOT_AVAILABLE
      resp.setProgressUpdateResponse(new TProgressUpdateResp(
        Collections.emptyList[String],
        Collections.emptyList[util.List[String]],
        0.0D,
        executionStatus,
        "",
        0L))
      if (opException != null) {
        resp.setSqlState(opException.getSQLState)
        resp.setErrorCode(opException.getErrorCode)
        if (opException.getErrorCode == 29999) {
          resp.setErrorMessage(StringUtils.stringifyException(opException))
        } else {
          resp.setErrorMessage(opException.getMessage)
        }
      } else if (OperationType.EXECUTE_STATEMENT == operationHandle.getOperationType) {
        resp.getProgressUpdateResponse.setProgressedPercentage(
          getProgressedPercentage(operationHandle))
      }
      resp.setStatus(ThriftCLIService.OK_STATUS)
    } catch {
      case e: Exception =>
        warn("Error getting operation status: ", e)
        resp.setStatus(HiveSQLException.toTStatus(e))
    }
    resp
  }

  @throws[TException]
  override def CancelOperation(req: TCancelOperationReq): TCancelOperationResp = {
    val resp = new TCancelOperationResp
    try {
      cliService.cancelOperation(new OperationHandle(req.getOperationHandle))
      resp.setStatus(ThriftCLIService.OK_STATUS)
    } catch {
      case e: Exception =>
        warn("Error cancelling operation: ", e)
        resp.setStatus(HiveSQLException.toTStatus(e))
    }
    resp
  }

  @throws[TException]
  override def CloseOperation(req: TCloseOperationReq): TCloseOperationResp = {
    val resp = new TCloseOperationResp
    try {
      cliService.closeOperation(new OperationHandle(req.getOperationHandle))
      resp.setStatus(ThriftCLIService.OK_STATUS)
    } catch {
      case e: Exception =>
        warn("Error closing operation: ", e)
        resp.setStatus(HiveSQLException.toTStatus(e))
    }
    resp
  }

  @throws[TException]
  override def GetResultSetMetadata(req: TGetResultSetMetadataReq): TGetResultSetMetadataResp = {
    val resp = new TGetResultSetMetadataResp
    try {
      val schema = cliService.getResultSetMetadata(new OperationHandle(req.getOperationHandle))
      resp.setSchema(schema.toTTableSchema)
      resp.setStatus(ThriftCLIService.OK_STATUS)
    } catch {
      case e: Exception =>
        warn("Error getting result set metadata: ", e)
        resp.setStatus(HiveSQLException.toTStatus(e))
    }
    resp
  }

  @throws[TException]
  override def FetchResults(req: TFetchResultsReq): TFetchResultsResp = {
    val resp = new TFetchResultsResp
    try {
      // Set fetch size
      val maxFetchSize = livyConf.getInt(LivyConf.THRIFT_RESULTSET_MAX_FETCH_SIZE)
      if (req.getMaxRows > maxFetchSize) {
        req.setMaxRows(maxFetchSize)
      }
      val rowSet = cliService.fetchResults(
        new OperationHandle(req.getOperationHandle),
        FetchOrientation.getFetchOrientation(req.getOrientation),
        req.getMaxRows,
        FetchType.getFetchType(req.getFetchType))
      resp.setResults(rowSet.toTRowSet)
      resp.setHasMoreRows(false)
      resp.setStatus(ThriftCLIService.OK_STATUS)
    } catch {
      case e: Exception =>
        warn("Error fetching results: ", e)
        resp.setStatus(HiveSQLException.toTStatus(e))
    }
    resp
  }

  @throws[TException]
  override def GetPrimaryKeys(req: TGetPrimaryKeysReq): TGetPrimaryKeysResp = {
    val resp = new TGetPrimaryKeysResp
    try {
      val opHandle = cliService.getPrimaryKeys(
        new SessionHandle(req.getSessionHandle),
        req.getCatalogName,
        req.getSchemaName,
        req.getTableName)
      resp.setOperationHandle(opHandle.toTOperationHandle)
      resp.setStatus(ThriftCLIService.OK_STATUS)
    } catch {
      case e: Exception =>
        warn("Error getting functions: ", e)
        resp.setStatus(HiveSQLException.toTStatus(e))
    }
    resp
  }

  @throws[TException]
  override def GetCrossReference(req: TGetCrossReferenceReq): TGetCrossReferenceResp = {
    val resp = new TGetCrossReferenceResp
    try {
      val opHandle = cliService.getCrossReference(
        new SessionHandle(req.getSessionHandle),
        req.getParentCatalogName,
        req.getParentSchemaName,
        req.getParentTableName,
        req.getForeignCatalogName,
        req.getForeignSchemaName,
        req.getForeignTableName)
      resp.setOperationHandle(opHandle.toTOperationHandle)
      resp.setStatus(ThriftCLIService.OK_STATUS)
    } catch {
      case e: Exception =>
        warn("Error getting functions: ", e)
        resp.setStatus(HiveSQLException.toTStatus(e))
    }
    resp
  }

  @throws[TException]
  override def GetQueryId(req: TGetQueryIdReq): TGetQueryIdResp = {
    try {
      new TGetQueryIdResp(cliService.getQueryId(req.getOperationHandle))
    } catch {
      case e: HiveSQLException => throw new TException(e)
    }
  }

  override def run(): Unit

  /**
   * If the proxy user name is provided then check privileges to substitute the user.
   */
  @throws[HiveSQLException]
  private def getProxyUser(
      realUser: String,
      sessionConf: util.Map[String, String],
      ipAddress: String): String = {
    var proxyUser: String = null
    // We set the thread local proxy username, in ThriftHttpServlet.
    if (livyConf.get(LivyConf.THRIFT_TRANSPORT_MODE).equalsIgnoreCase("http")) {
      proxyUser = SessionInfo.getProxyUserName
      debug("Proxy user from query string: " + proxyUser)
    }
    if (proxyUser == null && sessionConf != null &&
        sessionConf.containsKey(HiveAuthConstants.HS2_PROXY_USER)) {
      val proxyUserFromThriftBody = sessionConf.get(HiveAuthConstants.HS2_PROXY_USER)
      debug("Proxy user from thrift body: " + proxyUserFromThriftBody)
      proxyUser = proxyUserFromThriftBody
    }
    if (proxyUser == null) return realUser
    // check whether substitution is allowed
    if (!livyConf.getBoolean(LivyConf.THRIFT_ALLOW_USER_SUBSTITUTION)) {
      throw new HiveSQLException("Proxy user substitution is not allowed")
    }
    // If there's no authentication, then directly substitute the user
    if (AuthTypes.NONE.toString.equalsIgnoreCase(livyConf.get(LivyConf.THRIFT_AUTHENTICATION))) {
      return proxyUser
    }
    // Verify proxy user privilege of the realUser for the proxyUser
    verifyProxyAccess(realUser, proxyUser, ipAddress)
    debug("Verified proxy user: " + proxyUser)
    proxyUser
  }

  @throws[HiveSQLException]
  private def verifyProxyAccess(realUser: String, proxyUser: String, ipAddress: String): Unit = {
    try {
      val sessionUgi = if (UserGroupInformation.isSecurityEnabled) {
          UserGroupInformation.createProxyUser(
            new KerberosName(realUser).getServiceName, UserGroupInformation.getLoginUser)
        } else {
          UserGroupInformation.createRemoteUser(realUser)
        }
      if (!proxyUser.equalsIgnoreCase(realUser)) {
        ProxyUsers.refreshSuperUserGroupsConfiguration()
        ProxyUsers.authorize(UserGroupInformation.createProxyUser(proxyUser, sessionUgi), ipAddress)
      }
    } catch {
      case e: IOException =>
        throw new HiveSQLException(
          s"Failed to validate proxy privilege of $realUser for $proxyUser", "08S01", e)
    }
  }

  private def createSessionHandle(tHandle: TSessionHandle): SessionHandle = {
    val protocolVersion = cliService.getSessionManager
      .getSessionInfo(new SessionHandle(tHandle))
      .protocolVersion
    new SessionHandle(tHandle, protocolVersion)
  }
}

object ThriftCLIService {
  private val OK_STATUS: TStatus = new TStatus(TStatusCode.SUCCESS_STATUS)
}

private[thriftserver] class ThriftCLIServerContext extends ServerContext {
  private var sessionHandle: SessionHandle = _

  def setSessionHandle(sessionHandle: SessionHandle): Unit = {
    this.sessionHandle = sessionHandle
  }

  def getSessionHandle: SessionHandle = sessionHandle
}
