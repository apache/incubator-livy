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

import java.io.IOException
import java.util
import java.util.concurrent.{CancellationException, ExecutionException, TimeoutException, TimeUnit}
import javax.security.auth.login.LoginException

import scala.collection.JavaConverters._

import org.apache.hadoop.hive.common.log.ProgressMonitor
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.ql.parse.ParseUtils
import org.apache.hadoop.hive.shims.Utils
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hive.service.{CompositeService, ServiceException}
import org.apache.hive.service.auth.HiveAuthFactory
import org.apache.hive.service.cli._
import org.apache.hive.service.cli.operation.Operation
import org.apache.hive.service.rpc.thrift.{TOperationHandle, TProtocolVersion}

import org.apache.livy.{LIVY_VERSION, Logging}

class LivyCLIService(server: LivyThriftServer)
  extends CompositeService(classOf[LivyCLIService].getName) with ICLIService with Logging {
  import LivyCLIService._

  private var sessionManager: LivyThriftSessionManager = _
  private var defaultFetchRows: Int = _
  private var serviceUGI: UserGroupInformation = _
  private var httpUGI: UserGroupInformation = _

  override def init(hiveConf: HiveConf): Unit = {
    sessionManager = new LivyThriftSessionManager(server, hiveConf)
    addService(sessionManager)
    defaultFetchRows =
      hiveConf.getIntVar(ConfVars.HIVE_SERVER2_THRIFT_RESULTSET_DEFAULT_FETCH_SIZE)
    //  If the hadoop cluster is secure, do a kerberos login for the service from the keytab
    if (UserGroupInformation.isSecurityEnabled) {
      try {
        serviceUGI = Utils.getUGI
      } catch {
        case e: IOException =>
          throw new ServiceException("Unable to login to kerberos with given principal/keytab", e)
        case e: LoginException =>
          throw new ServiceException("Unable to login to kerberos with given principal/keytab", e)
      }
      // Also try creating a UGI object for the SPNego principal
      val principal = hiveConf.getVar(ConfVars.HIVE_SERVER2_SPNEGO_PRINCIPAL)
      val keyTabFile = hiveConf.getVar(ConfVars.HIVE_SERVER2_SPNEGO_KEYTAB)
      if (principal.isEmpty || keyTabFile.isEmpty) {
        info(s"SPNego httpUGI not created, spNegoPrincipal: $principal, ketabFile: $keyTabFile")
      } else try {
        httpUGI = HiveAuthFactory.loginFromSpnegoKeytabAndReturnUGI(hiveConf)
        info("SPNego httpUGI successfully created.")
      } catch {
        case e: IOException =>
          warn("SPNego httpUGI creation failed: ", e)
      }
    }
    super.init(hiveConf)
  }

  def getServiceUGI: UserGroupInformation = this.serviceUGI

  def getHttpUGI: UserGroupInformation = this.httpUGI

  def getSessionManager: LivyThriftSessionManager = sessionManager

  @throws[HiveSQLException]
  override def getInfo(sessionHandle: SessionHandle, getInfoType: GetInfoType): GetInfoValue = {
    getInfoType match {
      case GetInfoType.CLI_SERVER_NAME => new GetInfoValue("Livy JDBC")
      case GetInfoType.CLI_DBMS_NAME => new GetInfoValue("Livy JDBC")
      case GetInfoType.CLI_DBMS_VER => new GetInfoValue(LIVY_VERSION)
      // below values are copied from Hive
      case GetInfoType.CLI_MAX_COLUMN_NAME_LEN => new GetInfoValue(128)
      case GetInfoType.CLI_MAX_SCHEMA_NAME_LEN => new GetInfoValue(128)
      case GetInfoType.CLI_MAX_TABLE_NAME_LEN => new GetInfoValue(128)
      case GetInfoType.CLI_ODBC_KEYWORDS =>
        new GetInfoValue(ParseUtils.getKeywords(LivyCLIService.ODBC_KEYWORDS))
      case _ => throw new HiveSQLException(s"Unrecognized GetInfoType value: $getInfoType")
    }
  }

  @throws[HiveSQLException]
  def openSession(
      protocol: TProtocolVersion,
      username: String,
      password: String,
      ipAddress: String,
      configuration: util.Map[String, String]): SessionHandle = {
    val sessionHandle = sessionManager.openSession(
      protocol, username, password, ipAddress, configuration, false, null)
    debug(sessionHandle + ": openSession()")
    sessionHandle
  }

  @throws[HiveSQLException]
  def openSessionWithImpersonation(
      protocol: TProtocolVersion,
      username: String,
      password: String,
      ipAddress: String,
      configuration: util.Map[String, String],
      delegationToken: String): SessionHandle = {
    val sessionHandle = sessionManager.openSession(
      protocol, username, password, ipAddress, configuration, true, delegationToken)
    debug(sessionHandle + ": openSession()")
    sessionHandle
  }

  @throws[HiveSQLException]
  override def openSession(
      username: String,
      password: String,
      configuration: util.Map[String, String]): SessionHandle = {
    val sessionHandle = sessionManager.openSession(
      SERVER_VERSION, username, password, null, configuration, false, null)
    debug(sessionHandle + ": openSession()")
    sessionHandle
  }

  @throws[HiveSQLException]
  override def openSessionWithImpersonation(
      username: String,
      password: String,
      configuration: util.Map[String, String], delegationToken: String): SessionHandle = {
    val sessionHandle = sessionManager.openSession(
      SERVER_VERSION, username, password, null, configuration, true, delegationToken)
    debug(sessionHandle + ": openSession()")
    sessionHandle
  }

  @throws[HiveSQLException]
  override def closeSession(sessionHandle: SessionHandle): Unit = {
    sessionManager.closeSession(sessionHandle)
    debug(sessionHandle + ": closeSession()")
  }

  @throws[HiveSQLException]
  override def executeStatement(
      sessionHandle: SessionHandle,
      statement: String,
      confOverlay: util.Map[String, String]): OperationHandle = {
    executeStatement(sessionHandle, statement, confOverlay, 0)
  }

  /**
   * Execute statement on the server with a timeout. This is a blocking call.
   */
  @throws[HiveSQLException]
  override def executeStatement(
      sessionHandle: SessionHandle,
      statement: String,
      confOverlay: util.Map[String, String],
      queryTimeout: Long): OperationHandle = {
    val opHandle: OperationHandle = sessionManager.operationManager.executeStatement(
      sessionHandle, statement, confOverlay, runAsync = false, queryTimeout)
    debug(sessionHandle + ": executeStatement()")
    opHandle
  }

  @throws[HiveSQLException]
  override def executeStatementAsync(
      sessionHandle: SessionHandle,
      statement: String,
      confOverlay: util.Map[String, String]): OperationHandle = {
    executeStatementAsync(sessionHandle, statement, confOverlay, 0)
  }

  /**
   * Execute statement asynchronously on the server with a timeout. This is a non-blocking call
   */
  @throws[HiveSQLException]
  override def executeStatementAsync(
      sessionHandle: SessionHandle,
      statement: String,
      confOverlay: util.Map[String, String],
      queryTimeout: Long): OperationHandle = {
    val opHandle = sessionManager.operationManager.executeStatement(
      sessionHandle, statement, confOverlay, runAsync = true, queryTimeout)
    debug(sessionHandle + ": executeStatementAsync()")
    opHandle
  }

  @throws[HiveSQLException]
  override def getTypeInfo(sessionHandle: SessionHandle): OperationHandle = {
    debug(sessionHandle + ": getTypeInfo()")
    sessionManager.operationManager.getTypeInfo(sessionHandle)
  }

  @throws[HiveSQLException]
  override def getCatalogs(sessionHandle: SessionHandle): OperationHandle = {
    debug(sessionHandle + ": getCatalogs()")
    sessionManager.operationManager.getCatalogs(sessionHandle)
  }

  @throws[HiveSQLException]
  override def getSchemas(
      sessionHandle: SessionHandle,
      catalogName: String,
      schemaName: String): OperationHandle = {
    // TODO
    throw new HiveSQLException("Operation GET_SCHEMAS is not yet supported")
  }

  @throws[HiveSQLException]
  override def getTables(
      sessionHandle: SessionHandle,
      catalogName: String,
      schemaName: String,
      tableName: String,
      tableTypes: util.List[String]): OperationHandle = {
    // TODO
    throw new HiveSQLException("Operation GET_TABLES is not yet supported")
  }

  @throws[HiveSQLException]
  override def getTableTypes(sessionHandle: SessionHandle): OperationHandle = {
    debug(sessionHandle + ": getTableTypes()")
    sessionManager.operationManager.getTableTypes(sessionHandle)
  }

  @throws[HiveSQLException]
  override def getColumns(
      sessionHandle: SessionHandle,
      catalogName: String,
      schemaName: String,
      tableName: String,
      columnName: String): OperationHandle = {
    // TODO
    throw new HiveSQLException("Operation GET_COLUMNS is not yet supported")
  }

  @throws[HiveSQLException]
  override def getFunctions(
      sessionHandle: SessionHandle,
      catalogName: String,
      schemaName: String,
      functionName: String): OperationHandle = {
    // TODO
    throw new HiveSQLException("Operation GET_FUNCTIONS is not yet supported")
  }

  @throws[HiveSQLException]
  override def getPrimaryKeys(
      sessionHandle: SessionHandle,
      catalog: String,
      schema: String,
      table: String): OperationHandle = {
    // TODO
    throw new HiveSQLException("Operation GET_PRIMARY_KEYS is not yet supported")
  }

  @throws[HiveSQLException]
  override def getCrossReference(
      sessionHandle: SessionHandle,
      primaryCatalog: String,
      primarySchema: String,
      primaryTable: String,
      foreignCatalog: String,
      foreignSchema: String,
      foreignTable: String): OperationHandle = {
    // TODO
    throw new HiveSQLException("Operation GET_CROSS_REFERENCE is not yet supported")
  }

  @throws[HiveSQLException]
  override def getOperationStatus(
      opHandle: OperationHandle,
      getProgressUpdate: Boolean): OperationStatus = {
    val operation: Operation = sessionManager.operationManager.getOperation(opHandle)
    /**
     * If this is a background operation run asynchronously,
     * we block for a duration determined by a step function, before we return
     * However, if the background operation is complete, we return immediately.
     */
    if (operation.shouldRunAsync) {
      val maxTimeout: Long = HiveConf.getTimeVar(
        getHiveConf,
        HiveConf.ConfVars.HIVE_SERVER2_LONG_POLLING_TIMEOUT,
        TimeUnit.MILLISECONDS)
      val elapsed: Long = System.currentTimeMillis - operation.getBeginTime
      // A step function to increase the polling timeout by 500 ms every 10 sec,
      // starting from 500 ms up to HIVE_SERVER2_LONG_POLLING_TIMEOUT
      val timeout: Long = Math.min(maxTimeout, (elapsed / TimeUnit.SECONDS.toMillis(10) + 1) * 500)
      try {
        operation.getBackgroundHandle.get(timeout, TimeUnit.MILLISECONDS)
      } catch {
        case e: TimeoutException =>
          // No Op, return to the caller since long polling timeout has expired
          trace(opHandle + ": Long polling timed out")
        case e: CancellationException =>
          // The background operation thread was cancelled
          trace(opHandle + ": The background operation was cancelled", e)
        case e: ExecutionException =>
          // Note: Hive ops do not use the normal Future failure path, so this will not happen
          //       in case of actual failure; the Future will just be done.
          // The background operation thread was aborted
          warn(opHandle + ": The background operation was aborted", e)
        case _: InterruptedException =>
        // No op, this thread was interrupted
        // In this case, the call might return sooner than long polling timeout
      }
    }
    val opStatus: OperationStatus = operation.getStatus
    debug(opHandle + ": getOperationStatus()")
    opStatus.setJobProgressUpdate(new JobProgressUpdate(ProgressMonitor.NULL))
    opStatus
  }

  @throws[HiveSQLException]
  override def cancelOperation(opHandle: OperationHandle): Unit = {
    sessionManager.operationManager.cancelOperation(opHandle)
    debug(opHandle + ": cancelOperation()")
  }

  @throws[HiveSQLException]
  override def closeOperation(opHandle: OperationHandle): Unit = {
    sessionManager.operationManager.closeOperation(opHandle)
    debug(opHandle + ": closeOperation")
  }

  @throws[HiveSQLException]
  override def getResultSetMetadata(opHandle: OperationHandle): TableSchema = {
    debug(opHandle + ": getResultSetMetadata()")
    sessionManager.operationManager.getOperation(opHandle).getResultSetSchema
  }

  @throws[HiveSQLException]
  override def fetchResults(opHandle: OperationHandle): RowSet = {
    fetchResults(
      opHandle, Operation.DEFAULT_FETCH_ORIENTATION, defaultFetchRows, FetchType.QUERY_OUTPUT)
  }

  @throws[HiveSQLException]
  override def fetchResults(
      opHandle: OperationHandle,
      orientation: FetchOrientation,
      maxRows: Long,
      fetchType: FetchType): RowSet = {
    debug(opHandle + ": fetchResults()")
    sessionManager.operationManager.fetchResults(opHandle, orientation, maxRows, fetchType)
  }

  @throws[HiveSQLException]
  override def getDelegationToken(
      sessionHandle: SessionHandle,
      authFactory: HiveAuthFactory,
      owner: String,
      renewer: String): String = {
    throw new HiveSQLException("Operation not yet supported.")
  }

  @throws[HiveSQLException]
  override def setApplicationName(sh: SessionHandle, value: String): Unit = {
    throw new HiveSQLException("Operation not yet supported.")
  }

  override def cancelDelegationToken(
      sessionHandle: SessionHandle,
      authFactory: HiveAuthFactory,
      tokenStr: String): Unit = {
    throw new HiveSQLException("Operation not yet supported.")
  }

  override def renewDelegationToken(
      sessionHandle: SessionHandle,
      authFactory: HiveAuthFactory,
      tokenStr: String): Unit = {
    throw new HiveSQLException("Operation not yet supported.")
  }

  @throws[HiveSQLException]
  override def getQueryId(opHandle: TOperationHandle): String = {
    throw new HiveSQLException("Operation not yet supported.")
  }
}


object LivyCLIService {
  val SERVER_VERSION: TProtocolVersion = TProtocolVersion.values().last

  // scalastyle:off line.size.limit
  // From https://docs.microsoft.com/en-us/sql/t-sql/language-elements/reserved-keywords-transact-sql#odbc-reserved-keywords
  // scalastyle:on line.size.limit
  private val ODBC_KEYWORDS = Set("ABSOLUTE", "ACTION", "ADA", "ADD", "ALL", "ALLOCATE", "ALTER",
    "AND", "ANY", "ARE", "AS", "ASC", "ASSERTION", "AT", "AUTHORIZATION", "AVG", "BEGIN",
    "BETWEEN", "BIT_LENGTH", "BIT", "BOTH", "BY", "CASCADE", "CASCADED", "CASE", "CAST", "CATALOG",
    "CHAR_LENGTH", "CHAR", "CHARACTER_LENGTH", "CHARACTER", "CHECK", "CLOSE", "COALESCE",
    "COLLATE", "COLLATION", "COLUMN", "COMMIT", "CONNECT", "CONNECTION", "CONSTRAINT",
    "CONSTRAINTS", "CONTINUE", "CONVERT", "CORRESPONDING", "COUNT", "CREATE", "CROSS",
    "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "CURRENT", "CURSOR",
    "DATE", "DAY", "DEALLOCATE", "DEC", "DECIMAL", "DECLARE", "DEFAULT", "DEFERRABLE", "DEFERRED",
    "DELETE", "DESC", "DESCRIBE", "DESCRIPTOR", "DIAGNOSTICS", "DISCONNECT", "DISTINCT", "DOMAIN",
    "DOUBLE", "DROP", "ELSE", "END", "ESCAPE", "EXCEPT", "EXCEPTION", "EXEC", "EXECUTE", "EXISTS",
    "EXTERNAL", "EXTRACT", "FALSE", "FETCH", "FIRST", "FLOAT", "FOR", "FOREIGN", "FORTRAN",
    "FOUND", "FROM", "FULL", "GET", "GLOBAL", "GO", "GOTO", "GRANT", "GROUP", "HAVING", "HOUR",
    "IDENTITY", "IMMEDIATE", "IN", "INCLUDE", "INDEX", "INDICATOR", "INITIALLY", "INNER", "INPUT",
    "INSENSITIVE", "INSERT", "INT", "INTEGER", "INTERSECT", "INTERVAL", "INTO", "IS", "ISOLATION",
    "JOIN", "KEY", "LANGUAGE", "LAST", "LEADING", "LEFT", "LEVEL", "LIKE", "LOCAL", "LOWER",
    "MATCH", "MAX", "MIN", "MINUTE", "MODULE", "MONTH", "NAMES", "NATIONAL", "NATURAL", "NCHAR",
    "NEXT", "NO", "NONE", "NOT", "NULL", "NULLIF", "NUMERIC", "OCTET_LENGTH", "OF", "ON", "ONLY",
    "OPEN", "OPTION", "OR", "ORDER", "OUTER", "OUTPUT", "OVERLAPS", "PAD", "PARTIAL", "PASCAL",
    "POSITION", "PRECISION", "PREPARE", "PRESERVE", "PRIMARY", "PRIOR", "PRIVILEGES", "PROCEDURE",
    "PUBLIC", "READ", "REAL", "REFERENCES", "RELATIVE", "RESTRICT", "REVOKE", "RIGHT", "ROLLBACK",
    "ROWS", "SCHEMA", "SCROLL", "SECOND", "SECTION", "SELECT", "SESSION_USER", "SESSION", "SET",
    "SIZE", "SMALLINT", "SOME", "SPACE", "SQL", "SQLCA", "SQLCODE", "SQLERROR", "SQLSTATE",
    "SQLWARNING", "SUBSTRING", "SUM", "SYSTEM_USER", "TABLE", "TEMPORARY", "THEN", "TIME",
    "TIMESTAMP", "TIMEZONE_HOUR", "TIMEZONE_MINUTE", "TO", "TRAILING", "TRANSACTION", "TRANSLATE",
    "TRANSLATION", "TRIM", "TRUE", "UNION", "UNIQUE", "UNKNOWN", "UPDATE", "UPPER", "USAGE",
    "USER", "USING", "VALUE", "VALUES", "VARCHAR", "VARYING", "VIEW", "WHEN", "WHENEVER", "WHERE",
    "WITH", "WORK", "WRITE", "YEAR", "ZONE").asJava
}
