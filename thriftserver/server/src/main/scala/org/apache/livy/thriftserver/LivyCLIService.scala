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

import org.apache.hadoop.security.{SecurityUtil, UserGroupInformation}
import org.apache.hive.service.ServiceException
import org.apache.hive.service.cli._
import org.apache.hive.service.rpc.thrift.{TOperationHandle, TProtocolVersion}

import org.apache.livy.{LIVY_VERSION, LivyConf, Logging}
import org.apache.livy.thriftserver.auth.AuthFactory
import org.apache.livy.thriftserver.operation.{Operation, OperationStatus}
import org.apache.livy.thriftserver.serde.ThriftResultSet
import org.apache.livy.thriftserver.types.Schema

class LivyCLIService(server: LivyThriftServer)
  extends ThriftService(classOf[LivyCLIService].getName) with Logging {
  import LivyCLIService._

  private var sessionManager: LivyThriftSessionManager = _
  private var defaultFetchRows: Int = _
  private var serviceUGI: UserGroupInformation = _
  private var httpUGI: UserGroupInformation = _
  private var maxTimeout: Long = _

  override def init(livyConf: LivyConf): Unit = {
    sessionManager = new LivyThriftSessionManager(server, livyConf)
    addService(sessionManager)
    defaultFetchRows = livyConf.getInt(LivyConf.THRIFT_RESULTSET_DEFAULT_FETCH_SIZE)
    maxTimeout = livyConf.getTimeAsMs(LivyConf.THRIFT_LONG_POLLING_TIMEOUT)
    //  If the hadoop cluster is secure, do a kerberos login for the service from the keytab
    if (UserGroupInformation.isSecurityEnabled) {
      try {
        serviceUGI = UserGroupInformation.getCurrentUser
      } catch {
        case e: IOException =>
          throw new ServiceException("Unable to login to kerberos with given principal/keytab", e)
        case e: LoginException =>
          throw new ServiceException("Unable to login to kerberos with given principal/keytab", e)
      }
      // Also try creating a UGI object for the SPNego principal
      val principal = livyConf.get(LivyConf.AUTH_KERBEROS_PRINCIPAL)
      val keyTabFile = livyConf.get(LivyConf.AUTH_KERBEROS_KEYTAB)
      if (principal.isEmpty || keyTabFile.isEmpty) {
        info(s"SPNego httpUGI not created, SPNegoPrincipal: $principal, ketabFile: $keyTabFile")
      } else try {
        httpUGI = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
          SecurityUtil.getServerPrincipal(principal, "0.0.0.0"), keyTabFile)
        info("SPNego httpUGI successfully created.")
      } catch {
        case e: IOException =>
          warn("SPNego httpUGI creation failed: ", e)
      }
    }
    super.init(livyConf)
  }

  def getServiceUGI: UserGroupInformation = this.serviceUGI

  def getHttpUGI: UserGroupInformation = this.httpUGI

  def getSessionManager: LivyThriftSessionManager = sessionManager

  @throws[HiveSQLException]
  def getInfo(sessionHandle: SessionHandle, getInfoType: GetInfoType): GetInfoValue = {
    getInfoType match {
      case GetInfoType.CLI_SERVER_NAME => new GetInfoValue("Livy JDBC")
      case GetInfoType.CLI_DBMS_NAME => new GetInfoValue("Livy JDBC")
      case GetInfoType.CLI_DBMS_VER => new GetInfoValue(LIVY_VERSION)
      // below values are copied from Hive
      case GetInfoType.CLI_MAX_COLUMN_NAME_LEN => new GetInfoValue(128)
      case GetInfoType.CLI_MAX_SCHEMA_NAME_LEN => new GetInfoValue(128)
      case GetInfoType.CLI_MAX_TABLE_NAME_LEN => new GetInfoValue(128)
      case GetInfoType.CLI_ODBC_KEYWORDS =>
        new GetInfoValue(getKeywords)
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
  def openSession(
      username: String,
      password: String,
      configuration: util.Map[String, String]): SessionHandle = {
    val sessionHandle = sessionManager.openSession(
      SERVER_VERSION, username, password, null, configuration, false, null)
    debug(sessionHandle + ": openSession()")
    sessionHandle
  }

  @throws[HiveSQLException]
  def openSessionWithImpersonation(
      username: String,
      password: String,
      configuration: util.Map[String, String], delegationToken: String): SessionHandle = {
    val sessionHandle = sessionManager.openSession(
      SERVER_VERSION, username, password, null, configuration, true, delegationToken)
    debug(sessionHandle + ": openSession()")
    sessionHandle
  }

  @throws[HiveSQLException]
  def closeSession(sessionHandle: SessionHandle): Unit = {
    sessionManager.closeSession(sessionHandle)
    debug(sessionHandle + ": closeSession()")
  }

  @throws[HiveSQLException]
  def executeStatement(
      sessionHandle: SessionHandle,
      statement: String,
      confOverlay: util.Map[String, String]): OperationHandle = {
    executeStatement(sessionHandle, statement, confOverlay, 0)
  }

  /**
   * Execute statement on the server with a timeout. This is a blocking call.
   */
  @throws[HiveSQLException]
  def executeStatement(
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
  def executeStatementAsync(
      sessionHandle: SessionHandle,
      statement: String,
      confOverlay: util.Map[String, String]): OperationHandle = {
    executeStatementAsync(sessionHandle, statement, confOverlay, 0)
  }

  /**
   * Execute statement asynchronously on the server with a timeout. This is a non-blocking call
   */
  @throws[HiveSQLException]
  def executeStatementAsync(
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
  def getTypeInfo(sessionHandle: SessionHandle): OperationHandle = {
    debug(sessionHandle + ": getTypeInfo()")
    sessionManager.operationManager.getTypeInfo(sessionHandle)
  }

  @throws[HiveSQLException]
  def getCatalogs(sessionHandle: SessionHandle): OperationHandle = {
    debug(sessionHandle + ": getCatalogs()")
    sessionManager.operationManager.getCatalogs(sessionHandle)
  }

  @throws[HiveSQLException]
  def getSchemas(
      sessionHandle: SessionHandle,
      catalogName: String,
      schemaName: String): OperationHandle = {
    sessionManager.operationManager.getSchemas(
      sessionHandle, catalogName, schemaName)
  }

  @throws[HiveSQLException]
  def getTables(
      sessionHandle: SessionHandle,
      catalogName: String,
      schemaName: String,
      tableName: String,
      tableTypes: util.List[String]): OperationHandle = {
    sessionManager.operationManager.getTables(
      sessionHandle, catalogName, schemaName, tableName, tableTypes)
  }

  @throws[HiveSQLException]
  def getTableTypes(sessionHandle: SessionHandle): OperationHandle = {
    debug(sessionHandle + ": getTableTypes()")
    sessionManager.operationManager.getTableTypes(sessionHandle)
  }

  @throws[HiveSQLException]
  def getColumns(
      sessionHandle: SessionHandle,
      catalogName: String,
      schemaName: String,
      tableName: String,
      columnName: String): OperationHandle = {
    sessionManager.operationManager.getColumns(
      sessionHandle, catalogName, schemaName, tableName, columnName)
  }

  @throws[HiveSQLException]
  def getFunctions(
      sessionHandle: SessionHandle,
      catalogName: String,
      schemaName: String,
      functionName: String): OperationHandle = {
    sessionManager.operationManager.getFunctions(
      sessionHandle, catalogName, schemaName, functionName)
  }

  @throws[HiveSQLException]
  def getPrimaryKeys(
      sessionHandle: SessionHandle,
      catalog: String,
      schema: String,
      table: String): OperationHandle = {
    // TODO
    throw new HiveSQLException("Operation GET_PRIMARY_KEYS is not yet supported")
  }

  @throws[HiveSQLException]
  def getCrossReference(
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
  def getOperationStatus(
      opHandle: OperationHandle,
      getProgressUpdate: Boolean): OperationStatus = {
    val operation: Operation = sessionManager.operationManager.getOperation(opHandle)
    /**
     * If this is a background operation run asynchronously,
     * we block for a duration determined by a step function, before we return
     * However, if the background operation is complete, we return immediately.
     */
    if (operation.shouldRunAsync) {
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
    val opStatus = operation.getStatus
    debug(opHandle + ": getOperationStatus()")
    opStatus
  }

  @throws[HiveSQLException]
  def cancelOperation(opHandle: OperationHandle): Unit = {
    sessionManager.operationManager.cancelOperation(opHandle)
    debug(opHandle + ": cancelOperation()")
  }

  @throws[HiveSQLException]
  def closeOperation(opHandle: OperationHandle): Unit = {
    sessionManager.operationManager.closeOperation(opHandle)
    debug(opHandle + ": closeOperation")
  }

  @throws[HiveSQLException]
  def getResultSetMetadata(opHandle: OperationHandle): Schema = {
    debug(opHandle + ": getResultSetMetadata()")
    sessionManager.operationManager.getOperation(opHandle).getResultSetSchema
  }

  @throws[HiveSQLException]
  def fetchResults(opHandle: OperationHandle): ThriftResultSet = {
    fetchResults(
      opHandle, Operation.DEFAULT_FETCH_ORIENTATION, defaultFetchRows, FetchType.QUERY_OUTPUT)
  }

  @throws[HiveSQLException]
  def fetchResults(
      opHandle: OperationHandle,
      orientation: FetchOrientation,
      maxRows: Long,
      fetchType: FetchType): ThriftResultSet = {
    debug(opHandle + ": fetchResults()")
    sessionManager.operationManager.fetchResults(opHandle, orientation, maxRows, fetchType)
  }

  @throws[HiveSQLException]
  def getDelegationToken(
      sessionHandle: SessionHandle,
      authFactory: AuthFactory,
      owner: String,
      renewer: String): String = {
    throw new HiveSQLException("Operation not yet supported.")
  }

  @throws[HiveSQLException]
  def setApplicationName(sh: SessionHandle, value: String): Unit = {
    throw new HiveSQLException("Operation not yet supported.")
  }

  def cancelDelegationToken(
      sessionHandle: SessionHandle,
      authFactory: AuthFactory,
      tokenStr: String): Unit = {
    throw new HiveSQLException("Operation not yet supported.")
  }

  def renewDelegationToken(
      sessionHandle: SessionHandle,
      authFactory: AuthFactory,
      tokenStr: String): Unit = {
    throw new HiveSQLException("Operation not yet supported.")
  }

  @throws[HiveSQLException]
  def getQueryId(opHandle: TOperationHandle): String = {
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
    "WITH", "WORK", "WRITE", "YEAR", "ZONE")

  // scalastyle:off line.size.limit
  // https://github.com/apache/spark/blob/515708d5f33d5acdb4206c626192d1838f8e691f/sql/catalyst/src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBase.g4#L744-L1013
  // scalastyle:on line.size.limit
  private val SPARK_KEYWORDS = Seq("SELECT", "FROM", "ADD", "AS", "ALL", "ANY", "DISTINCT",
    "WHERE", "GROUP", "BY", "GROUPING", "SETS", "CUBE", "ROLLUP", "ORDER", "HAVING", "LIMIT",
    "AT", "OR", "AND", "IN", "NOT", "NO", "EXISTS", "BETWEEN", "LIKE", "RLIKE", "REGEXP", "IS",
    "NULL", "TRUE", "FALSE", "NULLS", "ASC", "DESC", "FOR", "INTERVAL", "CASE", "WHEN", "THEN",
    "ELSE", "END", "JOIN", "CROSS", "OUTER", "INNER", "LEFT", "SEMI", "RIGHT", "FULL", "NATURAL",
    "ON", "PIVOT", "LATERAL", "WINDOW", "OVER", "PARTITION", "RANGE", "ROWS", "UNBOUNDED",
    "PRECEDING", "FOLLOWING", "CURRENT", "FIRST", "AFTER", "LAST", "ROW", "WITH", "VALUES",
    "CREATE", "TABLE", "DIRECTORY", "VIEW", "REPLACE", "INSERT", "DELETE", "INTO", "DESCRIBE",
    "EXPLAIN", "FORMAT", "LOGICAL", "CODEGEN", "COST", "CAST", "SHOW", "TABLES", "COLUMNS",
    "COLUMN", "USE", "PARTITIONS", "FUNCTIONS", "DROP", "UNION", "EXCEPT", "MINUS", "INTERSECT",
    "TO", "TABLESAMPLE", "STRATIFY", "ALTER", "RENAME", "ARRAY", "MAP", "STRUCT", "COMMENT", "SET",
    "RESET", "DATA", "START", "TRANSACTION", "COMMIT", "ROLLBACK", "MACRO", "IGNORE", "BOTH",
    "LEADING", "TRAILING", "IF", "POSITION", "EXTRACT", "DIV", "PERCENT", "BUCKET", "OUT", "OF",
    "SORT", "CLUSTER", "DISTRIBUTE", "OVERWRITE", "TRANSFORM", "REDUCE", "USING", "SERDE",
    "SERDEPROPERTIES", "RECORDREADER", "RECORDWRITER", "DELIMITED", "FIELDS", "TERMINATED",
    "COLLECTION", "ITEMS", "KEYS", "ESCAPED", "LINES", "SEPARATED", "FUNCTION", "EXTENDED",
    "REFRESH", "CLEAR", "CACHE", "UNCACHE", "LAZY", "FORMATTED", "GLOBAL", "TEMPORARY", "TEMP",
    "OPTIONS", "UNSET", "TBLPROPERTIES", "DBPROPERTIES", "BUCKETS", "SKEWED", "STORED",
    "DIRECTORIES", "LOCATION", "EXCHANGE", "ARCHIVE", "UNARCHIVE", "FILEFORMAT", "TOUCH",
    "COMPACT", "CONCATENATE", "CHANGE", "CASCADE", "RESTRICT", "CLUSTERED", "SORTED", "PURGE",
    "INPUTFORMAT", "OUTPUTFORMAT", "DATABASE", "SCHEMA", "DATABASES", "SCHEMAS", "DFS", "TRUNCATE",
    "ANALYZE", "COMPUTE", "LIST", "STATISTICS", "PARTITIONED", "EXTERNAL", "DEFINED", "REVOKE",
    "GRANT", "LOCK", "UNLOCK", "MSCK", "REPAIR", "RECOVER", "EXPORT", "IMPORT", "LOAD", "ROLE",
    "ROLES", "COMPACTIONS", "PRINCIPALS", "TRANSACTIONS", "INDEX", "INDEXES", "LOCKS", "OPTION",
    "ANTI", "LOCAL", "INPATH")

  def getKeywords: String = SPARK_KEYWORDS.filter(ODBC_KEYWORDS.contains).mkString(",")
}
