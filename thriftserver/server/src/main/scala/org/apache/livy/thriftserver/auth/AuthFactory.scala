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

package org.apache.livy.thriftserver.auth

import java.io.IOException
import java.util
import javax.security.auth.callback._
import javax.security.auth.login.LoginException
import javax.security.sasl.{AuthorizeCallback, Sasl}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION
import org.apache.hadoop.security.SaslRpcServer.AuthMethod
import org.apache.hive.service.auth.{SaslQOP, TSetIpAddressProcessor}
import org.apache.hive.service.auth.AuthenticationProviderFactory.AuthMethods
import org.apache.hive.service.auth.HiveAuthConstants.AuthTypes
import org.apache.hive.service.cli.HiveSQLException
import org.apache.hive.service.rpc.thrift.TCLIService
import org.apache.hive.service.rpc.thrift.TCLIService.Iface
import org.apache.thrift.{TProcessor, TProcessorFactory}
import org.apache.thrift.transport.{TTransport, TTransportException, TTransportFactory}

import org.apache.livy.{LivyConf, Logging}
import org.apache.livy.thriftserver.cli.ThriftCLIService

/**
 * This class is a porting of the parts we use from `HiveAuthFactory` by Hive.
 */
class AuthFactory(val conf: LivyConf) extends Logging {

  private val authTypeStr = conf.get(LivyConf.THRIFT_AUTHENTICATION)
  // ShimLoader.getHadoopShims().isSecurityEnabled() will only check that
  // hadoopAuth is not simple, it does not guarantee it is kerberos
  private val hadoopAuth = new Configuration().get(HADOOP_SECURITY_AUTHENTICATION)

  private val secretManager = if (isSASLWithKerberizedHadoop) {
      val sm = new LivyDelegationTokenSecretManager(conf)
      try {
        sm.startThreads()
      } catch {
        case e: IOException =>
          throw new TTransportException("Failed to start token manager", e)
      }
      Some(sm)
    } else {
      None
    }

  private val saslServer: Option[AuthBridgeServer] = secretManager.map { sm =>
      new AuthBridgeServer(sm)
    }

  def getSaslProperties: util.Map[String, String] = {
    val saslProps = new util.HashMap[String, String]
    val saslQOP = SaslQOP.fromString(conf.get(LivyConf.THRIFT_SASL_QOP))
    saslProps.put(Sasl.QOP, saslQOP.toString)
    saslProps.put(Sasl.SERVER_AUTH, "true")
    saslProps
  }

  @throws[LoginException]
  def getAuthTransFactory: TTransportFactory = {
    val isAuthKerberos = authTypeStr.equalsIgnoreCase(AuthTypes.KERBEROS.getAuthName)
    val isAuthNoSASL = authTypeStr.equalsIgnoreCase(AuthTypes.NOSASL.getAuthName)
    // TODO: add LDAP and PAM when supported
    val isAuthOther = authTypeStr.equalsIgnoreCase(AuthTypes.NONE.getAuthName) ||
      authTypeStr.equalsIgnoreCase(AuthTypes.CUSTOM.getAuthName)

    saslServer.map { server =>
      val serverTransportFactory = try {
        server.createSaslServerTransportFactory(getSaslProperties)
      } catch {
        case e: TTransportException =>
          throw new LoginException(e.getMessage)
      }
      if (isAuthOther) {
        PlainSaslServer.addPlainServerDefinition(serverTransportFactory, authTypeStr, conf)
      } else if (!isAuthKerberos) {
        throw new LoginException(s"Unsupported authentication type $authTypeStr")
      }
      server.wrapTransportFactory(serverTransportFactory)
    }.getOrElse {
      if (isAuthOther) {
        PlainSaslServer.getPlainTransportFactory(authTypeStr, conf)
      } else if (isAuthNoSASL) {
        new TTransportFactory
      } else {
        throw new LoginException(s"Unsupported authentication type $authTypeStr")
      }
    }
  }

  /**
   * Returns the thrift processor factory for binary mode
   */
  @throws[LoginException]
  def getAuthProcFactory(service: ThriftCLIService): TProcessorFactory = {
    if (saslServer.isDefined) {
      new CLIServiceProcessorFactory(service, saslServer.get)
    } else {
      new SQLPlainProcessorFactory(service)
    }
  }

  def getRemoteUser: String = saslServer.map(_.getRemoteUser).orNull

  def getIpAddress: String =
    saslServer.flatMap(s => Option(s.getRemoteAddress)).map(_.getHostAddress).orNull

  def getUserAuthMechanism: String = saslServer.map(_.getUserAuthMechanism).orNull

  def isSASLWithKerberizedHadoop: Boolean = {
    "kerberos".equalsIgnoreCase(hadoopAuth) &&
      !authTypeStr.equalsIgnoreCase(AuthTypes.NOSASL.getAuthName)
  }

  def isSASLKerberosUser: Boolean = {
    AuthMethod.KERBEROS.getMechanismName == getUserAuthMechanism ||
      AuthMethod.TOKEN.getMechanismName == getUserAuthMechanism
  }

  @throws[HiveSQLException]
  def verifyDelegationToken(delegationToken: String): String = {
    if (secretManager.isEmpty) {
      throw new HiveSQLException(
        "Delegation token only supported over kerberos authentication", "08S01")
    }
    try {
      secretManager.get.verifyDelegationToken(delegationToken)
    } catch {
      case e: IOException =>
        val msg = s"Error verifying delegation token $delegationToken"
        error(msg, e)
        throw new HiveSQLException(msg, "08S01", e)
    }
  }
}

class SQLPlainProcessorFactory(val service: Iface) extends TProcessorFactory(null) {

  override def getProcessor(trans: TTransport): TProcessor = {
    new TSetIpAddressProcessor[Iface](service)
  }
}

class CLIServiceProcessorFactory(val service: Iface, val saslServer: AuthBridgeServer)
  extends TProcessorFactory(null) {

  override def getProcessor(trans: TTransport): TProcessor = {
    val sqlProcessor = new TCLIService.Processor[Iface](service)
    saslServer.wrapNonAssumingProcessor(sqlProcessor)
  }
}

/**
 * This is copied from Hive because its constructor is not accessible.
 */
class PlainServerCallbackHandler(authMethodStr: String, livyConf: LivyConf)
  extends CallbackHandler {

  private val authMethod: AuthMethods = AuthMethods.getValidAuthMethod(authMethodStr)

  override def handle(callbacks: Array[Callback]): Unit = {
    var username: String = null
    var password: String = null
    var ac: AuthorizeCallback = null

    callbacks.foreach {
      case nc: NameCallback => username = nc.getName
      case pc: PasswordCallback => password = new String(pc.getPassword)
      case c: AuthorizeCallback => ac = c
      case other => throw new UnsupportedCallbackException(other)
    }
    val provider =
      AuthenticationProvider.getAuthenticationProvider(authMethod.getAuthMethod, livyConf)
    provider.Authenticate(username, password)
    if (ac != null) {
      ac.setAuthorized(true)
    }
  }
}
