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
import java.net.InetAddress
import java.security.{PrivilegedAction, PrivilegedExceptionAction}
import java.util
import javax.security.auth.callback.{Callback, CallbackHandler, NameCallback, PasswordCallback, UnsupportedCallbackException}
import javax.security.sasl.{AuthorizeCallback, RealmCallback, SaslServer}

import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.security.{SaslRpcServer, UserGroupInformation}
import org.apache.hadoop.security.SaslRpcServer.AuthMethod
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
import org.apache.hadoop.security.token.SecretManager.InvalidToken
import org.apache.thrift.{TException, TProcessor}
import org.apache.thrift.protocol.TProtocol
import org.apache.thrift.transport.{TSaslServerTransport, TSocket, TTransport, TTransportException, TTransportFactory}

import org.apache.livy.Logging

/**
 * The class is taken from Hive's `HadoopThriftAuthBridge.Server`. It bridges Thrift's SASL
 * transports to Hadoop's SASL callback handlers and authentication classes.
 */
class AuthBridgeServer(private val secretManager: LivyDelegationTokenSecretManager) {
  private val ugi = try {
      UserGroupInformation.getCurrentUser
    } catch {
      case ioe: IOException => throw new TTransportException(ioe)
    }

  /**
   * Create a TTransportFactory that, upon connection of a client socket,
   * negotiates a Kerberized SASL transport. The resulting TTransportFactory
   * can be passed as both the input and output transport factory when
   * instantiating a TThreadPoolServer, for example.
   *
   * @param saslProps Map of SASL properties
   */
  @throws[TTransportException]
  def createTransportFactory(saslProps: util.Map[String, String]): TTransportFactory = {
    val transFactory: TSaslServerTransport.Factory = createSaslServerTransportFactory(saslProps)
    new TUGIAssumingTransportFactory(transFactory, ugi)
  }

  /**
   * Create a TSaslServerTransport.Factory that, upon connection of a client
   * socket, negotiates a Kerberized SASL transport.
   *
   * @param saslProps Map of SASL properties
   */
  @throws[TTransportException]
  def createSaslServerTransportFactory(
      saslProps: util.Map[String, String]): TSaslServerTransport.Factory = {
    // Parse out the kerberos principal, host, realm.
    val kerberosName: String = ugi.getUserName
    val names: Array[String] = SaslRpcServer.splitKerberosName(kerberosName)
    if (names.length != 3) {
      throw new TTransportException(s"Kerberos principal should have 3 parts: $kerberosName")
    }
    val transFactory: TSaslServerTransport.Factory = new TSaslServerTransport.Factory
    transFactory.addServerDefinition(AuthMethod.KERBEROS.getMechanismName,
      names(0), names(1), // two parts of kerberos principal
      saslProps,
      new SaslRpcServer.SaslGssCallbackHandler)
    transFactory.addServerDefinition(AuthMethod.TOKEN.getMechanismName,
      null,
      SaslRpcServer.SASL_DEFAULT_REALM,
      saslProps,
      new SaslDigestCallbackHandler(secretManager))
    transFactory
  }

  /**
   * Wrap a TTransportFactory in such a way that, before processing any RPC, it
   * assumes the UserGroupInformation of the user authenticated by
   * the SASL transport.
   */
  def wrapTransportFactory(transFactory: TTransportFactory): TTransportFactory = {
    new TUGIAssumingTransportFactory(transFactory, ugi)
  }

  /**
   * Wrap a TProcessor in such a way that, before processing any RPC, it
   * assumes the UserGroupInformation of the user authenticated by
   * the SASL transport.
   */
  def wrapProcessor(processor: TProcessor): TProcessor = {
    new TUGIAssumingProcessor(processor, secretManager, true)
  }

  /**
   * Wrap a TProcessor to capture the client information like connecting userid, ip etc
   */
  def wrapNonAssumingProcessor(processor: TProcessor): TProcessor = {
    new TUGIAssumingProcessor(processor, secretManager, false)
  }

  def getRemoteAddress: InetAddress = AuthBridgeServer.remoteAddress.get

  def getRemoteUser: String = AuthBridgeServer.remoteUser.get

  def getUserAuthMechanism: String = AuthBridgeServer.userAuthMechanism.get

}

/**
 * A TransportFactory that wraps another one, but assumes a specified UGI
 * before calling through.
 *
 * This is used on the server side to assume the server's Principal when accepting
 * clients.
 *
 * This class is derived from Hive's one.
 */
private[auth] class TUGIAssumingTransportFactory(
    val wrapped: TTransportFactory,
    val ugi: UserGroupInformation) extends TTransportFactory {
  assert(wrapped != null)
  assert(ugi != null)

  override def getTransport(trans: TTransport): TTransport = {
    ugi.doAs(new PrivilegedAction[TTransport]() {
      override def run: TTransport = wrapped.getTransport(trans)
    })
  }
}

/**
 * CallbackHandler for SASL DIGEST-MD5 mechanism.
 *
 * This code is pretty much completely based on Hadoop's SaslRpcServer.SaslDigestCallbackHandler -
 * the only reason we could not use that Hadoop class as-is was because it needs a
 * Server.Connection.
 */
sealed class SaslDigestCallbackHandler(
    val secretManager: LivyDelegationTokenSecretManager) extends CallbackHandler with Logging {
  @throws[InvalidToken]
  private def getPassword(tokenId: LivyDelegationTokenIdentifier): Array[Char] = {
    encodePassword(secretManager.retrievePassword(tokenId))
  }

  private def encodePassword(password: Array[Byte]): Array[Char] = {
    new String(Base64.encodeBase64(password)).toCharArray
  }

  @throws[InvalidToken]
  @throws[UnsupportedCallbackException]
  override def handle(callbacks: Array[Callback]): Unit = {
    var nc: NameCallback = null
    var pc: PasswordCallback = null
    callbacks.foreach {
      case ac: AuthorizeCallback =>
        val authid: String = ac.getAuthenticationID
        val authzid: String = ac.getAuthorizationID
        if (authid == authzid) {
          ac.setAuthorized(true)
        } else {
          ac.setAuthorized(false)
        }
        if (ac.isAuthorized) {
          if (logger.isDebugEnabled) {
            val username = SaslRpcServer.getIdentifier(authzid, secretManager).getUser.getUserName
            debug(s"SASL server DIGEST-MD5 callback: setting canonicalized client ID: $username")
          }
          ac.setAuthorizedID(authzid)
        }
      case c: NameCallback => nc = c
      case c: PasswordCallback => pc = c
      case _: RealmCallback => // Do nothing.
      case other =>
        throw new UnsupportedCallbackException(other, "Unrecognized SASL DIGEST-MD5 Callback")
    }
    if (pc != null) {
      val tokenIdentifier = SaslRpcServer.getIdentifier(nc.getDefaultName, secretManager)
      val password: Array[Char] = getPassword(tokenIdentifier)
      if (logger.isDebugEnabled) {
        debug("SASL server DIGEST-MD5 callback: setting password for client: " +
          tokenIdentifier.getUser)
      }
      pc.setPassword(password)
    }
  }
}

/**
 * Processor that pulls the SaslServer object out of the transport, and assumes the remote user's
 * UGI before calling through to the original processor.
 *
 * This is used on the server side to set the UGI for each specific call.
 *
 * This class is derived from Hive's one.
 */
sealed class TUGIAssumingProcessor(
    val wrapped: TProcessor,
    val secretManager: LivyDelegationTokenSecretManager,
    var useProxy: Boolean) extends TProcessor with Logging {

  @throws[TException]
  override def process(inProt: TProtocol, outProt: TProtocol): Boolean = {
    val trans = inProt.getTransport
    if (!trans.isInstanceOf[TSaslServerTransport]) {
      throw new TException(s"Unexpected non-SASL transport ${trans.getClass}")
    }
    val saslTrans: TSaslServerTransport = trans.asInstanceOf[TSaslServerTransport]
    val saslServer: SaslServer = saslTrans.getSaslServer
    val authId: String = saslServer.getAuthorizationID
    debug(s"AUTH ID ======> $authId")
    var endUser = authId
    val socket = saslTrans.getUnderlyingTransport.asInstanceOf[TSocket].getSocket
    AuthBridgeServer.remoteAddress.set(socket.getInetAddress)
    val mechanismName: String = saslServer.getMechanismName
    AuthBridgeServer.userAuthMechanism.set(mechanismName)
    if (AuthMethod.PLAIN.getMechanismName.equalsIgnoreCase(mechanismName)) {
      AuthBridgeServer.remoteUser.set(endUser)
      return wrapped.process(inProt, outProt)
    }
    AuthBridgeServer.authenticationMethod.set(UserGroupInformation.AuthenticationMethod.KERBEROS)
    if (AuthMethod.TOKEN.getMechanismName.equalsIgnoreCase(mechanismName)) {
      try {
        val tokenId = SaslRpcServer.getIdentifier(authId, secretManager)
        endUser = tokenId.getUser.getUserName
        AuthBridgeServer.authenticationMethod.set(UserGroupInformation.AuthenticationMethod.TOKEN)
      } catch {
        case e: InvalidToken => throw new TException(e.getMessage)
      }
    }
    var clientUgi: UserGroupInformation = null
    try {
      if (useProxy) {
        clientUgi = UserGroupInformation.createProxyUser(
          endUser, UserGroupInformation.getLoginUser)
        AuthBridgeServer.remoteUser.set(clientUgi.getShortUserName)
        debug(s"Set remoteUser : ${AuthBridgeServer.remoteUser.get}")
        clientUgi.doAs(new PrivilegedExceptionAction[Boolean]() {
          override def run: Boolean = try {
            wrapped.process(inProt, outProt)
          } catch {
            case te: TException => throw new RuntimeException(te)
          }
        })
      } else {
        // use the short user name for the request
        val endUserUgi: UserGroupInformation = UserGroupInformation.createRemoteUser(endUser)
        AuthBridgeServer.remoteUser.set(endUserUgi.getShortUserName)
        debug(s"Set remoteUser: ${AuthBridgeServer.remoteUser.get}, from endUser :" + endUser)
        wrapped.process(inProt, outProt)
      }
    } catch {
      case rte: RuntimeException if rte.getCause.isInstanceOf[TException] => throw rte.getCause
      case rte: RuntimeException => throw rte
      case ie: InterruptedException => throw new RuntimeException(ie) // unexpected!
      case ioe: IOException => throw new RuntimeException(ioe)
    } finally {
      if (clientUgi != null) {
        try {
          FileSystem.closeAllForUGI(clientUgi)
        } catch {
          case exception: IOException =>
            error(s"Could not clean up file-system handles for UGI: $clientUgi", exception)
        }
      }
    }
  }
}

object AuthBridgeServer extends Logging {
  private[auth] val remoteAddress: ThreadLocal[InetAddress] = new ThreadLocal[InetAddress]() {
    override protected def initialValue: InetAddress = null
  }
  private[auth] val authenticationMethod: ThreadLocal[AuthenticationMethod] =
    new ThreadLocal[AuthenticationMethod]() {
      override protected def initialValue: AuthenticationMethod = AuthenticationMethod.TOKEN
    }
  private[auth] val remoteUser: ThreadLocal[String] = new ThreadLocal[String]() {
    override protected def initialValue: String = null
  }
  private[auth] val userAuthMechanism: ThreadLocal[String] = new ThreadLocal[String]() {
    override protected def initialValue: String = AuthMethod.KERBEROS.getMechanismName
  }
}
