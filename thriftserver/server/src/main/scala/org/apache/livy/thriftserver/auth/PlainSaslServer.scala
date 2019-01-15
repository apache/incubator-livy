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
import java.security.{Provider, Security}
import java.util
import javax.security.auth.callback.Callback
import javax.security.auth.callback.CallbackHandler
import javax.security.auth.callback.NameCallback
import javax.security.auth.callback.PasswordCallback
import javax.security.auth.callback.UnsupportedCallbackException
import javax.security.auth.login.LoginException
import javax.security.sasl._

import org.apache.hive.service.auth.AuthenticationProviderFactory.AuthMethods
import org.apache.thrift.transport.TSaslServerTransport

import org.apache.livy.LivyConf


/**
 * Sun JDK only provides a PLAIN client and no server. This class implements the Plain SASL server
 * conforming to RFC #4616 (http://www.ietf.org/rfc/rfc4616.txt).
 */
class PlainSaslServer private[auth] (
    val handler: CallbackHandler,
    val authMethodStr: String) extends SaslServer {

  AuthMethods.getValidAuthMethod(authMethodStr)

  private var user: String = null
  override def getMechanismName: String = PlainSaslServer.PLAIN_METHOD

  @throws[SaslException]
  override def evaluateResponse(response: Array[Byte]): Array[Byte] = {
    try {
      // parse the response
      // message = [authzid] UTF8NUL authcid UTF8NUL passwd'
      val tokenList: util.Deque[String] = new util.ArrayDeque[String]
      var messageToken = new StringBuilder
      for (b <- response) {
        if (b == 0) {
          tokenList.addLast(messageToken.toString)
          messageToken = new StringBuilder
        } else {
          messageToken.append(b.toChar)
        }
      }
      tokenList.addLast(messageToken.toString)
      // validate response
      if (tokenList.size < 2 || tokenList.size > 3) {
        throw new SaslException("Invalid message format")
      }
      val passwd: String = tokenList.removeLast()
      user = tokenList.removeLast()
      // optional authzid
      var authzId: String = null
      if (tokenList.isEmpty) {
        authzId = user
      } else {
        authzId = tokenList.removeLast()
      }
      if (user == null || user.isEmpty) {
        throw new SaslException("No user name provided")
      }
      if (passwd == null || passwd.isEmpty) {
        throw new SaslException("No password name provided")
      }
      val nameCallback = new NameCallback("User")
      nameCallback.setName(user)
      val pcCallback = new PasswordCallback("Password", false)
      pcCallback.setPassword(passwd.toCharArray)
      val acCallback = new AuthorizeCallback(user, authzId)
      val cbList = Array[Callback](nameCallback, pcCallback, acCallback)
      handler.handle(cbList)
      if (!acCallback.isAuthorized) {
        throw new SaslException("Authentication failed")
      }
    } catch {
      case eL: IllegalStateException =>
        throw new SaslException("Invalid message format", eL)
      case eI: IOException =>
        throw new SaslException("Error validating the login", eI)
      case eU: UnsupportedCallbackException =>
        throw new SaslException("Error validating the login", eU)
    }
    null
  }

  override def isComplete: Boolean = user != null

  override def getAuthorizationID: String = user

  override def unwrap(incoming: Array[Byte], offset: Int, len: Int): Array[Byte] = {
    throw new UnsupportedOperationException
  }

  override def wrap(outgoing: Array[Byte], offset: Int, len: Int): Array[Byte] = {
    throw new UnsupportedOperationException
  }

  override def getNegotiatedProperty(propName: String): Object = null

  override def dispose(): Unit = {}
}

object PlainSaslServer {
  val PLAIN_METHOD = "PLAIN"

  Security.addProvider(new SaslPlainProvider)

  def getPlainTransportFactory(
      authTypeStr: String,
      conf: LivyConf): TSaslServerTransport.Factory = {
    val saslFactory = new TSaslServerTransport.Factory()
    addPlainServerDefinition(saslFactory, authTypeStr, conf)
    saslFactory
  }

  def addPlainServerDefinition(
      saslFactory: TSaslServerTransport.Factory,
      authTypeStr: String,
      conf: LivyConf): Unit = {
    try {
      saslFactory.addServerDefinition("PLAIN",
        authTypeStr,
        null,
        new util.HashMap[String, String](),
        new PlainServerCallbackHandler(authTypeStr, conf))
    } catch {
      case e: AuthenticationException =>
        throw new LoginException(s"Error setting callback handler $e")
    }
  }
}

class SaslPlainServerFactory extends SaslServerFactory {
  override def createSaslServer(
      mechanism: String,
      protocol: String,
      serverName: String,
      props: util.Map[String, _],
      cbh: CallbackHandler): PlainSaslServer = {
    if (PlainSaslServer.PLAIN_METHOD == mechanism) {
      try {
        new PlainSaslServer(cbh, protocol)
      } catch {
        case _: SaslException =>
          /* This is to fulfill the contract of the interface which states that an exception shall
             be thrown when a SaslServer cannot be created due to an error but null should be
             returned when a Server can't be created due to the parameters supplied. And the only
             thing PlainSaslServer can fail on is a non-supported authentication mechanism.
             That's why we return null instead of throwing the Exception */
          null
      }
    } else {
      null
    }
  }

  override def getMechanismNames(props: util.Map[String, _]): Array[String] = {
    Array[String](PlainSaslServer.PLAIN_METHOD)
  }
}

class SaslPlainProvider extends Provider("LivySaslPlain", 1.0, "Livy Plain SASL provider") {
  put("SaslServerFactory.PLAIN", classOf[SaslPlainServerFactory].getName)
}
