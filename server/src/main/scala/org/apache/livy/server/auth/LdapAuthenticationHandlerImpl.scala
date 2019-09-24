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

package org.apache.livy.server.auth

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.util
import java.util.Properties
import javax.naming.NamingException
import javax.naming.directory.InitialDirContext
import javax.naming.ldap.{Control, InitialLdapContext, StartTlsRequest, StartTlsResponse}
import javax.net.ssl.{HostnameVerifier, SSLSession}
import javax.servlet.ServletException
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.security.authentication.client.AuthenticationException
import org.apache.hadoop.security.authentication.server.{AuthenticationHandler, AuthenticationToken}

import org.apache.livy._

object LdapAuthenticationHandlerImpl {

  val AUTHORIZATION_SCHEME = "Basic"
  val TYPE = "ldap"
  val SECURITY_AUTHENTICATION = "simple"
  val PROVIDER_URL = "ldap.providerurl"
  val BASE_DN = "ldap.basedn"
  val LDAP_BIND_DOMAIN = "ldap.binddomain"
  val ENABLE_START_TLS = "ldap.enablestarttls"

  private def hasDomain(userName: String): Boolean = {
    indexOfDomainMatch(userName) > 0
  }

  /**
    * Get the index separating the user name from domain name (the user's name up
    * to the first '/' or '@').
    *
    * @param userName full user name.
    * @return index of domain match or -1 if not found
    */

  private def indexOfDomainMatch(userName: String): Integer = {
    if (userName == null) {
      -1
    } else {
      val idx = userName.indexOf('/')
      val idx2 = userName.indexOf('@')
      // Use the earlier match.
      var endIdx = Math.min(idx, idx2)

      // Unless at least one of '/' or '@' was not found, in
      // which case, user the latter match.
      if (endIdx == -1) endIdx = Math.max(idx, idx2)
      endIdx
    }
  }
}

class LdapAuthenticationHandlerImpl extends AuthenticationHandler with Logging {
  private var ldapDomain = "null"
  private var baseDN = "null"
  private var providerUrl = "null"
  private var enableStartTls = false
  private var disableHostNameVerification = false

  def getType: String = LdapAuthenticationHandlerImpl.TYPE

  @throws[ServletException]
  def init(config: Properties): Unit = {
    this.baseDN = config.getProperty(LdapAuthenticationHandlerImpl.BASE_DN)
    this.providerUrl = config.getProperty(LdapAuthenticationHandlerImpl.PROVIDER_URL)
    this.ldapDomain = config.getProperty(LdapAuthenticationHandlerImpl.LDAP_BIND_DOMAIN)
    this.enableStartTls = config.getProperty(LdapAuthenticationHandlerImpl.ENABLE_START_TLS,
      "false").toBoolean
    require(this.providerUrl != null, "The LDAP URI can not be null")

    if (this.enableStartTls.booleanValue) {
      val tmp = this.providerUrl.toLowerCase
      require(!tmp.startsWith("ldaps"), "Can not use ldaps and StartTLS option at the same time")
    }
  }

  def destroy(): Unit = {
  }

  @throws[IOException]
  @throws[AuthenticationException]
  def managementOperation(token: AuthenticationToken, request: HttpServletRequest,
    response: HttpServletResponse) : Boolean = true

  @throws[IOException]
  @throws[AuthenticationException]
  def authenticate(request: HttpServletRequest,
    response: HttpServletResponse): AuthenticationToken = {
    var token: AuthenticationToken = null
    var authorization = request.getHeader("Authorization")

    if (authorization != null && authorization.regionMatches(true, 0,
      LdapAuthenticationHandlerImpl.AUTHORIZATION_SCHEME, 0,
      LdapAuthenticationHandlerImpl.AUTHORIZATION_SCHEME.length)) {
      authorization = authorization.substring("Basic".length).trim
      val base64 = new Base64(0)
      val credentials = new String(base64.decode(authorization),
        StandardCharsets.UTF_8).split(":", 2)

      if (credentials.length == 2) {
        debug(s"Authenticating  [${credentials(0)}] user")
        token = this.authenticateUser(credentials(0), credentials(1))
        response.setStatus(HttpServletResponse.SC_OK)
      }
    } else {
      response.setHeader("WWW-Authenticate", "Basic")
      response.setStatus(HttpServletResponse.SC_UNAUTHORIZED)

      if (authorization == null) {
        trace("Basic auth starting")
      } else {
        warn("\'Authorization\' does not start " + s"with \'Basic\' :   ${authorization} ")
      }
    }
    token
  }

  @throws[AuthenticationException]
  private def authenticateUser(userName: String, password: String): AuthenticationToken = {
    if (userName != null && !userName.isEmpty) {
      var principle: String = userName

      if (!LdapAuthenticationHandlerImpl.hasDomain(userName) &&
        this.ldapDomain != null) {
        principle = userName + "@" + this.ldapDomain
      }

      if (password != null && !password.isEmpty) {
        var bindDN: String = principle

        if (this.baseDN != null) {
          bindDN = "uid=" + principle + "," + this.baseDN
        }

        if (this.enableStartTls.booleanValue) {
          this.authenticateWithTlsExtension(bindDN, password)
        } else {
          this.authenticateWithoutTlsExtension(bindDN, password)
        }

        new AuthenticationToken(userName, userName, "ldap")
      } else {
        throw new AuthenticationException(
          "Error validating LDAP user: a null or blank password has been provided")
      }
    } else {
      throw new AuthenticationException(
        "Error validating LDAP user: a null or blank username has been provided")
    }
  }

  @throws[AuthenticationException]
  private def authenticateWithTlsExtension(userDN: String, password: String): Unit = {
    var ctx: InitialLdapContext = null
    val env = new util.Hashtable[String, String]
    env.put("java.naming.factory.initial", "com.sun.jndi.ldap.LdapCtxFactory")
    env.put("java.naming.provider.url", this.providerUrl)

    try {
      ctx = new InitialLdapContext(env, null.asInstanceOf[Array[Control]])
      val ex = ctx.extendedOperation(new StartTlsRequest).asInstanceOf[StartTlsResponse]
      if (this.disableHostNameVerification.booleanValue) {
        ex.setHostnameVerifier(new HostnameVerifier() {
          override def verify(hostname: String, session: SSLSession) = true
        })
      }
      ex.negotiate

      ctx.addToEnvironment("java.naming.security.authentication",
        LdapAuthenticationHandlerImpl.SECURITY_AUTHENTICATION)
      ctx.addToEnvironment("java.naming.security.principal", userDN)
      ctx.addToEnvironment("java.naming.security.credentials", password)
      ctx.lookup(userDN)
      debug(s"Authentication successful for ${userDN}")
    } catch {
      case exception@(_: IOException | _: NamingException) =>
        throw new AuthenticationException("Error validating LDAP user", exception)
    } finally {
      if (ctx != null) {
        try {
          ctx.close()
        } catch {
          case exception: NamingException =>
        }
      }
    }
  }

  @throws[AuthenticationException]
  private def authenticateWithoutTlsExtension(userDN: String, password: String): Unit = {
    val env = new util.Hashtable[String, String]
    env.put("java.naming.factory.initial", "com.sun.jndi.ldap.LdapCtxFactory")
    env.put("java.naming.provider.url", this.providerUrl)
    env.put("java.naming.security.authentication",
      LdapAuthenticationHandlerImpl.SECURITY_AUTHENTICATION)
    env.put("java.naming.security.principal", userDN)
    env.put("java.naming.security.credentials", password)

    try {
      val e = new InitialDirContext(env)
      e.close()
      debug(s"Authentication successful for ${userDN}")
    } catch {
      case exception: NamingException =>
        throw new AuthenticationException("Error validating LDAP user", exception)
    }
  }
}
