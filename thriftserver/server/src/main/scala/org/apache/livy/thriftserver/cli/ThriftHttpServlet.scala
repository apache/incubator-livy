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
import java.security.{PrivilegedExceptionAction, SecureRandom}
import javax.servlet.ServletException
import javax.servlet.http.{Cookie, HttpServletRequest, HttpServletResponse}
import javax.ws.rs.core.NewCookie

import scala.collection.JavaConverters._

import org.apache.commons.codec.binary.{Base64, StringUtils}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.authentication.util.KerberosName
import org.apache.hive.service.CookieSigner
import org.apache.hive.service.auth.{HiveAuthConstants, HttpAuthenticationException, HttpAuthUtils}
import org.apache.hive.service.auth.HiveAuthConstants.AuthTypes
import org.apache.hive.service.cli.HiveSQLException
import org.apache.thrift.TProcessor
import org.apache.thrift.protocol.TProtocolFactory
import org.apache.thrift.server.TServlet
import org.ietf.jgss.{GSSContext, GSSCredential, GSSException, GSSManager, Oid}

import org.apache.livy.{LivyConf, Logging}
import org.apache.livy.thriftserver.SessionInfo
import org.apache.livy.thriftserver.auth.{AuthenticationProvider, AuthFactory}

/**
 * This class is a porting of the parts we use from `ThriftHttpServlet` by Hive.
 */
class ThriftHttpServlet(
    processor: TProcessor,
    protocolFactory: TProtocolFactory,
    val authType: String,
    val serviceUGI: UserGroupInformation,
    val httpUGI: UserGroupInformation,
    val authFactory: AuthFactory,
    val livyConf: LivyConf) extends TServlet(processor, protocolFactory) with Logging {

  private val isCookieAuthEnabled = livyConf.getBoolean(LivyConf.THRIFT_HTTP_COOKIE_AUTH_ENABLED)

  // Class members for cookie based authentication.
  private val signer: CookieSigner = if (isCookieAuthEnabled) {
      // Generate the signer with secret.
      val secret = ThriftHttpServlet.RAN.nextLong.toString
      debug("Using the random number as the secret for cookie generation " + secret)
      new CookieSigner(secret.getBytes())
    } else {
      null
    }

  private val cookieDomain = livyConf.get(LivyConf.THRIFT_HTTP_COOKIE_DOMAIN)
  private val cookiePath = livyConf.get(LivyConf.THRIFT_HTTP_COOKIE_PATH)
  private val cookieMaxAge =
    (livyConf.getTimeAsMs(LivyConf.THRIFT_HTTP_COOKIE_MAX_AGE) / 1000).toInt
  private val isCookieSecure = livyConf.getBoolean(LivyConf.THRIFT_USE_SSL)
  private val isHttpOnlyCookie = livyConf.getBoolean(LivyConf.THRIFT_HTTP_COOKIE_IS_HTTPONLY)
  private val xsrfFilterEnabled = livyConf.getBoolean(LivyConf.THRIFT_XSRF_FILTER_ENABLED)

  @throws[IOException]
  @throws[ServletException]
  override protected def doPost(
      request: HttpServletRequest, response: HttpServletResponse): Unit = {
    var clientUserName: String = null
    var requireNewCookie: Boolean = false

    try {
      if (xsrfFilterEnabled) {
        val continueProcessing = ThriftHttpServlet.doXsrfFilter(request, response)
        if (!continueProcessing) {
          warn("Request did not have valid XSRF header, rejecting.")
          return
        }
      }
      // If the cookie based authentication is already enabled, parse the
      // request and validate the request cookies.
      if (isCookieAuthEnabled) {
        clientUserName = validateCookie(request)
        requireNewCookie = clientUserName == null
        if (requireNewCookie) {
          info("Could not validate cookie sent, will try to generate a new cookie")
        }
      }
      // If the cookie based authentication is not enabled or the request does
      // not have a valid cookie, use the kerberos or password based authentication
      // depending on the server setup.
      if (clientUserName == null) {
        // For a kerberos setup
        if (ThriftHttpServlet.isKerberosAuthMode(authType)) {
          val delegationToken = request.getHeader(ThriftHttpServlet.HIVE_DELEGATION_TOKEN_HEADER)
          // Each http request must have an Authorization header
          if ((delegationToken != null) && (!delegationToken.isEmpty)) {
            clientUserName = doTokenAuth(request, response)
          } else {
            clientUserName = doKerberosAuth(request)
          }
        } else {
          // For password based authentication
          clientUserName = doPasswdAuth(request, authType)
        }
      }
      debug(s"Client username: $clientUserName")

      // Set the thread local username to be used for doAs if true
      SessionInfo.setUserName(clientUserName)

      // find proxy user if any from query param
      val doAsQueryParam = ThriftHttpServlet.getDoAsQueryParam(request.getQueryString)
      if (doAsQueryParam != null) {
        SessionInfo.setProxyUserName(doAsQueryParam)
      }

      val clientIpAddress = request.getRemoteAddr
      debug("Client IP Address: " + clientIpAddress)
      // Set the thread local ip address
      SessionInfo.setIpAddress(clientIpAddress)

      // get forwarded hosts address
      val forwardedFor = request.getHeader(ThriftHttpServlet.X_FORWARDED_FOR)
      if (forwardedFor != null) {
        debug(s"${ThriftHttpServlet.X_FORWARDED_FOR}:$forwardedFor")
        SessionInfo.setForwardedAddresses(forwardedFor.split(",").toList.asJava)
      } else {
        SessionInfo.setForwardedAddresses(List.empty.asJava)
      }

      // Generate new cookie and add it to the response
      if (requireNewCookie && !authType.equalsIgnoreCase(AuthTypes.NOSASL.toString)) {
        val cookieToken = HttpAuthUtils.createCookieToken(clientUserName)
        val hs2Cookie = createCookie(signer.signCookie(cookieToken))

        if (isHttpOnlyCookie) {
          response.setHeader("SET-COOKIE", ThriftHttpServlet.getHttpOnlyCookieHeader(hs2Cookie))
        } else {
          response.addCookie(hs2Cookie)
        }
        info("Cookie added for clientUserName " + clientUserName)
      }
      super.doPost(request, response);
    } catch {
      case e: HttpAuthenticationException =>
        error("Error: ", e)
        // Send a 401 to the client
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED)
        if(ThriftHttpServlet.isKerberosAuthMode(authType)) {
          response.addHeader(HttpAuthUtils.WWW_AUTHENTICATE, HttpAuthUtils.NEGOTIATE)
        }
        // scalastyle:off println
        response.getWriter.println("Authentication Error: " + e.getMessage)
        // scalastyle:on println
    } finally {
      // Clear the thread locals
      SessionInfo.clearUserName()
      SessionInfo.clearIpAddress()
      SessionInfo.clearProxyUserName()
      SessionInfo.clearForwardedAddresses()
    }
  }

  /**
   * Retrieves the client name from cookieString. If the cookie does not correspond to a valid
   * client, the function returns null.
   * @param cookies HTTP Request cookies.
   * @return Client Username if cookieString has a HS2 Generated cookie that is currently valid.
   *         Else, returns null.
   */
  private def getClientNameFromCookie(cookies: Array[Cookie]): String = {
    // Following is the main loop which iterates through all the cookies send by the client.
    // The HS2 generated cookies are of the format hive.server2.auth=<value>
    // A cookie which is identified as a hiveserver2 generated cookie is validated by calling
    // signer.verifyAndExtract(). If the validation passes, send the username for which the cookie
    // is validated to the caller. If no client side cookie passes the validation, return null to
    // the caller.
    cookies.filter(_.equals(ThriftHttpServlet.AUTH_COOKIE)).foreach { cookie =>
      val value = signer.verifyAndExtract(cookie.getValue)
      if (value != null) {
        val userName = HttpAuthUtils.getUserNameFromCookieToken(value)
        if (userName == null) {
          warn("Invalid cookie token " + value)
        } else {
          // We have found a valid cookie in the client request.
          if (logger.isDebugEnabled()) {
            debug("Validated the cookie for user " + userName)
          }
          return userName
        }
      }
    }
    // No valid generated cookies found, return null
    null
  }

  /**
   * Convert cookie array to human readable cookie string
   * @param cookies Cookie Array
   * @return String containing all the cookies separated by a newline character.
   * Each cookie is of the format [key]=[value]
   */
  private def toCookieStr(cookies: Array[Cookie]): String = {
    cookies.map(c => s"${c.getName} = ${c.getValue} ;\n").mkString
  }

  /**
   * Validate the request cookie. This function iterates over the request cookie headers
   * and finds a cookie that represents a valid client/server session. If it finds one, it
   * returns the client name associated with the session. Else, it returns null.
   * @param request The HTTP Servlet Request send by the client
   * @return Client Username if the request has valid HS2 cookie, else returns null
   */
  private def validateCookie(request: HttpServletRequest): String = {
    // Find all the valid cookies associated with the request.
    val cookies = request.getCookies

    if (cookies == null) {
      if (logger.isDebugEnabled()) {
        debug("No valid cookies associated with the request " + request)
      }
      null
    } else {
      if (logger.isDebugEnabled()) {
        debug("Received cookies: " + toCookieStr(cookies))
      }
      getClientNameFromCookie(cookies)
    }
  }

  /**
   * Generate a server side cookie given the cookie value as the input.
   * @param str Input string token.
   * @return The generated cookie.
   */
  private def createCookie(str: String): Cookie = {
    if (logger.isDebugEnabled()) {
      debug(s"Cookie name = ${ThriftHttpServlet.AUTH_COOKIE} value = $str")
    }
    val cookie = new Cookie(ThriftHttpServlet.AUTH_COOKIE, str)

    cookie.setMaxAge(cookieMaxAge)
    if (cookieDomain != null) {
      cookie.setDomain(cookieDomain)
    }
    if (cookiePath != null) {
      cookie.setPath(cookiePath)
    }
    cookie.setSecure(isCookieSecure)
    cookie
  }


  /**
   * Do the authentication (PAM not yet supported)
   */
  private def doPasswdAuth(request: HttpServletRequest, authType: String): String = {
    val userName = getUsername(request, authType)
    // No-op when authType is NOSASL
    if (!authType.equalsIgnoreCase(HiveAuthConstants.AuthTypes.NOSASL.toString)) {
      try {
        val provider = AuthenticationProvider.getAuthenticationProvider(authType, livyConf)
        provider.Authenticate(userName, getPassword(request, authType))
      } catch {
        case e: Exception => throw new HttpAuthenticationException(e)
      }
    }
    userName
  }

  private def doTokenAuth(request: HttpServletRequest, response: HttpServletResponse): String = {
    val tokenStr = request.getHeader(ThriftHttpServlet.HIVE_DELEGATION_TOKEN_HEADER)
    try {
      authFactory.verifyDelegationToken(tokenStr)
    } catch {
      case e: HiveSQLException => throw new HttpAuthenticationException(e);
    }
  }

  /**
   * Do the GSS-API kerberos authentication. We already have a logged in subject in the form of
   * serviceUGI, which GSS-API will extract information from.
   * In case of a SPNego request we use the httpUGI, for the authenticating service tickets.
   */
  private def doKerberosAuth(request: HttpServletRequest): String = {
    // Try authenticating with the http/_HOST principal
    if (httpUGI != null) {
      try {
        return httpUGI.doAs(new HttpKerberosServerAction(request, httpUGI, authType))
      } catch {
        case _: Exception =>
          info("Failed to authenticate with http/_HOST kerberos principal, trying with " +
            "livy/_HOST kerberos principal")
      }
    }
    // Now try with livy/_HOST principal
    try {
      serviceUGI.doAs(new HttpKerberosServerAction(request, serviceUGI, authType))
    } catch {
      case e: Exception =>
        error("Failed to authenticate with livy/_HOST kerberos principal")
        throw new HttpAuthenticationException(e)
    }
  }

  private def getUsername(request: HttpServletRequest, authType: String): String = {
    val creds = getAuthHeaderTokens(request, authType)
    // Username must be present
    if (creds(0) == null || creds(0).isEmpty) {
      throw new HttpAuthenticationException("Authorization header received " +
        "from the client does not contain username.")
    }
    creds(0)
  }

  private def getPassword(request: HttpServletRequest, authType: String): String = {
    val creds = getAuthHeaderTokens(request, authType)
    // Password must be present
    if (creds(1) == null || creds(1).isEmpty) {
      throw new HttpAuthenticationException("Authorization header received " +
        "from the client does not contain password.")
    }
    creds(1)
  }

  private def getAuthHeaderTokens(request: HttpServletRequest, authType: String): Array[String] = {
    val authHeaderBase64 = ThriftHttpServlet.getAuthHeader(request, authType)
    val authHeaderString = StringUtils.newStringUtf8(
      Base64.decodeBase64(authHeaderBase64.getBytes()))
    authHeaderString.split(":")
  }
}


object ThriftHttpServlet extends Logging {
  private val XSRF_HEADER_DEFAULT = "X-XSRF-HEADER"
  private val XSRF_METHODS_TO_IGNORE_DEFAULT = Set("GET", "OPTIONS", "HEAD", "TRACE")
  val AUTH_COOKIE = "hive.server2.auth"
  val RAN: SecureRandom = new SecureRandom()
  val HIVE_DELEGATION_TOKEN_HEADER: String = "X-Hive-Delegation-Token"
  val X_FORWARDED_FOR: String = "X-Forwarded-For"

  /**
   * Generate httponly cookie from HS2 cookie
   * @param cookie HS2 generated cookie
   * @return The httponly cookie
   */
  private def getHttpOnlyCookieHeader(cookie: Cookie): String = {
    val newCookie = new NewCookie(
      cookie.getName,
      cookie.getValue,
      cookie.getPath,
      cookie.getDomain,
      cookie.getVersion,
      cookie.getComment,
      cookie.getMaxAge,
      cookie.getSecure)
    newCookie + "; HttpOnly"
  }

  private def getDoAsQueryParam(queryString: String): String = {
    if (logger.isDebugEnabled()) {
      debug("URL query string:" + queryString)
    }
    Option(queryString).flatMap { qs =>
      val params = javax.servlet.http.HttpUtils.parseQueryString(qs)
      params.keySet().asScala.find(_.equalsIgnoreCase("doAs")).map(params.get(_).head)
    }.orNull
  }

  private def doXsrfFilter(request: HttpServletRequest, response: HttpServletResponse): Boolean = {
    if (XSRF_METHODS_TO_IGNORE_DEFAULT.contains(request.getMethod) ||
        request.getHeader(XSRF_HEADER_DEFAULT) != null) {
      true
    } else {
      response.sendError(
        HttpServletResponse.SC_BAD_REQUEST,
        "Missing Required Header for Vulnerability Protection")
      // scalastyle:off println
      response.getWriter.println(
        "XSRF filter denial, requests must contain header : " + XSRF_HEADER_DEFAULT)
      // scalastyle:on println
      false
    }
  }

  /**
   * Returns the base64 encoded auth header payload
   */
  @throws[HttpAuthenticationException]
  private[cli] def getAuthHeader(request: HttpServletRequest, authType: String): String = {
    val authHeader = request.getHeader(HttpAuthUtils.AUTHORIZATION)
    // Each http request must have an Authorization header
    if (authHeader == null || authHeader.isEmpty) {
      throw new HttpAuthenticationException("Authorization header received " +
        "from the client is empty.")
    }

    val beginIndex = if (isKerberosAuthMode(authType)) {
      (HttpAuthUtils.NEGOTIATE + " ").length()
    } else {
      (HttpAuthUtils.BASIC + " ").length()
    }
    val authHeaderBase64String = authHeader.substring(beginIndex)
    // Authorization header must have a payload
    if (authHeaderBase64String == null || authHeaderBase64String.isEmpty) {
      throw new HttpAuthenticationException("Authorization header received " +
        "from the client does not contain any data.")
    }
    authHeaderBase64String
  }

  private def isKerberosAuthMode(authType: String): Boolean = {
    authType.equalsIgnoreCase(AuthTypes.KERBEROS.toString)
  }
}

class HttpKerberosServerAction(
  val request: HttpServletRequest,
  val serviceUGI: UserGroupInformation,
  authType: String) extends PrivilegedExceptionAction[String] {

  @throws[HttpAuthenticationException]
  override def run(): String = {
    // Get own Kerberos credentials for accepting connection
    val manager = GSSManager.getInstance()
    var gssContext: Option[GSSContext] = None
    val serverPrincipal = getPrincipalWithoutRealm(serviceUGI.getUserName)
    try {
      // This Oid for Kerberos GSS-API mechanism.
      val kerberosMechOid = new Oid("1.2.840.113554.1.2.2")
      // Oid for SPNego GSS-API mechanism.
      val spnegoMechOid = new Oid("1.3.6.1.5.5.2")
      // Oid for kerberos principal name
      val krb5PrincipalOid = new Oid("1.2.840.113554.1.2.2.1")

      // GSS name for server
      val serverName = manager.createName(serverPrincipal, krb5PrincipalOid)

      // GSS credentials for server
      val serverCreds = manager.createCredential(serverName,
        GSSCredential.DEFAULT_LIFETIME,
        Array[Oid](kerberosMechOid, spnegoMechOid),
        GSSCredential.ACCEPT_ONLY)

      // Create a GSS context
      gssContext = Some(manager.createContext(serverCreds))
      // Get service ticket from the authorization header
      val serviceTicketBase64 = ThriftHttpServlet.getAuthHeader(request, authType)
      val inToken = Base64.decodeBase64(serviceTicketBase64.getBytes())
      gssContext.get.acceptSecContext(inToken, 0, inToken.length)
      // Authenticate or deny based on its context completion
      if (!gssContext.get.isEstablished) {
        throw new HttpAuthenticationException("Kerberos authentication failed: " +
          "unable to establish context with the service ticket provided by the client.")
      } else {
        getPrincipalWithoutRealmAndHost(gssContext.get.getSrcName.toString)
      }
    } catch {
      case e: GSSException =>
        throw new HttpAuthenticationException("Kerberos authentication failed: ", e)
    } finally {
      gssContext.foreach { ctx =>
        try {
          ctx.dispose()
        } catch {
          case _: GSSException => // No-op
        }
      }
    }
  }

  private def getPrincipalWithoutRealm(fullPrincipal: String): String = {
    val fullKerberosName = new KerberosName(fullPrincipal)
    val serviceName = fullKerberosName.getServiceName
    val hostName = fullKerberosName.getHostName
    if (hostName != null) {
      serviceName + "/" + hostName
    } else {
      serviceName
    }
  }

  private def getPrincipalWithoutRealmAndHost(fullPrincipal: String): String = {
    try {
      new KerberosName(fullPrincipal).getShortName
    } catch {
      case e: IOException => throw new HttpAuthenticationException(e)
    }
  }
}
