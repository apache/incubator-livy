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

import java.util.Properties
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

import org.apache.commons.codec.binary.Base64
import org.apache.directory.server.annotations.CreateLdapServer
import org.apache.directory.server.annotations.CreateTransport
import org.apache.directory.server.core.annotations.ApplyLdifs
import org.apache.directory.server.core.annotations.ContextEntry
import org.apache.directory.server.core.annotations.CreateDS
import org.apache.directory.server.core.annotations.CreatePartition
import org.apache.directory.server.core.integ.AbstractLdapTestUnit
import org.apache.directory.server.core.integ.FrameworkRunner
import org.apache.hadoop.security.authentication.client.AuthenticationException
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.Mockito

/**
 * This unit test verifies the functionality of LdapAuthenticationHandlerImpl.
 */
@RunWith(classOf[FrameworkRunner])
@CreateLdapServer(transports = Array(
  new CreateTransport(
    protocol = "LDAP",
    address = "localhost"
  )))
@CreateDS(
  allowAnonAccess = true,
  partitions = Array(
    new CreatePartition(
      name = "Test_Partition",
      suffix = "dc=example,dc=com",
      contextEntry = new ContextEntry(entryLdif = "dn: dc=example," +
        "dc=com \ndc: example\nobjectClass: top\nobjectClass: domain\n\n")
    )))
@ApplyLdifs(Array(
  "dn: uid=bjones,dc=example,dc=com",
  "cn: Bob Jones",
  "sn: Jones",
  "objectClass: inetOrgPerson",
  "uid: bjones",
  "userPassword: p@ssw0rd"
))
class TestLdapAuthenticationHandlerImpl extends AbstractLdapTestUnit {
  private val handler: LdapAuthenticationHandlerImpl = new LdapAuthenticationHandlerImpl
  // HTTP header used by the server endpoint during an authentication sequence.
  val WWW_AUTHENTICATE_HEADER = "WWW-Authenticate"
  // HTTP header used by the client endpoint during an authentication sequence.
  val AUTHORIZATION_HEADER = "Authorization"
  // HTTP header prefix used during the Basic authentication sequence.
  val BASIC = "Basic"

  @Before
  def setup(): Unit = {
    handler.init(getDefaultProperties)
  }

  protected def getDefaultProperties: Properties = {
    val p = new Properties
    p.setProperty("ldap.basedn", "dc=example,dc=com")
    p.setProperty("ldap.providerurl", String.format("ldap://%s:%s", "localhost",
      AbstractLdapTestUnit.getLdapServer.getPort.toString))
    p
  }

  @Test
  def testRequestWithAuthorization(): Unit = {
    val request = Mockito.mock(classOf[HttpServletRequest])
    val response = Mockito.mock(classOf[HttpServletResponse])
    val base64 = new Base64(0)
    val credentials = base64.encodeToString("bjones:p@ssw0rd".getBytes)
    val authHeader = BASIC + " " + credentials
    Mockito.when(request.getHeader(AUTHORIZATION_HEADER)).thenReturn(authHeader)

    val token = handler.authenticate(request, response)
    Assert.assertNotNull(token)
    Mockito.verify(response).setStatus(HttpServletResponse.SC_OK)
    Assert.assertEquals("bjones", token.getUserName)
    Assert.assertEquals("bjones", token.getName)
  }

  @Test
  def testRequestWithoutAuthorization(): Unit = {
    val request = Mockito.mock(classOf[HttpServletRequest])
    val response = Mockito.mock(classOf[HttpServletResponse])

    Assert.assertNull(handler.authenticate(request, response))
    Mockito.verify(response).setHeader(WWW_AUTHENTICATE_HEADER, BASIC)
    Mockito.verify(response).setStatus(HttpServletResponse.SC_UNAUTHORIZED)
  }

  @Test
  def testRequestWithInvalidAuthorization(): Unit = {
    val request = Mockito.mock(classOf[HttpServletRequest])
    val response = Mockito.mock(classOf[HttpServletResponse])
    val base64 = new Base64
    val credentials = "bjones:invalidpassword"
    Mockito.when(request.getHeader(AUTHORIZATION_HEADER)).
      thenReturn(base64.encodeToString(credentials.getBytes))

    Assert.assertNull(handler.authenticate(request, response))
    Mockito.verify(response).setHeader(WWW_AUTHENTICATE_HEADER, BASIC)
    Mockito.verify(response).setStatus(HttpServletResponse.SC_UNAUTHORIZED)
  }

  @Test
  def testRequestWithIncompleteAuthorization(): Unit = {
    val request = Mockito.mock(classOf[HttpServletRequest])
    val response = Mockito.mock(classOf[HttpServletResponse])
    Mockito.when(request.getHeader(AUTHORIZATION_HEADER)).thenReturn(BASIC)
    Assert.assertNull(handler.authenticate(request, response))
  }

  @Test
  def testRequestWithWrongCredentials(): Unit = {
    val request = Mockito.mock(classOf[HttpServletRequest])
    val response = Mockito.mock(classOf[HttpServletResponse])
    val base64 = new Base64
    val credentials = base64.encodeToString("bjones:foo123".getBytes)
    val authHeader = BASIC + " " + credentials
    Mockito.when(request.getHeader(AUTHORIZATION_HEADER)).thenReturn(authHeader)

    try {
      handler.authenticate(request, response)
      Assert.fail
    } catch {
      case ex: AuthenticationException =>
      // Expected
      case ex: Exception =>
        Assert.fail
    }
  }
}

