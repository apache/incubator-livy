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

import javax.security.sasl.AuthenticationException

import org.apache.directory.server.annotations.CreateLdapServer
import org.apache.directory.server.annotations.CreateTransport
import org.apache.directory.server.core.annotations.ApplyLdifs
import org.apache.directory.server.core.annotations.ContextEntry
import org.apache.directory.server.core.annotations.CreateDS
import org.apache.directory.server.core.annotations.CreatePartition
import org.apache.directory.server.core.integ.AbstractLdapTestUnit
import org.apache.directory.server.core.integ.FrameworkRunner
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith

import org.apache.livy.LivyConf

/**
  * This unit test verifies the functionality of LdapAuthenticationProviderImpl.
  */
@RunWith(classOf[FrameworkRunner])
@CreateLdapServer(transports = Array(new CreateTransport(protocol = "LDAP", address = "localhost")))
@CreateDS(allowAnonAccess = true, partitions = Array(new CreatePartition(name = "Test_Partition",
  suffix = "dc=example,dc=com", contextEntry = new ContextEntry(entryLdif = "dn: dc=example," +
    "dc=com \ndc: example\nobjectClass: top\n" + "objectClass: domain\n\n"))))
@ApplyLdifs(Array("dn: uid=bjones,dc=example,dc=com", "cn: Bob Jones", "sn: Jones",
  "objectClass: inetOrgPerson", "uid: bjones", "userPassword: p@ssw0rd"))
class TestLdapAuthenticationProviderImpl extends AbstractLdapTestUnit {
  private var handler: LdapAuthenticationProviderImpl = null
  private var user = "bjones"
  private var pwd = "p@ssw0rd"
  val livyConf = new LivyConf()

  @Before
  @throws[Exception]
  def setup(): Unit = {
    livyConf.set(LivyConf.THRIFT_AUTHENTICATION, "ldap")
    livyConf.set(LivyConf.THRIFT_LDAP_AUTHENTICATION_BASEDN, "dc=example,dc=com")
    livyConf.set(LivyConf.THRIFT_LDAP_AUTHENTICATION_URL, String.format("ldap://%s:%s", "localhost",
      AbstractLdapTestUnit.getLdapServer.getPort.toString))
    livyConf.set(LivyConf.THRIFT_LDAP_AUTHENTICATION_USERFILTER, "bjones,jake")
  }

  @Test(timeout = 60000)
  @throws[AuthenticationException]
  def testAuthenticatePasses(): Unit = {

    try {
      handler = new LdapAuthenticationProviderImpl(livyConf)
      handler.Authenticate(user, pwd)
    } catch {
      case e: AuthenticationException =>
        val message = String.format("Authentication failed for user '%s' with password '%s'",
          user, pwd)
        throw new AssertionError(message, e)
    }
  }

  @Test(timeout = 60000)
  @throws[Exception]
  def testAuthenticateWithWrongUser(): Unit = {

    val wrongUser = "jake"
    try {
      handler = new LdapAuthenticationProviderImpl(livyConf)
      handler.Authenticate(wrongUser, pwd)
    } catch {
      case ex: AuthenticationException =>
      // Expected
      case ex: Exception =>
        Assert.fail
    }
  }

  @Test(timeout = 60000)
  @throws[Exception]
  def testAuthenticateWithWrongPassword(): Unit = {

    val wrongPwd = "wrongPwd"
    try {
      handler = new LdapAuthenticationProviderImpl(livyConf)
      handler.Authenticate(user, wrongPwd)
    } catch {
      case ex: AuthenticationException =>
      // Expected
      case ex: Exception =>
        Assert.fail
    }
  }

  @Test(timeout = 60000)
  @throws[AuthenticationException]
  def testAuthenticateWithWrongGroup(): Unit = {

    livyConf.set(LivyConf.THRIFT_LDAP_AUTHENTICATION_USERFILTER, "user1,user2")

    try {
      handler = new LdapAuthenticationProviderImpl(livyConf)
      handler.Authenticate(user, pwd)
    } catch {
      case ex: AuthenticationException =>
      // Expected
      case ex: Exception =>
        Assert.fail
    }
  }
}
