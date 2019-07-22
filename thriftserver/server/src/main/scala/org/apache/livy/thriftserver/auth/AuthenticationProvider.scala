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

import java.lang.reflect.InvocationTargetException
import java.security.MessageDigest
import java.util.Hashtable
import javax.naming.{Context, NamingException}
import javax.naming.directory.InitialDirContext
import javax.security.sasl.AuthenticationException

import org.apache.hive.service.auth.PasswdAuthenticationProvider

import org.apache.livy.LivyConf

object AuthenticationProvider {
  // TODO: support PAM
  val AUTH_METHODS = Seq("LDAP", "NONE", "CUSTOM")

  @throws[AuthenticationException]
  def getAuthenticationProvider(method: String, conf: LivyConf): PasswdAuthenticationProvider = {
    method match {
      case "LDAP" => new LdapAuthenticationProvider(conf)
      case "NONE" => new NoneAuthenticationProvider
      case "CUSTOM" => new CustomAuthenticationProvider(conf)
      case _ => throw new AuthenticationException("Unsupported authentication method")
    }
  }
}

/**
 * An implementation of [[PasswdAuthenticationProvider]] doing nothing.
 */
class NoneAuthenticationProvider extends PasswdAuthenticationProvider {
  override def Authenticate(user: String, password: String): Unit = {
    // Do nothing.
  }
}

/**
 * An implementation of [[PasswdAuthenticationProvider]] delegating the class configured in
 * [[LivyConf.THRIFT_CUSTOM_AUTHENTICATION_CLASS]] to authenticate a user.
 */
class CustomAuthenticationProvider(conf: LivyConf) extends PasswdAuthenticationProvider {
  private val customClass: Class[_ <: PasswdAuthenticationProvider] = {
      Class.forName(conf.get(LivyConf.THRIFT_CUSTOM_AUTHENTICATION_CLASS))
        .asSubclass(classOf[PasswdAuthenticationProvider])
    }
  val provider: PasswdAuthenticationProvider = {
    // Try first a constructor with the LivyConf as parameter, then a constructor with no parameter
    // of none of them is available this fails with an exception.
    try {
      customClass.getConstructor(classOf[LivyConf]).newInstance(conf)
    } catch {
      case _: NoSuchMethodException | _: InstantiationException | _: IllegalAccessException |
           _: InvocationTargetException =>
        customClass.getConstructor().newInstance()
    }
  }

  override def Authenticate(user: String, password: String): Unit = {
    provider.Authenticate(user, password)
  }
}

/**
  * An implementation of [[PasswdAuthenticationProvider]] for LDAP to authenticate a user.
  */
class LdapAuthenticationProvider(conf: LivyConf) extends PasswdAuthenticationProvider {
  override def Authenticate(user: String, password: String): Unit = {
    val ldapURL = conf.get(LivyConf.THRIFT_LDAP_AUTHENTICATION_URL)
    val ldapBaseDN = conf.get(LivyConf.THRIFT_LDAP_AUTHENTICATION_BASEDN)
    val ldapDomain = conf.get(LivyConf.THRIFT_LDAP_AUTHENTICATION_DOMAIN)
    if (ldapURL == null || ldapBaseDN == null || user == null || password == null) {
      throw new AuthenticationException(s"Error validating LDAP ldapURL or ldapBaseDN or " +
        s"user or password, it is null")
    }
    var bindDn = "uid=" + user + "," + ldapBaseDN;
    if (ldapDomain != null) {
      bindDn = "uid=" + user + "@" + ldapDomain + "," + ldapBaseDN;
    }
    val env = new Hashtable[String, Any]()
    env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory")
    env.put(Context.SECURITY_AUTHENTICATION, "simple")
    env.put(Context.PROVIDER_URL, ldapURL)
    env.put(Context.SECURITY_PRINCIPAL, bindDn)
    env.put(Context.SECURITY_CREDENTIALS, password)
    try {
      val ctx = new InitialDirContext(env)
      ctx.close()
    } catch {
      case e: NamingException =>
        throw new AuthenticationException(s"Error validating " +
          s"LDAP user: $bindDn, password: $password", e)
    }
  }
}
