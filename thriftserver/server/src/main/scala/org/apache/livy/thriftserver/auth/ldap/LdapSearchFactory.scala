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
package org.apache.livy.thriftserver.auth.ldap

import java.util.Hashtable
import javax.naming.{Context, NamingException}
import javax.naming.directory.InitialDirContext
import javax.security.sasl.AuthenticationException

import org.apache.livy.{LivyConf, Logging}

/**
 * A factory for LDAP search objects.
 */
object LdapSearchFactory extends Logging {

  @throws[NamingException]
  private def createDirContext(
      conf: LivyConf,
      principal: String,
      password: String): InitialDirContext = {
    val env = new Hashtable[String, String]
    val ldapUrl = conf.get(LivyConf.AUTH_LDAP_URL)
    env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory")
    env.put(Context.PROVIDER_URL, ldapUrl)
    env.put(Context.SECURITY_AUTHENTICATION, "simple")
    env.put(Context.SECURITY_CREDENTIALS, password)
    env.put(Context.SECURITY_PRINCIPAL, principal)

    new InitialDirContext(env)
  }
}

class LdapSearchFactory extends DirSearchFactory with Logging {

  @throws(classOf[AuthenticationException])
  def getInstance(conf: LivyConf, principal: String, password: String): InitialDirContext = {
    try {
      LdapSearchFactory.createDirContext(conf, principal, password)
    } catch {
      case e: NamingException =>
        throw new AuthenticationException("Error validating LDAP user", e)
    }
  }
}
