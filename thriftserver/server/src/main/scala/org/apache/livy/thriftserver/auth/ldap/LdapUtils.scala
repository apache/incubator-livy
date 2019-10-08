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

import org.apache.livy.{LivyConf, Logging}

/**
 * Static utility methods related to LDAP authentication module.
 */
object LdapUtils extends Logging {

  /**
   * Extracts username from user DN.
   * <br>
   * <b>Examples:</b>
   * <pre>
   * LdapUtils.extractUserName("UserName")                        = "UserName"
   * LdapUtils.extractUserName("UserName@mycorp.com")             = "UserName"
   * LdapUtils.extractUserName("cn=UserName,dc=mycompany,dc=com") = "UserName"
   * </pre>
   */
  def extractUserName(userDn: String): String = {
    var userName = userDn

    if (!isDn(userDn) && !hasDomain(userDn)) {
      userName = userDn
    } else {
      val domainIdx = indexOfDomainMatch(userDn)
      if (domainIdx > 0) {
        userName = userDn.substring(0, domainIdx)
      } else if (userDn.contains("=")) {
        userName = userDn.substring(userDn.indexOf("=") + 1, userDn.indexOf(","))
      }
    }
    userName
  }

  /**
   * Get the index separating the user name from domain name (the user's name up
   * to the first '/' or '@'). Index of domain match or -1 if not found
   */
  def indexOfDomainMatch(userName: String): Int = {
    var endIdx = -1
    val idx = userName.indexOf('/')
    val idx2 = userName.indexOf('@')
    endIdx = Math.min(idx, idx2)

    // If neither '/' nor '@' was found, using the latter
    if (endIdx == -1) endIdx = Math.max(idx, idx2)

    endIdx
  }

  /**
   * Check for a domain part in the provided username.
   * <br>
   * <b>Example:</b>
   * <br>
   * <pre>
   * LdapUtils.hasDomain("user1@mycorp.com") = true
   * LdapUtils.hasDomain("user1")            = false
   * </pre>
   */
  def hasDomain(userName: String): Boolean = {
    indexOfDomainMatch(userName) > 0
  }

  /**
   * Detects DN names.
   * <br>
   * <b>Example:</b>
   * <br>
   * <pre>
   * LdapUtils.isDn("cn=UserName,dc=mycompany,dc=com") = true
   * LdapUtils.isDn("user1")                           = false
   * </pre>
   */
  def isDn(name: String): Boolean = {
    name.contains("=")
  }

  /**
   * Creates a principal to be used for user authentication.
   */
  def createCandidatePrincipal(conf: LivyConf, user: String): String = {
    val ldapDomain = conf.get(LivyConf.THRIFT_LDAP_AUTHENTICATION_DOMAIN)
    val ldapBaseDN = conf.get(LivyConf.THRIFT_LDAP_AUTHENTICATION_BASEDN)

    var principle: String = user
    if (!hasDomain(user) && ldapDomain != null) {
      principle = user + "@" + ldapDomain
    }

    var bindDN: String = principle
    if (ldapBaseDN != null) {
      bindDN = "uid=" + principle + "," + ldapBaseDN
    }
    bindDN
  }
}

