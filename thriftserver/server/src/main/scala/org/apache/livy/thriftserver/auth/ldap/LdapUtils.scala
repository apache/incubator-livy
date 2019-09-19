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

import org.apache.commons.lang.StringUtils

import org.apache.livy.{LivyConf, Logging}

/**
  * Static utility methods related to LDAP authentication module.
  */
object LdapUtils extends Logging{

  /**
    * Extracts username from user DN.
    * <br>
    * <b>Examples:</b>
    * <pre>
    * LdapUtils.extractUserName("UserName")                        = "UserName"
    * LdapUtils.extractUserName("UserName@mycorp.com")             = "UserName"
    * LdapUtils.extractUserName("cn=UserName,dc=mycompany,dc=com") = "UserName"
    * </pre>
    *
    * @param userDn
    * @return
    */
  def extractUserName(userDn: String): String = {
    if (!isDn(userDn) && !hasDomain(userDn)) return userDn
    val domainIdx = indexOfDomainMatch(userDn)
    if (domainIdx > 0) return userDn.substring(0, domainIdx)
    if (userDn.contains("=")) return userDn.substring(userDn.indexOf("=") + 1, userDn.indexOf(","))
    userDn
  }

  /**
    * Get the index separating the user name from domain name (the user's name up
    * to the first '/' or '@').
    *
    * @param userName full user name.
    * @return index of domain match or -1 if not found
    */
  def indexOfDomainMatch(userName: String): Int = {
    if (userName == null) return -1
    val idx = userName.indexOf('/')
    val idx2 = userName.indexOf('@')
    var endIdx = Math.min(idx, idx2) // Use the earlier match.
    // Unless at least one of '/' or '@' was not found, in
    // which case, user the latter match.
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
    *
    * @param userName username
    * @return true if { @code userName} contains { @code @<domain>} part
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
    *
    * @param name name to be checked
    * @return true if the provided name is a distinguished name
    */
  def isDn(name: String): Boolean = {
    name.contains("=")
  }

  /**
    * Creates a principal to be used for user authentication.
    *
    * @param conf Livy configuration
    * @param user username
    * @return a list of user's principal
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

