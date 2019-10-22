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

import org.apache.livy.LivyConf

/**
 * Static utility methods related to LDAP authentication module.
 */
object LdapUtils {

  def hasDomain(userName: String): Boolean = {
    indexOfDomainMatch(userName) > 0
  }

  /**
   * Get the index separating the user name from domain name (the user's name up
   * to the first '/' or '@').
   */
  def indexOfDomainMatch(userName: String): Int = {
    val idx = userName.indexOf('/')
    val idx2 = userName.indexOf('@')
    // Use the earlier match.
    val endIdx = Math.min(idx, idx2)
    // If neither '/' nor '@' was found, using the latter
    if (endIdx == -1) Math.max(idx, idx2) else endIdx
  }

  /**
   * Extracts username from user DN.
   */
  def extractUserName(userDn: String): String = {
    if (!isDn(userDn) && !hasDomain(userDn)) {
      userDn
    } else {
      val domainIdx = indexOfDomainMatch(userDn)
      if (domainIdx > 0) {
        userDn.substring(0, domainIdx)
      } else if (userDn.contains("=")) {
        userDn.substring(userDn.indexOf("=") + 1, userDn.indexOf(","))
      } else {
        userDn
      }
    }
  }

  /**
   * Detects DN names.
   */
  def isDn(name: String): Boolean = {
    name.contains("=")
  }

  /**
   * Creates a principal to be used for user authentication.
   */
  def createCandidatePrincipal(conf: LivyConf, user: String): String = {
    val ldapDomain = conf.get(LivyConf.AUTH_LDAP_USERNAME_DOMAIN)
    val ldapBaseDN = conf.get(LivyConf.AUTH_LDAP_BASE_DN )
    val principle = if (!hasDomain(user) && ldapDomain != null) {
      user + "@" + ldapDomain
    } else {
      user
    }

    if (ldapBaseDN != null) {
      "uid=" + principle + "," + ldapBaseDN
    } else {
      principle
    }
  }
}

