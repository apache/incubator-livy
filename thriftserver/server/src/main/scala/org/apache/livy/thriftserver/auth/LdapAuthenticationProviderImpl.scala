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

import org.apache.commons.lang.StringUtils
import org.apache.hive.service.auth.PasswdAuthenticationProvider

import org.apache.livy.thriftserver.auth.ldap._
import org.apache.livy.LivyConf

object LdapAuthenticationProviderImpl {

  // Initialize the Chain Filter List. Now GroupFilter is not supported.
  // If needed, GroupFilter can be added in this list.

  private def createFilters(conf: LivyConf): Filter = {
    val chainFilters: List[Filter] = List[Filter](new UserFilter(conf))
    val filter: Filter = new ChainFilter(chainFilters)
    filter
  }
}

class LdapAuthenticationProviderImpl(val conf: LivyConf) extends PasswdAuthenticationProvider {
  final private val filter: Filter = LdapAuthenticationProviderImpl.createFilters(conf)
  final private val searchFactory: DirSearchFactory = new LdapSearchFactory()

  @throws[AuthenticationException]
  def Authenticate(user: String, password: String): Unit = {
    createDirSearch(user, password)
    applyFilter(user)
  }

  @throws[AuthenticationException]
  private def createDirSearch(user: String, password: String): Unit = {
    if (StringUtils.isBlank(user) || StringUtils.isEmpty(user)) {
      throw new AuthenticationException("Error validating LDAP:" +
        " a null or blank user name has been provided")
    }
    if (StringUtils.isBlank(password) || StringUtils.isEmpty(password)) {
      throw new AuthenticationException("Error validating LDAP:" +
        " a null or blank password has been provided")
    }
    val principal = LdapUtils.createCandidatePrincipal(conf, user)
    try {
      searchFactory.getInstance(conf, principal, password)
    } catch {
      case e: AuthenticationException =>
        throw new AuthenticationException(s"Error validating " +
          s"LDAP user: $user, password: $password", e)
    }
  }

  @throws[AuthenticationException]
  private def applyFilter(user: String): Unit = {
    if (filter != null) {
      if (LdapUtils.hasDomain(user)) {
        filter.apply(LdapUtils.extractUserName(user))
      }
      else {
        filter.apply(user)
      }
    }
  }
}
