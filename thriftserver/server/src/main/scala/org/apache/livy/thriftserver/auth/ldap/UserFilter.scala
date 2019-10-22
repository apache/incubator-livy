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

import javax.security.sasl.AuthenticationException

import org.apache.livy.{LivyConf, Logging}
import org.apache.livy.server.auth.LdapUtils

/**
 * Filter out all users that are not in the provided in Livy configuration list.
 */
class UserFilter(conf: LivyConf) extends Filter with Logging {
  private val userFilterStr = conf.get(LivyConf.THRIFT_LDAP_AUTHENTICATION_USERFILTER)
  private val userFilter: Set[String] =
      if (userFilterStr != null) userFilterStr.split(",").toSet else Set()

  @throws[AuthenticationException]
  def apply(user: String): Unit = {
    if (!userFilter.isEmpty) {
      info("Authenticating user '{}' using user filter", user)
      val userName = LdapUtils.extractUserName(user).toLowerCase
      if (!userFilter.contains(userName)) {
        info("Authentication failed based on user membership")
        throw new AuthenticationException(
          "Authentication failed: User not a member of specified list")
      }
    }
  }
}
