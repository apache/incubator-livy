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

package org.apache.livy.server

import java.security.AccessControlException

import org.apache.livy.{LivyConf, Logging}

private[livy] class AccessManager(conf: LivyConf) extends Logging {
  private val aclsOn = conf.getBoolean(LivyConf.ACCESS_CONTROL_ENABLED)

  private val WILDCARD_ACL = "*"

  private val superUsers = conf.configToSeq(LivyConf.SUPERUSERS)
  private val modifyUsers = conf.configToSeq(LivyConf.ACCESS_CONTROL_MODIFY_USERS)
  private val viewUsers = conf.configToSeq(LivyConf.ACCESS_CONTROL_VIEW_USERS)
  private val allowedUsers = conf.configToSeq(LivyConf.ACCESS_CONTROL_ALLOWED_USERS).toSet

  private val viewAcls = (superUsers ++ modifyUsers ++ viewUsers).toSet
  private val modifyAcls = (superUsers ++ modifyUsers).toSet
  private val superAcls = superUsers.toSet
  private val allowedAcls = (superUsers ++ modifyUsers ++ viewUsers ++ allowedUsers).toSet

  info(s"AccessControlManager acls ${if (aclsOn) "enabled" else "disabled"};" +
    s"users with view permission: ${viewUsers.mkString(", ")};" +
    s"users with modify permission: ${modifyUsers.mkString(", ")};" +
    s"users with super permission: ${superUsers.mkString(", ")};" +
    s"other allowed users: ${allowedUsers.mkString(", ")}")

  /**
   * Check whether the given user has view access to the REST APIs.
   */
  def checkViewPermissions(user: String): Boolean = {
    debug(s"user=$user aclsOn=$aclsOn viewAcls=${viewAcls.mkString(", ")}")
    if (!aclsOn || user == null || viewAcls.contains(WILDCARD_ACL) || viewAcls.contains(user)) {
      true
    } else {
      false
    }
  }

  /**
   * Check whether the give user has modification access to the REST APIs.
   */
  def checkModifyPermissions(user: String): Boolean = {
    debug(s"user=$user aclsOn=$aclsOn modifyAcls=${modifyAcls.mkString(", ")}")
    if (!aclsOn || user == null || modifyAcls.contains(WILDCARD_ACL) || modifyAcls.contains(user)) {
      true
    } else {
      false
    }
  }

  /**
   * Check whether the give user has super access to the REST APIs. This will always be checked
   * no matter acls is on or off.
   */
  def checkSuperUser(user: String): Boolean = {
    debug(s"user=$user aclsOn=$aclsOn superAcls=${superAcls.mkString(", ")}")
    if (user == null || superUsers.contains(WILDCARD_ACL) || superUsers.contains(user)) {
      true
    } else {
      false
    }
  }

  /**
   * Check whether the given user has the permission to access REST APIs.
   */
  def isUserAllowed(user: String): Boolean = {
    debug(s"user=$user aclsOn=$aclsOn, allowedAcls=${allowedAcls.mkString(", ")}")
    if (!aclsOn || user == null || allowedAcls.contains(WILDCARD_ACL) ||
      allowedAcls.contains(user)) {
      true
    } else {
      false
    }
  }

  /**
   * Check whether access control is enabled or not.
   */
  def isAccessControlOn: Boolean = aclsOn

  /**
   * Checks that the requesting user can impersonate the target user.
   * If the user does not have permission to impersonate, then throws an `AccessControlException`.
   *
   * @return The user that should be impersonated. That can be the target user if defined, the
   *         request's user - which may not be defined - otherwise, or `None` if impersonation is
   *         disabled.
   */
  def checkImpersonation(
      target: Option[String],
      requestUser: String): Option[String] = {
    if (conf.getBoolean(LivyConf.IMPERSONATION_ENABLED)) {
      if (!target.forall(hasSuperAccess(_, requestUser))) {
        throw new AccessControlException(
          s"User '$requestUser' not allowed to impersonate '$target'.")
      }
      target.orElse(Option(requestUser))
    } else {
      None
    }
  }

  /**
   * Check that the requesting user has admin access to resources owned by the given target user.
   */
  def hasSuperAccess(target: String, requestUser: String): Boolean = {
    requestUser == target || checkSuperUser(requestUser)
  }

  /**
   * Check that the request's user has modify access to resources owned by the given target user.
   */
  def hasModifyAccess(target: String, requestUser: String): Boolean = {
    requestUser == target || checkModifyPermissions(requestUser)
  }

  /**
   * Check that the request's user has view access to resources owned by the given target user.
   */
  def hasViewAccess(target: String, requestUser: String): Boolean = {
    requestUser == target || checkViewPermissions(requestUser)
  }
}
