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

package org.apache.livy.server.batch

import javax.servlet.http.HttpServletRequest

import org.apache.livy.LivyConf
import org.apache.livy.server.{AccessManager, SessionServlet}
import org.apache.livy.server.recovery.SessionStore
import org.apache.livy.sessions.BatchSessionManager
import org.apache.livy.utils.AppInfo

case class BatchSessionView(
  id: Long,
  name: Option[String],
  owner: String,
  proxyUser: Option[String],
  state: String,
  appId: Option[String],
  appInfo: AppInfo,
  log: Seq[String])

class BatchSessionServlet(
    sessionManager: BatchSessionManager,
    sessionStore: SessionStore,
    livyConf: LivyConf,
    accessManager: AccessManager)
  extends SessionServlet(sessionManager, livyConf, accessManager)
{

  override protected def createSession(req: HttpServletRequest): BatchSession = {
    val createRequest = bodyAs[CreateBatchRequest](req)
    val sessionId = sessionManager.nextId()
    val sessionName = createRequest.name
    BatchSession.create(
      sessionId,
      sessionName,
      createRequest,
      livyConf,
      accessManager,
      remoteUser(req),
      proxyUser(req, createRequest.proxyUser),
      sessionStore)
  }

  override protected[batch] def clientSessionView(
      session: BatchSession,
      req: HttpServletRequest): Any = {
    val logs =
      if (accessManager.hasViewAccess(session.owner,
                                      effectiveUser(req),
                                      session.proxyUser.getOrElse(""))) {
        val lines = session.logLines()

        val size = 10
        val from = math.max(0, lines.length - size)
        val until = from + size

        lines.view(from, until).toSeq
      } else {
        Nil
      }
    BatchSessionView(session.id, session.name, session.owner, session.proxyUser,
      session.state.toString, session.appId, session.appInfo, logs)
  }

}
