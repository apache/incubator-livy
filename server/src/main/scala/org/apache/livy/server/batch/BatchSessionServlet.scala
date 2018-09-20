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

import org.scalatra.{BadRequest, Ok}
import org.scalatra.servlet.FileUploadSupport

import org.apache.livy.LivyConf
import org.apache.livy.server.{AccessManager, SessionServlet}
import org.apache.livy.server.recovery.SessionStore
import org.apache.livy.sessions.BatchSessionManager
import org.apache.livy.utils.AppInfo

case class BatchSessionView(
  id: Long,
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
    with FileUploadSupport
{

  override protected def createSession(req: HttpServletRequest): BatchSession = {
    val createRequest = bodyAs[CreateBatchRequest](req)
    val proxyUser = checkImpersonation(createRequest.proxyUser, req)
    BatchSession.create(
      sessionManager.nextId(), createRequest, livyConf, remoteUser(req), proxyUser, sessionStore)
  }

  override protected[batch] def clientSessionView(
      session: BatchSession,
      req: HttpServletRequest): Any = {
    val logs =
      if (hasViewAccess(session.owner, req)) {
        val lines = session.logLines()

        val size = 10
        val from = math.max(0, lines.length - size)
        val until = from + size

        lines.view(from, until).toSeq
      } else {
        Nil
      }
    BatchSessionView(session.id, session.state.toString, session.appId, session.appInfo, logs)
  }

  get("/:id/start") {
    withModifyAccessSession { lsession =>
      sessionManager.register(lsession.startDelayed())
      Ok(Map("msg" -> "started"))
    }
  }

  post("/:id/add-jar") {
    withModifyAccessSession { lsession =>
      fileParams.get("jar") match {
        case Some(file) =>
          lsession.addJar(file.getInputStream, file.name)
        case None =>
          BadRequest("No jar sent!")
      }
    }
  }

  post("/:id/add-pyfile") {
    withModifyAccessSession { lsession =>
      fileParams.get("file") match {
        case Some(file) =>
          lsession.addPyFile(file.getInputStream, file.name)
        case None =>
          BadRequest("No file sent!")
      }
    }
  }

  post("/:id/add-file") {
    withModifyAccessSession { lsession =>
      fileParams.get("file") match {
        case Some(file) =>
          lsession.addFile(file.getInputStream, file.name)
        case None =>
          BadRequest("No file sent!")
      }
    }
  }

  post("/:id/set-file") {
    withModifyAccessSession { lsession =>
      fileParams.get("file") match {
        case Some(file) =>
          lsession.setFile(file.getInputStream, file.name)
        case None =>
          BadRequest("No file sent!")
      }
    }
  }
}
