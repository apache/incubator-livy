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

package org.apache.livy.thriftserver.ui

import java.text.SimpleDateFormat

import org.apache.livy.server.JsonServlet
import org.apache.livy.thriftserver.LivyThriftServer


class ThriftJsonServlet(val basePath: String) extends JsonServlet {

  private val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z")

  case class SessionInfo(
      sessionId: String,
      livySessionId: String,
      owner: String,
      createdAt: String)

  get("/sessions") {
    val thriftSessions = LivyThriftServer.getInstance.map { server =>
      val sessionManager = server.getSessionManager
      sessionManager.getSessions.map { sessionHandle =>
        val info = sessionManager.getSessionInfo(sessionHandle)
        SessionInfo(sessionHandle.getSessionId.toString,
          sessionManager.livySessionId(sessionHandle).map(_.toString).getOrElse(""),
          info.username,
          df.format(info.creationTime))
      }.toSeq
    }.getOrElse(Seq.empty)
    val from = params.get("from").map(_.toInt).getOrElse(0)
    val size = params.get("size").map(_.toInt).getOrElse(100)

    Map(
      "from" -> from,
      "total" -> thriftSessions.length,
      "sessions" -> thriftSessions.view(from, from + size))
  }
}
