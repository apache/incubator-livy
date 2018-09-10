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

package org.apache.livy.server.ui

import scala.xml.Node

import org.apache.livy.LivyConf

class UIServlet(val basePath: String, livyConf: LivyConf) extends BaseServlet {
  before() { contentType = "text/html" }

  def thriftServerNavTab: Seq[Node] = if (livyConf.getBoolean(LivyConf.THRIFT_SERVER_ENABLED)) {
    <li><a href={basePath + "/thriftserver/"}>Thrift Server</a></li>
  } else {
    Seq.empty
  }

  private case class AllSessionsPage(name: String = "Sessions") extends Page {
    override def getNavTabs: Seq[Node] = thriftServerNavTab ++
      <li class="active"><a href="#">Sessions</a></li>
  }
  private case class SessionPage(id: Int) extends Page {
    val name: String = "Session " + id
    override def getNavTabs: Seq[Node] = {
      thriftServerNavTab ++
        <li><a href={basePath + "/ui"}>Sessions</a></li> ++
        <li class="active"><a href="#">{name}</a></li>
    }
  }
  private case class LogPage(sessionType: String, id: Int) extends Page {
    val sessionName: String = sessionType + " " + id
    val name: String = sessionName + " Log"
    override def getNavTabs: Seq[Node] = {
      val sessionLink = if (sessionType == "Session") {
        basePath + "/ui/session/" + id
      } else "#"
      thriftServerNavTab ++
        <li><a href={basePath + "/ui"}>Sessions</a></li> ++
        <li><a href={sessionLink}>{sessionName}</a></li> ++
        <li class="active"><a href="#">Log</a></li>
    }
  }

  get("/") {
    val content =
      <div id="all-sessions">
        <div id="interactive-sessions"></div>
        <div id="batches"></div>
        <script src={basePath + "/static/js/all-sessions.js"}></script>
      </div>

    createPage(AllSessionsPage(), content)
  }

  get("/session/:id") {
    val content =
      <div id="session-page">
        <div id="session-summary"></div>
        <div id="session-statements"></div>
        <script src={basePath + "/static/js/session.js"}></script>
      </div>

    createPage(SessionPage(params("id").toInt), content)
  }

  private def getLogPage(page: LogPage): Seq[Node] = {
    val content =
      <div id="log-page">
        <div id="session-log"></div>
        <script src={basePath + "/static/js/session-log.js"}></script>
      </div>

    createPage(page, content)
  }

  get("/session/:id/log") {
    getLogPage(LogPage("Session", params("id").toInt))
  }

  get("/batch/:id/log") {
    getLogPage(LogPage("Batch", params("id").toInt))
  }
}
