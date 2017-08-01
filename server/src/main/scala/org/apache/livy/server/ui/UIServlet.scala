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

import org.scalatra.ScalatraServlet

class UIServlet extends ScalatraServlet {
  before() { contentType = "text/html" }

  sealed trait Page { val name: String }
  private case class SimplePage(name: String) extends Page
  private case class AllSessionsPage(name: String = "Sessions") extends Page
  private case class SessionPage(id: Int) extends Page {
    val name: String = "Session " + id
  }
  private case class LogPage(sessionType: String, id: Int) extends Page {
    val sessionName: String = sessionType + " " + id
    val name: String = sessionName + " Log"
  }

  private def getHeader(pageName: String): Seq[Node] =
    <head>
      <link rel="stylesheet" href="/static/css/bootstrap.min.css" type="text/css"/>
      <link rel="stylesheet" href="/static/css/dataTables.bootstrap.min.css" type="text/css"/>
      <link rel="stylesheet" href="/static/css/livy-ui.css" type="text/css"/>
      <script src="/static/js/jquery-3.2.1.min.js"></script>
      <script src="/static/js/bootstrap.min.js"></script>
      <script src="/static/js/jquery.dataTables.min.js"></script>
      <script src="/static/js/dataTables.bootstrap.min.js"></script>
      <script src="/static/js/livy-ui.js"></script>
      <title>Livy - {pageName}</title>
    </head>

  private def wrapNavTabs(tabs: Seq[Node]): Seq[Node] =
    <nav class="navbar navbar-default">
      <div class="container-fluid">
        <div class="navbar-header">
          <a class="navbar-brand" href="/ui">
            <img alt="Livy" src="/static/img/livy-mini-logo.png"/>
          </a>
        </div>
        <div class="collapse navbar-collapse">
          <ul class="nav navbar-nav">
            {tabs}
          </ul>
        </div>
      </div>
    </nav>

  private def getNavBar(page: Page): Seq[Node] = {
    val tabs: Seq[Node] = page match {
      case _: AllSessionsPage => <li class="active"><a href="#">Sessions</a></li>
      case sessionPage: SessionPage => {
        <li><a href="/ui">Sessions</a></li> ++
          <li class="active"><a href="#">{sessionPage.name}</a></li>
      }
      case logPage: LogPage => {
        val sessionLink = if (logPage.sessionType == "Session") "/ui/session/" + logPage.id else "#"
        <li><a href="/ui">Sessions</a></li> ++
          <li><a href={sessionLink}>{logPage.sessionName}</a></li> ++
          <li class="active"><a href="#">Log</a></li>
      }
      case _ => Seq.empty
    }
    wrapNavTabs(tabs)
  }

  private def createPage(pageInfo: Page, pageContents: Seq[Node]): Seq[Node] =
    <html>
      {getHeader(pageInfo.name)}
      <body>
        <div class="container-fluid">
          {getNavBar(pageInfo)}
          {pageContents}
        </div>
      </body>
    </html>

  notFound {
    createPage(SimplePage("404"), <h3>404 No Such Page</h3>)
  }

  get("/") {
    val content =
      <div id="all-sessions">
        <div id="interactive-sessions"></div>
        <div id="batches"></div>
        <script src="/static/js/all-sessions.js"></script>
      </div>

    createPage(AllSessionsPage(), content)
  }

  get("/session/:id") {
    val content =
      <div id="session-page">
        <div id="session-summary"></div>
        <div id="session-statements"></div>
        <script src="/static/js/session.js"></script>
      </div>

    createPage(SessionPage(params("id").toInt), content)
  }

  private def getLogPage(page: LogPage): Seq[Node] = {
    val content =
      <div id="log-page">
        <div id="session-log"></div>
        <script src="/static/js/session-log.js"></script>
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
