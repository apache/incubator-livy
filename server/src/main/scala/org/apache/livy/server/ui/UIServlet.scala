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

import org.apache.livy.LivyConf

class UIServlet(val basePath: String, livyConf: LivyConf) extends ScalatraServlet {
  before() { contentType = "text/html" }

  private trait Page {
    val name: String
    def getNavCrumbs: Seq[Node] = Seq.empty
  }
  private case class SimplePage(name: String) extends Page

  private case class AllSessionsPage(name: String = "Sessions") extends Page {
    override def getNavCrumbs: Seq[Node] = <li class="active"><a href="#">Sessions</a></li>
  }
  private case class SessionPage(id: Int) extends Page {
    val name: String = "Session " + id
    override def getNavCrumbs: Seq[Node] = {
        <li><a href={basePath + "/ui"}>Sessions</a></li> ++
          <li class="active"><a href="#">{name}</a></li>
    }
  }
  private case class LogPage(sessionType: String, id: Int) extends Page {
    val sessionName: String = sessionType + " " + id
    val name: String = sessionName + " Log"
    override def getNavCrumbs: Seq[Node] = {
      val sessionLink = if (sessionType == "Session") {
        basePath + "/ui/session/" + id
      } else {
        "#"
      }
      <li><a href={basePath + "/ui"}>Sessions</a></li> ++
        <li><a href={sessionLink}>{sessionName}</a></li> ++
        <li class="active"><a href="#">Log</a></li>
    }
  }

  private def getHeader(pageName: String): Seq[Node] =
    <head>
      <link rel="stylesheet"
            href={basePath + "/static/css/bootstrap.min.css"}
            type="text/css"/>
      <link rel="stylesheet"
            href={basePath + "/static/css/dataTables.bootstrap.min.css"}
            type="text/css"/>
      <link rel="stylesheet" href={basePath + "/static/css/livy-ui.css"} type="text/css"/>
      <script src={basePath + "/static/js/jquery-3.4.1.min.js"}></script>
      <script src={basePath + "/static/js/bootstrap.min.js"}></script>
      <script src={basePath + "/static/js/jquery.dataTables.min.js"}></script>
      <script src={basePath + "/static/js/dataTables.bootstrap.min.js"}></script>
      <script src={basePath + "/static/js/livy-ui.js"}></script>
      <script type="text/javascript">
        setBasePath({"'" + basePath + "'"});
      </script>
      <title>Livy - {pageName}</title>
    </head>

  private def wrapNavCrumbs(crumbs: Seq[Node]): Seq[Node] =
    <nav class="navbar navbar-default">
      <div class="container-fluid">
        <div class="navbar-header">
          <a class="navbar-brand" href={basePath + "/ui"}>
            <img alt="Livy" src={basePath + "/static/img/livy-mini-logo.png"}/>
          </a>
        </div>
        <div class="collapse navbar-collapse">
          <ul class="nav navbar-nav">
            {crumbs}
          </ul>
        </div>
      </div>
    </nav>

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

  private def getNavBar(page: Page): Seq[Node] = wrapNavCrumbs(page.getNavCrumbs)

  notFound {
    createPage(SimplePage("404"), <h3>404 No Such Page</h3>)
  }

  private def thriftSessionsTable: Seq[Node] = {
    if (livyConf.getBoolean(LivyConf.THRIFT_SERVER_ENABLED)) {
      <div id="thrift-sessions"></div> ++
        <script src={s"$basePath/static/js/thrift-sessions.js"}></script>
    } else {
      Seq.empty
    }
  }

  get("/") {
    val content =
      <div id="all-sessions">
        <div id="interactive-sessions"></div>
        <div id="batches"></div>
        {thriftSessionsTable}
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
