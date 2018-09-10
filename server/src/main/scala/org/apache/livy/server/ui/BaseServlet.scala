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

trait BaseServlet extends ScalatraServlet {
  protected trait Page {
    val name: String
    def getNavTabs: Seq[Node] = Seq.empty
  }
  protected case class SimplePage(name: String) extends Page

  def basePath: String

  private def getHeader(pageName: String): Seq[Node] =
    <head>
      <link rel="stylesheet"
            href={basePath + "/static/css/bootstrap.min.css"}
            type="text/css"/>
      <link rel="stylesheet"
            href={basePath + "/static/css/dataTables.bootstrap.min.css"}
            type="text/css"/>
      <link rel="stylesheet" href={basePath + "/static/css/livy-ui.css"} type="text/css"/>
      <script src={basePath + "/static/js/jquery-3.2.1.min.js"}></script>
      <script src={basePath + "/static/js/bootstrap.min.js"}></script>
      <script src={basePath + "/static/js/jquery.dataTables.min.js"}></script>
      <script src={basePath + "/static/js/dataTables.bootstrap.min.js"}></script>
      <script src={basePath + "/static/js/livy-ui.js"}></script>
      <script type="text/javascript">
        setBasePath({"'" + basePath + "'"});
      </script>
      <title>Livy - {pageName}</title>
    </head>

  private def wrapNavTabs(tabs: Seq[Node]): Seq[Node] =
    <nav class="navbar navbar-default">
      <div class="container-fluid">
        <div class="navbar-header">
          <a class="navbar-brand" href={basePath + "/ui"}>
            <img alt="Livy" src={basePath + "/static/img/livy-mini-logo.png"}/>
          </a>
        </div>
        <div class="collapse navbar-collapse">
          <ul class="nav navbar-nav">
            {tabs}
          </ul>
        </div>
      </div>
    </nav>

  protected def createPage(pageInfo: Page, pageContents: Seq[Node]): Seq[Node] =
    <html>
      {getHeader(pageInfo.name)}
      <body>
        <div class="container-fluid">
          {getNavBar(pageInfo)}
          {pageContents}
        </div>
      </body>
    </html>

  private def getNavBar(page: Page): Seq[Node] = wrapNavTabs(page.getNavTabs)

  notFound {
    createPage(SimplePage("404"), <h3>404 No Such Page</h3>)
  }

}
