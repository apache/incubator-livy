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

package org.apache.livy.thriftserver

import javax.servlet.Servlet

import org.apache.livy.LivyConf
import org.apache.livy.server.{AccessManager, ThriftServerFactory}
import org.apache.livy.server.recovery.SessionStore
import org.apache.livy.sessions.InteractiveSessionManager
import org.apache.livy.thriftserver.ui.ThriftJsonServlet

class ThriftServerFactoryImpl extends ThriftServerFactory {
  override def start(
      livyConf: LivyConf,
      livySessionManager: InteractiveSessionManager,
      sessionStore: SessionStore,
      accessManager: AccessManager): Unit = {
    if (LivyThriftServer.getInstance.isDefined) {
      throw new RuntimeException(s"A ${classOf[LivyThriftServer].getName} has been already " +
        s"started, so a new one cannot be started.")
    }
    LivyThriftServer.start(livyConf, livySessionManager, sessionStore, accessManager)
  }

  override def stop(): Unit = {
    assert(LivyThriftServer.getInstance.isDefined)
    LivyThriftServer.getInstance.foreach(_.stop())
  }

  override def getServlet(basePath: String): Servlet = new ThriftJsonServlet(basePath)

  override def getServletMappings: Seq[String] = Seq("/thriftserver/*")
}
