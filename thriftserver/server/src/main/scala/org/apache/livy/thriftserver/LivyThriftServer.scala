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

import java.security.PrivilegedExceptionAction

import org.apache.hadoop.security.UserGroupInformation

import org.apache.livy.{LivyConf, Logging}
import org.apache.livy.server.AccessManager
import org.apache.livy.server.interactive.InteractiveSession
import org.apache.livy.server.recovery.SessionStore
import org.apache.livy.sessions.InteractiveSessionManager
import org.apache.livy.thriftserver.cli.{ThriftBinaryCLIService, ThriftHttpCLIService}

/**
 * The main entry point for the Livy thrift server leveraging HiveServer2. Starts up a
 * `HiveThriftServer2` thrift server.
 */
object LivyThriftServer extends Logging {

  // Visible for testing
  private[thriftserver] var thriftServerThread: Thread = _
  private var thriftServer: LivyThriftServer = _

  def start(
      livyConf: LivyConf,
      livySessionManager: InteractiveSessionManager,
      sessionStore: SessionStore,
      accessManager: AccessManager): Unit = synchronized {
    if (thriftServerThread == null) {
      info("Starting LivyThriftServer")
      val ugi = UserGroupInformation.getCurrentUser
      val runThriftServer = new Runnable {
        override def run(): Unit = {
          try {
            thriftServer = new LivyThriftServer(
              livyConf,
              livySessionManager,
              sessionStore,
              accessManager)
            if (UserGroupInformation.isSecurityEnabled) {
              ugi.doAs(new PrivilegedExceptionAction[Unit] {
                override def run(): Unit = {
                  doStart(livyConf)
                }
              })
            } else {
              doStart(livyConf)
            }
            info("LivyThriftServer started")
          } catch {
            case e: Exception =>
              error("Error starting LivyThriftServer", e)
          }
        }
      }
      thriftServerThread =
        new Thread(new ThreadGroup("thriftserver"), runThriftServer, "Livy-Thriftserver")
      thriftServerThread.start()
    } else {
      error("Livy Thriftserver is already started")
    }
  }

  private def doStart(livyConf: LivyConf): Unit = {
    thriftServer.init(livyConf)
    thriftServer.start()
  }

  private[thriftserver] def getInstance: Option[LivyThriftServer] = {
    Option(thriftServer)
  }

  // Used in testing
  def stopServer(): Unit = {
    if (thriftServerThread != null) {
      thriftServerThread.join()
    }
    thriftServerThread = null
    thriftServer.stop()
    thriftServer = null
  }

  def isHTTPTransportMode(livyConf: LivyConf): Boolean = {
    val transportMode = livyConf.get(LivyConf.THRIFT_TRANSPORT_MODE)
    transportMode != null && transportMode.equalsIgnoreCase("http")
  }
}


class LivyThriftServer(
    private[thriftserver] val livyConf: LivyConf,
    private[thriftserver] val livySessionManager: InteractiveSessionManager,
    private[thriftserver] val sessionStore: SessionStore,
    private[thriftserver] val accessManager: AccessManager)
  extends ThriftService(classOf[LivyThriftServer].getName) with Logging {

  val cliService = new LivyCLIService(this)

  override def init(livyConf: LivyConf): Unit = {
    addService(cliService)
    val server = this
    val oomHook = new Runnable() {
      override def run(): Unit = {
        server.stop()
      }
    }
    val thriftCLIService = if (LivyThriftServer.isHTTPTransportMode(livyConf)) {
      new ThriftHttpCLIService(cliService, oomHook)
    } else {
      new ThriftBinaryCLIService(cliService, oomHook)
    }
    addService(thriftCLIService)
    super.init(livyConf)
  }

  private[thriftserver] def getSessionManager = {
    cliService.getSessionManager
  }

  def isAllowedToUse(user: String, session: InteractiveSession): Boolean = {
    session.owner == user || accessManager.checkModifyPermissions(user)
  }

  override def stop(): Unit = {
    info("Shutting down LivyThriftServer")
    super.stop()
  }
}
