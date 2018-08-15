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

import scala.collection.JavaConverters._

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hive.service.server.HiveServer2
import org.scalatra.ScalatraServlet

import org.apache.livy.{LivyConf, Logging}
import org.apache.livy.server.AccessManager
import org.apache.livy.server.interactive.InteractiveSession
import org.apache.livy.server.recovery.SessionStore
import org.apache.livy.sessions.InteractiveSessionManager

/**
 * The main entry point for the Livy thrift server leveraging HiveServer2. Starts up a
 * `HiveThriftServer2` thrift server.
 */
object LivyThriftServer extends Logging {

  // Visible for testing
  private[thriftserver] var thriftServerThread: Thread = _
  private var thriftServer: LivyThriftServer = _

  private def hiveConf(livyConf: LivyConf): HiveConf = {
    val conf = new HiveConf()
    // Remove all configs coming from hive-site.xml which may be in the classpath for the Spark
    // applications to run.
    conf.getAllProperties.asScala.filter(_._1.startsWith("hive.")).foreach { case (key, _) =>
      conf.unset(key)
    }
    livyConf.asScala.foreach {
      case nameAndValue if nameAndValue.getKey.startsWith("livy.hive") =>
        conf.set(nameAndValue.getKey.stripPrefix("livy."), nameAndValue.getValue)
      case _ => // Ignore
    }
    conf
  }

  def start(
      livyConf: LivyConf,
      livySessionManager: InteractiveSessionManager,
      sessionStore: SessionStore,
      accessManager: AccessManager): Unit = synchronized {
    if (thriftServerThread == null) {
      info("Starting LivyThriftServer")
      val runThriftServer = new Runnable {
        override def run(): Unit = {
          try {
            thriftServer = new LivyThriftServer(
              livyConf,
              livySessionManager,
              sessionStore,
              accessManager)
            thriftServer.init(hiveConf(livyConf))
            thriftServer.start()
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
}


class LivyThriftServer(
      private[thriftserver] val livyConf: LivyConf,
      private[thriftserver] val livySessionManager: InteractiveSessionManager,
      private[thriftserver] val sessionStore: SessionStore,
      private val accessManager: AccessManager) extends HiveServer2 {
  override def init(hiveConf: HiveConf): Unit = {
    this.cliService = new LivyCLIService(this)
    super.init(hiveConf)
  }

  private[thriftserver] def getSessionManager(): LivyThriftSessionManager = {
    this.cliService.getSessionManager.asInstanceOf[LivyThriftSessionManager]
  }

  def isAllowedToUse(user: String, session: InteractiveSession): Boolean = {
    session.owner == user || accessManager.checkModifyPermissions(user)
  }
}
