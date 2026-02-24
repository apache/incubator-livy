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

package org.apache.livy.thriftserver.cli

import java.net.InetSocketAddress
import java.util
import java.util.concurrent._
import javax.net.ssl.SSLServerSocket

import org.apache.hive.service.cli.HiveSQLException
import org.apache.hive.service.server.ThreadFactoryWithGarbageCleanup
import org.apache.thrift.TProcessorFactory
import org.apache.thrift.protocol.{TBinaryProtocol, TProtocol}
import org.apache.thrift.server.{ServerContext, TServer, TServerEventHandler, TThreadPoolServer}
import org.apache.thrift.transport.{TServerSocket, TSSLTransportFactory, TTransport, TTransportFactory}

import org.apache.livy.LivyConf
import org.apache.livy.thriftserver.LivyCLIService
import org.apache.livy.thriftserver.auth.AuthFactory

/**
 * This class is ported from Hive. We cannot reuse Hive's one because we need to use the
 * `LivyCLIService`, `LivyConf` and `AuthFacotry` instead of Hive's one.
 */
class ThriftBinaryCLIService(override val cliService: LivyCLIService, val oomHook: Runnable)
    extends ThriftCLIService(cliService, classOf[ThriftBinaryCLIService].getSimpleName) {

  protected var server: TServer = _

  override lazy val hiveAuthFactory = new AuthFactory(livyConf)

  protected def initServer(): Unit = {
    try {
      // Server thread pool
      val executorService = new ThreadPoolExecutorWithOomHook(
        minWorkerThreads,
        maxWorkerThreads,
        workerKeepAliveTime,
        TimeUnit.SECONDS,
        new SynchronousQueue[Runnable],
        new ThreadFactoryWithGarbageCleanup("LivyThriftserver-Handler-Pool"),
        oomHook)
      // Thrift configs
      val transportFactory: TTransportFactory = hiveAuthFactory.getAuthTransFactory
      val processorFactory: TProcessorFactory = hiveAuthFactory.getAuthProcFactory(this)
      var serverSocket: TServerSocket = null
      val serverAddress = if (hiveHost == null || hiveHost.isEmpty) {
          new InetSocketAddress(portNum) // Wildcard bind
        } else {
          new InetSocketAddress(hiveHost, portNum)
        }
      if (!livyConf.getBoolean(LivyConf.THRIFT_USE_SSL)) {
        serverSocket = new TServerSocket(serverAddress)
      } else {
        val sslVersionBlacklist = new util.ArrayList[String]
        livyConf.get(LivyConf.THRIFT_SSL_PROTOCOL_BLACKLIST).split(",").foreach { sslVersion =>
          sslVersionBlacklist.add(sslVersion.trim.toLowerCase)
        }
        val keyStorePath = livyConf.get(LivyConf.SSL_KEYSTORE).trim
        if (keyStorePath.isEmpty) {
          throw new IllegalArgumentException(
            s"${LivyConf.SSL_KEYSTORE.key} Not configured for SSL connection")
        }
        val keyStorePassword = getKeyStorePassword()
        val keystoreType = livyConf.get(LivyConf.SSL_KEYSTORE_TYPE)
        val params = new TSSLTransportFactory.TSSLTransportParameters
        params.setKeyStore(keyStorePath, keyStorePassword, null, keystoreType)
        serverSocket =
          TSSLTransportFactory.getServerSocket(portNum, 0, serverAddress.getAddress, params)
        if (serverSocket.getServerSocket.isInstanceOf[SSLServerSocket]) {
          val sslServerSocket = serverSocket.getServerSocket.asInstanceOf[SSLServerSocket]
          val enabledProtocols = sslServerSocket.getEnabledProtocols.filter { protocol =>
              if (sslVersionBlacklist.contains(protocol.toLowerCase)) {
                debug(s"Disabling SSL Protocol: $protocol")
                false
              } else {
                true
              }
            }
          sslServerSocket.setEnabledProtocols(enabledProtocols)
          info(s"SSL Server Socket Enabled Protocols: ${sslServerSocket.getEnabledProtocols}")
        }
      }
      // Server args
      val maxMessageSize = livyConf.getInt(LivyConf.THRIFT_MAX_MESSAGE_SIZE)
      val requestTimeout =
        livyConf.getTimeAsMs(LivyConf.THRIFT_LOGIN_TIMEOUT).asInstanceOf[Int]
      val beBackoffSlotLength =
        livyConf.getTimeAsMs(LivyConf.THRIFT_LOGIN_BEBACKOFF_SLOT_LENGTH).asInstanceOf[Int]
      val sargs = new TThreadPoolServer.Args(serverSocket)
        .processorFactory(processorFactory)
        .transportFactory(transportFactory)
        .protocolFactory(new TBinaryProtocol.Factory)
        .inputProtocolFactory(
          new TBinaryProtocol.Factory(true, true, maxMessageSize, maxMessageSize))
        .requestTimeout(requestTimeout)
        .requestTimeoutUnit(TimeUnit.MILLISECONDS)
        .beBackoffSlotLength(beBackoffSlotLength)
        .beBackoffSlotLengthUnit(TimeUnit.MILLISECONDS)
        .executorService(executorService)
      // TCP Server
      server = new TThreadPoolServer(sargs)
      server.setServerEventHandler(new TServerEventHandler() {
        override def createContext(input: TProtocol, output: TProtocol): ServerContext = {
          new ThriftCLIServerContext
        }

        override def deleteContext(
           serverContext: ServerContext,
           input: TProtocol,
           output: TProtocol): Unit = {
          val context = serverContext.asInstanceOf[ThriftCLIServerContext]
          val sessionHandle = context.getSessionHandle
          if (sessionHandle != null) {
            info("Session disconnected without closing properly. ")
            try {
              val close = livyConf.getBoolean(LivyConf.THRIFT_CLOSE_SESSION_ON_DISCONNECT)
              info("Closing the session: " + sessionHandle)
              if (close) {
                cliService.closeSession(sessionHandle)
              }
            } catch {
              case e: HiveSQLException => warn("Failed to close session: " + e, e)
            }
          }
        }

        override def preServe(): Unit = {}

        override def processContext(
            serverContext: ServerContext,
            input: TTransport,
            output: TTransport): Unit = {
          currentServerContext.set(serverContext)
        }
      })
      info(s"Starting ${classOf[ThriftBinaryCLIService].getSimpleName} on port $portNum " +
        s"with $minWorkerThreads...$maxWorkerThreads worker threads")
    } catch {
      case e: Exception => throw new RuntimeException("Failed to init thrift server", e)
    }
  }

  override def run(): Unit = {
    try {
      server.serve()
    } catch {
      case t: InterruptedException =>
        // This is likely a shutdown
        info(s"Caught ${t.getClass.getSimpleName}. Shutting down thrift server.")
      case t: Throwable =>
        error(s"Exception caught by ${this.getClass.getSimpleName}. Exiting.", t)
        System.exit(-1)
    }
  }

  protected def stopServer(): Unit = {
    server.stop()
    server = null
    info("Thrift server has stopped")
  }
}
