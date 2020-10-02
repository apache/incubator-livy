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

import java.util.concurrent.SynchronousQueue
import java.util.concurrent.TimeUnit
import javax.ws.rs.HttpMethod

import org.apache.hive.service.rpc.thrift.TCLIService
import org.apache.hive.service.server.ThreadFactoryWithGarbageCleanup
import org.apache.thrift.protocol.TBinaryProtocol
import org.eclipse.jetty.server.HttpConfiguration
import org.eclipse.jetty.server.HttpConnectionFactory
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.ServerConnector
import org.eclipse.jetty.server.handler.gzip.GzipHandler
import org.eclipse.jetty.servlet.ServletContextHandler
import org.eclipse.jetty.servlet.ServletHolder
import org.eclipse.jetty.util.ssl.SslContextFactory
import org.eclipse.jetty.util.thread.ExecutorThreadPool

import org.apache.livy.LivyConf
import org.apache.livy.thriftserver.LivyCLIService
import org.apache.livy.thriftserver.auth.AuthFactory

/**
 * This class is ported from Hive. We cannot reuse Hive's one because we need to use the
 * `LivyCLIService`, `LivyConf` and `AuthFacotry` instead of Hive's one.
 */
class ThriftHttpCLIService(
    override val cliService: LivyCLIService,
    val oomHook: Runnable)
  extends ThriftCLIService(cliService, classOf[ThriftHttpCLIService].getSimpleName) {

  protected var server: Server = _

  override lazy val hiveAuthFactory = new AuthFactory(livyConf)

  /**
   * Configure Jetty to serve http requests. Example of a client connection URL:
   * http://localhost:10000/servlets/thrifths2/ A gateway may cause actual target URL to differ,
   * e.g. http://gateway:port/livy/servlets/thrifths2/
   */
  protected def initServer(): Unit = {
    try {
      // Server thread pool
      // Start with minWorkerThreads, expand till maxWorkerThreads and reject subsequent requests
      val executorService = new ThreadPoolExecutorWithOomHook(
        minWorkerThreads,
        maxWorkerThreads,
        workerKeepAliveTime,
        TimeUnit.SECONDS,
        new SynchronousQueue[Runnable],
        new ThreadFactoryWithGarbageCleanup("LivyThriftserver-HttpHandler-Pool"),
        oomHook)
      val threadPool = new ExecutorThreadPool(executorService)
      // HTTP Server
      server = new Server(threadPool)
      val conf = new HttpConfiguration
      // Configure header size
      val requestHeaderSize = livyConf.getInt(LivyConf.THRIFT_HTTP_REQUEST_HEADER_SIZE)
      val responseHeaderSize = livyConf.getInt(LivyConf.THRIFT_HTTP_RESPONSE_HEADER_SIZE)
      conf.setRequestHeaderSize(requestHeaderSize)
      conf.setResponseHeaderSize(responseHeaderSize)
      val http = new HttpConnectionFactory(conf)
      val useSsl = livyConf.getBoolean(LivyConf.THRIFT_USE_SSL)
      val schemeName = if (useSsl) "https" else "http"
      // Change connector if SSL is used
      val connector = if (useSsl) {
          val keyStorePath = livyConf.get(LivyConf.SSL_KEYSTORE).trim
          if (keyStorePath.isEmpty) {
            throw new IllegalArgumentException(
              s"${LivyConf.SSL_KEYSTORE.key} Not configured for SSL connection")
          }
          val keyStorePassword = getKeyStorePassword()
          val keystoreType = livyConf.get(LivyConf.SSL_KEYSTORE_TYPE)
          val sslContextFactory = new SslContextFactory
          val excludedProtocols = livyConf.get(LivyConf.THRIFT_SSL_PROTOCOL_BLACKLIST).split(",")
          info(s"HTTP Server SSL: adding excluded protocols: $excludedProtocols")
          sslContextFactory.addExcludeProtocols(excludedProtocols: _*)
          info("HTTP Server SSL: SslContextFactory.getExcludeProtocols = " +
            sslContextFactory.getExcludeProtocols)
          sslContextFactory.setKeyStorePath(keyStorePath)
          sslContextFactory.setKeyStorePassword(keyStorePassword)
          sslContextFactory.setKeyStoreType(keystoreType)
          new ServerConnector(server, sslContextFactory, http)
        } else {
          new ServerConnector(server, http)
        }
      connector.setPort(portNum)
      // Linux: yes, Windows:no
      connector.setReuseAddress(true)
      val maxIdleTime = livyConf.getTimeAsMs(LivyConf.THRIFT_HTTP_MAX_IDLE_TIME).asInstanceOf[Int]
      connector.setIdleTimeout(maxIdleTime)
      server.addConnector(connector)
      // Thrift configs
      val processor = new TCLIService.Processor[TCLIService.Iface](this)
      val protocolFactory = new TBinaryProtocol.Factory
      // Set during the init phase of LivyThriftserver if auth mode is kerberos
      // UGI for the livy/_HOST (kerberos) principal
      val serviceUGI = cliService.getServiceUGI
      // UGI for the http/_HOST (SPNego) principal
      val httpUGI = cliService.getHttpUGI
      val authType = livyConf.get(LivyConf.THRIFT_AUTHENTICATION)
      val thriftHttpServlet = new ThriftHttpServlet(
        processor,
        protocolFactory,
        authType,
        serviceUGI,
        httpUGI,
        hiveAuthFactory,
        livyConf)
      // Context handler
      val context = new ServletContextHandler(ServletContextHandler.SESSIONS)
      context.setContextPath("/")
      if (livyConf.getBoolean(LivyConf.THRIFT_XSRF_FILTER_ENABLED)) {
        // Filtering does not work here currently, doing filter in ThriftHttpServlet
        debug("XSRF filter enabled")
      } else {
        warn("XSRF filter disabled")
      }
      val httpPath = getHttpPath(livyConf.get(LivyConf.THRIFT_HTTP_PATH))
      if (livyConf.getBoolean(LivyConf.THRIFT_XSRF_FILTER_ENABLED)) {
        val gzipHandler = new GzipHandler
        gzipHandler.setHandler(context)
        gzipHandler.addIncludedMethods(HttpMethod.POST)
        gzipHandler.addIncludedMimeTypes(ThriftHttpCLIService.APPLICATION_THRIFT)
        server.setHandler(gzipHandler)
      } else {
        server.setHandler(context)
      }
      context.addServlet(new ServletHolder(thriftHttpServlet), httpPath)
      // TODO: check defaults: maxTimeout, keepalive, maxBodySize,
      // bodyRecieveDuration, etc.
      // Finally, start the server
      server.start()
      info(s"Started ${classOf[ThriftHttpCLIService].getSimpleName} in $schemeName mode on port " +
        s"$portNum path=$httpPath with $minWorkerThreads...$maxWorkerThreads worker threads")
    } catch {
      case e: Exception => throw new RuntimeException("Failed to init HttpServer", e)
    }
  }

  override def run(): Unit = {
    try {
      server.join()
    } catch {
      case t: InterruptedException =>
        // This is likely a shutdown
        info(s"Caught ${t.getClass.getSimpleName}. Shutting down thrift server.")
      case t: Throwable =>
        error(s"Exception caught by ${this.getClass.getSimpleName}. Exiting.", t)
        System.exit(-1)
    }
  }

  /**
   * The config parameter can be like "path", "/path", "/path/", "path&#47;*",
   * "/path1/path2&#47;*" and so on. httpPath should end up as "&#47;*", "/path&#47;*" or
   * "/path1/../pathN&#47;*"
   */
  private def getHttpPath(httpPath: String): String = {
    Option(httpPath) match {
      case None | Some("") => "/*"
      case Some(path) =>
        val withStartingSlash = if (!path.startsWith("/")) {
            s"/$path"
          } else {
            path
          }
        if (httpPath.endsWith("/")) {
          s"$withStartingSlash*"
        } else if (!httpPath.endsWith("/*")) {
          s"$withStartingSlash/*"
        } else {
          withStartingSlash
        }
    }
  }

  protected def stopServer(): Unit = {
    if ((server != null) && server.isStarted) {
      try {
        server.stop()
        server = null
        info("Thrift HTTP server has been stopped")
      } catch {
      case e: Exception =>
      error("Error stopping HTTP server: ", e)
      }
    }
  }
}

object ThriftHttpCLIService {
  private val APPLICATION_THRIFT = "application/x-thrift"
}
