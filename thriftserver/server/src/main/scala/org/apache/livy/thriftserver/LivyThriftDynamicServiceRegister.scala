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

import java.nio.charset.Charset

import org.apache.curator.framework.CuratorFramework
import org.apache.hive.common.util.HiveVersionInfo
import org.apache.hive.service.auth.HiveAuthConstants.AuthTypes
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.KeeperException

import org.apache.livy.thriftserver.cli.ThriftCLIService
import org.apache.livy.LivyConf
import org.apache.livy.Logging

class LivyThriftDynamicServiceRegister(server: LivyThriftServer, thriftCLIService: ThriftCLIService,
                                       zookeeperManager: LivyThriftZookeeperManager)
  extends ThriftService(classOf[LivyThriftDynamicServiceRegister].getName) with Logging {

  override def start(): Unit = {
    super.start()

    if (server.livyConf.getBoolean(LivyConf.THRIFT_SUPPORT_DYNAMIC_SERVICE_DISCOVERY)) {
      addServerInstanceToZooKeeper()
    }
  }

  private def addServerInstanceToZooKeeper(): Unit = {
    val rootNamespace = server.livyConf.get(LivyConf.THRIFT_ZOOKEEPER_NAMESPACE)
    val instanceURI = getServerInstanceURI(thriftCLIService)
    val znodePath = getZnodePath(rootNamespace, instanceURI)
    val publishConfigs = getPublishConfigs(server.livyConf)

    val zookeeperClient = zookeeperManager.getClient()
    createParent(zookeeperClient, rootNamespace)
    createZnode(zookeeperClient, znodePath, publishConfigs)
  }

  private def createZnode(zookeeperClient: CuratorFramework, znodePath: String,
                          publishConfigs: Map[String, String]): Unit = {
    try {
      val znodeData = publishConfigs.map(_.productIterator.mkString("=")).mkString(";")
      val znodeDataUTF8 = znodeData.getBytes(Charset.forName("UTF-8"))
      zookeeperClient.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
        .forPath(znodePath, znodeDataUTF8)
      info(s"Created a znode ${znodePath} on ZooKeeper for Livy ThriftServer")
    } catch {
      case e: Exception =>
        error("Unable to create a znode for this server instance", e)
        throw e
    }
  }

  private def createParent(zookeeperClient: CuratorFramework, rootNamespace: String): Unit = {
    try {
      zookeeperClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
        .forPath(LivyThriftZookeeperManager.ZOOKEEPER_PATH_SEPARATOR + rootNamespace)
      info(s"Created the root name space: $rootNamespace on ZooKeeper for Livy ThriftServer")
    } catch {
      case e: KeeperException =>
        if (e.code ne KeeperException.Code.NODEEXISTS) {
          error(s"Unable to create Livy ThriftServer " +
            s"namespace: $rootNamespace on ZooKeeper", e)
          throw e
        }
    }
  }

  private def getZnodePath(rootNamespace: String, instanceURI: String): String = {
    LivyThriftZookeeperManager.ZOOKEEPER_PATH_SEPARATOR + rootNamespace +
      LivyThriftZookeeperManager.ZOOKEEPER_PATH_SEPARATOR + "serverUri=" + instanceURI + ";" +
      "version=" + HiveVersionInfo.getVersion + ";" + "sequence="
  }

  private def getServerInstanceURI(thriftCLIService: ThriftCLIService): String = {
    return s"${getServerHostName()}:${getServerPortNumber()}"
  }

  private def getServerHostName(): String = {
    if ((thriftCLIService == null) || (thriftCLIService.getServerIPAddress == null)) {
      throw new Exception("Unable to get the server address; it hasn't been initialized yet.")
    }
    val thriftBindHost = server.livyConf.get(LivyConf.THRIFT_BIND_HOST)
    if (thriftBindHost != null && !thriftBindHost.isEmpty) {
      return thriftBindHost
    } else {
      return thriftCLIService.getServerIPAddress.getHostName
    }
  }

  private def getServerPortNumber(): Int = {
    if (thriftCLIService == null) {
      throw new Exception("Unable to get the server address; it hasn't been initialized yet.")
    }
    return thriftCLIService.getPortNumber
  }

  private def getPublishConfigs(livyConf: LivyConf): Map[String, String] = {
    val commonConfigs =
      Map("hive.server2.thrift.bind.host" -> getServerHostName(),
        "hive.server2.transport.mode" -> livyConf.get(LivyConf.THRIFT_TRANSPORT_MODE),
        "hive.server2.authentication" -> livyConf.get(LivyConf.THRIFT_AUTHENTICATION),
        "hive.server2.use.SSL" -> livyConf.getBoolean(LivyConf.THRIFT_USE_SSL).toString())

    val transportConfigs = if (LivyThriftServer.isHTTPTransportMode(livyConf)) {
      Map("hive.server2.thrift.http.port" -> getServerPortNumber().toString,
        "hive.server2.thrift.http.path" -> livyConf.get(LivyConf.THRIFT_HTTP_PATH))
    } else {
      Map("hive.server2.thrift.port" -> getServerPortNumber().toString,
        "hive.server2.thrift.sasl.qop" -> livyConf.get(LivyConf.THRIFT_SASL_QOP))
    }

    val isAuthKerberos =
      livyConf.get(LivyConf.THRIFT_AUTHENTICATION).equalsIgnoreCase(AuthTypes.KERBEROS.getAuthName)
    val kerberosConfigs = if (isAuthKerberos) {
      Map("hive.server2.authentication.kerberos.principal" ->
        livyConf.get(LivyConf.AUTH_KERBEROS_PRINCIPAL))
    } else {
      Map()
    }

    commonConfigs ++ transportConfigs ++ kerberosConfigs
  }
}
