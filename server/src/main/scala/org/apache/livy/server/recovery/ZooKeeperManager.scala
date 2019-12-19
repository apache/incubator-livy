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

package org.apache.livy.server.recovery

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import org.apache.curator.framework.api.UnhandledErrorListener
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.RetryNTimes
import org.apache.zookeeper.KeeperException.NoNodeException

import org.apache.livy.LivyConf
import org.apache.livy.LivyConf.Entry
import org.apache.livy.Logging

object ZooKeeperManager {
  val ZK_KEY_PREFIX_CONF = Entry("livy.server.recovery.zk-state-store.key-prefix", "livy")
  val ZK_RETRY_CONF = Entry("livy.server.recovery.zk-state-store.retry-policy", "5,100")
  val DUPLICATE_CREATE_EXCEPTION = "ZooKeeperManager single instance has already been created"

  @volatile private var zkManager: ZooKeeperManager = _

  def apply(livyConf: LivyConf,
    mockCuratorClient: Option[CuratorFramework] = None):
  ZooKeeperManager = synchronized {
    if (zkManager == null) {
      zkManager = new ZooKeeperManager(livyConf, mockCuratorClient)
    } else {
      throw new IllegalAccessException(DUPLICATE_CREATE_EXCEPTION)
    }

    zkManager
  }

  def get(): ZooKeeperManager = zkManager

  // for test
  private[recovery] def reset(): Unit = {
    zkManager = null
  }
}

class ZooKeeperManager private(
    livyConf: LivyConf,
    mockCuratorClient: Option[CuratorFramework] = None)
  extends JsonMapper with Logging {

  import ZooKeeperManager._

  private val zkAddress = livyConf.get(LivyConf.ZOOKEEPER_URL)
  require(!zkAddress.isEmpty, s"Please config ${LivyConf.RECOVERY_STATE_STORE_URL.key}.")
  private val zkKeyPrefix = livyConf.get(ZK_KEY_PREFIX_CONF)
  private val retryValue = livyConf.get(ZK_RETRY_CONF)
  // a regex to match patterns like "m, n" where m and n both are integer values
  private val retryPattern = """\s*(\d+)\s*,\s*(\d+)\s*""".r
  private[recovery] val retryPolicy = retryValue match {
    case retryPattern(n, sleepMs) => new RetryNTimes(n.toInt, sleepMs.toInt)
    case _ => throw new IllegalArgumentException(
      s"$ZK_KEY_PREFIX_CONF contains bad value: $retryValue. " +
        "Correct format is <max retry count>,<sleep ms between retry>. e.g. 5,100")
  }

  private val curatorClient = mockCuratorClient.getOrElse {
    CuratorFrameworkFactory.newClient(zkAddress, retryPolicy)
  }

  Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
    override def run(): Unit = {
      curatorClient.close()
    }
  }))

  curatorClient.getUnhandledErrorListenable().addListener(new UnhandledErrorListener {
    def unhandledError(message: String, e: Throwable): Unit = {
      error(s"Fatal Zookeeper error. Shutting down Livy server.")
      System.exit(1)
    }
  })
  curatorClient.start()
  // TODO Make sure ZK path has proper secure permissions so that other users cannot read its
  // contents.

  def set(key: String, value: Object): Unit = {
    val prefixedKey = prefixKey(key)
    val data = serializeToBytes(value)
    if (curatorClient.checkExists().forPath(prefixedKey) == null) {
      curatorClient.create().creatingParentsIfNeeded().forPath(prefixedKey, data)
    } else {
      curatorClient.setData().forPath(prefixedKey, data)
    }
  }

  def get[T: ClassTag](key: String): Option[T] = {
    val prefixedKey = prefixKey(key)
    if (curatorClient.checkExists().forPath(prefixedKey) == null) {
      None
    } else {
      Option(deserialize[T](curatorClient.getData().forPath(prefixedKey)))
    }
  }

  def getChildren(key: String): Seq[String] = {
    val prefixedKey = prefixKey(key)
    if (curatorClient.checkExists().forPath(prefixedKey) == null) {
      Seq.empty[String]
    } else {
      curatorClient.getChildren.forPath(prefixedKey).asScala
    }
  }

  def remove(key: String): Unit = {
    try {
      curatorClient.delete().guaranteed().forPath(prefixKey(key))
    } catch {
      case _: NoNodeException => warn(s"Fail to remove non-existed zookeeper node: ${key}")
    }
  }

  private def prefixKey(key: String) = s"/$zkKeyPrefix/$key"
}
