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
import org.apache.livy.Logging

class ZooKeeperManager(
    livyConf: LivyConf,
    mockCuratorClient: Option[CuratorFramework] = None)
  extends JsonMapper with Logging {

  def this(livyConf: LivyConf) {
    this(livyConf, None)
  }

  private val zkAddress = Option(livyConf.get(LivyConf.ZOOKEEPER_URL)).
    orElse(Option(livyConf.get(LivyConf.RECOVERY_STATE_STORE_URL))).
    map(_.trim).orNull

  require(zkAddress != null && !zkAddress.isEmpty,
    s"Please config ${LivyConf.ZOOKEEPER_URL.key}.")

  private val retryValue = Option(livyConf.get(LivyConf.ZK_RETRY_POLICY)).
    orElse(Option(livyConf.get(LivyConf.RECOVERY_ZK_STATE_STORE_RETRY_POLICY))).
    map(_.trim).orNull

  require(retryValue != null && !retryValue.isEmpty,
    s"Please config ${LivyConf.ZK_RETRY_POLICY.key}.")

  // a regex to match patterns like "m, n" where m and n both are integer values
  private val retryPattern = """\s*(\d+)\s*,\s*(\d+)\s*""".r
  private[recovery] val retryPolicy = retryValue match {
    case retryPattern(n, sleepMs) => new RetryNTimes(n.toInt, sleepMs.toInt)
    case _ => throw new IllegalArgumentException(
      s"contains bad value: $retryValue. " +
        "Correct format is <max retry count>,<sleep ms between retry>. e.g. 5,100")
  }

  private val curatorClient = mockCuratorClient.getOrElse {
    CuratorFrameworkFactory.newClient(zkAddress, retryPolicy)
  }

  curatorClient.getUnhandledErrorListenable().addListener(new UnhandledErrorListener {
    def unhandledError(message: String, e: Throwable): Unit = {
      error(s"Fatal Zookeeper error: ${message}.", e)
    }
  })

  def start(): Unit = {
    curatorClient.start()
  }

  def stop(): Unit = {
    curatorClient.close()
  }

  // TODO Make sure ZK path has proper secure permissions so that other users cannot read its
  // contents.

  def set(key: String, value: Object): Unit = {
    val data = serializeToBytes(value)
    if (curatorClient.checkExists().forPath(key) == null) {
      curatorClient.create().creatingParentsIfNeeded().forPath(key, data)
    } else {
      curatorClient.setData().forPath(key, data)
    }
  }

  def get[T: ClassTag](key: String): Option[T] = {
    if (curatorClient.checkExists().forPath(key) == null) {
      None
    } else {
      Option(deserialize[T](curatorClient.getData().forPath(key)))
    }
  }

  def getChildren(key: String): Seq[String] = {
    if (curatorClient.checkExists().forPath(key) == null) {
      Seq.empty[String]
    } else {
      curatorClient.getChildren.forPath(key).asScala
    }
  }

  def remove(key: String): Unit = {
    try {
      curatorClient.delete().guaranteed().forPath(key)
    } catch {
      case _: NoNodeException => warn(s"Fail to remove non-existed zookeeper node: ${key}")
    }
  }

}
