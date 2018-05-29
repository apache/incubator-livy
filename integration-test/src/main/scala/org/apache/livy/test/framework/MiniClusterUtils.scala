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

package org.apache.livy.test.framework

import java.io._
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Properties

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration

trait MiniClusterUtils {

  protected def saveProperties(props: Map[String, String], dest: File): Unit = {
    val jprops = new Properties()
    props.foreach { case (k, v) => jprops.put(k, v) }

    val tempFile = new File(dest.getAbsolutePath() + ".tmp")
    val out = new OutputStreamWriter(new FileOutputStream(tempFile), UTF_8)
    try {
      jprops.store(out, "Configuration")
    } finally {
      out.close()
    }
    tempFile.renameTo(dest)
  }

  protected def loadProperties(file: File): Map[String, String] = {
    val in = new InputStreamReader(new FileInputStream(file), UTF_8)
    val props = new Properties()
    try {
      props.load(in)
    } finally {
      in.close()
    }
    props.asScala.toMap
  }

  protected def saveConfig(conf: Configuration, dest: File): Unit = {
    val redacted = new Configuration(conf)
    // This setting references a test class that is not available when using a real Spark
    // installation, so remove it from client configs.
    redacted.unset("net.topology.node.switch.mapping.impl")

    val out = new FileOutputStream(dest)
    try {
      redacted.writeXml(out)
    } finally {
      out.close()
    }
  }
}
