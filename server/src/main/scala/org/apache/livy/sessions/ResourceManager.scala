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

package org.apache.livy.sessions

import java.io.{File, FileOutputStream, InputStream}
import java.util.UUID

import scala.util.control.NonFatal

import com.google.common.annotations.VisibleForTesting
import org.apache.commons.io.{FileUtils, IOUtils}

import org.apache.livy.{LivyConf, Logging}

final class ResourceManager(livyConf: LivyConf) extends Logging {

  @VisibleForTesting
  private[livy] val baseDir = new File(
    s"${livyConf.get(LivyConf.RESOURCE_DIR)}/livy-resources-${UUID.randomUUID().toString}")
  if (!baseDir.mkdir()) {
    throw new IllegalStateException(s"Fail to create base dir $baseDir")
  }

  private val whitelistLocalDir = livyConf.configToSeq(LivyConf.LOCAL_FS_WHITELIST) ++
    Seq(baseDir.getAbsolutePath)
  livyConf.set(LivyConf.LOCAL_FS_WHITELIST, whitelistLocalDir.mkString(","))

  /**
   * Register session with Id into ResourceManager.
   */
  def registerSession(sessionId: Int): Unit = {
    val folder = sessionFolder(sessionId)
    if (!folder.mkdir()) {
      throw new IllegalStateException(s"Fail to create session folder $folder")
    }
    info(s"Session $sessionId registered in ResourceManager")
  }

  /**
   * Save resource associated with session id and name into file.
   */
  def saveResource(sessionId: Int, name: String, is: InputStream): Unit = {
    val folder = sessionFolder(sessionId)
    if (!folder.exists() || !folder.isDirectory) {
      throw new IllegalStateException("Session folder is not existed, or is not a directory")
    }

    val resource = new File(folder, name)
    if (resource.exists()) {
      warn(s"Resource ${resource.getAbsolutePath} is already existed, will ignore duplicated " +
        s"resource $name")
      return
    }

    val tmpFile = new File(folder, name + UUID.randomUUID().toString)
    var fos: FileOutputStream = null
    try {
      fos = new FileOutputStream(tmpFile)
      IOUtils.copy(is, fos)
    } catch {
      case NonFatal(e) =>
        warn(s"Fail to write source to local file", e)
    } finally {
      if (fos != null) {
        fos.close()
        fos = null
      }
    }

    val resourceFile = new File(folder, name)
    tmpFile.renameTo(resourceFile)
    info(s"Resource $name added to the ResourcesManager as ${resourceFile.getAbsolutePath}")
  }

  /**
   * Get resource based on session id and name.
   */
  def retrieveResource(sessionId: Int, name: String): Option[String] = {
    val folder = sessionFolder(sessionId)
    val resource = new File(folder, name)
    if (resource.exists() && resource.canRead) {
      info(s"Resource $name for session $sessionId is retrieved at ${resource.getAbsolutePath}")
      Some(resource.getAbsolutePath)
    } else {
      info(s"Resource $name for session $sessionId is not found")
      None
    }
  }

  /**
   * Unregister the session with id specified.
   */
  def unregisterSession(sessionId: Int): Unit = {
    val folder = sessionFolder(sessionId)
    if (!folder.exists()) {
      return
    }

    folder.listFiles().foreach { f => f.delete() }
    folder.delete()
    info(s"Session $sessionId unregistered in ResourcesManager")
  }

  /**
   * Clean all the cached resource files when LivyServer is stopped.
   */
  def clean(): Unit = {
    FileUtils.deleteDirectory(baseDir)
  }

  private def sessionFolder(sessionId: Int): File = {
    new File(baseDir, sessionId.toString)
  }
}
