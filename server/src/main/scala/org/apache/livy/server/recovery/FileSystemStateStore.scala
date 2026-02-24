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

import java.io.{FileNotFoundException, IOException}
import java.net.URI
import java.util
import java.util.concurrent.TimeUnit

import scala.reflect.ClassTag
import scala.util.control.NonFatal

import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.Options.{CreateOpts, Rename}
import org.apache.hadoop.fs.permission.{FsAction, FsPermission}
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.hdfs.protocol.HdfsConstants

import org.apache.livy.{LivyConf, Logging}
import org.apache.livy.Utils.usingResource

class FileSystemStateStore(
    livyConf: LivyConf,
    mockFileContext: Option[FileContext])
  extends StateStore(livyConf) with Logging {

  // Constructor defined for StateStore factory to new this class using reflection.
  def this(livyConf: LivyConf) {
    this(livyConf, None)
  }

  private val fs = FileSystem.newInstance(livyConf.hadoopConf)

  private val fsUri = {
    val fsPath = livyConf.get(LivyConf.RECOVERY_STATE_STORE_URL)
    require(fsPath != null && !fsPath.isEmpty,
      s"Please config ${LivyConf.RECOVERY_STATE_STORE_URL.key}.")
    new URI(fsPath)
  }

  private val fileContext: FileContext = mockFileContext.getOrElse {
    FileContext.getFileContext(fsUri)
  }

  {
    // Only Livy user should have access to state files.
    fileContext.setUMask(new FsPermission("077"))

    startSafeModeCheck()

    // Create state store dir if it doesn't exist.
    val stateStorePath = absPath(".")
    try {
      fileContext.mkdir(stateStorePath, FsPermission.getDirDefault(), true)
    } catch {
      case _: FileAlreadyExistsException =>
        if (!fileContext.getFileStatus(stateStorePath).isDirectory()) {
          throw new IOException(s"$stateStorePath is not a directory.")
        }
    }

    // Check permission of state store dir.
    val fileStatus = fileContext.getFileStatus(absPath("."))
    require(fileStatus.getPermission.getUserAction() == FsAction.ALL,
      s"Livy doesn't have permission to access state store: $fsUri.")
    if (fileStatus.getPermission.getGroupAction != FsAction.NONE) {
      warn(s"Group users have permission to access state store: $fsUri. This is insecure.")
    }
    if (fileStatus.getPermission.getOtherAction != FsAction.NONE) {
      warn(s"Other users have permission to access state store: $fsUri. This is in secure.")
    }
  }

  override def set(key: String, value: Object): Unit = {
    // Write to a temp file then rename to avoid file corruption if livy-server crashes
    // in the middle of the write.
    val tmpPath = absPath(s"$key.tmp")
    val createFlag = util.EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE)

    usingResource(fileContext.create(tmpPath, createFlag, CreateOpts.createParent())) { tmpFile =>
      tmpFile.write(serializeToBytes(value))
      tmpFile.close()
      // Assume rename is atomic.
      fileContext.rename(tmpPath, absPath(key), Rename.OVERWRITE)
    }

    try {
      val crcPath = new Path(tmpPath.getParent, s".${tmpPath.getName}.crc")
      fileContext.delete(crcPath, false)
    } catch {
      case NonFatal(e) => // Swallow the exception.
    }
  }

  override def get[T: ClassTag](key: String): Option[T] = {
    try {
      usingResource(fileContext.open(absPath(key))) { is =>
        Option(deserialize[T](IOUtils.toByteArray(is)))
      }
    } catch {
      case _: FileNotFoundException => None
      case e: IOException =>
        warn(s"Failed to read $key from state store.", e)
        None
    }
  }

  override def getChildren(key: String): Seq[String] = {
    try {
      fileContext.util.listStatus(absPath(key)).map(_.getPath.getName)
    } catch {
      case _: FileNotFoundException => Seq.empty
      case e: IOException =>
        warn(s"Failed to list $key from state store.", e)
        Seq.empty
    }
  }

  override def remove(key: String): Unit = {
    try {
      fileContext.delete(absPath(key), false)
    } catch {
      case _: FileNotFoundException => warn(s"Failed to remove non-existed file: ${key}")
    }
  }

  private def absPath(key: String): Path = new Path(fsUri.getPath(), key)

  /**
   * Checks whether HDFS is in safe mode.
   *
   * Note that DistributedFileSystem is a `@LimitedPrivate` class, which for all practical reasons
   * makes it more public than not.
   */
  def isFsInSafeMode(): Boolean = fs match {
    case dfs: DistributedFileSystem =>
      isFsInSafeMode(dfs)
    case _ =>
      false
  }

  def isFsInSafeMode(dfs: DistributedFileSystem): Boolean = {
    /* true to check only for Active NNs status */
    dfs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_GET, true)
  }

  def startSafeModeCheck(): Unit = {
    // Cannot probe anything while the FS is in safe mode,
    // so wait for seconds which is configurable
    val safeModeInterval = livyConf.getInt(LivyConf.HDFS_SAFE_MODE_INTERVAL_IN_SECONDS)
    val safeModeMaxRetryAttempts = livyConf.getInt(LivyConf.HDFS_SAFE_MODE_MAX_RETRY_ATTEMPTS)
    for (retryAttempts <- 0 to safeModeMaxRetryAttempts if isFsInSafeMode()) {
      info("HDFS is still in safe mode. Waiting...")
      Thread.sleep(TimeUnit.SECONDS.toMillis(safeModeInterval))
    }

    // if hdfs is still in safe mode
    // even after max retry attempts
    // then throw IllegalStateException
    if (isFsInSafeMode()) {
      throw new IllegalStateException("Reached max retry attempts for safe mode check " +
        "in hdfs file system")
    }
  }

}
