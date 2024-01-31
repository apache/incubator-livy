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

package org.apache.livy.server.interactive

import java.net.URI
import java.util

import scala.util.control.NonFatal

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.Options.{CreateOpts, Rename}

import org.apache.livy.{LivyConf, Logging}
import org.apache.livy.sessions.SessionKindModule

class StatementStore(livyConf: LivyConf, mockFileContext: Option[FileContext] = None)
  extends Logging {

  protected val mapper: ObjectMapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .registerModule(new SessionKindModule())

  def serializeToBytes(value: Object): Array[Byte] = mapper.writeValueAsBytes(value)

  private val fsUri = {
    val fsPath = livyConf.get(LivyConf.STATEMENTS_PERSISTENT_PATH)
    require(fsPath != null && fsPath.nonEmpty,
      s"Please config ${LivyConf.STATEMENTS_PERSISTENT_PATH.key}.")
    new URI(fsPath)
  }

  private val fileContext: FileContext = mockFileContext.getOrElse {
    FileContext.getFileContext(fsUri)
  }

  def save(fileName: String, value: Object): Unit = {
    if (!livyConf.getBoolean(LivyConf.STATEMENTS_PERSISTENT_ENABLED)) {
      return
    }
    logger.info(s"Writing statements info $fileName.")
    // Write to a temp file then rename to avoid file corruption if livy-server crashes
    // in the middle of the write.
    val tmpPath = absPath(s"$fileName.tmp")
    val createFlag = util.EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE)
    try {
      val tmpFile = fileContext.create(tmpPath, createFlag, CreateOpts.createParent())
      tmpFile.write(serializeToBytes(value))
      tmpFile.close()
      // Assume rename is atomic.
      fileContext.rename(tmpPath, absPath(fileName), Rename.OVERWRITE)
    } catch {
      case e: Exception => logger.error(s"Failed to write statements into $fileName.", e)
    }

    try {
      val crcPath = new Path(tmpPath.getParent, s".${tmpPath.getName}.crc")
      fileContext.delete(crcPath, false)
    } catch {
      case NonFatal(_) => // Swallow the exception.
    }
  }

  private def absPath(fileName: String): Path = new Path(fsUri.getPath, fileName)
}
