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

import java.io.IOException
import java.util

import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.Options.{CreateOpts, Rename}
import org.hamcrest.Description
import org.mockito.ArgumentMatcher
import org.mockito.Matchers.{any, anyBoolean, argThat, eq => equal}
import org.mockito.Mockito.{atLeastOnce, never, verify, when}
import org.mockito.internal.matchers.Equals
import org.scalatest.FunSpec
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.livy.{LivyBaseUnitTestSuite, LivyConf}

class StatementStoreSpec extends FunSpec with LivyBaseUnitTestSuite {
  describe("StatementStore") {
    def pathEq(wantedPath: String): Path = argThat(new ArgumentMatcher[Path] {
      private val matcher = new Equals(wantedPath)

      override def matches(path: Any): Boolean = matcher.matches(path.toString)

      override def describeTo(d: Description): Unit = {
        matcher.describeTo(d)
      }
    })

    def makeConf(): LivyConf = {
      val conf = new LivyConf()
      conf.set(LivyConf.STATEMENTS_PERSISTENT_ENABLED, true)
      conf.set(LivyConf.STATEMENTS_PERSISTENT_PATH, "/tmp/")
      conf
    }

    def mockFileContext(rootDirPermission: String): FileContext = {
      val fileContext = mock[FileContext]
      val rootDirStatus = mock[FileStatus]
      when(fileContext.getFileStatus(any())).thenReturn(rootDirStatus)
      when(rootDirStatus.getPermission).thenReturn(new FsPermission(rootDirPermission))
      fileContext
    }

    it("save should write with an intermediate file") {
      val fileContext = mockFileContext("700")
      val outputStream = mock[FSDataOutputStream]
      when(fileContext.create(pathEq("/tmp/statements.tmp"), any[util.EnumSet[CreateFlag]],
        any[CreateOpts])).thenReturn(outputStream)

      val stateStore = new StatementStore(makeConf(), Some(fileContext))
      stateStore.save("statements", "value")

      verify(outputStream).write(""""value"""".getBytes)
      verify(outputStream, atLeastOnce).close()
      verify(fileContext).rename(pathEq("/tmp/statements.tmp"), pathEq("/tmp/statements"),
        equal(Rename.OVERWRITE))
      verify(fileContext).delete(pathEq("/tmp/.statements.tmp.crc"), equal(false))
    }

    it("save should not write with an intermediate file") {
      val fileContext = mockFileContext("700")
      val outputStream = mock[FSDataOutputStream]
      when(fileContext.create(pathEq("/tmp/statements.tmp"), any[util.EnumSet[CreateFlag]],
        any[CreateOpts])).thenReturn(outputStream)

      val stateStore = new StatementStore(new LivyConf(), Some(fileContext))
      stateStore.save("statements", "value")

      verify(outputStream, never).write(any[Byte])
    }

    it("save throws exception during file write") {
      val fileContext = mockFileContext("700")
      val outputStream = mock[FSDataOutputStream]
      when(outputStream.close()).thenThrow(new IOException())

      val stateStore = new StatementStore(makeConf(), Some(fileContext))
      stateStore.save("statements", "value")

      verify(outputStream, never).close()
    }

    it("save throws exception during file delete") {
      val fileContext = mockFileContext("700")
      val outputStream = mock[FSDataOutputStream]
      when(fileContext.create(pathEq("/tmp/statements.tmp"), any[util.EnumSet[CreateFlag]],
        any[CreateOpts])).thenReturn(outputStream)
      when(fileContext.delete(any[Path], anyBoolean())).thenThrow(new IOException())

      val stateStore = new StatementStore(makeConf(), Some(fileContext))
      stateStore.save("statements", "value")

      verify(outputStream).write(""""value"""".getBytes)
    }
  }
}
