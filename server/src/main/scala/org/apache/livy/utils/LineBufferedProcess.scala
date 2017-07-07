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

package org.apache.livy.utils

import org.apache.livy.{Logging, Utils}

class LineBufferedProcess(process: Process, logSize: Int) extends Logging {

  private[this] val _inputStream = new LineBufferedStream(process.getInputStream, logSize)
  private[this] val _errorStream = new LineBufferedStream(process.getErrorStream, logSize)

  def inputLines: IndexedSeq[String] = _inputStream.lines
  def errorLines: IndexedSeq[String] = _errorStream.lines

  def inputIterator: Iterator[String] = _inputStream.iterator
  def errorIterator: Iterator[String] = _errorStream.iterator

  def destroy(): Unit = {
    process.destroy()
  }

  /** Returns if the process is still actively running. */
  def isAlive: Boolean = Utils.isProcessAlive(process)

  def exitValue(): Int = {
    process.exitValue()
  }

  def waitFor(): Int = {
    val returnCode = process.waitFor()
    _inputStream.waitUntilClose()
    _errorStream.waitUntilClose()
    returnCode
  }
}

