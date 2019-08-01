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

import java.io.InputStream
import java.util
import java.util.concurrent.locks.ReentrantLock

import scala.io.Source

import org.apache.livy.Logging

class CircularQueue[T](var capacity: Int) extends util.LinkedList[T] {
  override def add(t: T): Boolean = {
    if (size >= capacity) removeFirst
    super.add(t)
  }
}

class LineBufferedStream(inputStream: InputStream, logSize: Int) extends Logging {

  private[this] val _lines: CircularQueue[String] = new CircularQueue[String](logSize)

  private[this] val _lock = new ReentrantLock()
  private[this] val _condition = _lock.newCondition()
  private[this] var _finished = false

  private val thread = new Thread {
    override def run() = {
      val lines = Source.fromInputStream(inputStream).getLines()
      for (line <- lines) {
        info(line)
        _lock.lock()
        try {
          _lines.add(line)
          _condition.signalAll()
        } finally {
          _lock.unlock()
        }
      }

      _lock.lock()
      try {
        _finished = true
        _condition.signalAll()
      } finally {
        _lock.unlock()
      }
    }
  }
  thread.setDaemon(true)
  thread.start()

  def lines: IndexedSeq[String] = {
    _lock.lock()
    val lines = IndexedSeq.empty[String] ++ _lines.toArray(Array.empty[String])
    _lock.unlock()
    lines
  }

  def iterator: Iterator[String] = {
    new LinesIterator
  }

  def waitUntilClose(): Unit = thread.join()

  private class LinesIterator extends Iterator[String] {

    override def hasNext: Boolean = {
      if (_lines.size > 0) {
        true
      } else {
        // Otherwise we might still have more data.
        _lock.lock()
        try {
          if (_finished) {
            false
          } else {
            _condition.await()
            _lines.size > 0
          }
        } finally {
          _lock.unlock()
        }
      }
    }

    override def next(): String = {
      _lock.lock()
      val line = _lines.poll()
      _lock.unlock()
      line
    }
  }
}
