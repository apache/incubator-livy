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

package org.apache.livy.thriftserver.serde

import java.nio.ByteBuffer
import java.util

import scala.collection.mutable

import org.apache.livy.thriftserver.types.{DataType, DataTypeUtils}

object ColumnBuffer {
  private val DEFAULT_SIZE = 100
  private val EMPTY_BINARY = ByteBuffer.allocate(0)
  private val EMPTY_STRING = ""
}

class ColumnBuffer(val dataType: DataType) {
  private val nulls = new mutable.BitSet()
  private var currentSize = 0
  private var boolVars: Array[Boolean] = _
  private var byteVars: Array[Byte] = _
  private var shortVars: Array[Short] = _
  private var intVars: Array[Int] = _
  private var longVars: Array[Long] = _
  private var doubleVars: Array[Double] = _
  private var stringVars: util.List[String] = _
  private var binaryVars: util.List[ByteBuffer] = _

  dataType.name match {
    case "boolean" =>
      boolVars = new Array[Boolean](ColumnBuffer.DEFAULT_SIZE)
    case "byte" =>
      byteVars = new Array[Byte](ColumnBuffer.DEFAULT_SIZE)
    case "short" =>
      shortVars = new Array[Short](ColumnBuffer.DEFAULT_SIZE)
    case "integer" =>
      intVars = new Array[Int](ColumnBuffer.DEFAULT_SIZE)
    case "long" =>
      longVars = new Array[Long](ColumnBuffer.DEFAULT_SIZE)
    case "float" | "double" =>
      doubleVars = new Array[Double](ColumnBuffer.DEFAULT_SIZE)
    case "binary" =>
      binaryVars = new util.ArrayList[ByteBuffer]
    case "void" => // all NULLs, nothing to do
    case _ =>
      stringVars = new util.ArrayList[String]
  }

  def get(index: Int): Any = {
    if (this.nulls(index)) {
      null
    } else {
      dataType.name match {
        case "boolean" =>
          boolVars(index)
        case "byte" =>
          byteVars(index)
        case "short" =>
          shortVars(index)
        case "integer" =>
          intVars(index)
        case "long" =>
          longVars(index)
        case "float" | "double" =>
          doubleVars(index)
        case "binary" =>
          binaryVars.get(index).array()
        case _ =>
          stringVars.get(index)
      }
    }
  }

  def size: Int = currentSize

  def addValue(field: Any): Unit = {
    if (field == null) {
      nulls += currentSize
      if (dataType.name == "string") {
        stringVars.add(ColumnBuffer.EMPTY_STRING)
      } else if (dataType.name == "binary") {
        binaryVars.add(ColumnBuffer.EMPTY_BINARY)
      }
    } else {
      dataType.name match {
        case "boolean" =>
          ensureBoolVarsSize()
          boolVars(currentSize) = field.asInstanceOf[Boolean]
        case "byte" =>
          ensureByteVarsSize()
          byteVars(currentSize) = field.asInstanceOf[Byte]
        case "short" =>
          ensureShortVarsSize()
          shortVars(currentSize) = field.asInstanceOf[Short]
        case "integer" =>
          ensureIntVarsSize()
          intVars(currentSize) = field.asInstanceOf[Int]
        case "long" =>
          ensureLongVarsSize()
          longVars(currentSize) = field.asInstanceOf[Long]
        case "float" =>
          ensureDoubleVarsSize()
          // We need to convert the float to string and then to double in order to avoid precision
          // issues caused by the poor precision of Float
          doubleVars(currentSize) = field.toString.toDouble
        case "double" =>
          ensureDoubleVarsSize()
          doubleVars(currentSize) = field.asInstanceOf[Double]
        case "binary" =>
          binaryVars.add(ByteBuffer.wrap(field.asInstanceOf[Array[Byte]]))
        case _ =>
          stringVars.add(DataTypeUtils.toHiveString(field, dataType))
      }
    }

    currentSize += 1
  }

  private def ensureBoolVarsSize(): Unit = if (boolVars.length == currentSize) {
    val newVars = new Array[Boolean](currentSize << 1)
    System.arraycopy(boolVars, 0, newVars, 0, currentSize)
    boolVars = newVars
  }

  private def ensureByteVarsSize(): Unit = if (byteVars.length == currentSize) {
    val newVars = new Array[Byte](currentSize << 1)
    System.arraycopy(byteVars, 0, newVars, 0, currentSize)
    byteVars = newVars
  }

  private def ensureShortVarsSize(): Unit = if (shortVars.length == currentSize) {
    val newVars = new Array[Short](currentSize << 1)
    System.arraycopy(shortVars, 0, newVars, 0, currentSize)
    shortVars = newVars
  }

  private def ensureIntVarsSize(): Unit = if (intVars.length == currentSize) {
    val newVars = new Array[Int](currentSize << 1)
    System.arraycopy(intVars, 0, newVars, 0, currentSize)
    intVars = newVars
  }

  private def ensureLongVarsSize(): Unit = if (longVars.length == currentSize) {
    val newVars = new Array[Long](currentSize << 1)
    System.arraycopy(longVars, 0, newVars, 0, currentSize)
    longVars = newVars
  }

  private def ensureDoubleVarsSize(): Unit = if (doubleVars.length == currentSize) {
    val newVars = new Array[Double](currentSize << 1)
    System.arraycopy(doubleVars, 0, newVars, 0, currentSize)
    doubleVars = newVars
  }

  private[thriftserver] def getColumnValues: Any = dataType.name match {
    case "boolean" => boolVars.take(currentSize)
    case "byte" => byteVars.take(currentSize)
    case "short" => shortVars.take(currentSize)
    case "integer" => intVars.take(currentSize)
    case "long" => longVars.take(currentSize)
    case "float" => doubleVars.take(currentSize)
    case "double" => doubleVars.take(currentSize)
    case "binary" => binaryVars
    case _ => stringVars
  }

  private[thriftserver] def getNulls: util.BitSet = util.BitSet.valueOf(nulls.toBitMask)
}
