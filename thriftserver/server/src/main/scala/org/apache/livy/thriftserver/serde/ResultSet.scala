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

import java.util

import org.apache.hive.service.rpc.thrift.{TProtocolVersion, TRow, TRowSet}
import org.apache.hive.service.rpc.thrift.TProtocolVersion._

import org.apache.livy.thriftserver.types.{DataType, Schema}

abstract class ResultSet(val types: Array[DataType]) {
  def toTRowSet: TRowSet
  def addRow(row: Array[Any]): Unit
  def setRowOffset(rowOffset: Long): Unit
  def numRows: Int
}

object ResultSet {
  def apply(types: Array[DataType], protocolVersion: TProtocolVersion): ResultSet = {
    // Older version of Hive protocol require the result set to be returned in a row
    // oriented way. We do not support those versions, so it is useless to implement it.
    assert(protocolVersion.getValue >= HIVE_CLI_SERVICE_PROTOCOL_V6.getValue)
    new ColumnOrientedResultSet(types)
  }

  def apply(schema: Schema, protocolVersion: TProtocolVersion): ResultSet = {
    apply(schema.fields.map(_.dataType), protocolVersion)
  }
}

/**
 * [[ResultSet]] implementation which uses columns to store data.
 */
class ColumnOrientedResultSet(types: Array[DataType]) extends ResultSet(types) {

  private var rowOffset: Long = _
  private val columns: Array[ColumnBuffer] = types.map(new ColumnBuffer(_))

  def addRow(fields: Array[Any]): Unit = {
    var i = 0
    while (i < fields.length) {
      val field = fields(i)
      columns(i).addValue(field)
      i += 1
    }
  }

  override def toTRowSet: TRowSet = {
    val tRowSet = new TRowSet(rowOffset, new util.ArrayList[TRow])
    columns.foreach { c =>
      tRowSet.addToColumns(c.toTColumn)
    }
    tRowSet
  }

  override def setRowOffset(rowOffset: Long): Unit = this.rowOffset = rowOffset

  override def numRows: Int = columns.headOption.map(_.size).getOrElse(0)
}
