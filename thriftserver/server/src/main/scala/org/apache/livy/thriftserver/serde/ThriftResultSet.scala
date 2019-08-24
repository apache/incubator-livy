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

import com.google.common.primitives.{Booleans, Bytes, Doubles, Ints, Longs, Shorts}
import org.apache.hive.service.rpc.thrift.{TBinaryColumn, TBoolColumn, TByteColumn, TColumn, TDoubleColumn, TI16Column, TI32Column, TI64Column, TProtocolVersion, TRow, TRowSet, TStringColumn}
import org.apache.hive.service.rpc.thrift.TProtocolVersion._

import org.apache.livy.thriftserver.session.{ColumnBuffer, DataType, ResultSet}
import org.apache.livy.thriftserver.types.Schema

abstract class ThriftResultSet {
  def toTRowSet: TRowSet
  def addRow(row: Array[Any]): Unit
  def setRowOffset(rowOffset: Long): Unit
  def numRows: Int
}

object ThriftResultSet {
  def apply(types: Array[DataType], protocolVersion: TProtocolVersion): ThriftResultSet = {
    // Older version of Hive protocol require the result set to be returned in a row
    // oriented way. We do not support those versions, so it is useless to implement it.
    assert(protocolVersion.getValue >= HIVE_CLI_SERVICE_PROTOCOL_V6.getValue)
    new ColumnOrientedResultSet(types.map(new ColumnBuffer(_)))
  }

  def apply(schema: Schema, protocolVersion: TProtocolVersion): ThriftResultSet = {
    apply(schema.fields.map(_.fieldType.dataType), protocolVersion)
  }

  def apply(resultSet: ResultSet): ThriftResultSet = {
    new ColumnOrientedResultSet(resultSet.getColumns)
  }

  def toTColumn(columnBuffer: ColumnBuffer): TColumn = {
    val value = new TColumn
    val nullsArray = columnBuffer.getNulls.toByteArray
    val nullMasks = ByteBuffer.wrap(nullsArray)
    columnBuffer.getType match {
      case DataType.BOOLEAN =>
        val bools = columnBuffer.getValues.asInstanceOf[Array[Boolean]]
        value.setBoolVal(new TBoolColumn(Booleans.asList(bools: _*), nullMasks))
      case DataType.BYTE =>
        val bytes = columnBuffer.getValues.asInstanceOf[Array[Byte]]
        value.setByteVal(new TByteColumn(Bytes.asList(bytes: _*), nullMasks))
      case DataType.SHORT =>
        val shorts = columnBuffer.getValues.asInstanceOf[Array[Short]]
        value.setI16Val(new TI16Column(Shorts.asList(shorts: _*), nullMasks))
      case DataType.INTEGER =>
        val integers = columnBuffer.getValues.asInstanceOf[Array[Int]]
        value.setI32Val(new TI32Column(Ints.asList(integers: _*), nullMasks))
      case DataType.LONG =>
        val longs = columnBuffer.getValues.asInstanceOf[Array[Long]]
        value.setI64Val(new TI64Column(Longs.asList(longs: _*), nullMasks))
      case DataType.FLOAT =>
        val floats = columnBuffer.getValues.asInstanceOf[Array[Float]]
        val doubles = new util.ArrayList[java.lang.Double](floats.length)
        var i = 0
        while (i < floats.length) {
          doubles.add(floats(i).toString.toDouble)
          i += 1
        }
        value.setDoubleVal(new TDoubleColumn(doubles, nullMasks))
      case DataType.DOUBLE =>
        val doubles = columnBuffer.getValues.asInstanceOf[Array[Double]]
        value.setDoubleVal(new TDoubleColumn(Doubles.asList(doubles: _*), nullMasks))
      case DataType.BINARY =>
        val binaries = columnBuffer.getValues.asInstanceOf[util.List[ByteBuffer]]
        value.setBinaryVal(new TBinaryColumn(binaries, nullMasks))
      case _ =>
        val strings = columnBuffer.getValues.asInstanceOf[util.List[String]]
        value.setStringVal(new TStringColumn(strings, nullMasks))
    }
    value
  }
}

/**
 * [[ThriftResultSet]] implementation which uses columns to store data.
 */
class ColumnOrientedResultSet(
    private val columns: Array[ColumnBuffer]) extends ThriftResultSet {

  private var rowOffset: Long = _

  def addRow(fields: Array[Any]): Unit = {
    var i = 0
    while (i < fields.length) {
      val field = fields(i)
      columns(i).add(field)
      i += 1
    }
  }

  override def toTRowSet: TRowSet = {
    // Spark beeline use old hive-jdbc-client doesn’t do null point ref check. When we new TRowSet,
    // setColumes make sure column set not null.
    val tRowSet = new TRowSet(rowOffset, new util.ArrayList[TRow])
    tRowSet.setColumns(new util.ArrayList[TColumn]())
    columns.foreach { c =>
      tRowSet.addToColumns(ThriftResultSet.toTColumn(c))
    }
    tRowSet
  }

  override def setRowOffset(rowOffset: Long): Unit = this.rowOffset = rowOffset

  override def numRows: Int = columns.headOption.map(_.size).getOrElse(0)
}
