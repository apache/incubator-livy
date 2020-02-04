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
package org.apache.livy.thriftserver.session;

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import scala.Tuple2;
import scala.collection.Map;
import scala.collection.Seq;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;

/**
 * Utility class used for transferring results from the Spark application to the Livy server.
 */
public class ResultSet {

  private final ColumnBuffer[] columns;

  public ResultSet() {
    this.columns = null;
  }

  public ResultSet(DataType[] types) {
    this.columns = new ColumnBuffer[types.length];
    for (int i = 0; i < columns.length; i++) {
      columns[i] = new ColumnBuffer(types[i]);
    }
  }

  public void addRow(Object[] fields) {
    if (fields.length != columns.length) {
      throw new IllegalArgumentException("Not enough columns in given row.");
    }

    for (int i = 0; i < fields.length; i++) {
      Object value;
      if (columns[i].getType() == DataType.STRING) {
        value = toHiveString(fields[i], false);
      } else {
        value = fields[i];
      }
      columns[i].add(value);
    }
  }

  public ColumnBuffer[] getColumns() {
    return columns;
  }

  /**
   * Converts a value from a Spark dataset into a string that looks like what Hive would
   * generate. Because Spark generates rows that contain Scala types for non-primitive
   * columns, this code depends on Scala and is thus susceptible to binary compatibility
   * changes in the Scala libraries.
   *
   * The supported types are described in Spark's SQL programming guide, in the table
   * listing the mapping of SQL types to Scala types.
   *
   * @param value The object to stringify.
   * @param quoteStrings Whether to wrap String instances in quotes.
   */
  private String toHiveString(Object value, boolean quoteStrings) {
    if (value == null) {
      return null;
    } else if (quoteStrings && value instanceof String) {
      return "\"" + value + "\"";
    } else if (value instanceof BigDecimal) {
      return ((BigDecimal) value).stripTrailingZeros().toString();
    } else if (value instanceof Map) {
      return stream(new ScalaIterator<>(((Map<?,?>) value).iterator()))
        .map(o -> toHiveString(o, true))
        .sorted()
        .collect(Collectors.joining(",", "{", "}"));
    } else if (value instanceof Seq) {
      return stream(new ScalaIterator<>(((Seq<?>) value).iterator()))
        .map(o -> toHiveString(o, true))
        .collect(Collectors.joining(",", "[", "]"));
    } else if (value instanceof Tuple2) {
      Tuple2 t = (Tuple2) value;
      return String.format("%s:%s", toHiveString(t._1(), true), toHiveString(t._2(), true));
    } else if (value instanceof Row) {
      Row r = (Row) value;
      final StructField[] fields = r.schema().fields();
      final AtomicInteger idx = new AtomicInteger();

      return stream(new ScalaIterator<>(r.toSeq().iterator()))
        .map(o -> {
          String fname = fields[idx.getAndIncrement()].name();
          String fval = toHiveString(o, true);
          return String.format("\"%s\":%s", fname, fval);
        })
        .collect(Collectors.joining(",", "{", "}"));
    } else {
      return value.toString();
    }
  }

  private Stream<?> stream(Iterator<?> it) {
    return StreamSupport.stream(
      Spliterators.spliteratorUnknownSize(it, Spliterator.ORDERED), false);
  }
}
