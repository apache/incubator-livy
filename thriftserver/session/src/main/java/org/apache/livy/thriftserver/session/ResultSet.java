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

/**
 * Utility class used for transferring results from the Spark application to the Livy server.
 */
public class ResultSet {

  private final String schema;
  private final ColumnBuffer[] columns;

  public ResultSet() {
    this.schema = null;
    this.columns = null;
  }

  public ResultSet(DataType[] types, String schema) {
    this.schema = schema;
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
      columns[i].add(fields[i]);
    }
  }

  public String getSchema() {
    return schema;
  }

  public ColumnBuffer[] getColumns() {
    return columns;
  }

}
