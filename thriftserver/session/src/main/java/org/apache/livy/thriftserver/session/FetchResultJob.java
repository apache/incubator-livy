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

import java.util.Iterator;

import org.apache.spark.sql.Row;

import org.apache.livy.Job;
import org.apache.livy.JobContext;

/**
 * Job used to fetch results of a query.
 */
public class FetchResultJob implements Job<ResultSet> {

  private final String sessionId;
  private final String statementId;
  private final int maxRows;

  public FetchResultJob() {
    this(null, null, -1);
  }

  public FetchResultJob(String sessionId, String statementId, int maxRows) {
    this.sessionId = sessionId;
    this.statementId = statementId;
    this.maxRows = maxRows;
  }

  @Override
  public ResultSet call(JobContext ctx) {
    ThriftSessionState session = ThriftSessionState.get(ctx, sessionId);
    StatementState st = session.findStatement(statementId);
    Iterator<Row> iter = st.iter;

    ResultSet rs = new ResultSet(st.types);
    int count = 0;
    while (iter.hasNext() && count < maxRows) {
      Row row = iter.next();
      Object[] cols = new Object[st.types.length];
      for (int i = 0; i < cols.length; i++) {
        cols[i] = row.get(i);
      }
      rs.addRow(cols);
      count++;
    }

    return rs;
  }

}
