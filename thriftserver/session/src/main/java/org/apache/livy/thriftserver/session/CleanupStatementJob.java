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

import org.apache.livy.Job;
import org.apache.livy.JobContext;

/**
 * Job used to clean up state held for a statement.
 */
public class CleanupStatementJob implements Job<Boolean> {

  private final String sessionId;
  private final String statementId;

  public CleanupStatementJob() {
    this(null, null);
  }

  public CleanupStatementJob(String sessionId, String statementId) {
    this.sessionId = sessionId;
    this.statementId = statementId;
  }

  @Override
  public Boolean call(JobContext ctx) {
    ThriftSessionState session = ThriftSessionState.get(ctx, sessionId);
    return session.cleanupStatement(statementId);
  }

}
