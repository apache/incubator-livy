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
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import org.apache.livy.JobContext;

/**
 * State related to one Thrift session. One instance of this class is stored in the session's
 * shared object map for each Thrift session that connects to the backing Livy session.
 */
class ThriftSessionState {

  private static String SESSION_STATE_KEY_PREFIX = "livy.sessionState.";
  private static Object CREATE_LOCK = new Object();

  private static String sessionKey(String sessionId) {
    return SESSION_STATE_KEY_PREFIX + sessionId;
  }

  static ThriftSessionState get(JobContext ctx, String sessionId) {
    checkNotNull(sessionId, "No session ID.");
    try {
      return (ThriftSessionState) ctx.getSharedObject(sessionKey(sessionId));
    } catch (NoSuchElementException nsee) {
      throw new NoSuchElementException(String.format("Session %s not found.", sessionId));
    }
  }

  static ThriftSessionState create(JobContext ctx, String sessionId) throws Exception {
    checkNotNull(sessionId, "No session ID.");
    String key = sessionKey(sessionId);
    synchronized (CREATE_LOCK) {
      try {
        ctx.getSharedObject(key);
        throw new IllegalStateException(String.format("Session %s already exists.", sessionId));
      } catch (NoSuchElementException nsee) {
        // Expected.
      }

      ThriftSessionState state = new ThriftSessionState(ctx, sessionId);
      ctx.setSharedObject(key, state);
      return state;
    }
  }

  private final JobContext ctx;
  private final SparkSession spark;
  private final String sessionId;
  private final ConcurrentMap<String, StatementState> statements;

  private ThriftSessionState(JobContext ctx, String sessionId) throws Exception {
    this.ctx = ctx;
    this.sessionId = sessionId;
    this.statements = new ConcurrentHashMap<>();
    this.spark = ctx.<SparkSession>sparkSession().newSession();
  }

  SparkSession spark() {
    return spark;
  }

  void registerStatement(String statementId, StructType schema, Iterator<Row> results) {
    checkNotNull(statementId, "No statement ID.");
    StatementState state = new StatementState(schema, results);
    if (statements.putIfAbsent(statementId, state) != null) {
      throw new IllegalStateException(
        String.format("Statement %s already registered.", statementId));
    }
  }

  StatementState findStatement(String statementId) {
    checkNotNull(statementId, "No statement ID.");
    StatementState st = statements.get(statementId);
    if (st == null) {
      throw statementNotFound(statementId);
    }
    return st;
  }

  void cleanupStatement(String statementId) {
    checkNotNull(statementId, "No statement ID.");
    if (statements.remove(statementId) == null) {
      throw statementNotFound(statementId);
    }
    ctx.sc().cancelJobGroup(statementId);
  }

  void dispose() {
    ctx.removeSharedObject(sessionKey(sessionId));
  }

  private NoSuchElementException statementNotFound(String statementId) {
    return new NoSuchElementException(
      String.format("Statement %s not found in session %s.", statementId, sessionId));
  }

  private static void checkNotNull(String what, String err) {
    if (what == null) {
      throw new IllegalArgumentException(err);
    }
  }

}
