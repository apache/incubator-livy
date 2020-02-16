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

import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;

import org.apache.livy.Job;
import org.apache.livy.JobContext;

public abstract class SparkCatalogJob implements Job<Void> {
  protected static final String DEFAULT_HIVE_CATALOG = "";

  private final String sessionId;
  private final String jobId;
  private final DataType[] resultTypes;

  public SparkCatalogJob(String sessionId, String jobId, DataType[] resultTypes) {
    this.sessionId = sessionId;
    this.jobId = jobId;
    this.resultTypes = resultTypes;
  }

  protected abstract List<Row> fetchCatalogObjects(SessionCatalog catalog);

  @Override
  public Void call(JobContext jc) throws Exception {
    SessionCatalog catalog = ((SparkSession)jc.sparkSession()).sessionState().catalog();
    List<Row> objects = fetchCatalogObjects(catalog);

    ThriftSessionState session = ThriftSessionState.get(jc, sessionId);
    session.registerStatement(
        jobId, SparkUtils.dataTypesToSchema(resultTypes), objects.iterator());
    return null;
  }

  /**
   * Convert wildchars and escape sequence from JDBC format to datanucleous/regex.
   *
   * This is ported from Spark Hive Thrift MetaOperation.
   */
  protected String convertIdentifierPattern(final String pattern, boolean datanucleusFormat) {
    if (pattern == null) {
      return convertPattern("%", true);
    } else {
      return convertPattern(pattern, datanucleusFormat);
    }
  }

  /**
   * Convert wildchars and escape sequence of schema pattern from JDBC format to datanucleous/regex
   * The schema pattern treats empty string also as wildchar.
   *
   * This is ported from Spark Hive Thrift MetaOperation.
   */
  protected String convertSchemaPattern(final String pattern) {
    if ((pattern == null) || pattern.isEmpty()) {
      return convertPattern("%", true);
    } else {
      return convertPattern(pattern, true);
    }
  }

  /**
   * Convert a pattern containing JDBC catalog search wildcards into
   * Java regex patterns.
   *
   * @param pattern input which may contain '%' or '_' wildcard characters.
   * @return replace %/_ with regex search characters, also handle escaped characters.
   *
   * The datanucleus module expects the wildchar as '*'. The columns search on the
   * other hand is done locally inside the hive code and that requires the regex wildchar
   * format '.*'  This is driven by the datanucleusFormat flag.
   *
   * This is ported from Spark Hive Thrift MetaOperation.
   */
  private String convertPattern(final String pattern, boolean datanucleusFormat) {
    String wStr;
    if (datanucleusFormat) {
      wStr = "*";
    } else {
      wStr = ".*";
    }
    return pattern
        .replaceAll("([^\\\\])%", "$1" + wStr).replaceAll("\\\\%", "%").replaceAll("^%", wStr)
        .replaceAll("([^\\\\])_", "$1.").replaceAll("\\\\_", "_").replaceAll("^_", ".");
  }
}
