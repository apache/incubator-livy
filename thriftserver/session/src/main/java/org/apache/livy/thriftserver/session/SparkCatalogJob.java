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

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;

import org.apache.livy.Job;
import org.apache.livy.JobContext;

public abstract class SparkCatalogJob implements Job<Void> {
  protected static final String DEFAULT_HIVE_CATALOG = "";

  private final String sessionId;
  private final String jobId;

  public SparkCatalogJob(String sessionId, String jobId) {
    this.sessionId = sessionId;
    this.jobId = jobId;
  }

  protected abstract List<Object[]> fetchCatalogObjects(SessionCatalog catalog);

  @Override
  public Void call(JobContext jc) throws Exception {
    SessionCatalog catalog = ((SparkSession)jc.sparkSession()).sessionState().catalog();
    List<Object[]> objects = fetchCatalogObjects(catalog);

    ThriftSessionState session = ThriftSessionState.get(jc, sessionId);
    session.registerCatalogJob(jobId, objects.iterator());
    return null;
  }
}
