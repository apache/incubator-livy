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

import java.util.ArrayList;
import java.util.List;

import static scala.collection.JavaConversions.seqAsJavaList;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;
import org.apache.spark.sql.catalyst.expressions.GenericRow;

public class GetSchemasJob extends SparkCatalogJob {
  private final String schemaPattern;

  public GetSchemasJob(
      String schemaPattern,
      String sessionId,
      String jobId,
      DataType[] resultTypes) {
    super(sessionId, jobId, resultTypes);
    this.schemaPattern = convertSchemaPattern(schemaPattern);
  }

  @Override
  protected List<Row> fetchCatalogObjects(SessionCatalog catalog) {
    List<String> databases = seqAsJavaList(catalog.listDatabases(schemaPattern));
    List<Row> schemas = new ArrayList<>();
    for (String db : databases) {
      schemas.add(new GenericRow(new Object[] {
        db,
        DEFAULT_HIVE_CATALOG,
      }));
    }
    return schemas;
  }
}
