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
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.CatalogTableType;
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;
import org.apache.spark.sql.catalyst.expressions.GenericRow;

public class GetTablesJob extends SparkCatalogJob {
  private final String databasePattern;
  private final String tablePattern;
  private final List<String> tableTypes = new ArrayList<String>();

  public GetTablesJob(
      String databasePattern,
      String tablePattern,
      List<String> tableTypes,
      String sessionId,
      String jobId,
      DataType[] resultTypes) {
    super(sessionId, jobId, resultTypes);
    this.databasePattern = convertSchemaPattern(databasePattern);
    this.tablePattern = convertIdentifierPattern(tablePattern, true);
    if (tableTypes != null) {
      for (String type : tableTypes) {
        this.tableTypes.add(type.toUpperCase());
      }
    }
  }

  @Override
  protected List<Row> fetchCatalogObjects(SessionCatalog catalog) {
    List<Row> tableList = new ArrayList<Row>();
    List<String> databases = seqAsJavaList(catalog.listDatabases(databasePattern));
    for (String db : databases) {
      List<TableIdentifier> tableIdentifiers =
        seqAsJavaList(catalog.listTables(db, tablePattern));
      for (TableIdentifier tableIdentifier : tableIdentifiers) {
        CatalogTable table = catalog.getTempViewOrPermanentTableMetadata(tableIdentifier);
        String type = convertTableType(table.tableType().name());
        if (tableTypes.isEmpty() || tableTypes.contains(type)) {
          tableList.add(new GenericRow(
            new Object[] {
              DEFAULT_HIVE_CATALOG,
              table.database(),
              table.identifier().table(),
              type,
              table.comment().isDefined() ? table.comment().get() : ""
            }));
        }
      }
    }
    return tableList;
  }

  private String convertTableType(String originalType) {
    if (originalType.equals(CatalogTableType.MANAGED().name())) {
      return ClassicTableTypes.TABLE.name();
    } else if (originalType.equals(CatalogTableType.EXTERNAL().name())) {
      return ClassicTableTypes.TABLE.name();
    } else if (originalType.equals(CatalogTableType.VIEW().name())) {
      return ClassicTableTypes.VIEW.name();
    } else {
      throw new IllegalArgumentException("Invalid spark table type " + originalType);
    }
  }

  private enum ClassicTableTypes {
    TABLE,
    VIEW,
  }
}
