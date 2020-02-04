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
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.StructField;

public class GetColumnsJob extends SparkCatalogJob {
  private final String databasePattern;
  private final String tablePattern;
  private final String columnPattern;

  public GetColumnsJob(
      String databasePattern,
      String tablePattern,
      String columnPattern,
      String sessionId,
      String jobId,
      DataType[] resultTypes) {
    super(sessionId, jobId, resultTypes);
    this.databasePattern = convertSchemaPattern(databasePattern);
    this.tablePattern = convertIdentifierPattern(tablePattern, true);
    this.columnPattern = convertIdentifierPattern(columnPattern, false);
  }

  @Override
  protected List<Row> fetchCatalogObjects(SessionCatalog catalog) {
    List<Row> columnList = new ArrayList<>();
    List<String> databases = seqAsJavaList(catalog.listDatabases(databasePattern));

    for (String db : databases) {
      List<TableIdentifier> tableIdentifiers =
        seqAsJavaList(catalog.listTables(db, tablePattern));
      for (TableIdentifier tableIdentifier : tableIdentifiers) {
        CatalogTable table = catalog.getTempViewOrPermanentTableMetadata(tableIdentifier);
        List<StructField> fields = seqAsJavaList(table.schema());
        int position = 0;
        for (StructField field : fields) {
          if (field.name().matches(columnPattern)) {
            columnList.add(new GenericRow(new Object[] {
              DEFAULT_HIVE_CATALOG,
              table.database(),
              table.identifier().table(),
              field.name(),
              SparkUtils.toJavaSQLType(field.dataType()), // datatype
              field.dataType().typeName(),
              SparkUtils.getColumnSize(field.dataType()), //columnsize,
              null, // BUFFER_LENGTH, unused,
              SparkUtils.getDecimalDigits(field.dataType()),
              SparkUtils.getNumPrecRadix(field.dataType()),
              field.nullable() ? 1 : 0,
              field.getComment().isDefined() ? field.getComment().get() : "",
              null, // COLUMN_DEF
              null, // SQL_DATA_TYPE
              null, // SQL_DATETIME_SUB
              null, // CHAR_OCTET_LENGTH
              position,
              field.nullable() ? "YES" : "NO", // IS_NULLABLE
              null, // SCOPE_CATALOG
              null, // SCOPE_SCHEMA
              null, // SCOPE_TABLE
              null, // SOURCE_DATA_TYPE
              "NO" // IS_AUTO_INCREMENT
            }));
            position += 1;
          }
        }
      }
    }
    return columnList;
  }
}
