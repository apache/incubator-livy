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

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;

import org.apache.livy.Job;
import org.apache.livy.JobContext;

public class GetTablesJob implements Job<List<Object[]>> {
    private final String databasePattern;
    private final String tablePattern;
    private final List<String> tableTypes = new ArrayList<String>();

    private static final String DEFAULT_HIVE_CATALOG = "";

    public GetTablesJob(String databasePattern, String tablePattern, List<String> tableTypes) {
        this.databasePattern = databasePattern;
        this.tablePattern = tablePattern;
        for (String type: tableTypes) {
            this.tableTypes.add(type.toUpperCase());
        }
    }

    @Override
    public List<Object[]> call(JobContext jc) throws Exception {
        SessionCatalog catalog = ((SparkSession)jc.sparkSession()).sessionState().catalog();
        List<Object[]> tableList = new ArrayList<Object[]>();
        List<String> databases = seqAsJavaList(catalog.listDatabases(databasePattern));
        for (String db: databases) {
            List<TableIdentifier> tableIdentifiers =
                seqAsJavaList(catalog.listTables(db, tablePattern));
            for(TableIdentifier tableIdentifier: tableIdentifiers) {
                CatalogTable table = catalog.getTempViewOrPermanentTableMetadata(tableIdentifier);
                String type = table.tableType().name();
                if (tableTypes.contains(type)) {
                    tableList.add(
                        new Object[] {
                            DEFAULT_HIVE_CATALOG,
                            table.database(),
                            table.identifier().table(),
                            type,
                            table.comment().isDefined() ? table.comment().get() : ""
                        }
                    );
                }
            }
        }

        return tableList;
    }
}
