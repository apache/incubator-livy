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

import scala.Option;
import static scala.collection.JavaConversions.seqAsJavaList;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.execution.command.ShowDatabasesCommand;

import org.apache.livy.Job;
import org.apache.livy.JobContext;

public class GetSchemasJob implements Job<List<Object[]>> {
    private final String schemaName;

    public GetSchemasJob(String schemaName) {
        this.schemaName = schemaName;
    }

    @Override
    public List<Object[]> call(JobContext jc) throws Exception {
        ShowDatabasesCommand cmd = new ShowDatabasesCommand(Option.apply(schemaName));
        List<Row> rows = seqAsJavaList(cmd.run(jc.sparkSession()));
        List<Object[]> schemas = new ArrayList<>();
        for(Row r : rows) {
            schemas.add(new Object[]{
                r.getString(0),
                ""
            });
        }
        return schemas;
    }
}
