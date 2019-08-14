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

import scala.Tuple2;
import static scala.collection.JavaConversions.seqAsJavaList;

import org.apache.spark.sql.catalyst.FunctionIdentifier;
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo;

public class GetFunctionsJob extends SparkCatalogJob {
  private final String databasePattern;
  private final String functionName;

  public GetFunctionsJob(
      String databasePattern,
      String functionName,
      String sessionId,
      String jobId) {
    super(sessionId, jobId);
    this.databasePattern = databasePattern;
    this.functionName = functionName;
  }

  @Override
  protected List<Object[]> fetchCatalogObjects(SessionCatalog catalog) {
    List<Object[]> funcList = new ArrayList<Object[]>();

    List<String> databases = seqAsJavaList(catalog.listDatabases(databasePattern));
    for (String db : databases) {
      List<Tuple2<FunctionIdentifier, String>> identifiersTypes =
        seqAsJavaList(catalog.listFunctions(db, functionName));
      for (Tuple2<FunctionIdentifier, String> identifierType : identifiersTypes) {
        FunctionIdentifier function = identifierType._1;
        ExpressionInfo info = catalog.lookupFunctionInfo(function);
        funcList.add(new Object[]{
          null,
          function.database().isDefined() ? function.database().get() : null,
          function.funcName(),
          info.getUsage() + info.getExtended(),
          null,
          info.getClassName()
        });
      }
    }
    return funcList;
  }
}
