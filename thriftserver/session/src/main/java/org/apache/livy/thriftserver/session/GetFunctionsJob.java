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

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.FunctionIdentifier;
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo;
import org.apache.spark.sql.catalyst.expressions.GenericRow;

public class GetFunctionsJob extends SparkCatalogJob {
  private static final char SEARCH_STRING_ESCAPE = '\\';
  private final String databasePattern;
  private final String functionRegex;

  public GetFunctionsJob(
      String databasePattern,
      String functionSQLSearch,
      String sessionId,
      String jobId,
      DataType[] resultTypes) {
    super(sessionId, jobId, resultTypes);
    // Spark is using regex to filter the function name, instead of SQL search patterns,
    // so we need to convert them
    this.databasePattern = convertSchemaPattern(databasePattern);
    this.functionRegex = patternToRegex(functionSQLSearch);
  }

  @Override
  protected List<Row> fetchCatalogObjects(SessionCatalog catalog) {
    List<Row> funcList = new ArrayList<>();

    List<String> databases = seqAsJavaList(catalog.listDatabases(databasePattern));
    for (String db : databases) {
      List<Tuple2<FunctionIdentifier, String>> identifiersTypes =
        seqAsJavaList(catalog.listFunctions(db, functionRegex));
      for (Tuple2<FunctionIdentifier, String> identifierType : identifiersTypes) {
        FunctionIdentifier function = identifierType._1;
        ExpressionInfo info = catalog.lookupFunctionInfo(function);
        funcList.add(new GenericRow(new Object[] {
          null,
          function.database().isDefined() ? function.database().get() : null,
          function.funcName(),
          info.getUsage() + info.getExtended(),
          null,
          info.getClassName()
        }));
      }
    }
    return funcList;
  }

  /**
   * Ported from Spark's CLIServiceUtils.
   * Converts a SQL search pattern into an equivalent Java Regex.
   *
   * @param pattern input which may contain '%' or '_' wildcard characters, or
   *                these characters escaped using { @code getSearchStringEscape()}.
   * @return replace %/_ with regex search characters, also handle escaped characters.
   */
  private String patternToRegex(String pattern) {
    if (pattern == null) {
      return ".*";
    } else {
      StringBuilder result = new StringBuilder(pattern.length());

      boolean escaped = false;
      for (int i = 0, len = pattern.length(); i < len; i++) {
        char c = pattern.charAt(i);
        if (escaped) {
          if (c != SEARCH_STRING_ESCAPE) {
            escaped = false;
          }
          result.append(c);
        } else {
          if (c == SEARCH_STRING_ESCAPE) {
            escaped = true;
          } else if (c == '%') {
            result.append(".*");
          } else if (c == '_') {
            result.append('.');
          } else {
            result.append(Character.toLowerCase(c));
          }
        }
      }
      return result.toString();
    }
  }
}
