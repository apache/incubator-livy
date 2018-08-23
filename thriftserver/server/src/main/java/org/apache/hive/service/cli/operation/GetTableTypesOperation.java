/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.service.cli.operation;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hive.service.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.livy.thriftserver.LivyThriftServer$;

/**
 * GetTableTypesOperation.
 *
 */
public class GetTableTypesOperation extends MetadataOperation {

  private static final Logger LOG = LoggerFactory.getLogger(GetTableTypesOperation.class.getName());

  protected static TableSchema RESULT_SET_SCHEMA = new TableSchema()
  .addStringColumn("TABLE_TYPE", "Table type name.");

  private final RowSet rowSet;
  private final TableTypeMapping tableTypeMapping;

  public GetTableTypesOperation(SessionHandle sessionHandle) {
    super(sessionHandle, OperationType.GET_TABLE_TYPES);
    String tableMappingStr = LivyThriftServer$.MODULE$.getInstance().get().getHiveConf()
        .getVar(HiveConf.ConfVars.HIVE_SERVER2_TABLE_TYPE_MAPPING);
    tableTypeMapping = TableTypeMappingFactory.getTableTypeMapping(tableMappingStr);
    rowSet = RowSetFactory.create(RESULT_SET_SCHEMA, getProtocolVersion(), false);
    LOG.info("Starting GetTableTypesOperation");
  }

  @Override
  public void runInternal() throws HiveSQLException {
    setState(OperationState.RUNNING);
    LOG.info("Fetching table type metadata");
    try {
      for (TableType type : TableType.values()) {
        String tableType = tableTypeMapping.mapToClientType(type.toString());
        rowSet.addRow(new String[] {tableType});
        if (LOG.isDebugEnabled()) {
          String debugMessage = getDebugMessage("table type", RESULT_SET_SCHEMA);
          LOG.debug(debugMessage, tableType);
        }
      }
      if (LOG.isDebugEnabled() && rowSet.numRows() == 0) {
        LOG.debug("No table type metadata has been returned.");
      }
      setState(OperationState.FINISHED);
      LOG.info("Fetching table type metadata has been successfully finished");
    } catch (Exception e) {
      setState(OperationState.ERROR);
      throw new HiveSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.Operation#getResultSetSchema()
   */
  @Override
  public TableSchema getResultSetSchema() throws HiveSQLException {
    assertState(new ArrayList<OperationState>(Arrays.asList(OperationState.FINISHED)));
    return RESULT_SET_SCHEMA;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.Operation#getNextRowSet(org.apache.hive.service.cli.FetchOrientation, long)
   */
  @Override
  public RowSet getNextRowSet(FetchOrientation orientation, long maxRows) throws HiveSQLException {
    assertState(new ArrayList<OperationState>(Arrays.asList(OperationState.FINISHED)));
    validateDefaultFetchOrientation(orientation);
    if (orientation.equals(FetchOrientation.FETCH_FIRST)) {
      rowSet.setStartOffset(0);
    }
    return rowSet.extractSubset((int)maxRows);
  }

}
