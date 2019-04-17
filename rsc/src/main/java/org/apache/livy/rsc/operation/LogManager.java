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

package org.apache.livy.rsc.operation;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.OperationLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.livy.rsc.RSCConf;

/**
 * LogManager
 */
public class LogManager {

    private static final Logger LOG = LoggerFactory.getLogger(LogManager.class);

    public static final String prefix = "livy-";

    private String rootDirPath = null;

    private boolean isOperationLogEnabled = false;

    private HiveConf hiveConf;

    private Map<String, OperationLog> logs = Collections.synchronizedMap(new LinkedHashMap());

    public List<String> readLog(String statementId, Long maxRow) throws SQLException {
        OperationLog tl = this.getLog(statementId) ;
        if(tl!=null){
            return tl.readOperationLog(false, maxRow) ;
        }
        return null;
    }

    public void removeLog(String statementId){
        OperationLog operationLog=logs.get(statementId);
        if(operationLog!=null){
            operationLog.close();
        }
        logs.remove(statementId);
    }

    public OperationLog getLog(String statementId) {
        return logs.get(statementId);
    }

    public LogManager(RSCConf rscConf) {
        initialize(rscConf);
    }

    private void initialize(RSCConf rscConf) {
        String operationLogLocation = rscConf.get(RSCConf.Entry.LOGGING_OPERATION_LOG_LOCATION);
        this.rootDirPath = operationLogLocation + File.separator +
                prefix + rscConf.get(RSCConf.Entry.SESSION_ID);
        isOperationLogEnabled = rscConf.getBoolean(RSCConf.Entry.LOGGING_OPERATION_ENABLED)
                && new File(operationLogLocation).isDirectory();
        String logLevel = rscConf.get(RSCConf.Entry.LOGGING_OPERATION_LEVEL);
        if(isOperationLogEnabled){
            hiveConf = new HiveConf();
            hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_LOGGING_OPERATION_ENABLED,
                    isOperationLogEnabled);
            hiveConf.set(HiveConf.ConfVars.HIVE_SERVER2_LOGGING_OPERATION_LOG_LOCATION.varname,
                    operationLogLocation);
            hiveConf.set(HiveConf.ConfVars.HIVE_SERVER2_LOGGING_OPERATION_LEVEL.varname,
                    logLevel);
            File rootDir = new File(this.rootDirPath);
            if (!rootDir.exists()) {
                rootDir.mkdir();
            }
        }
    }

    public void removeCurrentOperationLog() {
        OperationLog.removeCurrentOperationLog();
    }

    public void registerOperationLog(String statementId) {
        if (isOperationLogEnabled) {
            OperationLog operationLog = logs.get(statementId);
            if (operationLog == null) {
                synchronized (this) {
                    operationLog = logs.get(statementId);
                    if (operationLog == null) {
                        File operationLogFile = new File(
                                rootDirPath,
                                statementId);
                        try {
                            if (operationLogFile.exists()) {
                                LOG.warn("The operation log file should not exist," +
                                                " but it is already there: {}",
                                        operationLogFile.getAbsolutePath());
                                operationLogFile.delete();
                            }
                            if (!operationLogFile.createNewFile()) {
                                if (!operationLogFile.canRead() || !operationLogFile.canWrite()) {
                                    LOG.warn(
                                            "The operation log file can't be recreated," +
                                                    "or it cannot be read or written: {}",
                                            operationLogFile.getAbsolutePath());
                                    return;
                                }
                            }
                        } catch (Exception e) {
                            LOG.warn(
                                    String.format("Unable to create operation log file: %s",
                                            operationLogFile.getAbsolutePath()), e);
                            return;
                        }
                        try {
                            operationLog = new OperationLog(statementId,
                                    operationLogFile, hiveConf);
                            logs.put(statementId, operationLog);
                        } catch (FileNotFoundException e) {
                            LOG.warn(String.format(
                                    "Unable to instantiate OperationLog object for operation: %s ",
                                    statementId), e);
                            return;
                        }
                    }
                }
            }
            if(operationLog!=null) {
                // register this operationLog to current thread
                OperationLog.setCurrentOperationLog(operationLog);
            }
        }
    }

    public void shutdown() {
        if (isOperationLogEnabled) {
            logs.forEach((k, v) -> v.close());
            File rootDirFile = new File(rootDirPath);
            if (rootDirFile.isDirectory()) {
                rootDirFile.deleteOnExit();
            }
        }
    }

    public static OperationLog getOperationLogByThread() {
        return OperationLog.getCurrentOperationLog();
    }

}
