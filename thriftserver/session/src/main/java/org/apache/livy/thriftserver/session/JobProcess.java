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

import java.sql.Timestamp;

/**
 * Utility class used for transferring process status to the Livy server.
 */
public class JobProcess {
    private final int allTask;
    private final int completedTask;
    private final int activeTask;
    private final int failedTask;
    private final String PROCESS_FORMAT = "%-23s percentage: %3d%% active: %s " +
            "completed: %s failed: %s all: %s";

    public JobProcess() {
        this(0, 0, 0, 0);
    }

    public JobProcess(int allTask, int completedTask, int activeTask, int failedTask) {
        this.allTask = allTask;
        this.completedTask = completedTask;
        this.activeTask = activeTask;
        this.failedTask = failedTask;
    }

    @Override
    public String toString() {
        int percentage = (int) Math.round(this.completedTask / (double) this.allTask * 100);
        return String.format(
                PROCESS_FORMAT,
                new Timestamp(System.currentTimeMillis()),
                percentage,
                this.activeTask,
                this.completedTask,
                this.failedTask,
                this.allTask);
    }
}
