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

import org.apache.spark.SparkJobInfo;
import org.apache.spark.SparkStageInfo;
import org.apache.spark.SparkStatusTracker;

import org.apache.livy.Job;
import org.apache.livy.JobContext;

public class FetchProcessJob implements Job<JobProcess> {

    private final String statementId;

    public FetchProcessJob(String statementId) {
        this.statementId = statementId;
    }

    @Override
    public JobProcess call(JobContext jc) throws Exception {
        SparkStatusTracker sparkStatusTracker = jc.sc().sc().statusTracker();
        int[] jobIdsForGroup = sparkStatusTracker.getJobIdsForGroup(statementId);
        int allTask = 0;
        int completedTask = 0;
        int activeTask = 0;
        int failedTask = 0;
        for (int sparkJobId : jobIdsForGroup) {
            SparkJobInfo jobInfo = sparkStatusTracker.getJobInfo(sparkJobId).get();
            if (jobInfo != null) {
                for (int stageId : jobInfo.stageIds()) {
                    SparkStageInfo sparkStageInfo = sparkStatusTracker.getStageInfo(stageId).get();
                    if (sparkStageInfo != null) {
                        allTask += sparkStageInfo.numTasks();
                        completedTask += sparkStageInfo.numCompletedTasks();
                        activeTask += sparkStageInfo.numActiveTasks();
                        failedTask += sparkStageInfo.numFailedTasks();
                    }
                }
            }
        }
        return new JobProcess(allTask, completedTask, activeTask, failedTask);
    }
}
