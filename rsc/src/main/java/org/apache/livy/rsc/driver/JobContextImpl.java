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

package org.apache.livy.rsc.driver;

import java.io.File;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import org.apache.livy.JobContext;
import org.apache.livy.rsc.Utils;

public class JobContextImpl implements JobContext {

  private final File localTmpDir;
  private volatile JavaStreamingContext streamingctx;
  private final RSCDriver driver;
  private final SparkEntries sparkEntries;

  public JobContextImpl(SparkEntries sparkEntries, File localTmpDir, RSCDriver driver) {
    this.sparkEntries = sparkEntries;
    this.localTmpDir = localTmpDir;
    this.driver = driver;
  }

  @Override
  public JavaSparkContext sc() {
    return sparkEntries.sc();
  }

  @Override
  public Object sparkSession() throws Exception {
    return sparkEntries.sparkSession();
  }

  @Override
  public SQLContext sqlctx() {
    return sparkEntries.sqlctx();
  }

  @Override
  public HiveContext hivectx() {
    return sparkEntries.hivectx();
  }

  @Override
  public synchronized JavaStreamingContext streamingctx(){
    Utils.checkState(streamingctx != null, "method createStreamingContext must be called first.");
    return streamingctx;
  }

  @Override
  public synchronized void createStreamingContext(long batchDuration) {
    Utils.checkState(streamingctx == null, "Streaming context is not null.");
    streamingctx = new JavaStreamingContext(sparkEntries.sc(), new Duration(batchDuration));
  }

  @Override
  public synchronized void stopStreamingCtx() {
    Utils.checkState(streamingctx != null, "Streaming Context is null");
    streamingctx.stop(false);
    streamingctx = null;
  }

  @Override
  public File getLocalTmpDir() {
    return localTmpDir;
  }

  public synchronized void stop() {
    if (streamingctx != null) {
      stopStreamingCtx();
    }
    sparkEntries.stop();
  }

  public void addFile(String path) {
    driver.addFile(path);
  }

  public void addJarOrPyFile(String path) throws Exception {
    driver.addJarOrPyFile(path);
  }
}
