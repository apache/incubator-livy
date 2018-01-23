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

package org.apache.livy;

import java.io.File;
import java.util.NoSuchElementException;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Holds runtime information about the job execution context.
 *
 * An instance of this class is kept on the node hosting a remote Spark context and is made
 * available to jobs being executed via RemoteSparkContext#submit().
 */
public interface JobContext {

  /**
   * @return The shared SparkContext instance.
   */
  JavaSparkContext sc();

  /**
   * @return The shared SQLContext instance.
   */
  SQLContext sqlctx();

  /**
   * @return The shared HiveContext instance.
   */
  HiveContext hivectx();

  /**
   * @return The JavaStreamingContext which has already been created.
   */
  JavaStreamingContext streamingctx();

  /**
   * Get shared object
   *
   * @param <E> The type of the shared object
   * @param name Name of the shared object to return
   * @return The shared object matching name
   */
  <E> E getSharedObject(String name) throws NoSuchElementException;

  /**
   * Set shared object, it will replace the old one if already existed
   *
   * @param <E> The type of the shared object
   * @param name Name of the shared object to be set
   * @param object The object to be set
   */
  <E> void setSharedObject(String name, E object);

  /**
   * Remove shared object from cache
   *
   * @param <E> The type of the shared object
   * @param name Name of the shared object to be removed
   * @return The object that was removed
   */
  <E> E removeSharedObject(String name);

  /**
   * Creates the SparkStreaming context.
   *
   * @param batchDuration Time interval at which streaming data will be divided into batches,
   *                      in milliseconds.
   */
  void createStreamingContext(long batchDuration);

  /** Stops the SparkStreaming context. */
  void stopStreamingCtx();

  /**
   * @return A local tmp dir specific to the context
   */
  File getLocalTmpDir();

  /**
   *
   * @param <E> The type of the sparksession object
   * @return The SparkSession if it exists
   * @throws Exception If SparkSession does not exist
   */
  <E> E sparkSession() throws Exception;
}
