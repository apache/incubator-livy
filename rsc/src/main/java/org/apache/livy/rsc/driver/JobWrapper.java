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

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.livy.Job;
import org.apache.livy.rsc.RSCConf;

public class JobWrapper<T> implements Callable<Void> {

  private static final Logger LOG = LoggerFactory.getLogger(JobWrapper.class);

  public final String jobId;

  private final RSCDriver driver;

  private final Job<T> job;

  private boolean isCancelled = false;

  private Future<?> future;

  private final long firstDelayMSec = 500L;
  private final long updatePeriodMSec;

  private Timer timer = new Timer("refresh progress", true);

  public JobWrapper(RSCDriver driver, String jobId, Job<T> job) {
    this.driver = driver;
    this.jobId = jobId;
    this.job = job;
    this.updatePeriodMSec =
            driver.livyConf.getTimeAsMs(RSCConf.Entry.JOB_PROCESS_MSG_UPDATE_INTERVAL);
  }

  @Override
  public Void call() throws Exception {
    try {
      // this is synchronized to avoid races with cancel()
      synchronized (this) {
        if (isCancelled) {
          throw new CancellationException("Job has been cancelled");
        } else {
          driver.jobContext().sc().setJobGroup(jobId, "", true);
        }
      }

      timer.schedule(new TimerTask() {
        @Override
        public void run() {
          driver.handleProcessMessage(jobId);
        }
      }, firstDelayMSec, updatePeriodMSec);
      LOG.debug("refreshing process timer is started!");

      jobStarted();
      T result = job.call(driver.jobContext());
      finished(result, null);
    } catch (Throwable t) {
      // Catch throwables in a best-effort to report job status back to the client. It's
      // re-thrown so that the executor can destroy the affected thread (or the JVM can
      // die or whatever would happen if the throwable bubbled up).
      LOG.info("Failed to run job " + jobId, t);
      finished(null, t);
      throw new ExecutionException(t);
    } finally {
      driver.activeJobs.remove(jobId);
    }
    return null;
  }

  synchronized void submit(ExecutorService executor) {
    if (!isCancelled) {
      this.future = executor.submit(this);
    }
  }

  synchronized boolean cancel() {
    if (isCancelled) {
      return false;
    } else {
      isCancelled = true;
      timer.cancel();
      LOG.debug("refreshing process timer is canceled by cancel method!");
      driver.jobContext().sc().cancelJobGroup(jobId);
      return future != null ? future.cancel(true) : true;
    }
  }

  protected void finished(T result, Throwable error) {
    if (error == null) {
      driver.jobFinished(jobId, result, null);
    } else {
      driver.jobFinished(jobId, null, error);
    }
    timer.cancel();
    LOG.debug("refreshing process timer is canceled by finished method!");
  }

  protected void jobStarted() {
    driver.jobStarted(jobId);
  }

}
