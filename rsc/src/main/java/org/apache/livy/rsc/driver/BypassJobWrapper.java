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

import java.util.List;

import org.apache.livy.Job;
import org.apache.livy.JobHandle;
import org.apache.livy.rsc.BypassJobStatus;
import org.apache.livy.rsc.Utils;

public class BypassJobWrapper extends JobWrapper<byte[]> {

  private volatile byte[] result;
  private volatile Throwable error;
  private volatile JobHandle.State state;
  private volatile List<Integer> newSparkJobs;

  public BypassJobWrapper(RSCDriver driver, String jobId, Job<byte[]> serializedJob) {
    super(driver, jobId, serializedJob);
    state = JobHandle.State.QUEUED;
  }

  @Override
  public Void call() throws Exception {
    // we ignore the return value here, because super.call() will detect cancellation
    // and still needs to perform some cleanup
    tryTransitionToState(JobHandle.State.STARTED);

    return super.call();
  }

  @Override
  protected synchronized void finished(byte[] result, Throwable error) {
    if (error == null && tryTransitionToState(JobHandle.State.SUCCEEDED)) {
      this.result = result;
    } else if (error != null && tryTransitionToState(JobHandle.State.FAILED)) {
      this.error = error;
    }
  }

  @Override
  synchronized boolean cancel() {
    return tryTransitionToState(JobHandle.State.CANCELLED) && super.cancel();
  }

  private synchronized boolean tryTransitionToState(JobHandle.State newState) {
    boolean success = false;

    switch (this.state) {
      case QUEUED:
        if (newState == JobHandle.State.STARTED || newState == JobHandle.State.CANCELLED) {
          this.state = newState;
          success = true;
        }
        break;
      case STARTED:
        if (newState == JobHandle.State.CANCELLED || newState == JobHandle.State.SUCCEEDED  ||
            newState == JobHandle.State.FAILED) {
          this.state = newState;
          success = true;
        }
        break;
      default:
        break;
    }

    return success;
  }

  @Override
  protected void jobStarted() {
    // Do nothing; just avoid sending data back to the driver.
  }

  synchronized BypassJobStatus getStatus() {
    String stackTrace = error != null ? Utils.stackTraceAsString(error) : null;
    return new BypassJobStatus(state, result, stackTrace);
  }
}
