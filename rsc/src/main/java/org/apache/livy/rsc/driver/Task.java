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

import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.annotation.JsonRawValue;

/**
 * Represents a submitted task (job) in an interactive session.
 */
public class Task {
  public final Integer id;
  public final AtomicReference<TaskState> state;
  @JsonRawValue
  public volatile byte[] output;
  public volatile byte[] serializedException;
  public volatile String error;
  public double progress;
  public long submitted = 0;
  public long completed = 0;

  public Task(Integer id, TaskState state, byte[] output, String error) {
    this.id = id;
    this.state = new AtomicReference<>(state);
    this.output = output;
    this.error = error;
  }

  public Task() {
    this(null, null, null, null);
  }

  public boolean compareAndTransit(final TaskState from, final TaskState to) {
    if (state.compareAndSet(from, to)) {
        TaskState.validate(from, to);
        return true;
    }
    return false;
  }

  public void updateProgress(double p) {
    if (this.state.get().isOneOf(TaskState.Cancelled, TaskState.Available)) {
        this.progress = 1.0;
    } else {
        this.progress = p;
    }
  }
}
