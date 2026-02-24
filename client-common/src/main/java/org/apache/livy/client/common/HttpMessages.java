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

package org.apache.livy.client.common;

import java.util.List;
import java.util.Map;

import org.apache.livy.JobHandle.State;
import org.apache.livy.annotations.Private;

/**
 * There are the Java representations of the JSON messages used by the client protocol.
 *
 * Note that Jackson requires an empty constructor (or annotations) to be able to instantiate
 * types, so the extra noise is necessary here.
 */
@Private
public class HttpMessages {

  public static interface ClientMessage {

  }

  public static class CreateClientRequest implements ClientMessage {

    public final Map<String, String> conf;

    public CreateClientRequest(Map<String, String> conf) {
      this.conf = conf;
    }

    private CreateClientRequest() {
      this(null);
    }

  }

  public static class SessionInfo implements ClientMessage {

    public final int id;
    public final String name;
    public final String appId;
    public final String owner;
    public final String proxyUser;
    public final String state;
    public final String kind;
    public final Map<String, String> appInfo;
    public final List<String> log;
    public final String ttl;
    public final String idleTimeout;
    public final String driverMemory;
    public final int driverCores;
    public final String executorMemory;
    public final int executorCores;
    public final Map<String, String> conf;
    public final List<String> archives;
    public final List<String> files;
    public final int heartbeatTimeoutInSecond;
    public final List<String> jars;
    public final int numExecutors;
    public final List<String> pyFiles;
    public final String queue;

    public SessionInfo(int id, String name, String appId, String owner, String state,
        String kind, Map<String, String> appInfo, List<String> log,
        String ttl, String idleTimeout, String driverMemory,
        int driverCores, String executorMemory,  int executorCores, Map<String, String> conf,
        List<String> archives, List<String> files, int heartbeatTimeoutInSecond, List<String> jars,
        int numExecutors, String proxyUser, List<String> pyFiles, String queue) {
      this.id = id;
      this.name = name;
      this.appId = appId;
      this.owner = owner;
      this.proxyUser = proxyUser;
      this.state = state;
      this.kind = kind;
      this.appInfo = appInfo;
      this.log = log;
      this.ttl = ttl;
      this.idleTimeout = idleTimeout;
      this.driverMemory = driverMemory;
      this.driverCores = driverCores;
      this.executorMemory = executorMemory;
      this.executorCores = executorCores;
      this.conf = conf;
      this.archives = archives;
      this.files = files;
      this.heartbeatTimeoutInSecond = heartbeatTimeoutInSecond;
      this.jars = jars;
      this.numExecutors = numExecutors;
      this.pyFiles = pyFiles;
      this.queue = queue;
    }

    private SessionInfo() {
      this(-1, null, null, null, null, null, null, null, null, null, null, 0, null, 0, null, null,
              null, 0, null, 0, null, null, null);
    }

  }

  public static class SerializedJob implements ClientMessage {

    public final byte[] job;
    public final String jobType;

    public SerializedJob(byte[] job, String jobType) {
      this.job = job;
      this.jobType = jobType;
    }

    private SerializedJob() {
      this(null, null);
    }

  }

  public static class AddResource implements ClientMessage {

    public final String uri;

    public AddResource(String uri) {
      this.uri = uri;
    }

    private AddResource() {
      this(null);
    }

  }

  public static class JobStatus implements ClientMessage {

    public final long id;
    public final State state;
    public final byte[] result;
    public final String error;

    public JobStatus(long id, State state, byte[] result, String error) {
      this.id = id;
      this.state = state;
      this.error = error;

      // json4s, at least, seems confused about whether a "null" in the JSON payload should
      // become a null array or a byte array with length 0. Since there shouldn't be any
      // valid serialized object in a byte array of size 0, translate that to null.
      this.result = (result != null && result.length > 0) ? result : null;

      if (this.result != null && state != State.SUCCEEDED) {
        throw new IllegalArgumentException("Result cannot be set unless job succeeded.");
      }
      // The check for "result" is not completely correct, but is here to make the unit tests work.
      if (this.result == null && error != null && state != State.FAILED) {
        throw new IllegalArgumentException("Error cannot be set unless job failed.");
      }
    }

    private JobStatus() {
      this(-1, null, null, null);
    }

  }

}
