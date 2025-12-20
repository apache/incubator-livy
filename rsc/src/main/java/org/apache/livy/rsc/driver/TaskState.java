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

import java.util.*;

import com.fasterxml.jackson.annotation.JsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum TaskState {
    Waiting("waiting"),
    Started("started"),
    Running("running"),
    Available("available"),
    Cancelling("cancelling"),
    Cancelled("cancelled"),
    Starting("starting"),
    Failed("failed");
    private static final Logger LOG = LoggerFactory.getLogger(TaskState.class);

    private final String state;

    TaskState(final String text) {
        this.state = text;
    }

    @JsonValue
    @Override
    public String toString() {
        return state;
    }

    public boolean isOneOf(TaskState... states) {
        for (TaskState s : states) {
            if (s == this) {
                return true;
            }
        }
        return false;
    }

    private static final Map<TaskState, List<TaskState>> PREDECESSORS;

    static void put(TaskState key,
                    Map<TaskState, List<TaskState>> map,
                    TaskState... values) {
        map.put(key, Collections.unmodifiableList(Arrays.asList(values)));
    }

    static {
        final Map<TaskState, List<TaskState>> predecessors =
                new EnumMap<>(TaskState.class);
        put(Waiting, predecessors);
        put(Running, predecessors, Waiting);
        put(Available, predecessors, Running);
        put(Cancelling, predecessors, Running);
        put(Failed, predecessors, Running, Cancelling);
        put(Cancelled, predecessors, Waiting, Cancelling);

        PREDECESSORS = Collections.unmodifiableMap(predecessors);
    }

    static boolean isValid(TaskState from, TaskState to) {
        return PREDECESSORS.get(to).contains(from);
    }

    static void validate(TaskState from, TaskState to) {
        LOG.debug("{} -> {}", from, to);
        if (!isValid(from, to)) {
            throw new IllegalStateException("Illegal Transition: " + from + " -> " + to);
        }
    }

}
