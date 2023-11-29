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

package org.apache.livy.client.http;

    import com.fasterxml.jackson.annotation.JsonValue;
    import org.apache.livy.JobHandle;
    import org.apache.livy.client.common.AbstractJobHandle;
    import org.apache.livy.client.common.HttpMessages.ClientMessage;
    import org.apache.livy.client.common.HttpMessages.SerializedStatement;
    import org.apache.livy.client.common.Serializer;

    import java.util.Map;
    import java.util.concurrent.*;

    class StatementHandleImpl<T> extends AbstractJobHandle<T> {

        private final long sessionId;
        private final LivyConnection conn;
        private final ScheduledExecutorService executor;
        private final Object lock;

        private final long initialPollInterval;
        private final long maxPollInterval;

        private long statementId;
        private T result;
        private Throwable error;
        private volatile boolean isDone;
        private volatile boolean isCancelled;
        private volatile boolean isCancelPending;

        StatementHandleImpl(HttpConf config, LivyConnection conn, long sessionId,
                            ScheduledExecutorService executor, Serializer s) {
            this.conn = conn;
            this.sessionId = sessionId;
            this.executor = executor;
            this.lock = new Object();
            this.isDone = false;

            this.initialPollInterval = config.getTimeAsMs(HttpConf.Entry.JOB_INITIAL_POLL_INTERVAL);
            this.maxPollInterval = config.getTimeAsMs(HttpConf.Entry.JOB_MAX_POLL_INTERVAL);

            if (initialPollInterval <= 0) {
                throw new IllegalArgumentException("Invalid initial poll interval.");
            }
            if (maxPollInterval <= 0 || maxPollInterval < initialPollInterval) {
                throw new IllegalArgumentException("Invalid max poll interval, or lower than initial interval.");
            }

            // The job ID is set asynchronously, and there might be a call to
            // cancel() before it's
            // set. So cancel() will always set the isCancelPending flag, even if
            // there's no job
            // ID yet. If the thread setting the job ID sees that flag, it will send
            // a cancel request
            // to the server. There's still a possibility that two cancel requests
            // will be sent,
            // but that doesn't cause any harm.
            this.isCancelPending = false;
            this.statementId = -1;
        }

        @Override
        public T get() throws ExecutionException, InterruptedException {
            try {
                return get(true, -1, TimeUnit.MILLISECONDS);
            } catch (TimeoutException te) {
                // Not gonna happen.
                throw new RuntimeException(te);
            }
        }

        @Override
        public T get(long timeout, TimeUnit unit)
                throws ExecutionException, InterruptedException, TimeoutException {
            return get(false, timeout, unit);
        }

        @Override
        public boolean isDone() {
            return isDone;
        }

        @Override
        public boolean isCancelled() {
            return isCancelled;
        }

        @Override
        public boolean cancel(final boolean mayInterrupt) {
            // Do a best-effort to detect if already cancelled, but the final say is
            // always
            // on the server side. Don't block the caller, though.
            if (!isCancelled && !isCancelPending) {
                isCancelPending = true;
                if (statementId > -1) {
                    sendCancelRequest(statementId);
                }
                return true;
            }

            return false;
        }

        @Override
        protected T result() {
            return result;
        }

        @Override
        protected Throwable error() {
            return error;
        }

        public long getStatementId() {
            return statementId;
        }

        void start(final String command, String code, String kind) {
            Runnable task = new Runnable() {
                @Override
                public void run() {
                    try {
                        ClientMessage msg = new SerializedStatement(code, kind);
                        StatementStatus statement = conn.post(msg, StatementStatus.class, "/%d/%s", sessionId,
                                command);

                        if (isCancelPending) {
                            sendCancelRequest(statement.id);
                        }

                        statementId = statement.id;

                        executor.schedule(new StatementPollTask(initialPollInterval), initialPollInterval,
                                TimeUnit.MILLISECONDS);
                    } catch (Exception e) {
                        setResult(null, e, State.FAILED);
                    }
                }
            };
            executor.submit(task);
        }

        private void sendCancelRequest(final long id) {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        // TODO: Check if sessions/ is required
                        conn.post(null, Void.class, "/%d/statements/%d/cancel", sessionId, id);
                    } catch (Exception e) {
                        setResult(null, e, State.FAILED);
                    }
                }
            });
        }

        private T get(boolean waitIndefinitely, long timeout, TimeUnit unit)
                throws ExecutionException, InterruptedException, TimeoutException {
            if (!isDone) {
                synchronized (lock) {
                    if (waitIndefinitely) {
                        while (!isDone) {
                            lock.wait();
                        }
                    } else {
                        long now = System.nanoTime();
                        long deadline = now + unit.toNanos(timeout);
                        while (!isDone && deadline > now) {
                            lock.wait(TimeUnit.NANOSECONDS.toMillis(deadline - now));
                            now = System.nanoTime();
                        }
                        if (!isDone) {
                            throw new TimeoutException();
                        }
                    }
                }
            }
            if (isCancelled) {
                throw new CancellationException();
            }
            if (error != null) {
                throw new ExecutionException(error);
            }
            return result;
        }

        private JobHandle.State getJobState(StatementStatus statement) {
            switch (StatementState.getValue(statement.state)) {
                case Waiting:
                    return JobHandle.State.QUEUED;
                case Running:
                    return JobHandle.State.STARTED;
                case Cancelled:
                    return JobHandle.State.CANCELLED;
                case Cancelling:
                    return JobHandle.State.CANCELLING;
                case Error:
                    return JobHandle.State.FAILED;
                case Available:
                    String stmtOutputStatus = (String) statement.output.get("status");
                    switch (stmtOutputStatus) {
                        case "error":
                            return JobHandle.State.FAILED;
                        case "ok":
                            return JobHandle.State.SUCCEEDED;
                        default:
                            throw new IllegalStateException(
                                    String.format("Unknown statement state %s", stmtOutputStatus));
                    }
                default:
                    throw new IllegalStateException(String.format("Unknown statement state %s", statement.state));
            }

        }

        private void setResult(T result, Throwable error, State newState) {
            if (!isDone) {
                synchronized (lock) {
                    if (!isDone) {
                        this.result = result;
                        this.error = error;
                        this.isDone = true;
                        changeState(newState);
                    }
                    lock.notifyAll();
                }
            }
        }

        private class StatementPollTask implements Runnable {

            private long currentInterval;

            StatementPollTask(long currentInterval) {
                this.currentInterval = currentInterval;
            }

            @Override
            public void run() {
                try {
                    StatementStatus statement = conn.get(StatementStatus.class, "/%d/statements/%d", sessionId,
                            statementId);
                    T result = null;
                    Throwable error = null;
                    boolean finished = false;
                    State stState = getJobState(statement);
                    switch (stState) {
                        case SUCCEEDED:
                            finished = true;
                            @SuppressWarnings("unchecked")
                            T localResult = (T) ((java.util.LinkedHashMap<String, String>) statement.output.get("data")).get("text/plain");

                            result = localResult;
                            break;

                        case FAILED:
                            finished = true;
                            if (StatementState.getValue(statement.state).equals(StatementState.Error)) {
                                // TODO find out how to retrieve error in this case
                                error = new RuntimeException((String) statement.output.get("evalue"));
                            }
                            error = new RuntimeException((String) statement.output.get("evalue"));
                            break;

                        case CANCELLED:
                            isCancelled = true;
                            finished = true;
                            break;

                        default:
                            // Nothing to do.
                    }
                    if (finished) {
                        setResult(result, error, stState);
                    } else if (stState != state) {
                        changeState(stState);
                    }
                    if (!finished) {
                        currentInterval = Math.min(currentInterval * 2, maxPollInterval);
                        executor.schedule(this, currentInterval, TimeUnit.MILLISECONDS);
                    }
                } catch (Exception e) {
                    setResult(null, e, State.FAILED);
                }
            }
        }

        @Override
        public String getId() {
            return Long.toString(statementId);
        }

        public static class StatementStatus {
            public final int id;
            public final String code;
            public final String state;
            public final Map<String, Object> output;
            public final double progress;
            public final long started;
            public final long completed;

            public StatementStatus(Integer id, String code, String state, Map<String, Object> output,
                                   double progress, long started, long completed) {
                this.id = id;
                this.code = code;
                this.state = state;
                this.output = output;
                this.progress = progress;
                this.started = started;
                this.completed = completed;
            }

            private StatementStatus() {
                this(0, null, null, null, 0.0, 0, 0);
            }
        }

        public enum StatementState {
            Waiting("waiting"), Running("running"), Available("available"), Cancelling("cancelling"), Cancelled(
                    "cancelled"), Error("error");

            private final String state;

            StatementState(final String text) {
                this.state = text;
            }

            @JsonValue
            @Override
            public String toString() {
                return state;
            }

            // TODO: Change it to capitalize
            public static StatementState getValue(String state) {
                String firstLetter = state.substring(0, 1);
                String remainingLetters = state.substring(1, state.length());
                return valueOf(firstLetter.toUpperCase().concat(remainingLetters));
            }
        }
    }

