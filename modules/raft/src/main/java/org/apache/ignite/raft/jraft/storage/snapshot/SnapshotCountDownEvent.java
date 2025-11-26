/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.storage.snapshot;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.util.FastTimestamps;

/** Snapshot countdown event, with logging of long waits for events to complete. */
class SnapshotCountDownEvent {
    private static final IgniteLogger LOG = Loggers.forClass(SnapshotCountDownEvent.class);

    private static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z").withZone(ZoneId.systemDefault());

    static final int INIT_SNAPSHOT_OP = 0;

    static final int DO_SNAPSHOT_OP = 1;

    static final int REGISTER_DOWNLOADING_SNAPSHOT_OP = 2;

    private final Lock lock = new ReentrantLock();

    private final Condition condition = lock.newCondition();

    /** Guarded by {@link #lock}. */
    private final Deque<State> stateStack = new ArrayDeque<>();

    public int incrementAndGet(int state) {
        lock.lock();
        try {
            stateStack.add(new State(state));

            return stateStack.size();
        }
        finally {
            lock.unlock();
        }
    }

    public void countDown() {
        lock.lock();
        try {
            stateStack.pollLast();

            if (stateStack.isEmpty()) {
                condition.signalAll();
            }
        }
        finally {
            lock.unlock();
        }
    }

    public void await() throws InterruptedException {
        lock.lock();
        try {
            while (!stateStack.isEmpty()) {
                if (!condition.await(30, TimeUnit.SECONDS)) {
                    LOG.warn("Failed to wait for all snapshot events: [remaining={}]", stateStack);
                }
            }
        }
        finally {
            lock.unlock();
        }
    }

    private static final class State {
        private final int state;

        private final long coarseTimeMillis;

        private State(int state, long coarseTimeMillis) {
            this.state = state;
            this.coarseTimeMillis = coarseTimeMillis;
        }

        private State(int state) {
            this(state, FastTimestamps.coarseCurrentTimeMillis());
        }

        @Override public String toString() {
            return "State ["
                + "state=" + toStringIntState(state)
                + ", coarseTime=" + toStringCoarseTimeMillis(coarseTimeMillis)
                + ']';
        }
    }

    private static String toStringIntState(int state) {
        switch (state) {
            case INIT_SNAPSHOT_OP:
                return "init";
            case DO_SNAPSHOT_OP:
                return "do";
            case REGISTER_DOWNLOADING_SNAPSHOT_OP:
                return "register downloading snapshot";
            default:
                return "unknown_" + state;
        }
    }

    private static String toStringCoarseTimeMillis(long coarseTimeMillis) {
        return DATE_TIME_FORMATTER.format(Instant.ofEpochMilli(coarseTimeMillis));
    }
}
