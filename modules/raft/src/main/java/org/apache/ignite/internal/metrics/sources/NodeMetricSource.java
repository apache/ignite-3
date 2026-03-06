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

package org.apache.ignite.internal.metrics.sources;

import java.util.List;
import org.apache.ignite.internal.metrics.AbstractMetricSource;
import org.apache.ignite.internal.metrics.AtomicLongMetric;
import org.apache.ignite.internal.metrics.DistributionMetric;
import org.apache.ignite.internal.metrics.Metric;

/**
 * Metrics of fsm caller.
 */
public class NodeMetricSource extends AbstractMetricSource<NodeMetricSource.Holder> {
    public static final String SOURCE_NAME = "raft.node";

    /**
     * Constructor.
     */
    public NodeMetricSource(String groupId) {
        super(sourceName(groupId), "Raft node metrics.", "nodes");
    }

    /**
     * Returns a metric source name for the given group name.
     *
     * @param groupId Raft group id.
     * @return Metric source name.
     */
    public static String sourceName(String groupId) {
        return SOURCE_NAME + '.' + groupId;
    }

    @Override
    protected Holder createHolder() {
        return new Holder();
    }

    /**
     * Called when a node apply queue overload is detected.
     */
    public void onOverload() {
        Holder holder = holder();

        if (holder != null) {
            holder.overloadCount.increment();
        }
    }

    /**
     * Called when append entries are handled.
     *
     * @param duration Duration of the operation.
     * @param entriesCount Number of entries handled.
     * @param success Whether the operation was successful.
     */
    public void onHandleAppendEntries(long duration, int entriesCount, boolean success) {
        Holder holder = holder();

        if (holder != null) {
            holder.lastHandleAppendEntriesDuration.value(duration);
            if (success) {
                holder.appendEntriesBatchSize.add(entriesCount);
            }
        }
    }

    /**
     * Called when a heartbeat request is handled.
     *
     * @param duration Duration of the operation.
     */
    public void onHandleHeartbeatRequest(long duration) {
        Holder holder = holder();

        if (holder != null) {
            holder.lastHeartBeatRequestTime.value(duration);
        }
    }

    /**
     * Called when a request vote is handled.
     *
     * @param duration Duration of the operation.
     */
    public void onRequestVote(long duration) {
        Holder holder = holder();

        if (holder != null) {
            holder.lastRequestVoteDuration.value(duration);
        }
    }

    /**
     * Called when a read request is handled.
     *
     * @param duration Duration of the operation.
     * @param entriesCount Number of entries in the read request.
     */
    public void onHandleReadRequest(long duration, int entriesCount) {
        Holder holder = holder();

        if (holder != null) {
            holder.lastHandleReadRequestDuration.value(duration);
            holder.readRequestBatchSize.add(entriesCount);
        }
    }

    /**
     * Called when a get leader request is handled.
     *
     * @param duration Duration of the operation.
     */
    public void onHandleGetLeader(long duration) {
        Holder holder = holder();

        if (holder != null) {
            holder.lastHandleGetLeaderDuration.value(duration);
        }
    }

    /**
     * Called when a pre-vote is handled.
     *
     * @param duration Duration of the operation.
     */
    public void onPreVote(long duration) {
        Holder holder = holder();

        if (holder != null) {
            holder.lastPreVoteDuration.value(duration);
        }
    }

    /**
     * Called when an install snapshot request is handled.
     *
     * @param duration Duration of the operation.
     */
    public void onInstallSnapshot(long duration) {
        Holder holder = holder();

        if (holder != null) {
            holder.lastInstallSnapshotDuration.value(duration);
        }
    }

    /**
     * Called when a lock is blocked.
     *
     * @param blockedMs Duration of the lock being blocked in milliseconds.
     */
    public void onLockBlocked(long blockedMs) {
        Holder holder = holder();

        if (holder != null) {
            holder.lockBlockedCount.increment();
            holder.lastLockBlockedDuration.value(blockedMs);
        }
    }

    /** Metric holder for node metrics. */
    static class Holder implements AbstractMetricSource.Holder<Holder> {
        private final AtomicLongMetric overloadCount = new AtomicLongMetric(
                "OverloadCount",
                "The number of times when node impl was overloaded."
        );

        private final AtomicLongMetric lastHandleAppendEntriesDuration = new AtomicLongMetric(
                "HandleAppendEntriesDuration",
                "The last duration to handle append entries request in node impl."
        );

        private final AtomicLongMetric lastHeartBeatRequestTime = new AtomicLongMetric(
                "HeartbeatRequestTime",
                "The last duration of receiving heartbeat request"
        );

        private final DistributionMetric appendEntriesBatchSize = new DistributionMetric(
                "AppendEntriesBatchSize",
                "The histogram of the batch size to handle append entries request",
                new long[]{10L, 20L, 30L, 40L, 50L}
        );

        private final AtomicLongMetric lastRequestVoteDuration = new AtomicLongMetric(
                "RequestVoteDuration",
                "The last duration of handling request vote request"
        );

        private final AtomicLongMetric lastHandleReadRequestDuration = new AtomicLongMetric(
                "ReadRequestDuration",
                "The last duration of handling read request"
        );

        private final DistributionMetric readRequestBatchSize = new DistributionMetric(
                "ReadRequestBatchSize",
                "The histogram of the batch size to handle read request",
                new long[]{10L, 20L, 30L, 40L, 50L}
        );

        private final AtomicLongMetric lastHandleGetLeaderDuration = new AtomicLongMetric(
                "GetLeaderDuration",
                "The last duration of handling get leader request"
        );

        private final AtomicLongMetric lastPreVoteDuration = new AtomicLongMetric(
                "PreVoteDuration",
                "The last duration of handling pre-vote request"
        );

        private final AtomicLongMetric lastInstallSnapshotDuration = new AtomicLongMetric(
                "InstallSnapshotDuration",
                "The last duration of handling install snapshot request"
        );

        private final AtomicLongMetric lockBlockedCount = new AtomicLongMetric(
                "LockBlockedCount",
                "The number of times when node lock was blocked"
        );

        private final AtomicLongMetric lastLockBlockedDuration = new AtomicLongMetric(
                "LockBlockedDuration",
                "The last duration of lock being blocked"
        );

        private final List<Metric> metrics = List.of(
                overloadCount,
                lastHandleAppendEntriesDuration,
                lastHeartBeatRequestTime,
                appendEntriesBatchSize,
                lastRequestVoteDuration,
                lastHandleReadRequestDuration,
                readRequestBatchSize,
                lastHandleGetLeaderDuration,
                lastPreVoteDuration,
                lastInstallSnapshotDuration,
                lockBlockedCount,
                lastLockBlockedDuration
        );

        @Override
        public Iterable<Metric> metrics() {
            return metrics;
        }
    }
}
