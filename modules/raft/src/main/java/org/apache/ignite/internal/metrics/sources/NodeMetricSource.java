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
 * Metrics of raft node.
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
            holder.handleAppendEntriesDuration.add(duration);
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
            holder.heartBeatRequestTime.add(duration);
        }
    }

    /**
     * Called when a request vote RPC completes (measured on the sender side, covers network round-trip).
     *
     * @param duration Duration of the operation.
     */
    public void onRequestVote(long duration) {
        Holder holder = holder();

        if (holder != null) {
            holder.requestVoteDuration.add(duration);
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
            holder.handleReadRequestDuration.add(duration);
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
            holder.handleGetLeaderDuration.add(duration);
        }
    }

    /**
     * Called when a pre-vote RPC completes (measured on the sender side, covers network round-trip).
     *
     * @param duration Duration of the operation.
     */
    public void onPreVote(long duration) {
        Holder holder = holder();

        if (holder != null) {
            holder.preVoteDuration.add(duration);
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
            holder.installSnapshotDuration.add(duration);
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
            holder.lockBlockedDuration.add(blockedMs);
        }
    }

    /** Metric holder for node metrics. */
    static class Holder implements AbstractMetricSource.Holder<Holder> {
        // Disk I/O path: append entries on follower (disk write + log append).
        private static final long[] APPEND_ENTRIES_BUCKETS = {2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500};

        // Fast in-memory protocol messages: heartbeat, pre-vote, read-request.
        private static final long[] IN_MEMORY_PROTOCOL_BUCKETS = {1, 2, 5, 10, 25, 50, 100, 250, 500};

        // Disk I/O path with disk write to persist vote.
        private static final long[] REQUEST_VOTE_BUCKETS = {1, 2, 5, 10, 25, 50, 100, 250, 500};

        // Batch sizes for both append-entries and read-request batches.
        private static final long[] BATCH_SIZE_BUCKETS = {1, 5, 10, 25, 50, 100, 250, 500};

        // Snapshot transfer: seconds to minutes.
        private static final long[] INSTALL_SNAPSHOT_BUCKETS = {100, 500, 1000, 2000, 5000, 10000, 30000, 60000, 120000, 300000};

        private final AtomicLongMetric overloadCount = new AtomicLongMetric(
                "OverloadCount",
                "The number of times when node was overloaded."
        );

        private final DistributionMetric handleAppendEntriesDuration = new DistributionMetric(
                "HandleAppendEntriesDuration",
                "Duration of handling append entries request on the node in milliseconds",
                APPEND_ENTRIES_BUCKETS
        );

        private final DistributionMetric heartBeatRequestTime = new DistributionMetric(
                "HeartbeatRequestDuration",
                "Duration of receiving heartbeat request in milliseconds",
                IN_MEMORY_PROTOCOL_BUCKETS
        );

        private final DistributionMetric appendEntriesBatchSize = new DistributionMetric(
                "AppendEntriesBatchSize",
                "Append entries batch sizes",
                BATCH_SIZE_BUCKETS
        );

        private final DistributionMetric requestVoteDuration = new DistributionMetric(
                "RequestVoteDuration",
                "Round-trip duration of request vote RPC in milliseconds",
                REQUEST_VOTE_BUCKETS
        );

        private final DistributionMetric handleReadRequestDuration = new DistributionMetric(
                "ReadRequestDuration",
                "Duration of handling read request in milliseconds",
                IN_MEMORY_PROTOCOL_BUCKETS
        );

        private final DistributionMetric readRequestBatchSize = new DistributionMetric(
                "ReadRequestBatchSize",
                "Read request batch sizes",
                BATCH_SIZE_BUCKETS
        );

        private final DistributionMetric handleGetLeaderDuration = new DistributionMetric(
                "GetLeaderDuration",
                "Duration of handling get leader request in milliseconds",
                new long[]{1, 2, 5, 10, 25, 50}
        );

        private final DistributionMetric preVoteDuration = new DistributionMetric(
                "PreVoteDuration",
                "Round-trip duration of pre-vote RPC in milliseconds",
                IN_MEMORY_PROTOCOL_BUCKETS
        );

        private final DistributionMetric installSnapshotDuration = new DistributionMetric(
                "InstallSnapshotDuration",
                "Duration of handling install snapshot request in milliseconds",
                INSTALL_SNAPSHOT_BUCKETS
        );

        private final AtomicLongMetric lockBlockedCount = new AtomicLongMetric(
                "LockBlockedCount",
                "The number of times when node lock was blocked"
        );

        private final DistributionMetric lockBlockedDuration = new DistributionMetric(
                "LockBlockedDuration",
                "Duration of node lock being blocked in milliseconds",
                new long[]{1, 2, 5, 10, 25, 50, 100}
        );

        private final List<Metric> metrics = List.of(
                overloadCount,
                handleAppendEntriesDuration,
                heartBeatRequestTime,
                appendEntriesBatchSize,
                requestVoteDuration,
                handleReadRequestDuration,
                readRequestBatchSize,
                handleGetLeaderDuration,
                preVoteDuration,
                installSnapshotDuration,
                lockBlockedCount,
                lockBlockedDuration
        );

        @Override
        public Iterable<Metric> metrics() {
            return metrics;
        }
    }
}
