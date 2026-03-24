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
     * Called when a request vote is handled.
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
     * Called when a pre-vote is handled.
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
        private static final long[] HISTOGRAM_BUCKETS =
                {10, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 20000, 50000, 100000, 200000, 500000};

        private final AtomicLongMetric overloadCount = new AtomicLongMetric(
                "OverloadCount",
                "The number of times when node was overloaded."
        );

        private final DistributionMetric handleAppendEntriesDuration = new DistributionMetric(
                "HandleAppendEntriesDuration",
                "Duration of handling append entries request on the node in milliseconds",
                HISTOGRAM_BUCKETS
        );

        private final DistributionMetric heartBeatRequestTime = new DistributionMetric(
                "HeartbeatRequestDuration",
                "Duration of receiving heartbeat request in milliseconds",
                HISTOGRAM_BUCKETS
        );

        private final DistributionMetric appendEntriesBatchSize = new DistributionMetric(
                "AppendEntriesBatchSize",
                "The histogram of the batch size to handle append entries request",
                new long[]{10L, 20L, 30L, 40L, 50L}
        );

        private final DistributionMetric requestVoteDuration = new DistributionMetric(
                "RequestVoteDuration",
                "Duration of handling request vote request in milliseconds",
                HISTOGRAM_BUCKETS
        );

        private final DistributionMetric handleReadRequestDuration = new DistributionMetric(
                "ReadRequestDuration",
                "Duration of handling read request in milliseconds",
                HISTOGRAM_BUCKETS
        );

        private final DistributionMetric readRequestBatchSize = new DistributionMetric(
                "ReadRequestBatchSize",
                "The histogram of the batch size to handle read request",
                new long[]{10L, 20L, 30L, 40L, 50L}
        );

        private final DistributionMetric handleGetLeaderDuration = new DistributionMetric(
                "GetLeaderDuration",
                "Duration of handling get leader request in milliseconds",
                HISTOGRAM_BUCKETS
        );

        private final DistributionMetric preVoteDuration = new DistributionMetric(
                "PreVoteDuration",
                "Duration of handling pre-vote request in milliseconds",
                HISTOGRAM_BUCKETS
        );

        private final DistributionMetric installSnapshotDuration = new DistributionMetric(
                "InstallSnapshotDuration",
                "Duration of handling install snapshot request in milliseconds",
                HISTOGRAM_BUCKETS
        );

        private final AtomicLongMetric lockBlockedCount = new AtomicLongMetric(
                "LockBlockedCount",
                "The number of times when node lock was blocked"
        );

        private final DistributionMetric lockBlockedDuration = new DistributionMetric(
                "LockBlockedDuration",
                "Duration of lock being blocked in milliseconds",
                HISTOGRAM_BUCKETS
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
