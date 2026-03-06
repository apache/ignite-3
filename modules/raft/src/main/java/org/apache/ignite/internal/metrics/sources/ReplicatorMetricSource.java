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
import org.apache.ignite.internal.metrics.IntGauge;
import org.apache.ignite.internal.metrics.LongGauge;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.raft.jraft.core.Replicator;
import org.apache.ignite.raft.jraft.option.ReplicatorOptions;

/** Replicator metric source. */
public class ReplicatorMetricSource extends AbstractMetricSource<ReplicatorMetricSource.Holder> {
    public static final String SOURCE_NAME = "raft.replicator";
    private final Replicator replicator;
    private final ReplicatorOptions opts;

    /**
     * Constructor.
     *
     * @param groupId Raft group id.
     * @param replicator Replicator instance.
     * @param opts Replicator options.
     */
    public ReplicatorMetricSource(String groupId, Replicator replicator, ReplicatorOptions opts) {
        super(sourceName(groupId), "Replicator metrics.", "replicators");

        this.replicator = replicator;
        this.opts = opts;
    }

    private static String sourceName(String groupId) {
        return SOURCE_NAME + '.' + groupId;
    }

    @Override
    protected Holder createHolder() {
        return new Holder();
    }

    /**
     * Called when entries are replicated.
     *
     * @param duration Duration of the operation.
     * @param entriesSize Number of entries replicated.
     * @param bytesSize Bytes replicated.
     */
    public void onReplicateEntries(long duration, int entriesSize, int bytesSize) {
        Holder holder = holder();

        if (holder != null) {
            holder.appendEntriesDuration.value(duration);
            holder.appendEntriesSize.add(entriesSize);
            holder.appendEntriesBytes.add(bytesSize);
        }
    }

    /** Metric holder for replicator metrics. */
    protected class Holder implements AbstractMetricSource.Holder<Holder> {
        private final IntGauge inflightCount = new IntGauge(
                "InflightCount",
                "The current count of inflight messages in replicator.",
                replicator::getInflightsSize
        );

        private final LongGauge logLags = new LongGauge(
                "LogLags",
                "The lags of logs between leader and follower.",
                () -> opts.getLogManager().getLastLogIndex() - (replicator.getNextIndex() - 1)
        );

        private final LongGauge nextIndex = new LongGauge(
                "NextIndex",
                "The next log index to send to follower.",
                replicator::getNextIndex
        );

        private final LongGauge heartbeatTimes = new LongGauge(
                "HeartbeatTimes",
                "The times of sending heartbeat messages.",
                replicator::getHeartbeatCounter
        );

        private final LongGauge installSnapshotTimes = new LongGauge(
                "InstallSnapshotTimes",
                "The times of sending install snapshot requests.",
                replicator::getInstallSnapshotCounter
        );

        private final LongGauge probeTimes = new LongGauge(
                "ProbeTimes",
                "The times of sending probe requests.",
                replicator::getProbeCounter
        );

        private final AtomicLongMetric appendEntriesDuration = new AtomicLongMetric(
                "AppendEntriesDuration",
                "The last duration of sending append entries request."
        );

        private final IntGauge consecutiveErrorTimes = new IntGauge(
                "ConsecutiveErrorTimes",
                "The count of consecutive errors.",
                replicator::getConsecutiveErrorTimes
        );

        private final IntGauge state = new IntGauge(
                "State",
                "The ordinal of the current replicator state.",
                replicator::getStateOrdinal
        );

        private final IntGauge runningState = new IntGauge(
                "RunningState",
                "The ordinal of the current running state.",
                replicator::getRunningStateOrdinal
        );

        private final IntGauge locked = new IntGauge(
                "Locked",
                "The lock status: -1 if id is null, 1 if locked, 0 if unlocked.",
                replicator::getLockedStatus
        );

        private final DistributionMetric appendEntriesSize = new DistributionMetric(
                "AppendEntriesSize",
                "The distribution of the entries size of append entries requests.",
                new long[] {10, 20, 30, 40, 50}
        );

        private final AtomicLongMetric appendEntriesBytes = new AtomicLongMetric(
                "AppendEntriesBytes",
                "The total bytes of append entries requests."
        );

        private final List<Metric> metrics = List.of(
                inflightCount,
                logLags,
                nextIndex,
                heartbeatTimes,
                installSnapshotTimes,
                probeTimes,
                appendEntriesDuration,
                appendEntriesSize,
                appendEntriesBytes,
                consecutiveErrorTimes,
                state,
                runningState,
                locked
        );

        @Override
        public Iterable<Metric> metrics() {
            return metrics;
        }
    }
}
