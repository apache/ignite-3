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

package org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metrics.AbstractMetricSource;
import org.apache.ignite.internal.metrics.AtomicIntMetric;
import org.apache.ignite.internal.metrics.AtomicLongMetric;
import org.apache.ignite.internal.metrics.Duration;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.StringGauge;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionKey;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.incoming.IncomingSnapshotCopier;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.OutgoingSnapshotMetricsSource.Holder;
import org.apache.ignite.internal.raft.RaftGroupConfiguration;

/**
 * Tracks important metrics for the outgoing snapshot.
 *
 * <p>Note: not thread safe. Thread safe guaranteed by operation's order in {@link IncomingSnapshotCopier}.
 */
public class OutgoingSnapshotMetricsSource extends AbstractMetricSource<Holder> {
    private static final IgniteLogger LOG = Loggers.forClass(OutgoingSnapshotMetricsSource.class);

    private final Duration currentBatchDuration = new Duration();

    private long combinedBatchDuration;

    private final Duration totalSnapshotDuration = new Duration();

    private final List<String> peers = new ArrayList<>();

    private final List<String> learners = new ArrayList<>();

    private final UUID snapshotId;

    private final PartitionKey partitionKey;

    /** Constructor. */
    public OutgoingSnapshotMetricsSource(UUID snapshotId, PartitionKey partitionKey) {
        super("snapshots.outgoing." + snapshotId);

        this.snapshotId = snapshotId;
        this.partitionKey = partitionKey;
    }

    void onSnapshotStart() {
        totalSnapshotDuration.onStart();
    }

    void onSnapshotEnd() {
        totalSnapshotDuration.onEnd();

        Holder h = holder();

        if (h != null) {
            h.totalSnapshotInstallationTime.value(totalSnapshotDuration.duration(MILLISECONDS));

            long totalBatches = h.totalBatches.value();

            if (totalBatches > 0) {
                h.avgBatchDuration.value(combinedBatchDuration / totalBatches);
            }
        }
    }

    void updateSnapshotMeta(long lastAppliedIndex, long lastAppliedTerm, RaftGroupConfiguration config, int catalogVersion) {
        Holder h = holder();

        if (h != null) {
            h.lastAppliedIndex.value(lastAppliedIndex);
            h.lastAppliedTerm.value(lastAppliedTerm);
            h.catalogVersion.value(catalogVersion);

            peers.addAll(config.peers());

            learners.addAll(config.learners());
        }
    }

    void onStartMvDataBatchProcessing() {
        this.currentBatchDuration.onStart();
    }

    void onEndMvDataBatchProcessing() {
        this.currentBatchDuration.onEnd();

        Holder h = holder();

        if (h != null) {
            h.totalBatches.increment();

            long batchDuration = currentBatchDuration.duration(MILLISECONDS);

            h.minBatchDuration.value(h.totalBatches.value() == 1
                    ? batchDuration
                    : Math.min(h.minBatchDuration.value(), batchDuration)
            );

            h.maxBatchDuration.value(Math.max(h.maxBatchDuration.value(), batchDuration));

            combinedBatchDuration += batchDuration;
        }
    }

    void onProcessOutOfOrderRow(long rowVersions, long totalBytes) {
        Holder h = holder();
        if (h != null) {
            h.outOfOrderRowsSent.increment();

            h.outOfOrderVersionsSent.add(rowVersions);
            h.outOfOrderTotalBytesSent.add(totalBytes);
        }
    }

    void onProcessRow(long rowVersions, long totalBytes) {
        Holder h = holder();
        if (h != null) {
            h.rowsSent.increment();

            h.rowVersionsSent.add(rowVersions);
            h.totalBytesSent.add(totalBytes);
        }
    }

    void printSnapshotStats() {
        Holder h = holder();

        if (h != null && LOG.isInfoEnabled()) {
            LOG.info("Outgoing snapshot installation completed [partitionKey={}, snapshotId={}, rows={}, rowVersions={}, totalBytes={}, "
                            + " outOfOrderRows={}, outOfOrderVersions={}, outOfOrderTotalBytes={}, totalBatches={},"
                            + " avgBatchProcessingTime={}ms, minBatchProcessingTime={}ms, maxBatchProcessingTime={}ms,"
                            + " totalSnapshotInstallationTime={}ms, lastAppliedIndex={}, lastAppliedTerm={}, peers=[{}], learners=[{}],"
                            + " catalogVersion={}]",
                    partitionKey,
                    snapshotId,
                    h.rowsSent.value(),
                    h.rowVersionsSent.value(),
                    h.totalBytesSent.value(),
                    h.outOfOrderRowsSent.value(),
                    h.outOfOrderVersionsSent.value(),
                    h.outOfOrderTotalBytesSent.value(),
                    h.totalBatches.value(),
                    h.avgBatchDuration.value(),
                    h.minBatchDuration.value(),
                    h.maxBatchDuration.value(),
                    h.totalSnapshotInstallationTime.value(),
                    h.lastAppliedIndex.value(),
                    h.lastAppliedTerm.value(),
                    h.peerList.value(),
                    h.learnerList.value(),
                    h.catalogVersion.value()
            );
        }
    }

    @Override
    protected Holder createHolder() {
        return new Holder();
    }

    class Holder implements AbstractMetricSource.Holder<Holder> {
        final AtomicLongMetric rowsSent = new AtomicLongMetric("RowsSent", "Total rows sent");

        final AtomicLongMetric rowVersionsSent = new AtomicLongMetric("RowVersionsSent", "Total row versions sent");

        final AtomicLongMetric totalBytesSent = new AtomicLongMetric("TotalBytesSent", "Total bytes sent");

        final AtomicLongMetric outOfOrderRowsSent = new AtomicLongMetric("OutOfOrderRowsSent", "Total out of order rows sent");

        final AtomicLongMetric outOfOrderVersionsSent = new AtomicLongMetric("OutOfOrderVersionsSent", "Total out of order versions sent");

        final AtomicLongMetric outOfOrderTotalBytesSent = new AtomicLongMetric("OutOfOrderTotalBytesSent", "Total out of order bytes sent");

        final AtomicLongMetric totalBatches = new AtomicLongMetric("TotalBatches", "Total batches");

        final AtomicLongMetric avgBatchDuration = new AtomicLongMetric("AvgBatchProcessingTime", "Average batch processing time");

        final AtomicLongMetric minBatchDuration = new AtomicLongMetric("MinBatchProcessingTime", "Minimum batch processing time");

        final AtomicLongMetric maxBatchDuration = new AtomicLongMetric("MaxBatchProcessingTime", "Maximum batch processing time");

        final AtomicLongMetric totalSnapshotInstallationTime =
                new AtomicLongMetric("TotalSnapshotInstallationTime", "Total snapshot installation time");

        final AtomicLongMetric lastAppliedIndex = new AtomicLongMetric("LastAppliedIndex", "Last applied index");

        final AtomicLongMetric lastAppliedTerm = new AtomicLongMetric("LastAppliedTerm", "Last applied term");

        final StringGauge peerList = new StringGauge("PeerList", "Raft group peer list", () -> {
            return String.join(", ", OutgoingSnapshotMetricsSource.this.peers);
        });

        final StringGauge learnerList = new StringGauge("LearnerList", "Raft group learner list", () -> {
            return String.join(", ", OutgoingSnapshotMetricsSource.this.learners);
        });

        final AtomicIntMetric catalogVersion = new AtomicIntMetric("CatalogVersion", "Snapshot associated catalog version");

        final List<Metric> metrics = List.of(
                rowsSent,
                rowVersionsSent,
                totalBytesSent,
                outOfOrderRowsSent,
                outOfOrderVersionsSent,
                outOfOrderTotalBytesSent,
                totalBatches,
                avgBatchDuration,
                minBatchDuration,
                maxBatchDuration,
                totalSnapshotInstallationTime,
                lastAppliedIndex,
                lastAppliedTerm,
                peerList,
                learnerList,
                catalogVersion
        );

        @Override
        public Iterable<Metric> metrics() {
            return metrics;
        }
    }
}
