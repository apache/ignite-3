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
import org.apache.ignite.internal.metrics.StopWatchTimer;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionKey;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.incoming.IncomingSnapshotCopier;
import org.apache.ignite.internal.raft.RaftGroupConfiguration;

/**
 * Tracks important metrics for the outgoing snapshot.
 *
 * <p>Note: not thread safe. Thread safety guaranteed by operation's order in {@link IncomingSnapshotCopier}.
 */
class OutgoingSnapshotStats {
    private static final IgniteLogger LOG = Loggers.forClass(OutgoingSnapshotStats.class);

    long rowsSent;

    long rowVersionsSent;

    long totalBytesSent;

    long outOfOrderRowsSent;

    long outOfOrderVersionsSent;

    long outOfOrderTotalBytesSent;

    int totalBatches;

    private final StopWatchTimer currentBatchTimer = new StopWatchTimer();

    long combinedBatchDuration;

    long minBatchDuration;

    long maxBatchDuration;

    final StopWatchTimer totalSnapshotTimer = new StopWatchTimer();

    long lastAppliedIndex;

    long lastAppliedTerm;

    final List<String> peers = new ArrayList<>();

    final List<String> oldPeers = new ArrayList<>();

    final List<String> learners = new ArrayList<>();

    final List<String> oldLearners = new ArrayList<>();

    int catalogVersion;

    private final UUID snapshotId;

    private final PartitionKey partitionKey;

    /** Constructor. */
    OutgoingSnapshotStats(UUID snapshotId, PartitionKey partitionKey) {
        this.snapshotId = snapshotId;
        this.partitionKey = partitionKey;
    }

    void onSnapshotStart() {
        totalSnapshotTimer.start();
    }

    void onSnapshotEnd() {
        totalSnapshotTimer.end();
    }

    void setSnapshotMeta(long lastAppliedIndex, long lastAppliedTerm, RaftGroupConfiguration config, int catalogVersion) {
        this.lastAppliedIndex = lastAppliedIndex;
        this.lastAppliedTerm = lastAppliedTerm;
        this.catalogVersion = catalogVersion;

        peers.addAll(config.peers());

        List<String> oldPeers = config.oldPeers();
        if (oldPeers != null) {
            this.oldPeers.addAll(oldPeers);
        }

        learners.addAll(config.learners());

        List<String> oldLearners = config.oldLearners();
        if (oldLearners != null) {
            this.oldLearners.addAll(oldLearners);
        }
    }

    void onStartMvDataBatchProcessing() {
        this.currentBatchTimer.start();
    }

    void onEndMvDataBatchProcessing() {
        this.currentBatchTimer.end();

        totalBatches++;

        long batchDuration = currentBatchTimer.duration(MILLISECONDS);

        minBatchDuration = totalBatches == 1
                ? batchDuration
                : Math.min(minBatchDuration, batchDuration);

        maxBatchDuration = Math.max(maxBatchDuration, batchDuration);

        combinedBatchDuration += batchDuration;
    }

    void onProcessOutOfOrderRow(long rowVersions, long totalBytes) {
        outOfOrderRowsSent++;

        outOfOrderVersionsSent += rowVersions;
        outOfOrderTotalBytesSent += totalBytes;
    }

    void onProcessRow(long rowVersions, long totalBytes) {
        rowsSent++;

        rowVersionsSent += rowVersions;
        totalBytesSent += totalBytes;
    }

    void logSnapshotStats() {
        if (LOG.isInfoEnabled()) {
            LOG.info("Outgoing snapshot installation completed [partitionKey={}, snapshotId={}, rows={}, rowVersions={}, totalBytes={}, "
                            + " outOfOrderRows={}, outOfOrderVersions={}, outOfOrderTotalBytes={}, totalBatches={},"
                            + " avgBatchProcessingTime={}ms, minBatchProcessingTime={}ms, maxBatchProcessingTime={}ms,"
                            + " totalSnapshotInstallationTime={}ms, lastAppliedIndex={}, lastAppliedTerm={}, peers=[{}], oldPeers=[{}],"
                            + " learners=[{}], oldLearners=[{}], catalogVersion={}]",
                    partitionKey,
                    snapshotId,
                    rowsSent + outOfOrderRowsSent,
                    rowVersionsSent + outOfOrderVersionsSent,
                    totalBytesSent + outOfOrderTotalBytesSent,
                    outOfOrderRowsSent,
                    outOfOrderVersionsSent,
                    outOfOrderTotalBytesSent,
                    totalBatches,
                    totalBatches > 0 ? combinedBatchDuration / totalBatches : 0,
                    minBatchDuration,
                    maxBatchDuration,
                    totalSnapshotTimer.duration(MILLISECONDS),
                    lastAppliedIndex,
                    lastAppliedTerm,
                    peers,
                    oldPeers,
                    learners,
                    oldLearners,
                    catalogVersion
            );
        }
    }
}
