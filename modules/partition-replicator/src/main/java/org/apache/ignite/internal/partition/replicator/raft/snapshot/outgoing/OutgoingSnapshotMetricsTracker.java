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

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.metrics.Duration;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.incoming.IncomingSnapshotCopier;
import org.apache.ignite.internal.raft.RaftGroupConfiguration;

/**
 * Tracks important metrics for the outgoing snapshot.
 *
 * <p>Note: not thread safe. Thread safe guaranteed by operation's order in {@link IncomingSnapshotCopier}.
 */
class OutgoingSnapshotMetricsTracker {
    private long rowsSent;

    private long rowVersionsSent;

    private long totalBytesSent;

    private long outOfOrderRowsSent;

    private long outOfOrderVersionsSent;

    private long outOfOrderTotalBytesSent;

    private long totalBatches;

    private long combinedBatchDuration;

    private long avgBatchDuration;

    private long minBatchDuration;

    private long maxBatchDuration;

    private final Duration currentBatchDuration = new Duration();

    private final Duration totalSnapshotDuration = new Duration();

    private long lastAppliedIndex;

    private long lastAppliedTerm;

    private final List<String> peerList = new ArrayList<>();

    private final List<String> learnerList = new ArrayList<>();

    private int catalogVersion;

    void onSnapshotStart() {
        totalSnapshotDuration.onStart();
    }

    void onSnapshotEnd() {
        totalSnapshotDuration.onEnd();

        avgBatchDuration = combinedBatchDuration / totalBatches;
    }

    void updateSnapshotMeta(long lastAppliedIndex, long lastAppliedTerm, RaftGroupConfiguration config, int catalogVersion) {
        this.lastAppliedIndex = lastAppliedIndex;
        this.lastAppliedTerm = lastAppliedTerm;
        this.catalogVersion = catalogVersion;

        peerList.addAll(config.peers());

        learnerList.addAll(config.learners());
    }

    void onStartMvDataBatchProcessing() {
        this.currentBatchDuration.onStart();
    }

    void onEndMvDataBatchProcessing() {
        this.currentBatchDuration.onEnd();

        totalBatches++;

        long batchDuration = currentBatchDuration.duration(MILLISECONDS);

        minBatchDuration = totalBatches == 1 ? batchDuration : Math.min(minBatchDuration, batchDuration);
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

    String stats() {
        return format(
                "rows=%d, rowVersions=%d, totalBytes=%d, outOfOrderRows=%d, outOfOrderVersions=%d, outOfOrderTotalBytes=%d,"
                        + " totalBatches=%d, avgBatchProcessingTime=%dms, minBatchProcessingTime=%dms, maxBatchProcessingTime=%dms,"
                        + " totalSnapshotInstallationTime=%dms, lastAppliedIndex=%d, lastAppliedTerm=%d, peers=%s, learners=%s,"
                        + " catalogVersion=%d",
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
                totalSnapshotDuration.duration(MILLISECONDS),
                lastAppliedIndex,
                lastAppliedTerm,
                peerList,
                learnerList,
                catalogVersion
        );
    }
}
