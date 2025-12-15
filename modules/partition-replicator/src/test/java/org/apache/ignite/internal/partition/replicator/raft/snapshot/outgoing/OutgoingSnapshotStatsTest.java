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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.ZonePartitionKey;
import org.apache.ignite.internal.raft.RaftGroupConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link OutgoingSnapshotStats}.
 */
class OutgoingSnapshotStatsTest {
    private static final long LAST_APPLIED_INDEX = 1;

    private static final long LAST_APPLIED_TERM = 2;

    private static final List<String> PEERS = List.of("node1", "node2");

    private static final List<String> OLD_PEERS = List.of("node3", "node4");

    private static final List<String> LEARNERS = List.of("node5", "node6");

    private static final List<String> OLD_LEARNERS = List.of("node7", "node8");

    private static final RaftGroupConfiguration CONFIG = new RaftGroupConfiguration(
            LAST_APPLIED_INDEX,
            LAST_APPLIED_TERM,
            0,
            0,
            PEERS,
            LEARNERS,
            OLD_PEERS,
            OLD_LEARNERS
    );

    private static final int CATALOG_VERSION = 10;

    private OutgoingSnapshotStats stats;

    @BeforeEach
    void setUp() {
        stats = new OutgoingSnapshotStats(UUID.randomUUID(), new ZonePartitionKey(0, 0));
    }

    @Test
    void totalSnapshotDuration() throws InterruptedException {
        stats.onSnapshotStart();

        TimeUnit.MILLISECONDS.sleep(1);

        stats.onSnapshotEnd();

        assertThat(stats.totalSnapshotTimer.duration(TimeUnit.MILLISECONDS), greaterThan(0L));
    }

    @Test
    void batchProcessingStats() throws InterruptedException {
        stats.onStartMvDataBatchProcessing();

        TimeUnit.MILLISECONDS.sleep(1);

        stats.onEndMvDataBatchProcessing();

        assertThat(stats.totalBatches, is(1));

        assertThat(stats.combinedBatchDuration, greaterThan(0L));
        assertThat(stats.minBatchDuration, greaterThan(0L));
        assertThat(stats.maxBatchDuration, greaterThan(0L));
    }

    @Test
    void updateSnapshotMeta() {
        stats.updateSnapshotMeta(LAST_APPLIED_INDEX, LAST_APPLIED_TERM, CONFIG, CATALOG_VERSION);

        assertThat(stats.lastAppliedIndex, is(LAST_APPLIED_INDEX));
        assertThat(stats.lastAppliedTerm, is(LAST_APPLIED_TERM));

        assertThat(stats.peers, containsInAnyOrder(PEERS.toArray()));
        assertThat(stats.oldPeers, containsInAnyOrder(OLD_PEERS.toArray()));
        assertThat(stats.learners, containsInAnyOrder(LEARNERS.toArray()));
        assertThat(stats.oldLearners, containsInAnyOrder(OLD_LEARNERS.toArray()));

        assertThat(stats.catalogVersion, is(CATALOG_VERSION));
    }

    @Test
    void onRowProcessing() {
        long rowVersions = 10;
        long bytes = 100;

        stats.onProcessRow(rowVersions, bytes);

        assertThat(stats.rowsSent, is(1L));
        assertThat(stats.rowVersionsSent, is(rowVersions));
        assertThat(stats.totalBytesSent, is(bytes));
    }

    @Test
    void onOutOfOrderRowProcessing() {
        long rowVersions = 10;
        long bytes = 100;

        stats.onProcessOutOfOrderRow(rowVersions, bytes);

        assertThat(stats.outOfOrderRowsSent, is(1L));
        assertThat(stats.outOfOrderVersionsSent, is(rowVersions));
        assertThat(stats.outOfOrderTotalBytesSent, is(bytes));
    }
}
