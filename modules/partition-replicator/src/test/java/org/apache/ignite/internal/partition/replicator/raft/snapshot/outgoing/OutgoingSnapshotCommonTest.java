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

import static java.util.stream.StreamSupport.stream;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.raft.SnapshotMetaRequest;
import org.apache.ignite.internal.partition.replicator.network.raft.SnapshotMetaResponse;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionKey;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionMvStorageAccess;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionTxStateAccess;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.metrics.RaftSnapshotsMetricsSource;
import org.apache.ignite.internal.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.storage.lease.LeaseInfo;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class OutgoingSnapshotCommonTest extends BaseIgniteAbstractTest {
    private static final int ZONE_ID = 0;
    private static final int TABLE_ID_1 = 1;
    private static final int TABLE_ID_2 = 2;

    @Mock
    private PartitionMvStorageAccess partitionAccess1;

    @Mock
    private PartitionMvStorageAccess partitionAccess2;

    @Mock
    private CatalogService catalogService;

    private OutgoingSnapshot snapshot;

    private final PartitionReplicationMessagesFactory messagesFactory = new PartitionReplicationMessagesFactory();

    private final PartitionKey partitionKey = new PartitionKey(ZONE_ID, 1);

    private static final int REQUIRED_CATALOG_VERSION = 42;

    private RaftSnapshotsMetricsSource snapshotsMetricsSource;

    @BeforeEach
    void createTestInstance(
            @Mock Catalog catalog,
            @Mock CatalogTableDescriptor tableDescriptor
    ) {
        lenient().when(catalogService.catalog(anyInt())).thenReturn(catalog);
        lenient().when(catalog.table(anyInt())).thenReturn(tableDescriptor);
        lenient().when(tableDescriptor.zoneId()).thenReturn(ZONE_ID);

        var partitionsByTableId = new Int2ObjectOpenHashMap<PartitionMvStorageAccess>();

        partitionsByTableId.put(TABLE_ID_1, partitionAccess1);
        partitionsByTableId.put(TABLE_ID_2, partitionAccess2);

        UUID snapshotId = UUID.randomUUID();

        snapshotsMetricsSource = new RaftSnapshotsMetricsSource();

        snapshot = new OutgoingSnapshot(
                snapshotId,
                partitionKey,
                partitionsByTableId,
                mock(PartitionTxStateAccess.class),
                catalogService,
                snapshotsMetricsSource
        );
    }

    @Test
    void returnsKeyFromStorage() {
        assertThat(snapshot.partitionKey(), is(partitionKey));
    }

    @Test
    void sendsSnapshotMeta() {
        // Test that max applied index across all storages is sent.
        when(partitionAccess1.lastAppliedIndex()).thenReturn(99L);

        when(partitionAccess2.lastAppliedIndex()).thenReturn(100L);
        when(partitionAccess2.lastAppliedTerm()).thenReturn(3L);
        when(partitionAccess2.committedGroupConfiguration()).thenReturn(new RaftGroupConfiguration(
                13L,
                37L,
                111L,
                110L,
                List.of("peer1:3000", "peer2:3000"),
                List.of("learner1:3000", "learner2:3000"),
                List.of("peer1:3000"),
                List.of("learner1:3000")
        ));

        when(catalogService.latestCatalogVersion()).thenReturn(REQUIRED_CATALOG_VERSION);

        var leaseInfo = new LeaseInfo(333L, new UUID(1, 2), "primary");

        when(partitionAccess2.leaseInfo()).thenReturn(leaseInfo);

        snapshot.freezeScopeUnderMvLock();

        SnapshotMetaResponse response = getSnapshotMetaResponse();

        assertThat(response.meta().cfgIndex(), is(13L));
        assertThat(response.meta().cfgTerm(), is(37L));
        assertThat(response.meta().lastIncludedIndex(), is(100L));
        assertThat(response.meta().lastIncludedTerm(), is(3L));
        assertThat(response.meta().peersList(), is(List.of("peer1:3000", "peer2:3000")));
        assertThat(response.meta().learnersList(), is(List.of("learner1:3000", "learner2:3000")));
        assertThat(response.meta().oldPeersList(), is(List.of("peer1:3000")));
        assertThat(response.meta().oldLearnersList(), is(List.of("learner1:3000")));
        assertThat(response.meta().requiredCatalogVersion(), is(REQUIRED_CATALOG_VERSION));
        assertThat(response.meta().leaseStartTime(), is(leaseInfo.leaseStartTime()));
        assertThat(response.meta().primaryReplicaNodeId(), is(leaseInfo.primaryReplicaNodeId()));
        assertThat(response.meta().primaryReplicaNodeName(), is(leaseInfo.primaryReplicaNodeName()));
    }

    private SnapshotMetaResponse getSnapshotMetaResponse() {
        SnapshotMetaResponse response = getNullableSnapshotMetaResponse();

        assertThat(response, is(notNullValue()));

        return response;
    }

    @Nullable
    private SnapshotMetaResponse getNullableSnapshotMetaResponse() {
        SnapshotMetaRequest request = messagesFactory.snapshotMetaRequest()
                .id(snapshot.id())
                .build();

        return snapshot.handleSnapshotMetaRequest(request);
    }

    @Test
    void doesNotSendOldConfigWhenItIsNotThere() {
        when(partitionAccess1.committedGroupConfiguration()).thenReturn(new RaftGroupConfiguration(
                13L, 37L, 111L, 110L, List.of(), List.of(), null, null
        ));

        snapshot.freezeScopeUnderMvLock();

        SnapshotMetaResponse response = getSnapshotMetaResponse();

        assertThat(response.meta().oldPeersList(), is(nullValue()));
        assertThat(response.meta().oldLearnersList(), is(nullValue()));
    }

    @Test
    void returnsNullMetaResponseWhenClosed() {
        snapshot.close();

        assertThat(getNullableSnapshotMetaResponse(), is(nullValue()));
    }

    @Test
    void metricsCalculateCorrectly() {
        when(partitionAccess1.committedGroupConfiguration()).thenReturn(new RaftGroupConfiguration(
                13L, 37L, 111L, 110L, List.of(), List.of(), null, null
        ));

        // Given metric source enabled.

        snapshotsMetricsSource.enable();

        Metric metric = retrieveOutgoingSnapshotMetric();

        // Before outgoing snapshot is started TotalOutgoingSnapshots metric should return 0.

        assertThat(metric.getValueAsString(), is("0"));

        // After outgoing snapshot is started TotalOutgoingSnapshots metric should return 1.

        snapshot.freezeScopeUnderMvLock();

        metric = retrieveOutgoingSnapshotMetric();

        assertThat(metric.getValueAsString(), is("1"));

        // And finally after outgoing snapshot is closed TotalOutgoingSnapshots metric should return 0.

        snapshot.close();

        metric = retrieveOutgoingSnapshotMetric();

        assertThat(metric.getValueAsString(), is("0"));
    }

    private Metric retrieveOutgoingSnapshotMetric() {
        return stream(snapshotsMetricsSource.holder().metrics().spliterator(), false)
                .filter(metric -> "OutgoingSnapshots".equals(metric.name()))
                .findAny()
                .get();
    }
}
