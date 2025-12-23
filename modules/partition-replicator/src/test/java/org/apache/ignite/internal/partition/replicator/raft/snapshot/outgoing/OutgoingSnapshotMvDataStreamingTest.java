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

import static java.util.Collections.emptyList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.raft.SnapshotMvDataRequest;
import org.apache.ignite.internal.partition.replicator.network.raft.SnapshotMvDataResponse;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionKey;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionMvStorageAccess;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionTxStateAccess;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.ZonePartitionKey;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.metrics.RaftSnapshotsMetricsSource;
import org.apache.ignite.internal.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowImpl;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class OutgoingSnapshotMvDataStreamingTest extends BaseIgniteAbstractTest {
    private static final int ZONE_ID = 0;

    private static final BinaryRow ROW_1 = new BinaryRowImpl(0, ByteBuffer.wrap(new byte[]{1}));
    private static final BinaryRow ROW_2 = new BinaryRowImpl(0, ByteBuffer.wrap(new byte[]{2}));

    private static final int TABLE_ID_1 = 1;
    private static final int TABLE_ID_2 = 2;

    private static final int PARTITION_ID = 1;

    @Mock
    private PartitionMvStorageAccess partitionAccess1;

    @Mock
    private PartitionMvStorageAccess partitionAccess2;

    private final Int2ObjectMap<PartitionMvStorageAccess> partitionsByTableId = new Int2ObjectOpenHashMap<>();

    @Mock
    private CatalogService catalogService;

    private OutgoingSnapshot snapshot;

    private final PartitionReplicationMessagesFactory messagesFactory = new PartitionReplicationMessagesFactory();

    private final RowId lowestRowId = RowId.lowestRowId(PARTITION_ID);
    private final RowId rowId1 = Objects.requireNonNull(lowestRowId.increment());
    private final RowId rowId2 = Objects.requireNonNull(rowId1.increment());
    private final RowId rowId3 = Objects.requireNonNull(rowId2.increment());

    private RowId rowIdOutOfOrder;

    private final HybridClock clock = new HybridClockImpl();

    private final UUID transactionId = UUID.randomUUID();
    private final int commitTableId = 999;

    private final PartitionKey partitionKey = new ZonePartitionKey(ZONE_ID, PARTITION_ID);

    @BeforeEach
    void createTestInstance(
            @Mock Catalog catalog,
            @Mock RaftGroupConfiguration raftGroupConfiguration
    ) {
        when(partitionAccess1.tableId()).thenReturn(TABLE_ID_1);
        lenient().when(partitionAccess1.partitionId()).thenReturn(PARTITION_ID);
        when(partitionAccess1.committedGroupConfiguration()).thenReturn(raftGroupConfiguration);

        when(partitionAccess2.tableId()).thenReturn(TABLE_ID_2);
        lenient().when(partitionAccess2.partitionId()).thenReturn(PARTITION_ID);
        lenient().when(partitionAccess2.committedGroupConfiguration()).thenReturn(raftGroupConfiguration);

        when(catalogService.catalog(anyInt())).thenReturn(catalog);

        partitionsByTableId.put(TABLE_ID_1, partitionAccess1);
        partitionsByTableId.put(TABLE_ID_2, partitionAccess2);

        UUID snapshotId = UUID.randomUUID();

        snapshot = new OutgoingSnapshot(
                snapshotId,
                partitionKey,
                partitionsByTableId,
                mock(PartitionTxStateAccess.class),
                catalogService,
                new RaftSnapshotsMetricsSource()
        );

        snapshot.acquireMvLock();

        try {
            snapshot.freezeScopeUnderMvLock();
        } finally {
            snapshot.releaseMvLock();
        }
    }

    @BeforeEach
    void initRowIdOutOfOrder() {
        RowId id = rowId3;

        for (int i = 0; i < 100; i++) {
            id = Objects.requireNonNull(id.increment());
        }

        rowIdOutOfOrder = id;
    }

    @Test
    void sendsEmptyResponseForEmptyMvData(
            @Mock PartitionTxStateAccess txStateAccess,
            @Mock RaftGroupConfiguration raftGroupConfiguration
    ) {
        when(txStateAccess.committedGroupConfiguration()).thenReturn(raftGroupConfiguration);

        UUID snapshotId = UUID.randomUUID();

        snapshot = new OutgoingSnapshot(
                snapshotId,
                partitionKey,
                Int2ObjectMaps.emptyMap(),
                txStateAccess,
                catalogService,
                new RaftSnapshotsMetricsSource()
        );

        snapshot.acquireMvLock();

        try {
            snapshot.freezeScopeUnderMvLock();
        } finally {
            snapshot.releaseMvLock();
        }

        SnapshotMvDataResponse response = getMvDataResponse(Long.MAX_VALUE);

        assertThat(response.rows(), is(emptyList()));
        assertThat(response.finish(), is(true));
    }

    @Test
    void sendsCommittedAndUncommittedVersionsFromStorage() {
        ReadResult version1 = ReadResult.createFromCommitted(rowId1, ROW_1, clock.now());
        ReadResult version2 = ReadResult.createFromWriteIntent(
                rowId1,
                ROW_2,
                transactionId,
                commitTableId,
                42,
                clock.now()
        );

        configurePartitionAccessToHaveExactlyOneRowWith(List.of(version2, version1));

        SnapshotMvDataResponse response = getMvDataResponse(Long.MAX_VALUE);

        assertThat(response.rows(), hasSize(1));
        SnapshotMvDataResponse.ResponseEntry responseRow = response.rows().get(0);

        assertThat(responseRow.rowId(), is(rowId1.uuid()));
        assertThat(responseRow.txId(), is(transactionId));
        assertThat(responseRow.commitTableOrZoneId(), is(commitTableId));
        assertThat(responseRow.commitPartitionId(), is(42));
        //noinspection ConstantConditions
        assertThat(responseRow.timestamps(), is(equalTo(new long[] {version1.commitTimestamp().longValue()})));

        assertThat(responseRow.rowVersions(), hasSize(2));
        assertThat(responseRow.rowVersions().get(0).asBinaryRow(), is(ROW_1));
        assertThat(responseRow.rowVersions().get(1).asBinaryRow(), is(ROW_2));
    }

    private void configurePartitionAccessToHaveExactlyOneRowWith(List<ReadResult> versions) {
        when(partitionAccess1.closestRowId(lowestRowId)).thenReturn(rowId1);
        when(partitionAccess1.getAllRowVersions(rowId1)).thenReturn(versions);
        lenient().when(partitionAccess1.closestRowId(rowId2)).thenReturn(null);
    }

    private SnapshotMvDataResponse getMvDataResponse(long batchSizeHint) {
        SnapshotMvDataResponse response = getNullableMvDataResponse(batchSizeHint);

        assertThat(response, is(notNullValue()));

        return response;
    }

    @Nullable
    private SnapshotMvDataResponse getNullableMvDataResponse(long batchSizeHint) {
        SnapshotMvDataRequest request = messagesFactory.snapshotMvDataRequest()
                .id(snapshot.id())
                .batchSizeHint(batchSizeHint)
                .build();

        return snapshot.handleSnapshotMvDataRequest(request);
    }

    @Test
    void reversesOrderOfVersionsObtainedFromPartition() {
        ReadResult version1 = ReadResult.createFromCommitted(rowId1, ROW_1, clock.now());
        ReadResult version2 = ReadResult.createFromCommitted(rowId1, ROW_2, clock.now());

        configurePartitionAccessToHaveExactlyOneRowWith(List.of(version1, version2));

        SnapshotMvDataResponse response = getMvDataResponse(Long.MAX_VALUE);

        SnapshotMvDataResponse.ResponseEntry responseRow = response.rows().get(0);

        //noinspection ConstantConditions
        assertThat(
                responseRow.timestamps(),
                is(equalTo(new long[] {version2.commitTimestamp().longValue(), version1.commitTimestamp().longValue()}))
        );

        assertThat(responseRow.rowVersions(), hasSize(2));
        assertThat(responseRow.rowVersions().get(0).asBinaryRow(), is(ROW_2));
        assertThat(responseRow.rowVersions().get(1).asBinaryRow(), is(ROW_1));
    }

    @Test
    void iteratesRowsInPartition() {
        ReadResult version1 = ReadResult.createFromCommitted(rowId1, ROW_1, clock.now());
        ReadResult version2 = ReadResult.createFromCommitted(rowId2, ROW_2, clock.now());

        when(partitionAccess1.closestRowId(lowestRowId)).thenReturn(rowId1);
        when(partitionAccess1.getAllRowVersions(rowId1)).thenReturn(List.of(version1));
        when(partitionAccess1.closestRowId(rowId2)).thenReturn(rowId2);
        when(partitionAccess1.getAllRowVersions(rowId2)).thenReturn(List.of(version2));
        when(partitionAccess1.closestRowId(rowId3)).thenReturn(null);

        SnapshotMvDataResponse response = getMvDataResponse(Long.MAX_VALUE);

        assertThat(response.rows(), hasSize(2));

        assertThat(response.rows().get(0).rowVersions(), hasSize(1));
        assertThat(response.rows().get(0).rowVersions().get(0).asBinaryRow(), is(ROW_1));
        assertThat(response.rows().get(1).rowVersions(), hasSize(1));
        assertThat(response.rows().get(1).rowVersions().get(0).asBinaryRow(), is(ROW_2));
    }

    @Test
    void iteratesRowsInPartitionAndTable() {
        ReadResult version1 = ReadResult.createFromCommitted(rowId1, ROW_1, clock.now());
        ReadResult version2 = ReadResult.createFromCommitted(rowId2, ROW_2, clock.now());
        ReadResult version3 = ReadResult.createFromCommitted(rowId3, ROW_1, clock.now());

        when(partitionAccess1.closestRowId(lowestRowId)).thenReturn(rowId1);
        when(partitionAccess1.getAllRowVersions(rowId1)).thenReturn(List.of(version1));
        when(partitionAccess1.closestRowId(rowId2)).thenReturn(rowId2);
        when(partitionAccess1.getAllRowVersions(rowId2)).thenReturn(List.of(version2));

        when(partitionAccess2.closestRowId(lowestRowId)).thenReturn(rowId3);
        when(partitionAccess2.getAllRowVersions(rowId3)).thenReturn(List.of(version3));

        SnapshotMvDataResponse response = getMvDataResponse(Long.MAX_VALUE);

        assertThat(response.rows(), hasSize(3));

        assertThat(response.rows().get(0).rowVersions(), hasSize(1));
        assertThat(response.rows().get(0).rowVersions().get(0).asBinaryRow(), is(ROW_1));
        assertThat(response.rows().get(1).rowVersions(), hasSize(1));
        assertThat(response.rows().get(1).rowVersions().get(0).asBinaryRow(), is(ROW_2));
        assertThat(response.rows().get(2).rowVersions(), hasSize(1));
        assertThat(response.rows().get(2).rowVersions().get(0).asBinaryRow(), is(ROW_1));
    }

    @Test
    void rowsWithIdsToSkipAreIgnored() {
        snapshot.acquireMvLock();

        try {
            snapshot.addRowIdToSkip(rowId1);
        } finally {
            snapshot.releaseMvLock();
        }

        when(partitionAccess1.closestRowId(lowestRowId)).thenReturn(rowId1);
        when(partitionAccess1.closestRowId(rowId2)).thenReturn(null);

        SnapshotMvDataResponse response = getMvDataResponse(Long.MAX_VALUE);

        assertThat(response.rows(), is(empty()));
    }

    @Test
    void sendsCommittedAndUncommittedVersionsFromQueue() {
        ReadResult version1 = ReadResult.createFromCommitted(rowIdOutOfOrder, ROW_1, clock.now());
        ReadResult version2 = ReadResult.createFromWriteIntent(
                rowIdOutOfOrder,
                ROW_2,
                transactionId,
                commitTableId,
                42,
                clock.now()
        );

        when(partitionAccess1.getAllRowVersions(rowIdOutOfOrder)).thenReturn(List.of(version2, version1));

        snapshot.acquireMvLock();

        try {
            snapshot.enqueueForSending(TABLE_ID_1, rowIdOutOfOrder);
        } finally {
            snapshot.releaseMvLock();
        }

        configureClosestRowIdToBeEmpty();

        SnapshotMvDataResponse response = getMvDataResponse(Long.MAX_VALUE);

        assertThat(response.rows(), hasSize(1));
        SnapshotMvDataResponse.ResponseEntry responseRow = response.rows().get(0);

        assertThat(responseRow.rowId(), is(rowIdOutOfOrder.uuid()));
        assertThat(responseRow.txId(), is(transactionId));
        assertThat(responseRow.commitTableOrZoneId(), is(commitTableId));
        assertThat(responseRow.commitPartitionId(), is(42));
        //noinspection ConstantConditions
        assertThat(responseRow.timestamps(), is(equalTo(new long[] {version1.commitTimestamp().longValue()})));

        assertThat(responseRow.rowVersions(), hasSize(2));
        assertThat(responseRow.rowVersions().get(0).asBinaryRow(), is(ROW_1));
        assertThat(responseRow.rowVersions().get(1).asBinaryRow(), is(ROW_2));
    }

    private void configureClosestRowIdToBeEmpty() {
        lenient().when(partitionAccess1.closestRowId(lowestRowId)).thenReturn(null);
    }

    @Test
    void sendsOutOfOrderRowsWithHighestPriority() {
        ReadResult version1 = ReadResult.createFromCommitted(rowIdOutOfOrder, ROW_1, clock.now());
        ReadResult version2 = ReadResult.createFromCommitted(rowId1, ROW_2, clock.now());

        when(partitionAccess1.getAllRowVersions(rowIdOutOfOrder)).thenReturn(List.of(version1));

        snapshot.acquireMvLock();

        try {
            snapshot.enqueueForSending(TABLE_ID_1, rowIdOutOfOrder);
        } finally {
            snapshot.releaseMvLock();
        }

        configurePartitionAccessToHaveExactlyOneRowWith(List.of(version2));

        SnapshotMvDataResponse response = getMvDataResponse(Long.MAX_VALUE);

        assertThat(response.rows(), hasSize(2));

        assertThat(response.rows().get(0).rowId(), is(rowIdOutOfOrder.uuid()));
        assertThat(response.rows().get(1).rowId(), is(rowId1.uuid()));
    }

    @Test
    void doesNotEnqueueMissingRows() {
        when(partitionAccess1.getAllRowVersions(rowIdOutOfOrder)).thenReturn(emptyList());

        snapshot.acquireMvLock();

        try {
            snapshot.enqueueForSending(TABLE_ID_1, rowIdOutOfOrder);
        } finally {
            snapshot.releaseMvLock();
        }

        SnapshotMvDataResponse response = getMvDataResponse(Long.MAX_VALUE);

        assertThat(response.rows(), is(empty()));
    }

    @Test
    void sendsTombstonesWithNullBuffers() {
        ReadResult version = ReadResult.createFromCommitted(rowId1, null, clock.now());

        configurePartitionAccessToHaveExactlyOneRowWith(List.of(version));

        SnapshotMvDataResponse response = getMvDataResponse(Long.MAX_VALUE);

        assertThat(response.rows().get(0).rowVersions().get(0), is(nullValue()));
    }

    @Test
    void doesNotSendWriteIntentTimestamp() {
        ReadResult version = ReadResult.createFromWriteIntent(
                rowId1,
                ROW_1,
                transactionId,
                commitTableId,
                42,
                clock.now()
        );

        configurePartitionAccessToHaveExactlyOneRowWith(List.of(version));

        SnapshotMvDataResponse response = getMvDataResponse(Long.MAX_VALUE);

        assertThat(response.rows().get(0).timestamps().length, is(0));
    }

    @Test
    void finalMvDataChunkHasFinishTrue() {
        configureClosestRowIdToBeEmpty();

        SnapshotMvDataResponse response = getMvDataResponse(Long.MAX_VALUE);

        assertTrue(response.finish());
    }

    @Test
    void mvDataHandlingRespectsBatchSizeHintForMessagesFromPartition() {
        ReadResult version1 = ReadResult.createFromCommitted(rowId1, ROW_1, clock.now());
        ReadResult version2 = ReadResult.createFromCommitted(rowId2, ROW_2, clock.now());

        when(partitionAccess1.closestRowId(lowestRowId)).thenReturn(rowId1);
        when(partitionAccess1.getAllRowVersions(rowId1)).thenReturn(List.of(version1));
        lenient().when(partitionAccess1.closestRowId(rowId1)).thenReturn(rowId1);
        lenient().when(partitionAccess1.getAllRowVersions(rowId2)).thenReturn(List.of(version2));

        SnapshotMvDataResponse response = getMvDataResponse(1);

        assertThat(response.rows(), hasSize(1));
    }

    @Test
    void mvDataHandlingRespectsBatchSizeHintForOutOfOrderMessages() {
        ReadResult version1 = ReadResult.createFromCommitted(rowIdOutOfOrder, ROW_1, clock.now());
        ReadResult version2 = ReadResult.createFromCommitted(rowId1, ROW_2, clock.now());

        when(partitionAccess1.getAllRowVersions(rowIdOutOfOrder)).thenReturn(List.of(version1));

        snapshot.acquireMvLock();

        try {
            snapshot.enqueueForSending(TABLE_ID_1, rowIdOutOfOrder);
        } finally {
            snapshot.releaseMvLock();
        }

        lenient().when(partitionAccess1.closestRowId(lowestRowId)).thenReturn(rowId1);
        lenient().when(partitionAccess1.getAllRowVersions(rowId1)).thenReturn(List.of(version2));
        lenient().when(partitionAccess1.closestRowId(rowId2)).thenReturn(null);

        SnapshotMvDataResponse response = getMvDataResponse(1);

        assertThat(response.rows(), hasSize(1));
    }

    @Test
    void mvDataResponseThatIsNotLastHasFinishFalse() {
        ReadResult version1 = ReadResult.createFromCommitted(rowId1, ROW_1, clock.now());
        ReadResult version2 = ReadResult.createFromCommitted(rowId1, ROW_2, clock.now());

        when(partitionAccess1.closestRowId(lowestRowId)).thenReturn(rowId1);
        when(partitionAccess1.getAllRowVersions(rowId1)).thenReturn(List.of(version1, version2));
        lenient().when(partitionAccess1.closestRowId(rowId2)).thenReturn(rowId2);

        SnapshotMvDataResponse response = getMvDataResponse(1);

        assertFalse(response.finish());
    }

    @Test
    void sendsRowsFromPartitionBiggerThanHint() {
        var row = new BinaryRowImpl(0, ByteBuffer.allocate(1000));

        ReadResult version = ReadResult.createFromCommitted(rowId1, row, clock.now());

        configurePartitionAccessToHaveExactlyOneRowWith(List.of(version));

        SnapshotMvDataResponse response = getMvDataResponse(1);

        assertThat(response.rows(), hasSize(1));
        assertThat(response.rows().get(0).rowVersions(), hasSize(1));
        assertThat(response.rows().get(0).rowVersions().get(0).asBinaryRow(), is(row));
    }

    @Test
    void sendsRowsFromOutOfOrderQueueBiggerThanHint() {
        var row = new BinaryRowImpl(0, ByteBuffer.allocate(1000));

        ReadResult version = ReadResult.createFromCommitted(rowIdOutOfOrder, row, clock.now());

        when(partitionAccess1.getAllRowVersions(rowIdOutOfOrder)).thenReturn(List.of(version));

        snapshot.acquireMvLock();

        try {
            snapshot.enqueueForSending(TABLE_ID_1, rowIdOutOfOrder);
        } finally {
            snapshot.releaseMvLock();
        }

        configureClosestRowIdToBeEmpty();

        SnapshotMvDataResponse response = getMvDataResponse(1);

        assertThat(response.rows(), hasSize(1));
        assertThat(response.rows().get(0).rowVersions(), hasSize(1));
        assertThat(response.rows().get(0).rowVersions().get(0).asBinaryRow(), is(row));
    }

    @Test
    void whenNotStartedThenEvenLowestRowIdIsNotPassed() {
        snapshot.acquireMvLock();

        try {
            assertFalse(snapshot.alreadyPassedOrIrrelevant(TABLE_ID_1, lowestRowId));
        } finally {
            snapshot.releaseMvLock();
        }
    }

    @Test
    void lastSentRowIdIsPassed() {
        ReadResult version1 = ReadResult.createFromCommitted(rowId1, ROW_1, clock.now());
        ReadResult version2 = ReadResult.createFromCommitted(rowId1, ROW_2, clock.now());

        when(partitionAccess1.closestRowId(lowestRowId)).thenReturn(rowId1);
        when(partitionAccess1.getAllRowVersions(rowId1)).thenReturn(List.of(version1, version2));
        lenient().when(partitionAccess1.closestRowId(rowId2)).thenReturn(rowId2);

        getMvDataResponse(1);

        snapshot.acquireMvLock();

        try {
            assertTrue(snapshot.alreadyPassedOrIrrelevant(TABLE_ID_1, rowId1));
        } finally {
            snapshot.releaseMvLock();
        }
    }

    @Test
    void notYetSentRowIdIsNotPassed() {
        ReadResult version1 = ReadResult.createFromCommitted(rowId1, ROW_1, clock.now());
        ReadResult version2 = ReadResult.createFromCommitted(rowId1, ROW_2, clock.now());

        when(partitionAccess1.closestRowId(lowestRowId)).thenReturn(rowId1);
        when(partitionAccess1.getAllRowVersions(rowId1)).thenReturn(List.of(version1, version2));
        lenient().when(partitionAccess1.closestRowId(rowId2)).thenReturn(rowId2);

        getMvDataResponse(1);

        snapshot.acquireMvLock();

        try {
            assertFalse(snapshot.alreadyPassedOrIrrelevant(TABLE_ID_1, rowId2));
        } finally {
            snapshot.releaseMvLock();
        }
    }

    @Test
    void notYetSentRowIdIsNotPassedAcrossMultipleTables() {
        ReadResult version1 = ReadResult.createFromCommitted(rowId1, ROW_1, clock.now());
        ReadResult version2 = ReadResult.createFromCommitted(rowId1, ROW_2, clock.now());

        when(partitionAccess1.closestRowId(lowestRowId)).thenReturn(rowId1);
        when(partitionAccess1.getAllRowVersions(rowId1)).thenReturn(List.of(version1, version2));
        when(partitionAccess1.closestRowId(rowId2)).thenReturn(rowId2);
        when(partitionAccess1.getAllRowVersions(rowId2)).thenReturn(List.of(version1));

        when(partitionAccess2.closestRowId(lowestRowId)).thenReturn(rowId1);
        when(partitionAccess2.getAllRowVersions(rowId1)).thenReturn(List.of(version1, version2));
        when(partitionAccess2.closestRowId(rowId2)).thenReturn(rowId2);
        when(partitionAccess2.getAllRowVersions(rowId2)).thenReturn(List.of(version1));

        snapshot.acquireMvLock();

        try {
            assertFalse(snapshot.alreadyPassedOrIrrelevant(TABLE_ID_1, rowId1));
            assertFalse(snapshot.alreadyPassedOrIrrelevant(TABLE_ID_1, rowId2));
            assertFalse(snapshot.alreadyPassedOrIrrelevant(TABLE_ID_2, rowId1));
            assertFalse(snapshot.alreadyPassedOrIrrelevant(TABLE_ID_2, rowId2));

            getMvDataResponse(1);

            assertTrue(snapshot.alreadyPassedOrIrrelevant(TABLE_ID_1, rowId1));
            assertFalse(snapshot.alreadyPassedOrIrrelevant(TABLE_ID_1, rowId2));
            assertFalse(snapshot.alreadyPassedOrIrrelevant(TABLE_ID_2, rowId1));
            assertFalse(snapshot.alreadyPassedOrIrrelevant(TABLE_ID_2, rowId2));

            getMvDataResponse(1);

            assertTrue(snapshot.alreadyPassedOrIrrelevant(TABLE_ID_1, rowId1));
            assertTrue(snapshot.alreadyPassedOrIrrelevant(TABLE_ID_1, rowId2));
            assertFalse(snapshot.alreadyPassedOrIrrelevant(TABLE_ID_2, rowId1));
            assertFalse(snapshot.alreadyPassedOrIrrelevant(TABLE_ID_2, rowId2));

            getMvDataResponse(1);

            assertTrue(snapshot.alreadyPassedOrIrrelevant(TABLE_ID_1, rowId1));
            assertTrue(snapshot.alreadyPassedOrIrrelevant(TABLE_ID_1, rowId2));
            assertTrue(snapshot.alreadyPassedOrIrrelevant(TABLE_ID_2, rowId1));
            assertFalse(snapshot.alreadyPassedOrIrrelevant(TABLE_ID_2, rowId2));

            getMvDataResponse(1);

            assertTrue(snapshot.alreadyPassedOrIrrelevant(TABLE_ID_1, rowId1));
            assertTrue(snapshot.alreadyPassedOrIrrelevant(TABLE_ID_1, rowId2));
            assertTrue(snapshot.alreadyPassedOrIrrelevant(TABLE_ID_2, rowId1));
            assertTrue(snapshot.alreadyPassedOrIrrelevant(TABLE_ID_2, rowId2));
        } finally {
            snapshot.releaseMvLock();
        }
    }

    @Test
    void anyRowIdIsPassedForFinishedSnapshot() {
        ReadResult version = ReadResult.createFromCommitted(rowId1, ROW_1, clock.now());

        configurePartitionAccessToHaveExactlyOneRowWith(List.of(version));

        getMvDataResponse(Long.MAX_VALUE);

        snapshot.acquireMvLock();

        try {
            //noinspection ConstantConditions
            assertTrue(snapshot.alreadyPassedOrIrrelevant(TABLE_ID_1, rowId3.increment().increment().increment()));
        } finally {
            snapshot.releaseMvLock();
        }
    }

    @Test
    void throwsSnapshotClosedExceptionWhenClosed() {
        snapshot.close();

        assertThat(getNullableMvDataResponse(Long.MAX_VALUE), is(nullValue()));
    }

    @Test
    void doesNotReturnRowsFromNewTables(
            @Mock PartitionMvStorageAccess partitionAccess3,
            @Mock PartitionMvStorageAccess partitionAccess4
    ) {
        partitionsByTableId.put(Integer.MIN_VALUE, partitionAccess3);
        partitionsByTableId.put(Integer.MAX_VALUE, partitionAccess4);

        ReadResult version1 = ReadResult.createFromCommitted(rowId1, ROW_1, clock.now());
        ReadResult version2 = ReadResult.createFromCommitted(rowId1, ROW_2, clock.now());

        for (PartitionMvStorageAccess partitionAccess : List.of(partitionAccess1, partitionAccess2, partitionAccess3, partitionAccess4)) {
            lenient().when(partitionAccess.closestRowId(lowestRowId)).thenReturn(rowId1);
            lenient().when(partitionAccess.getAllRowVersions(rowId1)).thenReturn(List.of(version1, version2));
            lenient().when(partitionAccess.closestRowId(rowId2)).thenReturn(rowId2);
            lenient().when(partitionAccess.getAllRowVersions(rowId2)).thenReturn(List.of(version1));
        }

        snapshot.acquireMvLock();

        try {
            assertThat(snapshot.alreadyPassedOrIrrelevant(Integer.MIN_VALUE, rowId1), is(true));
            assertThat(snapshot.alreadyPassedOrIrrelevant(Integer.MAX_VALUE, rowId1), is(true));
        } finally {
            snapshot.releaseMvLock();
        }

        SnapshotMvDataResponse response = getMvDataResponse(Long.MAX_VALUE);

        assertThat(response.rows(), hasSize(4));

        snapshot.acquireMvLock();

        try {
            assertThat(snapshot.alreadyPassedOrIrrelevant(Integer.MIN_VALUE, rowId1), is(true));
            assertThat(snapshot.alreadyPassedOrIrrelevant(Integer.MAX_VALUE, rowId1), is(true));
        } finally {
            snapshot.releaseMvLock();
        }
    }
}
