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

package org.apache.ignite.internal.table.distributed.raft.snapshot.outgoing;

import static java.util.Collections.emptyIterator;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.raft.SnapshotTxDataRequest;
import org.apache.ignite.internal.partition.replicator.network.raft.SnapshotTxDataResponse;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.table.distributed.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionAccess;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionKey;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class OutgoingSnapshotTxDataStreamingTest extends BaseIgniteAbstractTest {
    @Mock
    private PartitionAccess partitionAccess;

    @Mock
    private CatalogService catalogService;

    private OutgoingSnapshot snapshot;

    private final PartitionReplicationMessagesFactory messagesFactory = new PartitionReplicationMessagesFactory();

    private final UUID txId1 = new UUID(0, 1);
    private final UUID txId2 = new UUID(0, 2);

    private final HybridClock clock = new HybridClockImpl();

    private final TablePartitionId partition1Id = new TablePartitionId(1, 1);
    private final TablePartitionId partition2Id = new TablePartitionId(2, 2);

    private final TxMeta meta1 = new TxMeta(TxState.ABORTED, List.of(partition1Id), clock.now());
    private final TxMeta meta2 = new TxMeta(TxState.COMMITTED, List.of(partition1Id, partition2Id), clock.now());

    private final PartitionKey partitionKey = new PartitionKey(1, 1);

    @BeforeEach
    void createTestInstance() {
        when(partitionAccess.partitionKey()).thenReturn(partitionKey);

        lenient().when(partitionAccess.committedGroupConfiguration()).thenReturn(mock(RaftGroupConfiguration.class));

        snapshot = new OutgoingSnapshot(UUID.randomUUID(), partitionAccess, catalogService);
    }

    @Test
    void sendsTxDataFromStorage() {
        configurePartitionAccessToHaveExactly(txId1, meta1, txId2, meta2);

        SnapshotTxDataResponse response = getTxDataResponse(Integer.MAX_VALUE);

        assertThat(response.txIds(), is(List.of(txId1, txId2)));

        assertThat(response.txMeta(), hasSize(2));

        TxMeta txMeta0 = response.txMeta().get(0).asTxMeta();

        assertThat(txMeta0.txState(), is(TxState.ABORTED));
        assertThat(txMeta0.enlistedPartitions(), contains(partition1Id));
        assertThat(txMeta0.commitTimestamp(), is(meta1.commitTimestamp()));

        TxMeta txMeta1 = response.txMeta().get(1).asTxMeta();

        assertThat(txMeta1.txState(), is(TxState.COMMITTED));
        assertThat(txMeta1.enlistedPartitions(), contains(partition1Id, partition2Id));
        assertThat(txMeta1.commitTimestamp(), is(meta2.commitTimestamp()));
    }

    private void configurePartitionAccessToHaveExactly(UUID txId1, TxMeta meta1, UUID txId2, TxMeta meta2) {
        when(partitionAccess.getAllTxMeta()).thenReturn(Cursor.fromBareIterator(
                List.of(new IgniteBiTuple<>(txId1, meta1), new IgniteBiTuple<>(txId2, meta2)).iterator())
        );

        snapshot.freezeScopeUnderMvLock();
    }

    private SnapshotTxDataResponse getTxDataResponse(int maxTxsInBatch) {
        SnapshotTxDataResponse response = getNullableTxDataResponse(maxTxsInBatch);

        assertThat(response, is(notNullValue()));

        return response;
    }

    @Nullable
    private SnapshotTxDataResponse getNullableTxDataResponse(int maxTxsInBatch) {
        SnapshotTxDataRequest request = messagesFactory.snapshotTxDataRequest()
                .id(snapshot.id())
                .maxTransactionsInBatch(maxTxsInBatch)
                .build();

        return snapshot.handleSnapshotTxDataRequest(request);
    }

    private void configurePartitionAccessToBeEmpty() {
        when(partitionAccess.getAllTxMeta()).thenReturn(Cursor.fromBareIterator(emptyIterator()));

        snapshot.freezeScopeUnderMvLock();
    }

    @Test
    void finalTxDataChunkHasFinishTrue() {
        configurePartitionAccessToBeEmpty();

        SnapshotTxDataResponse response = getTxDataResponse(Integer.MAX_VALUE);

        assertTrue(response.finish());
    }

    @Test
    void txDataHandlingRespectsBatchSizeHintForMessagesFromPartition() {
        configurePartitionAccessToHaveExactly(txId1, meta1, txId2, meta2);

        SnapshotTxDataResponse response = getTxDataResponse(1);

        assertThat(response.txIds(), hasSize(1));
    }

    @Test
    void txDataResponseThatIsNotLastHasFinishFalse() {
        configurePartitionAccessToHaveExactly(txId1, meta1, txId2, meta2);

        SnapshotTxDataResponse response = getTxDataResponse(1);

        assertFalse(response.finish());
    }

    @Test
    void closesCursorWhenTxDataIsExhaustedInPartition() {
        Cursor<IgniteBiTuple<UUID, TxMeta>> cursor = spy(Cursor.fromBareIterator(emptyIterator()));

        when(partitionAccess.getAllTxMeta()).thenReturn(cursor);
        snapshot.freezeScopeUnderMvLock();

        getTxDataResponse(Integer.MAX_VALUE);

        verify(cursor).close();
    }

    @Test
    void returnsNullTxDataResponseWhenClosed() {
        snapshot.close();

        assertThat(getNullableTxDataResponse(Integer.MAX_VALUE), is(nullValue()));
    }
}
