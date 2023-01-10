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

package org.apache.ignite.internal.table.distributed.raft.snapshot.incoming;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedIn;
import static org.apache.ignite.internal.tx.TxState.ABORTED;
import static org.apache.ignite.internal.tx.TxState.COMMITED;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryConverter;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.TableRow;
import org.apache.ignite.internal.schema.TableRowConverter;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RaftGroupConfiguration;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.impl.TestMvPartitionStorage;
import org.apache.ignite.internal.storage.impl.TestMvTableStorage;
import org.apache.ignite.internal.table.distributed.TableMessagesFactory;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionAccessImpl;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionKey;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionSnapshotStorage;
import org.apache.ignite.internal.table.distributed.raft.snapshot.SnapshotUri;
import org.apache.ignite.internal.table.distributed.raft.snapshot.message.SnapshotMetaRequest;
import org.apache.ignite.internal.table.distributed.raft.snapshot.message.SnapshotMvDataRequest;
import org.apache.ignite.internal.table.distributed.raft.snapshot.message.SnapshotMvDataResponse.ResponseEntry;
import org.apache.ignite.internal.table.distributed.raft.snapshot.message.SnapshotTxDataRequest;
import org.apache.ignite.internal.table.distributed.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.table.distributed.replicator.TablePartitionId;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.tx.storage.state.TxStateTableStorage;
import org.apache.ignite.internal.tx.storage.state.test.TestTxStateStorage;
import org.apache.ignite.internal.tx.storage.state.test.TestTxStateTableStorage;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.entity.RaftOutter.SnapshotMeta;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.option.SnapshotCopierOptions;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotCopier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * For {@link IncomingSnapshotCopier} testing.
 */
@ExtendWith(ConfigurationExtension.class)
public class IncomingSnapshotCopierTest {
    private static final String NODE_NAME = "node";

    private static final int TEST_PARTITION = 0;

    private static final SchemaDescriptor SCHEMA_DESCRIPTOR = new SchemaDescriptor(
            1,
            new Column[]{new Column("key", NativeTypes.stringOf(256), false)},
            new Column[]{new Column("value", NativeTypes.stringOf(256), false)}
    );

    private static final HybridClock HYBRID_CLOCK = new HybridClockImpl();

    private static final TableMessagesFactory TABLE_MSG_FACTORY = new TableMessagesFactory();

    private static final RaftMessagesFactory RAFT_MSG_FACTORY = new RaftMessagesFactory();

    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    @InjectConfiguration(value = "mock.tables.foo {}")
    private TablesConfiguration tablesConfig;

    private final ClusterNode clusterNode = mock(ClusterNode.class);

    private final UUID snapshotId = UUID.randomUUID();

    @AfterEach
    void tearDown() {
        shutdownAndAwaitTermination(executorService, 1, TimeUnit.SECONDS);
    }

    @Test
    void test() throws Exception {
        MvPartitionStorage outgoingMvPartitionStorage = new TestMvPartitionStorage(TEST_PARTITION);
        TxStateStorage outgoingTxStatePartitionStorage = new TestTxStateStorage();

        long expLastAppliedIndex = 100500L;
        long expLastAppliedTerm = 100L;
        RaftGroupConfiguration expLastGroupConfig = new RaftGroupConfiguration(
                List.of("peer"),
                List.of("learner"),
                List.of("old-peer"),
                List.of("old-learner")
        );

        List<RowId> rowIds = fillMvPartitionStorage(outgoingMvPartitionStorage);
        List<UUID> txIds = fillTxStatePartitionStorage(outgoingTxStatePartitionStorage);

        outgoingMvPartitionStorage.lastApplied(expLastAppliedIndex, expLastAppliedTerm);
        outgoingMvPartitionStorage.committedGroupConfiguration(expLastGroupConfig);
        outgoingTxStatePartitionStorage.lastApplied(expLastAppliedIndex, expLastAppliedTerm);

        MvTableStorage incomingMvTableStorage = spy(new TestMvTableStorage(tablesConfig.tables().get("foo"), tablesConfig));
        TxStateTableStorage incomingTxStateTableStorage = spy(new TestTxStateTableStorage());

        incomingMvTableStorage.getOrCreateMvPartition(TEST_PARTITION);
        incomingTxStateTableStorage.getOrCreateTxStateStorage(TEST_PARTITION);

        MessagingService messagingService = messagingServiceForSuccessScenario(outgoingMvPartitionStorage,
                outgoingTxStatePartitionStorage, expLastAppliedIndex, expLastAppliedTerm, expLastGroupConfig, rowIds, txIds, snapshotId);

        PartitionSnapshotStorage partitionSnapshotStorage = createPartitionSnapshotStorage(
                snapshotId,
                incomingMvTableStorage,
                incomingTxStateTableStorage,
                messagingService
        );

        SnapshotCopier snapshotCopier = partitionSnapshotStorage.startToCopyFrom(
                SnapshotUri.toStringUri(snapshotId, NODE_NAME),
                mock(SnapshotCopierOptions.class)
        );

        assertThat(runAsync(snapshotCopier::join), willSucceedIn(1, TimeUnit.SECONDS));

        assertEquals(Status.OK().getCode(), snapshotCopier.getCode());

        MvPartitionStorage incomingMvPartitionStorage = incomingMvTableStorage.getMvPartition(TEST_PARTITION);
        TxStateStorage incomingTxStatePartitionStorage = incomingTxStateTableStorage.getTxStateStorage(TEST_PARTITION);

        assertEquals(expLastAppliedIndex, outgoingMvPartitionStorage.lastAppliedIndex());
        assertEquals(expLastAppliedTerm, outgoingMvPartitionStorage.lastAppliedTerm());
        assertEquals(expLastGroupConfig, outgoingMvPartitionStorage.committedGroupConfiguration());
        assertEquals(expLastAppliedIndex, outgoingTxStatePartitionStorage.lastAppliedIndex());
        assertEquals(expLastAppliedTerm, outgoingTxStatePartitionStorage.lastAppliedTerm());

        assertEqualsMvRows(outgoingMvPartitionStorage, incomingMvPartitionStorage, rowIds);
        assertEqualsTxStates(outgoingTxStatePartitionStorage, incomingTxStatePartitionStorage, txIds);

        // TODO: IGNITE-18030 - uncomment the following line or remove it if not needed after the rework
        //verify(incomingMvTableStorage, times(1)).destroyPartition(eq(TEST_PARTITION));
        verify(incomingMvTableStorage, times(2)).getOrCreateMvPartition(eq(TEST_PARTITION));

        // TODO: IGNITE-18030 - uncomment the following line or remove it if not needed after the rework
        //verify(incomingTxStateTableStorage, times(1)).destroyTxStateStorage(eq(TEST_PARTITION));
        verify(incomingTxStateTableStorage, times(2)).getOrCreateTxStateStorage(eq(TEST_PARTITION));
    }

    private MessagingService messagingServiceForSuccessScenario(MvPartitionStorage outgoingMvPartitionStorage,
            TxStateStorage outgoingTxStatePartitionStorage, long expLastAppliedIndex, long expLastAppliedTerm,
            RaftGroupConfiguration expLastGroupConfig, List<RowId> rowIds, List<UUID> txIds, UUID snapshotId) {
        MessagingService messagingService = mock(MessagingService.class);

        when(messagingService.invoke(eq(clusterNode), any(SnapshotMetaRequest.class), anyLong())).then(answer -> {
            SnapshotMetaRequest snapshotMetaRequest = answer.getArgument(1);

            assertEquals(snapshotId, snapshotMetaRequest.id());

            return completedFuture(
                    TABLE_MSG_FACTORY.snapshotMetaResponse()
                            .meta(
                                    RAFT_MSG_FACTORY.snapshotMeta()
                                            .lastIncludedIndex(expLastAppliedIndex)
                                            .lastIncludedTerm(expLastAppliedTerm)
                                            .peersList(expLastGroupConfig.peers())
                                            .learnersList(expLastGroupConfig.learners())
                                            .oldPeersList(expLastGroupConfig.oldPeers())
                                            .oldLearnersList(expLastGroupConfig.oldLearners())
                                            .build()
                            )
                            .build()
            );
        });

        when(messagingService.invoke(eq(clusterNode), any(SnapshotMvDataRequest.class), anyLong())).then(answer -> {
            SnapshotMvDataRequest snapshotMvDataRequest = answer.getArgument(1);

            assertEquals(snapshotId, snapshotMvDataRequest.id());

            List<ResponseEntry> responseEntries = createSnapshotMvDataEntries(outgoingMvPartitionStorage, rowIds);

            assertThat(responseEntries, not(empty()));

            return completedFuture(TABLE_MSG_FACTORY.snapshotMvDataResponse().rows(responseEntries).finish(true).build());
        });

        when(messagingService.invoke(eq(clusterNode), any(SnapshotTxDataRequest.class), anyLong())).then(answer -> {
            SnapshotTxDataRequest snapshotTxDataRequest = answer.getArgument(1);

            assertEquals(snapshotId, snapshotTxDataRequest.id());

            List<TxMeta> txMetas = txIds.stream().map(outgoingTxStatePartitionStorage::get).collect(toList());

            return completedFuture(TABLE_MSG_FACTORY.snapshotTxDataResponse().txIds(txIds).txMeta(txMetas).finish(true).build());
        });

        return messagingService;
    }

    private PartitionSnapshotStorage createPartitionSnapshotStorage(
            UUID snapshotId,
            MvTableStorage incomingTableStorage,
            TxStateTableStorage incomingTxStateTableStorage,
            MessagingService messagingService
    ) {
        TopologyService topologyService = mock(TopologyService.class);

        when(topologyService.getByConsistentId(NODE_NAME)).thenReturn(clusterNode);

        OutgoingSnapshotsManager outgoingSnapshotsManager = mock(OutgoingSnapshotsManager.class);

        when(outgoingSnapshotsManager.messagingService()).thenReturn(messagingService);

        return new PartitionSnapshotStorage(
                topologyService,
                outgoingSnapshotsManager,
                SnapshotUri.toStringUri(snapshotId, NODE_NAME),
                mock(RaftOptions.class),
                new PartitionAccessImpl(
                        new PartitionKey(UUID.randomUUID(), TEST_PARTITION),
                        incomingTableStorage,
                        incomingTxStateTableStorage
                ),
                mock(SnapshotMeta.class),
                executorService
        );
    }

    private static List<RowId> fillMvPartitionStorage(MvPartitionStorage storage) {
        List<RowId> rowIds = List.of(
                new RowId(TEST_PARTITION),
                new RowId(TEST_PARTITION),
                new RowId(TEST_PARTITION),
                new RowId(TEST_PARTITION)
        );

        storage.runConsistently(() -> {
            // Writes committed version.
            storage.addWriteCommitted(rowIds.get(0), createRow("k0", "v0"), HYBRID_CLOCK.now());
            storage.addWriteCommitted(rowIds.get(1), createRow("k1", "v1"), HYBRID_CLOCK.now());

            storage.addWriteCommitted(rowIds.get(2), createRow("k20", "v20"), HYBRID_CLOCK.now());
            storage.addWriteCommitted(rowIds.get(2), createRow("k21", "v21"), HYBRID_CLOCK.now());

            // Writes an intent to write (uncommitted version).
            storage.addWrite(rowIds.get(2), createRow("k22", "v22"), UUID.randomUUID(), UUID.randomUUID(), TEST_PARTITION);

            storage.addWrite(
                    rowIds.get(3),
                    createRow("k3", "v3"),
                    UUID.randomUUID(),
                    UUID.randomUUID(),
                    TEST_PARTITION
            );

            return null;
        });

        return rowIds;
    }

    private static List<UUID> fillTxStatePartitionStorage(TxStateStorage storage) {
        List<UUID> txIds = List.of(
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID()
        );

        UUID tableId = UUID.randomUUID();

        storage.put(txIds.get(0), new TxMeta(COMMITED, List.of(new TablePartitionId(tableId, TEST_PARTITION)), HYBRID_CLOCK.now()));
        storage.put(txIds.get(1), new TxMeta(COMMITED, List.of(new TablePartitionId(tableId, TEST_PARTITION)), HYBRID_CLOCK.now()));
        storage.put(txIds.get(2), new TxMeta(ABORTED, List.of(new TablePartitionId(tableId, TEST_PARTITION)), HYBRID_CLOCK.now()));
        storage.put(txIds.get(3), new TxMeta(ABORTED, List.of(new TablePartitionId(tableId, TEST_PARTITION)), HYBRID_CLOCK.now()));

        return txIds;
    }

    private static List<ResponseEntry> createSnapshotMvDataEntries(MvPartitionStorage storage, List<RowId> rowIds) {
        List<ResponseEntry> responseEntries = new ArrayList<>();

        for (RowId rowId : rowIds) {
            List<ReadResult> readResults = storage.scanVersions(rowId).stream().collect(toList());

            Collections.reverse(readResults);

            List<ByteBuffer> rowVersions = new ArrayList<>();
            List<HybridTimestamp> timestamps = new ArrayList<>();

            UUID txId = null;
            UUID commitTableId = null;
            int commitPartitionId = ReadResult.UNDEFINED_COMMIT_PARTITION_ID;

            for (ReadResult readResult : readResults) {
                rowVersions.add(readResult.tableRow().byteBuffer());

                if (readResult.isWriteIntent()) {
                    txId = readResult.transactionId();
                    commitTableId = readResult.commitTableId();
                    commitPartitionId = readResult.commitPartitionId();
                } else {
                    timestamps.add(readResult.commitTimestamp());
                }
            }

            responseEntries.add(
                    TABLE_MSG_FACTORY.responseEntry()
                            .rowId(rowId.uuid())
                            .rowVersions(rowVersions)
                            .timestamps(timestamps)
                            .txId(txId)
                            .commitTableId(commitTableId)
                            .commitPartitionId(commitPartitionId)
                            .build()
            );
        }

        return responseEntries;
    }

    private static TableRow createRow(String key, String value) {
        BinaryRow binaryRow = new RowAssembler(SCHEMA_DESCRIPTOR, 1, 1).appendString(key).appendString(value).build();
        return TableRowConverter.fromBinaryRow(binaryRow, BinaryConverter.forRow(SCHEMA_DESCRIPTOR));
    }

    private static void assertEqualsMvRows(MvPartitionStorage expected, MvPartitionStorage actual, List<RowId> rowIds) {
        for (RowId rowId : rowIds) {
            List<ReadResult> expReadResults = expected.scanVersions(rowId).stream().collect(toList());
            List<ReadResult> actReadResults = actual.scanVersions(rowId).stream().collect(toList());

            assertEquals(expReadResults.size(), actReadResults.size(), rowId.toString());

            for (int i = 0; i < expReadResults.size(); i++) {
                ReadResult expReadResult = expReadResults.get(i);
                ReadResult actReadResult = actReadResults.get(i);

                String msg = "RowId=" + rowId + ", i=" + i;

                BinaryTupleReader expTuple = new BinaryTupleReader(SCHEMA_DESCRIPTOR.length(), expReadResult.tableRow().tupleSlice());
                BinaryTupleReader actTuple = new BinaryTupleReader(SCHEMA_DESCRIPTOR.length(), actReadResult.tableRow().tupleSlice());

                assertEquals(expTuple.stringValue(0), actTuple.stringValue(0), msg);
                assertEquals(expTuple.stringValue(1), actTuple.stringValue(1), msg);

                assertEquals(expReadResult.commitTimestamp(), actReadResult.commitTimestamp(), msg);
                assertEquals(expReadResult.transactionId(), actReadResult.transactionId(), msg);
                assertEquals(expReadResult.commitTableId(), actReadResult.commitTableId(), msg);
                assertEquals(expReadResult.commitPartitionId(), actReadResult.commitPartitionId(), msg);
                assertEquals(expReadResult.isWriteIntent(), actReadResult.isWriteIntent(), msg);
            }
        }
    }

    private static void assertEqualsTxStates(TxStateStorage expected, TxStateStorage actual, List<UUID> txIds) {
        for (UUID txId : txIds) {
            assertEquals(expected.get(txId), actual.get(txId));
        }
    }

    @Test
    void cancellationMakesJoinFinishIfHangingOnNetworkCall() throws Exception {
        MvTableStorage incomingMvTableStorage = spy(new TestMvTableStorage(tablesConfig.tables().get("foo"), tablesConfig));
        TxStateTableStorage incomingTxStateTableStorage = spy(new TestTxStateTableStorage());

        incomingMvTableStorage.getOrCreateMvPartition(TEST_PARTITION);
        incomingTxStateTableStorage.getOrCreateTxStateStorage(TEST_PARTITION);

        CountDownLatch networkInvokeLatch = new CountDownLatch(1);

        MessagingService messagingService = mock(MessagingService.class);

        when(messagingService.invoke(any(), any(), anyLong())).then(invocation -> {
            networkInvokeLatch.countDown();

            return new CompletableFuture<>();
        });

        PartitionSnapshotStorage partitionSnapshotStorage = createPartitionSnapshotStorage(
                snapshotId,
                incomingMvTableStorage,
                incomingTxStateTableStorage,
                messagingService
        );

        SnapshotCopier snapshotCopier = partitionSnapshotStorage.startToCopyFrom(
                SnapshotUri.toStringUri(snapshotId, NODE_NAME),
                mock(SnapshotCopierOptions.class)
        );

        networkInvokeLatch.await(1, TimeUnit.SECONDS);

        CompletableFuture<?> cancelAndJoinFuture = runAsync(() -> {
            snapshotCopier.cancel();

            snapshotCopier.join();
        });

        assertThat(cancelAndJoinFuture, willSucceedIn(1, TimeUnit.SECONDS));
    }
}
