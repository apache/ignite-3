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

package org.apache.ignite.internal.partition.replicator.raft.snapshot.incoming;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PARTITION_COUNT;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestampToLong;
import static org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.SnapshotMetaUtils.snapshotMetaAt;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedIn;
import static org.apache.ignite.internal.tx.TxState.ABORTED;
import static org.apache.ignite.internal.tx.TxState.COMMITTED;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lowwatermark.TestLowWatermark;
import org.apache.ignite.internal.lowwatermark.message.GetLowWatermarkRequest;
import org.apache.ignite.internal.lowwatermark.message.GetLowWatermarkResponse;
import org.apache.ignite.internal.lowwatermark.message.LowWatermarkMessagesFactory;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.raft.PartitionSnapshotMeta;
import org.apache.ignite.internal.partition.replicator.network.raft.SnapshotMetaRequest;
import org.apache.ignite.internal.partition.replicator.network.raft.SnapshotMetaResponse;
import org.apache.ignite.internal.partition.replicator.network.raft.SnapshotMvDataRequest;
import org.apache.ignite.internal.partition.replicator.network.raft.SnapshotMvDataResponse;
import org.apache.ignite.internal.partition.replicator.network.raft.SnapshotMvDataResponse.ResponseEntry;
import org.apache.ignite.internal.partition.replicator.network.raft.SnapshotTxDataRequest;
import org.apache.ignite.internal.partition.replicator.network.raft.SnapshotTxDataResponse;
import org.apache.ignite.internal.partition.replicator.network.replication.BinaryRowMessage;
import org.apache.ignite.internal.partition.replicator.raft.PartitionSnapshotInfo;
import org.apache.ignite.internal.partition.replicator.raft.PartitionSnapshotInfoSerializer;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.LogStorageAccessImpl;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionKey;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionSnapshotStorage;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionTxStateAccessImpl;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.SnapshotUri;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.metrics.RaftSnapshotsMetricsSource;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.raft.RaftGroupConfigurationConverter;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.engine.MvPartitionMeta;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.impl.TestMvPartitionStorage;
import org.apache.ignite.internal.storage.impl.TestMvTableStorage;
import org.apache.ignite.internal.storage.lease.LeaseInfo;
import org.apache.ignite.internal.table.distributed.gc.GcUpdateHandler;
import org.apache.ignite.internal.table.distributed.gc.MvGc;
import org.apache.ignite.internal.table.distributed.index.IndexUpdateHandler;
import org.apache.ignite.internal.table.distributed.raft.snapshot.FullStateTransferIndexChooser;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionMvStorageAccessImpl;
import org.apache.ignite.internal.table.impl.DummySchemaManagerImpl;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.TransactionIds;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.impl.EnlistedPartitionGroup;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.internal.tx.message.TxMetaMessage;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.tx.storage.state.test.TestTxStatePartitionStorage;
import org.apache.ignite.internal.tx.storage.state.test.TestTxStateStorage;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotCopier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/** For {@link IncomingSnapshotCopier} testing. */
@ExtendWith(MockitoExtension.class)
public class IncomingSnapshotCopierTest extends BaseIgniteAbstractTest {
    private static final int ZONE_ID = 0;
    private static final int TABLE_ID = 1;

    private static final String NODE_NAME = "node";

    private static final int PARTITION_ID = 0;

    private static final SchemaDescriptor SCHEMA = new SchemaDescriptor(
            1,
            new Column[]{new Column("key", NativeTypes.stringOf(256), false)},
            new Column[]{new Column("value", NativeTypes.stringOf(256), false)}
    );

    private static final HybridClock CLOCK = new HybridClockImpl();

    private static final PartitionReplicationMessagesFactory TABLE_MSG_FACTORY = new PartitionReplicationMessagesFactory();

    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    private static final LowWatermarkMessagesFactory LWM_MSG_FACTORY = new LowWatermarkMessagesFactory();

    private static final TxMessagesFactory TX_MESSAGES_FACTORY = new TxMessagesFactory();

    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    private final InternalClusterNode clusterNode = mock(InternalClusterNode.class);

    private final UUID snapshotId = UUID.randomUUID();

    private final RaftGroupConfigurationConverter raftGroupConfigurationConverter = new RaftGroupConfigurationConverter();

    private final MvGc mvGc = mock(MvGc.class);

    private final CatalogService catalogService = mock(CatalogService.class);

    private final MvPartitionStorage outgoingMvPartitionStorage = new TestMvPartitionStorage(PARTITION_ID);
    private final TxStatePartitionStorage outgoingTxStatePartitionStorage = new TestTxStatePartitionStorage();

    private final MvTableStorage incomingMvTableStorage = spy(new TestMvTableStorage(TABLE_ID, DEFAULT_PARTITION_COUNT));
    private final TxStateStorage incomingTxStateStorage = spy(new TestTxStateStorage());

    private final long expLastAppliedIndex = 100500L;
    private final long expLastAppliedTerm = 100L;
    private final RaftGroupConfiguration expLastGroupConfig = generateRaftGroupConfig();

    private final LeaseInfo expLeaseInfo = new LeaseInfo(3000000, new UUID(1, 2), "primary");

    private final List<RowId> rowIds = generateRowIds();
    private final List<UUID> txIds = generateTxIds();

    private final IndexUpdateHandler indexUpdateHandler = mock(IndexUpdateHandler.class);

    private final int indexId = 1;

    private final RowId nextRowIdToBuildIndex = new RowId(PARTITION_ID);

    private final TestLowWatermark lowWatermark = spy(new TestLowWatermark());

    private final ReplicaManager replicaManager = mock(ReplicaManager.class);

    @BeforeEach
    void setUp(
            @Mock Catalog catalog,
            @Mock CatalogIndexDescriptor catalogIndexDescriptor
    ) {
        when(mvGc.removeStorage(any(TablePartitionId.class))).then(invocation -> nullCompletedFuture());

        when(catalogService.catalogReadyFuture(anyInt())).thenReturn(nullCompletedFuture());
        when(catalogService.catalog(anyInt())).thenReturn(catalog);

        lenient().when(catalog.index(indexId)).thenReturn(catalogIndexDescriptor);
        lenient().when(catalogIndexDescriptor.tableId()).thenReturn(TABLE_ID);
    }

    @AfterEach
    void tearDown() {
        shutdownAndAwaitTermination(executorService, 1, TimeUnit.SECONDS);
    }

    @Test
    void test() throws Exception {
        fillOriginalStorages();

        createTargetStorages();

        MessagingService messagingService = messagingServiceForSuccessScenario(outgoingMvPartitionStorage,
                outgoingTxStatePartitionStorage, rowIds, txIds);

        PartitionSnapshotStorage partitionSnapshotStorage = createPartitionSnapshotStorage(
                incomingMvTableStorage,
                incomingTxStateStorage,
                messagingService
        );

        HybridTimestamp newLowWatermarkValue = CLOCK.now();
        assertThat(lowWatermark.updateAndNotify(newLowWatermarkValue), willCompleteSuccessfully());
        clearInvocations(lowWatermark);

        SnapshotCopier snapshotCopier = partitionSnapshotStorage.startIncomingSnapshot(
                SnapshotUri.toStringUri(snapshotId, NODE_NAME)
        );

        assertThat(runAsync(snapshotCopier::join), willSucceedIn(1, TimeUnit.SECONDS));

        assertEquals(Status.OK().getCode(), snapshotCopier.getCode());

        TablePartitionId tablePartitionId = new TablePartitionId(TABLE_ID, PARTITION_ID);

        verify(mvGc, times(1)).removeStorage(eq(tablePartitionId));
        verify(mvGc, times(1)).addStorage(eq(tablePartitionId), any(GcUpdateHandler.class));

        MvPartitionStorage incomingMvPartitionStorage = incomingMvTableStorage.getMvPartition(PARTITION_ID);
        TxStatePartitionStorage incomingTxStatePartitionStorage = incomingTxStateStorage.getPartitionStorage(PARTITION_ID);

        assertEquals(expLastAppliedIndex, outgoingMvPartitionStorage.lastAppliedIndex());
        assertEquals(expLastAppliedTerm, outgoingMvPartitionStorage.lastAppliedTerm());
        assertArrayEquals(
                raftGroupConfigurationConverter.toBytes(expLastGroupConfig),
                outgoingMvPartitionStorage.committedGroupConfiguration()
        );
        assertEquals(expLeaseInfo, outgoingMvPartitionStorage.leaseInfo());

        assertEquals(expLastAppliedIndex, outgoingTxStatePartitionStorage.lastAppliedIndex());
        assertEquals(expLastAppliedTerm, outgoingTxStatePartitionStorage.lastAppliedTerm());

        assertEqualsMvRows(outgoingMvPartitionStorage, incomingMvPartitionStorage, rowIds);
        assertEqualsTxStates(outgoingTxStatePartitionStorage, incomingTxStatePartitionStorage, txIds);

        verify(incomingMvTableStorage).startRebalancePartition(PARTITION_ID);
        verify(incomingTxStatePartitionStorage).startRebalance();
        verify(replicaManager).destroyReplicationProtocolStorages(any(), anyBoolean());

        var expSnapshotInfo = new PartitionSnapshotInfo(
                expLastAppliedIndex,
                expLastAppliedTerm,
                expLeaseInfo,
                raftGroupConfigurationConverter.toBytes(expLastGroupConfig),
                Set.of(TABLE_ID)
        );

        byte[] expSnapshotInfoBytes = VersionedSerialization.toBytes(expSnapshotInfo, PartitionSnapshotInfoSerializer.INSTANCE);

        var expMvPartitionMeta = new MvPartitionMeta(
                expSnapshotInfo.lastAppliedIndex(),
                expSnapshotInfo.lastAppliedTerm(),
                expSnapshotInfo.configurationBytes(),
                expSnapshotInfo.leaseInfo(),
                expSnapshotInfoBytes
        );

        verify(incomingTxStatePartitionStorage).finishRebalance(expMvPartitionMeta);

        verify(indexUpdateHandler).setNextRowIdToBuildIndex(eq(indexId), eq(nextRowIdToBuildIndex));

        verify(lowWatermark).updateLowWatermark(eq(newLowWatermarkValue));
    }

    private void createTargetStorages() {
        assertThat(incomingMvTableStorage.createMvPartition(PARTITION_ID), willCompleteSuccessfully());
        incomingTxStateStorage.getOrCreatePartitionStorage(PARTITION_ID);
    }

    private void fillOriginalStorages() {
        fillMvPartitionStorage(
                outgoingMvPartitionStorage,
                expLastAppliedIndex,
                expLastAppliedTerm,
                expLastGroupConfig,
                rowIds,
                expLeaseInfo
        );
        fillTxStatePartitionStorage(outgoingTxStatePartitionStorage, expLastAppliedIndex, expLastAppliedTerm, txIds);
    }

    private MessagingService messagingServiceForSuccessScenario(MvPartitionStorage outgoingMvPartitionStorage,
            TxStatePartitionStorage outgoingTxStatePartitionStorage, List<RowId> rowIds, List<UUID> txIds) {
        MessagingService messagingService = mock(MessagingService.class);

        returnSnapshotMetaWhenAskedForIt(messagingService);

        when(messagingService.invoke(eq(clusterNode), any(SnapshotMvDataRequest.class), anyLong())).then(answer -> {
            SnapshotMvDataRequest snapshotMvDataRequest = answer.getArgument(1);

            assertEquals(snapshotId, snapshotMvDataRequest.id());

            List<ResponseEntry> responseEntries = createSnapshotMvDataEntries(outgoingMvPartitionStorage, rowIds);

            assertThat(responseEntries, not(empty()));

            return completedFuture(TABLE_MSG_FACTORY.snapshotMvDataResponse().rows(responseEntries).finish(true).build());
        });

        lenient().when(messagingService.invoke(eq(clusterNode), any(SnapshotTxDataRequest.class), anyLong())).then(answer -> {
            SnapshotTxDataRequest snapshotTxDataRequest = answer.getArgument(1);

            assertEquals(snapshotId, snapshotTxDataRequest.id());

            List<TxMetaMessage> txMetas = txIds.stream()
                    .map(outgoingTxStatePartitionStorage::get)
                    .map(txMeta -> txMeta.toTransactionMetaMessage(REPLICA_MESSAGES_FACTORY, TX_MESSAGES_FACTORY))
                    .collect(toList());

            return completedFuture(TABLE_MSG_FACTORY.snapshotTxDataResponse().txIds(txIds).txMeta(txMetas).finish(true).build());
        });

        lenient().when(messagingService.invoke(eq(clusterNode), any(GetLowWatermarkRequest.class), anyLong())).thenAnswer(invocation -> {
            long lowWatermarkValue = hybridTimestampToLong(lowWatermark.getLowWatermark());

            return completedFuture(LWM_MSG_FACTORY.getLowWatermarkResponse().lowWatermark(lowWatermarkValue).build());
        });

        return messagingService;
    }

    private void returnSnapshotMetaWhenAskedForIt(MessagingService messagingService) {
        when(messagingService.invoke(eq(clusterNode), any(SnapshotMetaRequest.class), anyLong())).then(answer -> {
            SnapshotMetaRequest snapshotMetaRequest = answer.getArgument(1);

            assertEquals(snapshotId, snapshotMetaRequest.id());

            return completedFuture(snapshotMetaResponse(0));
        });
    }

    private SnapshotMetaResponse snapshotMetaResponse(int requiredCatalogVersion) {
        return TABLE_MSG_FACTORY.snapshotMetaResponse()
                .meta(snapshotMetaAt(
                        expLastAppliedIndex,
                        expLastAppliedTerm,
                        expLastGroupConfig,
                        requiredCatalogVersion,
                        Map.of(indexId, nextRowIdToBuildIndex.uuid()),
                        expLeaseInfo
                ))
                .build();
    }

    private PartitionSnapshotStorage createPartitionSnapshotStorage(
            MvTableStorage incomingTableStorage,
            TxStateStorage incomingTxStateStorage,
            MessagingService messagingService
    ) {
        return createPartitionSnapshotStorage(incomingTableStorage, incomingTxStateStorage, messagingService, catalogService);
    }

    private PartitionSnapshotStorage createPartitionSnapshotStorage(
            MvTableStorage incomingTableStorage,
            TxStateStorage incomingTxStateStorage,
            MessagingService messagingService,
            CatalogService catalogService
    ) {
        TopologyService topologyService = mock(TopologyService.class);

        when(topologyService.getByConsistentId(NODE_NAME)).thenReturn(clusterNode);

        OutgoingSnapshotsManager outgoingSnapshotsManager = mock(OutgoingSnapshotsManager.class);

        when(outgoingSnapshotsManager.messagingService()).thenReturn(messagingService);

        var storage = new PartitionSnapshotStorage(
                new PartitionKey(ZONE_ID, PARTITION_ID),
                topologyService,
                outgoingSnapshotsManager,
                new PartitionTxStateAccessImpl(incomingTxStateStorage.getPartitionStorage(PARTITION_ID)),
                catalogService,
                mock(FailureProcessor.class),
                executorService,
                0,
                new LogStorageAccessImpl(replicaManager),
                new RaftSnapshotsMetricsSource()
        );

        storage.addMvPartition(TABLE_ID, spy(new PartitionMvStorageAccessImpl(
                PARTITION_ID,
                incomingTableStorage,
                mvGc,
                indexUpdateHandler,
                mock(GcUpdateHandler.class),
                mock(FullStateTransferIndexChooser.class),
                new DummySchemaManagerImpl(SCHEMA),
                lowWatermark
        )));

        return storage;
    }

    private void fillMvPartitionStorage(
            MvPartitionStorage storage,
            long lastAppliedIndex,
            long lastAppliedTerm,
            RaftGroupConfiguration raftGroupConfig,
            List<RowId> rowIds,
            LeaseInfo leaseInfo
    ) {
        assertEquals(0, rowIds.size() % 2, "size=" + rowIds.size());

        storage.runConsistently(locker -> {
            for (int i = 0; i < rowIds.size(); i++) {
                if (i % 2 == 0) {
                    // Writes committed version.
                    storage.addWriteCommitted(rowIds.get(i), createRow("k" + i, "v" + i), CLOCK.now());
                } else {
                    // Writes an intent to write (uncommitted version).
                    storage.addWrite(rowIds.get(i), createRow("k" + i, "v" + i), generateTxId(), 999, PARTITION_ID);
                }
            }

            storage.lastApplied(lastAppliedIndex, lastAppliedTerm);

            storage.committedGroupConfiguration(raftGroupConfigurationConverter.toBytes(raftGroupConfig));

            storage.updateLease(leaseInfo);

            return null;
        });
    }

    private static void fillTxStatePartitionStorage(
            TxStatePartitionStorage storage,
            long lastAppliedIndex,
            long lastAppliedTerm,
            List<UUID> txIds
    ) {
        assertEquals(0, txIds.size() % 2, "size=" + txIds.size());

        int tableId = 2;

        int zoneId = 20;

        for (int i = 0; i < txIds.size(); i++) {
            TxState txState = i % 2 == 0 ? COMMITTED : ABORTED;

            List<EnlistedPartitionGroup> enlistedPartitions = List.of(
                    new EnlistedPartitionGroup(new ZonePartitionId(zoneId, PARTITION_ID), Set.of(tableId))
            );
            storage.putForRebalance(txIds.get(i), new TxMeta(txState, enlistedPartitions, CLOCK.now()));
        }

        storage.lastApplied(lastAppliedIndex, lastAppliedTerm);
    }

    private static List<ResponseEntry> createSnapshotMvDataEntries(MvPartitionStorage storage, List<RowId> rowIds) {
        List<ResponseEntry> responseEntries = new ArrayList<>();

        for (RowId rowId : rowIds) {
            List<ReadResult> readResults = storage.scanVersions(rowId).stream().collect(toList());

            Collections.reverse(readResults);

            List<BinaryRowMessage> rowVersions = new ArrayList<>();
            long[] timestamps = new long[readResults.size() + (readResults.get(0).isWriteIntent() ? -1 : 0)];

            UUID txId = null;
            Integer commitZoneId = null;
            int commitPartitionId = ReadResult.UNDEFINED_COMMIT_PARTITION_ID;

            int j = 0;
            for (ReadResult readResult : readResults) {
                BinaryRowMessage rowMessage = TABLE_MSG_FACTORY.binaryRowMessage()
                        .binaryTuple(readResult.binaryRow().tupleSlice())
                        .schemaVersion(readResult.binaryRow().schemaVersion())
                        .build();

                rowVersions.add(rowMessage);

                if (readResult.isWriteIntent()) {
                    txId = readResult.transactionId();
                    commitZoneId = readResult.commitZoneId();
                    commitPartitionId = readResult.commitPartitionId();
                } else {
                    timestamps[j++] = readResult.commitTimestamp().longValue();
                }
            }

            responseEntries.add(
                    TABLE_MSG_FACTORY.responseEntry()
                            .rowId(rowId.uuid())
                            .rowVersions(rowVersions)
                            .timestamps(timestamps)
                            .txId(txId)
                            .commitZoneId(commitZoneId)
                            .commitPartitionId(commitPartitionId)
                            .tableId(TABLE_ID)
                            .build()
            );
        }

        return responseEntries;
    }

    private static BinaryRow createRow(String key, String value) {
        return new RowAssembler(SCHEMA, -1)
                .appendStringNotNull(key)
                .appendStringNotNull(value)
                .build();
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

                BinaryTupleReader expTuple = new BinaryTupleReader(SCHEMA.length(), expReadResult.binaryRow().tupleSlice());
                BinaryTupleReader actTuple = new BinaryTupleReader(SCHEMA.length(), actReadResult.binaryRow().tupleSlice());

                assertEquals(expTuple.stringValue(0), actTuple.stringValue(0), msg);
                assertEquals(expTuple.stringValue(1), actTuple.stringValue(1), msg);

                assertEquals(expReadResult.commitTimestamp(), actReadResult.commitTimestamp(), msg);
                assertEquals(expReadResult.transactionId(), actReadResult.transactionId(), msg);
                assertEquals(expReadResult.commitZoneId(), actReadResult.commitZoneId(), msg);
                assertEquals(expReadResult.commitPartitionId(), actReadResult.commitPartitionId(), msg);
                assertEquals(expReadResult.isWriteIntent(), actReadResult.isWriteIntent(), msg);
            }
        }
    }

    private static void assertEqualsTxStates(TxStatePartitionStorage expected, TxStatePartitionStorage actual, List<UUID> txIds) {
        for (UUID txId : txIds) {
            assertEquals(expected.get(txId), actual.get(txId));
        }
    }

    @Test
    void cancellationMakesJoinFinishIfHangingOnNetworkCallToSnapshotMetadata() throws Exception {
        createTargetStorages();

        CountDownLatch networkInvokeLatch = new CountDownLatch(1);

        MessagingService messagingService = mock(MessagingService.class);

        when(messagingService.invoke(any(InternalClusterNode.class), any(SnapshotMetaRequest.class), anyLong())).then(invocation -> {
            networkInvokeLatch.countDown();

            return new CompletableFuture<>();
        });

        PartitionSnapshotStorage partitionSnapshotStorage = createPartitionSnapshotStorage(
                incomingMvTableStorage,
                incomingTxStateStorage,
                messagingService
        );

        SnapshotCopier snapshotCopier = partitionSnapshotStorage.startIncomingSnapshot(
                SnapshotUri.toStringUri(snapshotId, NODE_NAME)
        );

        networkInvokeLatch.await(1, TimeUnit.SECONDS);

        CompletableFuture<?> cancelAndJoinFuture = runAsync(() -> {
            snapshotCopier.cancel();

            snapshotCopier.join();
        });

        assertThat(cancelAndJoinFuture, willSucceedIn(1, TimeUnit.SECONDS));

        verify(partitionSnapshotStorage.partitionsByTableId().get(TABLE_ID), never()).startRebalance();
        verify(partitionSnapshotStorage.partitionsByTableId().get(TABLE_ID), never()).abortRebalance();
    }

    @Test
    void cancellationMakesJoinFinishIfHangingOnNetworkCallWhenGettingData() throws Exception {
        createTargetStorages();

        CountDownLatch networkInvokeLatch = new CountDownLatch(1);

        MessagingService messagingService = mock(MessagingService.class);

        returnSnapshotMetaWhenAskedForIt(messagingService);
        when(messagingService.invoke(any(InternalClusterNode.class), any(SnapshotMvDataRequest.class), anyLong())).then(invocation -> {
            networkInvokeLatch.countDown();

            return new CompletableFuture<>();
        });

        PartitionSnapshotStorage partitionSnapshotStorage = createPartitionSnapshotStorage(
                incomingMvTableStorage,
                incomingTxStateStorage,
                messagingService
        );

        SnapshotCopier snapshotCopier = partitionSnapshotStorage.startIncomingSnapshot(
                SnapshotUri.toStringUri(snapshotId, NODE_NAME)
        );

        networkInvokeLatch.await(1, TimeUnit.SECONDS);

        CompletableFuture<?> cancelAndJoinFuture = runAsync(() -> {
            snapshotCopier.cancel();

            snapshotCopier.join();
        });

        assertThat(cancelAndJoinFuture, willSucceedIn(1, TimeUnit.SECONDS));

        verify(partitionSnapshotStorage.partitionsByTableId().get(TABLE_ID)).abortRebalance();
    }

    @Test
    void testCancelOnMiddleRebalance() {
        fillOriginalStorages();

        createTargetStorages();

        MessagingService messagingService = messagingServiceForSuccessScenario(outgoingMvPartitionStorage,
                outgoingTxStatePartitionStorage, rowIds, txIds);

        PartitionSnapshotStorage partitionSnapshotStorage = createPartitionSnapshotStorage(
                incomingMvTableStorage,
                incomingTxStateStorage,
                messagingService
        );

        // Let's add a rebalance interruption in the middle.
        CompletableFuture<Void> startAddWriteFuture = new CompletableFuture<>();
        CompletableFuture<Void> finishAddWriteFuture = new CompletableFuture<>();

        doAnswer(answer -> {
            startAddWriteFuture.complete(null);

            assertThat(finishAddWriteFuture, willCompleteSuccessfully());

            return null;
        }).when(partitionSnapshotStorage.partitionsByTableId().get(TABLE_ID))
                .addWrite(any(RowId.class), any(BinaryRow.class), any(UUID.class), anyInt(), anyInt(), anyInt());

        // Let's start rebalancing.
        SnapshotCopier snapshotCopier = partitionSnapshotStorage.startIncomingSnapshot(
                SnapshotUri.toStringUri(snapshotId, NODE_NAME)
        );

        // Let's try to cancel it in the middle of the rebalance.
        CompletableFuture<?> cancelRebalanceFuture = runAsync(() -> {
            assertThat(startAddWriteFuture, willCompleteSuccessfully());

            CompletableFuture<?> cancelCopierFuture = runAsync(() -> finishAddWriteFuture.complete(null));

            snapshotCopier.cancel();

            snapshotCopier.join();

            assertThat(cancelCopierFuture, willCompleteSuccessfully());
        });

        assertThat(cancelRebalanceFuture, willCompleteSuccessfully());

        verify(partitionSnapshotStorage.partitionsByTableId().get(TABLE_ID)).abortRebalance();
    }

    @Test
    void testErrorInProcessOfRebalance() {
        fillOriginalStorages();

        createTargetStorages();

        MessagingService messagingService = messagingServiceForSuccessScenario(outgoingMvPartitionStorage,
                outgoingTxStatePartitionStorage, rowIds, txIds);

        PartitionSnapshotStorage partitionSnapshotStorage = createPartitionSnapshotStorage(
                incomingMvTableStorage,
                incomingTxStateStorage,
                messagingService
        );

        // Let's add an error on the rebalance.
        doThrow(new StorageException("Mocked storage exception.")).when(partitionSnapshotStorage.partitionsByTableId().get(TABLE_ID))
                .addWrite(any(RowId.class), any(BinaryRow.class), any(UUID.class), anyInt(), anyInt(), anyInt());

        // Let's start rebalancing.
        SnapshotCopier snapshotCopier = partitionSnapshotStorage.startIncomingSnapshot(
                SnapshotUri.toStringUri(snapshotId, NODE_NAME)
        );

        // Let's wait for an error on rebalancing.
        assertThat(runAsync(snapshotCopier::join), willThrowFast(IllegalStateException.class));

        verify(partitionSnapshotStorage.partitionsByTableId().get(TABLE_ID)).abortRebalance();

        TablePartitionId tablePartitionId = new TablePartitionId(TABLE_ID, PARTITION_ID);

        verify(mvGc, times(1)).removeStorage(eq(tablePartitionId));
        verify(mvGc, times(1)).addStorage(eq(tablePartitionId), any(GcUpdateHandler.class));
    }

    @Test
    @Timeout(1)
    void cancellationsFromMultipleThreadsDoNotBlockEachOther() throws Exception {
        PartitionSnapshotStorage partitionSnapshotStorage = mock(PartitionSnapshotStorage.class);

        when(partitionSnapshotStorage.partitionKey()).thenReturn(new PartitionKey(1, 0));

        IncomingSnapshotCopier copier = new IncomingSnapshotCopier(
                partitionSnapshotStorage,
                SnapshotUri.fromStringUri(SnapshotUri.toStringUri(snapshotId, NODE_NAME)),
                mock(Executor.class),
                0,
                new RaftSnapshotsMetricsSource()
        );

        Thread anotherThread = new Thread(copier::cancel);
        anotherThread.start();
        anotherThread.join();

        copier.cancel();
    }

    private static List<RowId> generateRowIds() {
        return List.of(
                new RowId(PARTITION_ID),
                new RowId(PARTITION_ID),
                new RowId(PARTITION_ID),
                new RowId(PARTITION_ID)
        );
    }

    private static List<UUID> generateTxIds() {
        return List.of(
                generateTxId(),
                generateTxId(),
                generateTxId(),
                generateTxId()
        );
    }

    private static RaftGroupConfiguration generateRaftGroupConfig() {
        return new RaftGroupConfiguration(
                13L,
                37L,
                111L,
                110L,
                List.of("peer"),
                List.of("learner"),
                List.of("old-peer"),
                List.of("old-learner")
        );
    }

    @Test
    void laggingSchemasPreventSnapshotInstallation() {
        fillOriginalStorages();

        createTargetStorages();

        MessagingService messagingService = mock(MessagingService.class);

        int leaderCatalogVersion = 42;
        when(catalogService.catalogReadyFuture(leaderCatalogVersion)).thenReturn(new CompletableFuture<>());
        when(messagingService.invoke(eq(clusterNode), any(SnapshotMetaRequest.class), anyLong()))
                .thenReturn(completedFuture(snapshotMetaResponse(leaderCatalogVersion)));

        PartitionSnapshotStorage partitionSnapshotStorage = createPartitionSnapshotStorage(
                incomingMvTableStorage,
                incomingTxStateStorage,
                messagingService
        );

        SnapshotCopier snapshotCopier = partitionSnapshotStorage.startIncomingSnapshot(
                SnapshotUri.toStringUri(snapshotId, NODE_NAME)
        );

        assertThat(runAsync(snapshotCopier::join), willSucceedIn(10, TimeUnit.SECONDS));

        assertEquals(RaftError.EBUSY.getNumber(), snapshotCopier.getCode());

        verify(messagingService, never()).invoke(any(InternalClusterNode.class), any(SnapshotMvDataRequest.class), anyLong());
        verify(messagingService, never()).invoke(any(InternalClusterNode.class), any(SnapshotTxDataRequest.class), anyLong());

        verify(partitionSnapshotStorage.partitionsByTableId().get(TABLE_ID), never()).startRebalance();
        verify(partitionSnapshotStorage.partitionsByTableId().get(TABLE_ID), never()).abortRebalance();

        assertThatTargetStoragesAreEmpty(incomingMvTableStorage, incomingTxStateStorage);
    }

    @Test
    void metricsCalculateCorrectly() throws InterruptedException {
        incomingMvTableStorage.createMvPartition(PARTITION_ID);
        incomingTxStateStorage.getOrCreatePartitionStorage(PARTITION_ID);

        PartitionSnapshotMeta meta = mock(PartitionSnapshotMeta.class);

        when(meta.requiredCatalogVersion()).thenReturn(1);

        SnapshotMetaResponse metaResponse = mock(SnapshotMetaResponse.class);

        when(metaResponse.meta()).thenReturn(meta);

        CompletableFuture<NetworkMessage> loadSnapshotMetaFuture = new CompletableFuture<>();

        CompletableFuture<NetworkMessage> loadMvDataFuture = new CompletableFuture<>();

        CompletableFuture<NetworkMessage> loadTxMetaFuture = new CompletableFuture<>();

        SnapshotMvDataResponse mvDataResponse = mock(SnapshotMvDataResponse.class);
        when(mvDataResponse.finish()).thenReturn(true);

        SnapshotTxDataResponse txMetaResponse = mock(SnapshotTxDataResponse.class);
        when(txMetaResponse.finish()).thenReturn(true);

        MessagingService messagingService = messagingServiceForMetrics(loadSnapshotMetaFuture, loadMvDataFuture, loadTxMetaFuture);

        CompletableFuture<Void> catalogReadyFuture = new CompletableFuture<>();

        CatalogService catalogService = mock(CatalogService.class);

        when(catalogService.catalogReadyFuture(anyInt())).thenReturn(catalogReadyFuture);

        PartitionSnapshotStorage partitionSnapshotStorage = createPartitionSnapshotStorage(
                incomingMvTableStorage,
                incomingTxStateStorage,
                messagingService,
                catalogService
        );

        var snapshotMetricSource = new RaftSnapshotsMetricsSource();

        snapshotMetricSource.enable();

        IncomingSnapshotCopier copier = new IncomingSnapshotCopier(
                partitionSnapshotStorage,
                SnapshotUri.fromStringUri(SnapshotUri.toStringUri(snapshotId, NODE_NAME)),
                executorService,
                1000,
                snapshotMetricSource
        );

        waitTillMetricHasValue(snapshotMetricSource, "IncomingSnapshots", "0");
        waitTillMetricHasValue(snapshotMetricSource, "IncomingSnapshotsLoadingMeta", "0");

        copier.start();

        waitTillMetricHasValue(snapshotMetricSource, "IncomingSnapshots", "1");
        waitTillMetricHasValue(snapshotMetricSource, "IncomingSnapshotsLoadingMeta", "1");

        loadSnapshotMetaFuture.complete(metaResponse);

        waitTillMetricHasValue(snapshotMetricSource, "IncomingSnapshotsLoadingMeta", "0");
        waitTillMetricHasValue(snapshotMetricSource, "IncomingSnapshotsWaitingCatalog", "1");

        catalogReadyFuture.complete(null);

        waitTillMetricHasValue(snapshotMetricSource, "IncomingSnapshotsWaitingCatalog", "0");
        waitTillMetricHasValue(snapshotMetricSource, "IncomingSnapshotsLoadingMvData", "1");

        loadMvDataFuture.complete(mvDataResponse);

        waitTillMetricHasValue(snapshotMetricSource, "IncomingSnapshotsLoadingMvData", "0");
        waitTillMetricHasValue(snapshotMetricSource, "IncomingSnapshotsLoadingTxMeta", "1");

        loadTxMetaFuture.complete(txMetaResponse);

        waitTillMetricHasValue(snapshotMetricSource, "IncomingSnapshotsLoadingTxMeta", "0");

        copier.join();

        waitTillMetricHasValue(snapshotMetricSource, "IncomingSnapshots", "0");
    }

    private static void assertThatTargetStoragesAreEmpty(
            MvTableStorage incomingMvTableStorage,
            TxStateStorage incomingTxStateStorage
    ) {
        MvPartitionStorage incomingMvPartitionStorage = incomingMvTableStorage.getMvPartition(PARTITION_ID);
        TxStatePartitionStorage incomingTxStatePartitionStorage = incomingTxStateStorage.getPartitionStorage(PARTITION_ID);

        assertEquals(0L, incomingMvPartitionStorage.lastAppliedIndex());
        assertEquals(0L, incomingMvPartitionStorage.lastAppliedTerm());
        assertArrayEquals(
                null,
                incomingMvPartitionStorage.committedGroupConfiguration()
        );
        assertEquals(0L, incomingTxStatePartitionStorage.lastAppliedIndex());
        assertEquals(0L, incomingTxStatePartitionStorage.lastAppliedTerm());

        assertFalse(incomingMvPartitionStorage.scan(HybridTimestamp.MAX_VALUE).hasNext());
        assertFalse(incomingTxStatePartitionStorage.scan().hasNext());
    }

    private static UUID generateTxId() {
        return TransactionIds.transactionId(CLOCK.now(), 1);
    }

    private static Metric retrieveOutgoingSnapshotMetric(RaftSnapshotsMetricsSource snapshotsMetricsSource, String metricName) {
        return stream(snapshotsMetricsSource.holder().metrics().spliterator(), false)
                .filter(metric -> metricName.equals(metric.name()))
                .findAny()
                .get();
    }

    private static void waitTillMetricHasValue(
            RaftSnapshotsMetricsSource snapshotsMetricsSource,
            String metricName,
            String expectedValue
    ) {
        Metric metric = retrieveOutgoingSnapshotMetric(snapshotsMetricsSource, metricName);

        await().until(metric::getValueAsString, is(expectedValue));
    }

    private MessagingService messagingServiceForMetrics(
            CompletableFuture<NetworkMessage> loadSnapshotMetaFuture,
            CompletableFuture<NetworkMessage> loadMvDataFuture,
            CompletableFuture<NetworkMessage> loadTxMetaFuture
    ) {
        MessagingService messagingService = mock(MessagingService.class);

        GetLowWatermarkResponse getLowWatermarkResponse = mock(GetLowWatermarkResponse.class);
        when(getLowWatermarkResponse.lowWatermark()).thenReturn(HybridTimestamp.NULL_HYBRID_TIMESTAMP);

        when(messagingService.invoke(eq(clusterNode), any(SnapshotMetaRequest.class), anyLong()))
                .thenReturn(loadSnapshotMetaFuture);

        when(messagingService.invoke(eq(clusterNode), any(SnapshotMvDataRequest.class), anyLong()))
                .thenReturn(loadMvDataFuture);

        when(messagingService.invoke(eq(clusterNode), any(SnapshotTxDataRequest.class), anyLong()))
                .thenReturn(loadTxMetaFuture);

        when(messagingService.invoke(eq(clusterNode), any(GetLowWatermarkRequest.class), anyLong()))
                .thenReturn(completedFuture(getLowWatermarkResponse));

        return messagingService;
    }
}
