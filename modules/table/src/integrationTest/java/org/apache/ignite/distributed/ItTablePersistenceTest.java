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

package org.apache.ignite.distributed;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.replicator.ReplicaManager.DEFAULT_IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS;
import static org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbStorageEngineConfigurationSchema.DEFAULT_DATA_REGION_NAME;
import static org.apache.ignite.internal.table.distributed.replicator.PartitionReplicaListener.tablePartitionId;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.ArrayUtils.asList;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.placementdriver.TestPlacementDriver;
import org.apache.ignite.internal.raft.Marshaller;
import org.apache.ignite.internal.raft.server.RaftServer;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.raft.service.ItAbstractListenerSnapshotTest;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.TestReplicationGroupId;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowConverter;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.ColumnsExtractor;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptorSupplier;
import org.apache.ignite.internal.storage.rocksdb.RocksDbStorageEngine;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbStorageEngineConfiguration;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.distributed.StorageUpdateHandler;
import org.apache.ignite.internal.table.distributed.TableMessagesFactory;
import org.apache.ignite.internal.table.distributed.command.FinishTxCommand;
import org.apache.ignite.internal.table.distributed.command.TxCleanupCommand;
import org.apache.ignite.internal.table.distributed.command.UpdateCommand;
import org.apache.ignite.internal.table.distributed.index.IndexUpdateHandler;
import org.apache.ignite.internal.table.distributed.raft.PartitionDataStorage;
import org.apache.ignite.internal.table.distributed.raft.PartitionListener;
import org.apache.ignite.internal.table.distributed.replication.request.ReadOnlyDirectSingleRowReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteSingleRowPkReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteSingleRowReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.SingleRowPkReplicaRequest;
import org.apache.ignite.internal.table.distributed.replicator.PartitionReplicaListener;
import org.apache.ignite.internal.table.distributed.replicator.action.RequestType;
import org.apache.ignite.internal.table.distributed.schema.ThreadLocalPartitionCommandsMarshaller;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.tx.HybridTimestampTracker;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.TransactionIdGenerator;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.message.TxFinishReplicaRequest;
import org.apache.ignite.internal.tx.storage.state.test.TestTxStateStorage;
import org.apache.ignite.internal.tx.storage.state.test.TestTxStateTableStorage;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterNodeImpl;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Persistent partitions raft group snapshots tests.
 */
@ExtendWith({WorkDirectoryExtension.class, ConfigurationExtension.class})
public class ItTablePersistenceTest extends ItAbstractListenerSnapshotTest<PartitionListener> {
    private static final String NODE_NAME = "node1";

    private static final String NODE_ID = "node1";

    private static final TestPlacementDriver TEST_PLACEMENT_DRIVER = new TestPlacementDriver(NODE_NAME, NODE_ID);

    /** Factory to create RAFT command messages. */
    private final TableMessagesFactory msgFactory = new TableMessagesFactory();

    @InjectConfiguration("mock {flushDelayMillis = 0, defaultRegion {size = 16777216, writeBufferSize = 16777216}}")
    private RocksDbStorageEngineConfiguration engineConfig;

    private static final SchemaDescriptor SCHEMA = new SchemaDescriptor(
            1,
            new Column[]{new Column("key", NativeTypes.INT64, false)},
            new Column[]{new Column("value", NativeTypes.INT64, false)}
    );

    private static final ColumnsExtractor PK_EXTRACTOR = BinaryRowConverter.keyExtractor(SCHEMA);

    private static final Row FIRST_VALUE_PK = createKeyRow(1);

    private static final Row FIRST_VALUE = createKeyValueRow(1, 1);

    private static final Row SECOND_VALUE_PK = createKeyRow(2);

    private static final Row SECOND_VALUE = createKeyValueRow(2, 2);

    /** Paths for created partition listeners. */
    private final Map<PartitionListener, Path> paths = new ConcurrentHashMap<>();

    /** Map of node indexes to partition listeners. */
    private final Map<Integer, PartitionListener> partListeners = new ConcurrentHashMap<>();

    /** Map of node indexes to table storages. */
    private final Map<Integer, MvTableStorage> mvTableStorages = new ConcurrentHashMap<>();

    /** Map of node indexes to partition storages. */
    private final Map<Integer, MvPartitionStorage> mvPartitionStorages = new ConcurrentHashMap<>();

    /** Map of node indexes to transaction managers. */
    private final Map<Integer, TxManager> txManagers = new ConcurrentHashMap<>();

    private final ReplicaService replicaService = mock(ReplicaService.class, RETURNS_DEEP_STUBS);

    private final Function<String, ClusterNode> consistentIdToNode = addr
            -> new ClusterNodeImpl(NODE_ID, NODE_NAME, new NetworkAddress(addr, 3333));

    private final HybridClock hybridClock = new HybridClockImpl();

    private int stoppedNodeIndex;

    private InternalTable table;

    private final LinkedList<AutoCloseable> closeables = new LinkedList<>();

    @BeforeEach
    @Override
    public void beforeTest(TestInfo testInfo) {
        super.beforeTest(testInfo);

        closeables.clear();
    }

    @AfterEach
    @Override
    public void afterTest() throws Exception {
        super.afterTest();

        closeAll(closeables);
    }

    @Override
    public void beforeFollowerStop(RaftGroupService service, RaftServer server) throws Exception {
        PartitionReplicaListener partitionReplicaListener = mockPartitionReplicaListener(service);

        when(replicaService.invoke(any(ClusterNode.class), any()))
                .thenAnswer(invocationOnMock -> {
                    ClusterNode node = invocationOnMock.getArgument(0);

                    return partitionReplicaListener.invoke(invocationOnMock.getArgument(1), node.id());
                });

        for (int i = 0; i < nodes(); i++) {
            if (!txManagers.containsKey(i)) {
                TxManager txManager = new TxManagerImpl(
                        service.clusterService(),
                        replicaService,
                        new HeapLockManager(),
                        hybridClock,
                        new TransactionIdGenerator(i),
                        TEST_PLACEMENT_DRIVER,
                        () -> DEFAULT_IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS
                );

                txManager.start();

                txManagers.put(i, txManager);
                closeables.add(txManager::stop);
            }
        }

        TxManager txManager = new TxManagerImpl(
                service.clusterService(),
                replicaService,
                new HeapLockManager(),
                hybridClock,
                new TransactionIdGenerator(-1),
                TEST_PLACEMENT_DRIVER,
                () -> DEFAULT_IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS
        );

        txManager.start();

        closeables.add(txManager::stop);

        table = new InternalTableImpl(
                "table",
                1,
                Int2ObjectMaps.singleton(0, service),
                1,
                consistentIdToNode,
                txManager,
                mock(MvTableStorage.class),
                new TestTxStateTableStorage(),
                replicaService,
                hybridClock,
                new HybridTimestampTracker(),
                TEST_PLACEMENT_DRIVER
        );

        closeables.add(() -> table.close());

        table.upsert(FIRST_VALUE, null).get();
    }

    private PartitionReplicaListener mockPartitionReplicaListener(RaftGroupService service) {
        PartitionReplicaListener partitionReplicaListener = mock(PartitionReplicaListener.class);

        when(partitionReplicaListener.invoke(any(), any())).thenAnswer(invocationOnMock -> {
            ReplicaRequest req = invocationOnMock.getArgument(0);

            if (req instanceof ReadWriteSingleRowPkReplicaRequest || req instanceof ReadOnlyDirectSingleRowReplicaRequest) {
                SingleRowPkReplicaRequest req0 = (SingleRowPkReplicaRequest) req;

                if (req0.requestType() == RequestType.RW_GET || req0.requestType() == RequestType.RO_GET) {
                    List<JraftServerImpl> servers = servers();

                    JraftServerImpl leader = servers.stream()
                            .filter(server -> server.localPeers(raftGroupId()).contains(service.leader()))
                            .findFirst().orElseThrow();

                    // We only read from the leader, every other node may not have the latest data.
                    int storageIndex = servers.indexOf(leader);

                    // Here we must account for the stopped node, index in "servers" and index in "mvPartitionStorages" will differ
                    // for "serverIndex >= stoppedNodeIndex".
                    if (storageIndex >= stoppedNodeIndex) {
                        storageIndex++;
                    }

                    MvPartitionStorage partitionStorage = mvPartitionStorages.get(storageIndex);

                    Map<ByteBuffer, RowId> primaryIndex = pkIndex(partitionStorage);
                    RowId rowId = primaryIndex.get(req0.primaryKey());

                    if (rowId == null) {
                        return completedFuture(null);
                    }

                    BinaryRow row = partitionStorage.read(rowId, HybridTimestamp.MAX_VALUE).binaryRow();

                    return completedFuture(row);
                } else if (req0.requestType() == RequestType.RW_DELETE) {
                    ReadWriteSingleRowPkReplicaRequest rwReq = (ReadWriteSingleRowPkReplicaRequest) req0;

                    UpdateCommand cmd = msgFactory.updateCommand()
                            .txId(rwReq.transactionId())
                            .tablePartitionId(tablePartitionId(new TablePartitionId(1, 0)))
                            .rowUuid(new RowId(0).uuid())
                            .safeTimeLong(hybridClock.nowLong())
                            .txCoordinatorId(UUID.randomUUID().toString())
                            .build();

                    return service.run(cmd);
                }
            } else if (req instanceof ReadWriteSingleRowReplicaRequest) {
                ReadWriteSingleRowReplicaRequest req0 = (ReadWriteSingleRowReplicaRequest) req;

                UpdateCommand cmd = msgFactory.updateCommand()
                        .txId(req0.transactionId())
                        .tablePartitionId(tablePartitionId(new TablePartitionId(1, 0)))
                        .rowUuid(new RowId(0).uuid())
                        .messageRowToUpdate(msgFactory.timedBinaryRowMessage()
                                .binaryRowMessage(msgFactory.binaryRowMessage()
                                        .schemaVersion(req0.schemaVersion())
                                        .binaryTuple(req0.binaryTuple())
                                        .build())
                                .build())
                        .safeTimeLong(hybridClock.nowLong())
                        .txCoordinatorId(UUID.randomUUID().toString())
                        .build();

                return service.run(cmd);
            } else if (req instanceof TxFinishReplicaRequest) {
                TxFinishReplicaRequest req0 = (TxFinishReplicaRequest) req;

                FinishTxCommand cmd = msgFactory.finishTxCommand()
                        .txId(req0.txId())
                        .commit(req0.commit())
                        .commitTimestampLong(req0.commitTimestampLong())
                        .tablePartitionIds(asList(tablePartitionId(new TablePartitionId(1, 0))))
                        .safeTimeLong(hybridClock.nowLong())
                        .txCoordinatorId(UUID.randomUUID().toString())
                        .build();

                return service.run(cmd)
                        .thenCompose(ignored -> {
                            TxCleanupCommand cleanupCmd = msgFactory.txCleanupCommand()
                                    .txId(req0.txId())
                                    .commit(req0.commit())
                                    .commitTimestampLong(req0.commitTimestampLong())
                                    .safeTimeLong(hybridClock.nowLong())
                                    .txCoordinatorId(UUID.randomUUID().toString())
                                    .build();

                            return service.run(cleanupCmd);
                        });
            }

            throw new AssertionError("Unexpected request: " + req);
        });

        return partitionReplicaListener;
    }

    @Override
    public void afterFollowerStop(RaftGroupService service, RaftServer server, int stoppedNodeIndex) throws Exception {
        // Remove the first key
        table.delete(FIRST_VALUE_PK, null).get();

        // Put deleted data again
        table.upsert(FIRST_VALUE, null).get();

        this.stoppedNodeIndex = stoppedNodeIndex;

        mvTableStorages.get(stoppedNodeIndex).stop();

        paths.remove(partListeners.get(stoppedNodeIndex));
    }

    @Override
    public void afterSnapshot(RaftGroupService service) throws Exception {
        table.upsert(SECOND_VALUE, null).get();

        assertNotNull(table.get(SECOND_VALUE_PK, null).join());
    }

    @Override
    public BooleanSupplier snapshotCheckClosure(JraftServerImpl restarted, boolean interactedAfterSnapshot) {
        MvPartitionStorage storage = getListener(restarted, raftGroupId()).getMvStorage();

        return () -> {
            Map<ByteBuffer, RowId> primaryIndex = pkIndex(storage);

            Row pk = interactedAfterSnapshot ? SECOND_VALUE_PK : FIRST_VALUE_PK;

            RowId rowId = primaryIndex.get(pk.byteBuffer());

            if (rowId == null) {
                return false;
            }

            ReadResult read = storage.read(rowId, HybridTimestamp.MAX_VALUE);

            if (read == null) {
                return false;
            }

            Row value = interactedAfterSnapshot ? SECOND_VALUE : FIRST_VALUE;

            return value.tupleSlice().equals(read.binaryRow().tupleSlice());
        };
    }

    private static Map<ByteBuffer, RowId> pkIndex(MvPartitionStorage storage) {
        Map<ByteBuffer, RowId> result = new HashMap<>();

        RowId rowId = storage.closestRowId(RowId.lowestRowId(0));

        while (rowId != null) {
            BinaryRow binaryRow = storage.read(rowId, HybridTimestamp.MAX_VALUE).binaryRow();

            if (binaryRow != null) {
                result.put(PK_EXTRACTOR.extractColumns(binaryRow).byteBuffer(), rowId);
            }

            RowId incremented = rowId.increment();
            if (incremented == null) {
                break;
            }

            rowId = storage.closestRowId(incremented);
        }

        return result;
    }

    @Override
    public Path getListenerPersistencePath(PartitionListener listener, RaftServer server) {
        return paths.get(listener);
    }

    @Override
    public RaftGroupListener createListener(ClusterService service, Path path, int index) {
        return paths.entrySet().stream()
                .filter(entry -> entry.getValue().equals(path))
                .map(Map.Entry::getKey)
                .findAny()
                .orElseGet(() -> {
                    RocksDbStorageEngine storageEngine = new RocksDbStorageEngine("test", engineConfig, path);
                    storageEngine.start();

                    int tableId = 1;

                    MvTableStorage mvTableStorage = storageEngine.createMvTable(
                            new StorageTableDescriptor(tableId, 1, DEFAULT_DATA_REGION_NAME),
                            new StorageIndexDescriptorSupplier(mock(CatalogService.class))
                    );
                    mvTableStorage.start();

                    mvTableStorages.put(index, mvTableStorage);

                    int partitionId = 0;

                    MvPartitionStorage mvPartitionStorage = getOrCreateMvPartition(mvTableStorage, partitionId);
                    mvPartitionStorages.put(index, mvPartitionStorage);

                    PartitionDataStorage partitionDataStorage = new TestPartitionDataStorage(tableId, partitionId, mvPartitionStorage);

                    PendingComparableValuesTracker<HybridTimestamp, Void> safeTime = new PendingComparableValuesTracker<>(
                            new HybridTimestamp(1, 0)
                    );

                    IndexUpdateHandler indexUpdateHandler = new IndexUpdateHandler(
                            DummyInternalTableImpl.createTableIndexStoragesSupplier(Map.of())
                    );

                    StorageUpdateHandler storageUpdateHandler = new StorageUpdateHandler(
                            partitionId,
                            partitionDataStorage,
                            indexUpdateHandler
                    );

                    TxManager txManager = txManagers.computeIfAbsent(index, k -> {
                        TxManager txMgr = new TxManagerImpl(
                                service,
                                replicaService,
                                new HeapLockManager(),
                                hybridClock,
                                new TransactionIdGenerator(index),
                                TEST_PLACEMENT_DRIVER,
                                () -> DEFAULT_IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS
                        );
                        txMgr.start();
                        closeables.add(txMgr::stop);

                        return txMgr;
                    });

                    PartitionListener listener = new PartitionListener(
                            txManager,
                            partitionDataStorage,
                            storageUpdateHandler,
                            new TestTxStateStorage(),
                            safeTime,
                            new PendingComparableValuesTracker<>(0L)
                    ) {
                        @Override
                        public void onShutdown() {
                            super.onShutdown();

                            try {
                                closeAll(mvPartitionStorage::close, mvTableStorage::stop, storageEngine::stop);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }
                    };

                    paths.put(listener, path);
                    partListeners.put(index, listener);

                    return listener;
                });
    }

    @Override
    public TestReplicationGroupId raftGroupId() {
        return new TestReplicationGroupId("partitions");
    }

    @Override
    protected Marshaller commandsMarshaller(ClusterService clusterService) {
        return new ThreadLocalPartitionCommandsMarshaller(clusterService.serializationRegistry());
    }

    /**
     * Creates a {@link Row} with the supplied key and value.
     *
     * @param id    Key.
     * @param value Value.
     * @return Row.
     */
    private static Row createKeyValueRow(long id, long value) {
        RowAssembler rowBuilder = new RowAssembler(SCHEMA);

        rowBuilder.appendLong(id);
        rowBuilder.appendLong(value);

        return Row.wrapBinaryRow(SCHEMA, rowBuilder.build());
    }

    private static Row createKeyRow(long id) {
        RowAssembler rowBuilder = RowAssembler.keyAssembler(SCHEMA);

        rowBuilder.appendLong(id);

        return Row.wrapKeyOnlyBinaryRow(SCHEMA, rowBuilder.build());
    }

    private static MvPartitionStorage getOrCreateMvPartition(MvTableStorage tableStorage, int partitionId) {
        MvPartitionStorage mvPartition = tableStorage.getMvPartition(partitionId);

        if (mvPartition != null) {
            return mvPartition;
        }

        CompletableFuture<MvPartitionStorage> createMvPartitionFuture = tableStorage.createMvPartition(0);

        assertThat(createMvPartitionFuture, willCompleteSuccessfully());

        return createMvPartitionFuture.join();
    }
}
