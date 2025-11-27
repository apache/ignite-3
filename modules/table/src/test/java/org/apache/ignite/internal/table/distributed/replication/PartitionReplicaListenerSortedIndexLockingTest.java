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

package org.apache.ignite.internal.table.distributed.replication;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_DELETE;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_DELETE_ALL;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_DELETE_EXACT;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_DELETE_EXACT_ALL;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_GET_AND_DELETE;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_GET_AND_REPLACE;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_GET_AND_UPSERT;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_INSERT;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_INSERT_ALL;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_REPLACE_IF_EXIST;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_UPSERT;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_UPSERT_ALL;
import static org.apache.ignite.internal.table.distributed.replication.PartitionReplicaListenerIndexLockingTest.LOCAL_NODE_ID;
import static org.apache.ignite.internal.table.distributed.replication.PartitionReplicaListenerTest.binaryRowsToBuffers;
import static org.apache.ignite.internal.table.distributed.replication.PartitionReplicaListenerTest.zonePartitionIdMessage;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.tx.TxState.checkTransitionCorrectness;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasItem;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.apache.ignite.distributed.TestPartitionDataStorage;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.components.SystemPropertiesNodeProperties;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.NoOpFailureManager;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.lowwatermark.TestLowWatermark;
import org.apache.ignite.internal.network.ClusterNodeResolver;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.replication.RequestType;
import org.apache.ignite.internal.placementdriver.TestPlacementDriver;
import org.apache.ignite.internal.raft.service.LeaderWithTerm;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.schema.AlwaysSyncedSchemaSyncService;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowConverter;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.ColumnsExtractor;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.KvMarshaller;
import org.apache.ignite.internal.schema.marshaller.reflection.ReflectionMarshallerFactory;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.impl.TestMvPartitionStorage;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor.StorageSortedIndexColumnDescriptor;
import org.apache.ignite.internal.storage.index.impl.TestSortedIndexStorage;
import org.apache.ignite.internal.table.TableTestUtils;
import org.apache.ignite.internal.table.distributed.IndexLocker;
import org.apache.ignite.internal.table.distributed.SortedIndexLocker;
import org.apache.ignite.internal.table.distributed.StorageUpdateHandler;
import org.apache.ignite.internal.table.distributed.TableSchemaAwareIndexStorage;
import org.apache.ignite.internal.table.distributed.index.IndexMetaStorage;
import org.apache.ignite.internal.table.distributed.index.IndexUpdateHandler;
import org.apache.ignite.internal.table.distributed.replicator.PartitionReplicaListener;
import org.apache.ignite.internal.table.distributed.replicator.TransactionStateResolver;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.internal.table.impl.DummySchemaManagerImpl;
import org.apache.ignite.internal.table.impl.DummyValidationSchemasSource;
import org.apache.ignite.internal.table.metrics.TableMetricSource;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.tx.Lock;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.LockMode;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.RemotelyTriggeredResourceRegistry;
import org.apache.ignite.internal.tx.impl.WaitDieDeadlockPreventionPolicy;
import org.apache.ignite.internal.tx.storage.state.test.TestTxStatePartitionStorage;
import org.apache.ignite.internal.tx.test.TestTransactionIds;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.Lazy;
import org.apache.ignite.internal.util.Pair;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.table.QualifiedName;
import org.hamcrest.CustomMatcher;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** There are tests for partition replica listener. */
@ExtendWith(ConfigurationExtension.class)
public class PartitionReplicaListenerSortedIndexLockingTest extends IgniteAbstractTest {
    private static final int PART_ID = 0;
    private static final int TABLE_ID = 1;
    private static final int PK_INDEX_ID = 1;
    private static final int ZONE_ID = 2;
    private static final UUID TRANSACTION_ID = TestTransactionIds.newTransactionId();
    private static final HybridClock CLOCK = new HybridClockImpl();
    private static final ClockService CLOCK_SERVICE = new TestClockService(CLOCK);
    private static final LockManager LOCK_MANAGER = lockManager();
    private static final ZonePartitionId ZONE_PARTITION_ID = new ZonePartitionId(ZONE_ID, PART_ID);
    private static final PartitionReplicationMessagesFactory TABLE_MESSAGES_FACTORY = new PartitionReplicationMessagesFactory();
    private static final TestMvPartitionStorage TEST_MV_PARTITION_STORAGE = new TestMvPartitionStorage(PART_ID);

    private static SchemaDescriptor schemaDescriptor;
    private static KvMarshaller<Integer, Integer> kvMarshaller;
    private static Lazy<TableSchemaAwareIndexStorage> pkStorage;
    private static PartitionReplicaListener partitionReplicaListener;
    private static ColumnsExtractor row2HashKeyConverter;

    @InjectConfiguration
    private static ReplicationConfiguration replicationConfiguration;

    @BeforeAll
    public static void beforeAll() {
        RaftGroupService mockRaftClient = mock(RaftGroupService.class);

        when(mockRaftClient.refreshAndGetLeaderWithTerm())
                .thenAnswer(invocationOnMock -> completedFuture(LeaderWithTerm.NO_LEADER));
        when(mockRaftClient.run(any()))
                .thenAnswer(invocationOnMock -> nullCompletedFuture());

        schemaDescriptor = new SchemaDescriptor(1, new Column[]{
                new Column("id".toUpperCase(Locale.ROOT), NativeTypes.INT32, false),
        }, new Column[]{
                new Column("val".toUpperCase(Locale.ROOT), NativeTypes.INT32, false),
        });

        row2HashKeyConverter = BinaryRowConverter.keyExtractor(schemaDescriptor);

        StorageSortedIndexDescriptor pkIndexDescriptor = new StorageSortedIndexDescriptor(
                PK_INDEX_ID,
                List.of(new StorageSortedIndexColumnDescriptor("ID", NativeTypes.INT32, false, true, false)),
                false
        );

        TableSchemaAwareIndexStorage pkSortedIndexStorage = new TableSchemaAwareIndexStorage(
                PK_INDEX_ID,
                new TestSortedIndexStorage(PART_ID, pkIndexDescriptor),
                row2HashKeyConverter
        );

        pkStorage = new Lazy<>(() -> pkSortedIndexStorage);

        IndexLocker pkLocker = new SortedIndexLocker(
                PK_INDEX_ID,
                PART_ID,
                LOCK_MANAGER,
                (SortedIndexStorage) pkSortedIndexStorage.storage(),
                row2HashKeyConverter,
                true
        );

        DummySchemaManagerImpl schemaManager = new DummySchemaManagerImpl(schemaDescriptor);
        PendingComparableValuesTracker<HybridTimestamp, Void> safeTime = new PendingComparableValuesTracker<>(CLOCK.now());

        IndexUpdateHandler indexUpdateHandler = new IndexUpdateHandler(
                DummyInternalTableImpl.createTableIndexStoragesSupplier(Map.of(pkStorage.get().id(), pkStorage.get()))
        );

        TestPartitionDataStorage partitionDataStorage = new TestPartitionDataStorage(TABLE_ID, PART_ID, TEST_MV_PARTITION_STORAGE);

        CatalogService catalogService = mock(CatalogService.class);
        Catalog catalog = mock(Catalog.class);

        when(catalogService.catalog(anyInt())).thenReturn(catalog);
        when(catalogService.activeCatalog(anyLong())).thenReturn(catalog);
        when(catalogService.catalogReadyFuture(anyInt())).thenReturn(nullCompletedFuture());

        CatalogTableDescriptor tableDescriptor = mock(CatalogTableDescriptor.class);
        when(tableDescriptor.latestSchemaVersion()).thenReturn(schemaDescriptor.version());

        when(catalog.table(anyInt())).thenReturn(tableDescriptor);
        when(catalog.table(anyInt())).thenReturn(tableDescriptor);

        CatalogIndexDescriptor indexDescriptor = mock(CatalogIndexDescriptor.class);
        when(indexDescriptor.id()).thenReturn(PK_INDEX_ID);

        when(catalog.indexes(anyInt())).thenReturn(List.of(indexDescriptor));

        InternalClusterNode localNode = DummyInternalTableImpl.LOCAL_NODE;

        partitionReplicaListener = new PartitionReplicaListener(
                TEST_MV_PARTITION_STORAGE,
                mockRaftClient,
                newTxManager(),
                LOCK_MANAGER,
                Runnable::run,
                new ZonePartitionId(ZONE_ID, PART_ID),
                TABLE_ID,
                () -> Map.of(
                        pkLocker.id(), pkLocker
                ),
                pkStorage,
                () -> Map.of(),
                CLOCK_SERVICE,
                safeTime,
                new TestTxStatePartitionStorage(),
                mock(TransactionStateResolver.class),
                new StorageUpdateHandler(
                        PART_ID,
                        partitionDataStorage,
                        indexUpdateHandler,
                        replicationConfiguration,
                        TableTestUtils.NOOP_PARTITION_MODIFICATION_COUNTER
                ),
                new DummyValidationSchemasSource(schemaManager),
                localNode,
                new AlwaysSyncedSchemaSyncService(),
                catalogService,
                new TestPlacementDriver(localNode),
                mock(ClusterNodeResolver.class),
                new RemotelyTriggeredResourceRegistry(),
                schemaManager,
                mock(IndexMetaStorage.class),
                new TestLowWatermark(),
                new NoOpFailureManager(),
                new TableMetricSource(QualifiedName.fromSimple("test_table"))
        );

        kvMarshaller = new ReflectionMarshallerFactory().create(schemaDescriptor, Integer.class, Integer.class);
    }

    private static TxManager newTxManager() {
        TxManager txManager = mock(TxManager.class);

        ConcurrentHashMap<UUID, TxStateMeta> txStateMap = new ConcurrentHashMap<>();

        doAnswer(invocation -> txStateMap.get(invocation.getArgument(0)))
                .when(txManager).stateMeta(any());

        doAnswer(invocation -> {
            UUID txId = invocation.getArgument(0);
            Function<TxStateMeta, TxStateMeta> updater = invocation.getArgument(1);
            txStateMap.compute(txId, (k, oldMeta) -> {
                TxStateMeta newMeta = updater.apply(oldMeta);

                if (newMeta == null) {
                    return null;
                }

                TxState oldState = oldMeta == null ? null : oldMeta.txState();

                return checkTransitionCorrectness(oldState, newMeta.txState()) ? newMeta : oldMeta;
            });
            return null;
        }).when(txManager).updateTxMeta(any(), any());

        return txManager;
    }

    @BeforeEach
    public void beforeTest() {
        ((TestSortedIndexStorage) pkStorage.get().storage()).clear();
        TEST_MV_PARTITION_STORAGE.clear();

        LOCK_MANAGER.releaseAll(TRANSACTION_ID);
    }

    private static LockManager lockManager() {
        HeapLockManager lockManager = HeapLockManager.smallInstance();
        lockManager.start(new WaitDieDeadlockPreventionPolicy());
        return lockManager;
    }

    /** Verifies the mode in which the lock was acquired on the index key for a particular operation. */
    @ParameterizedTest
    @MethodSource("readWriteSingleTestArguments")
    void testReadWriteSingle(ReadWriteTestArg arg) {
        BinaryRow testPk = kvMarshaller.marshal(1);
        BinaryRow testBinaryRow = kvMarshaller.marshal(1, 1);

        if (arg.type != RW_INSERT) {
            var rowId = new RowId(PART_ID);
            insertRows(List.of(new Pair<>(testBinaryRow, rowId)), TestTransactionIds.newTransactionId());
        }

        InternalClusterNode localNode = DummyInternalTableImpl.LOCAL_NODE;

        ReplicaRequest request;

        switch (arg.type) {
            case RW_DELETE:
            case RW_GET_AND_DELETE:
                request = TABLE_MESSAGES_FACTORY.readWriteSingleRowPkReplicaRequest()
                        .groupId(zonePartitionIdMessage(ZONE_PARTITION_ID))
                        .tableId(TABLE_ID)
                        .enlistmentConsistencyToken(1L)
                        .commitPartitionId(zonePartitionIdMessage(ZONE_PARTITION_ID))
                        .transactionId(TRANSACTION_ID)
                        .schemaVersion(testPk.schemaVersion())
                        .primaryKey(testPk.tupleSlice())
                        .requestType(arg.type)
                        .coordinatorId(localNode.id())
                        .timestamp(CLOCK.now())
                        .build();

                break;

            case RW_DELETE_EXACT:
            case RW_INSERT:
            case RW_UPSERT:
            case RW_REPLACE_IF_EXIST:
            case RW_GET_AND_REPLACE:
            case RW_GET_AND_UPSERT:
                request = TABLE_MESSAGES_FACTORY.readWriteSingleRowReplicaRequest()
                        .groupId(zonePartitionIdMessage(ZONE_PARTITION_ID))
                        .tableId(TABLE_ID)
                        .enlistmentConsistencyToken(1L)
                        .commitPartitionId(zonePartitionIdMessage(ZONE_PARTITION_ID))
                        .transactionId(TRANSACTION_ID)
                        .schemaVersion(testBinaryRow.schemaVersion())
                        .binaryTuple(testBinaryRow.tupleSlice())
                        .requestType(arg.type)
                        .coordinatorId(localNode.id())
                        .timestamp(CLOCK.now())
                        .build();
                break;

            default:
                throw new AssertionError("Unexpected operation type: " + arg.type);
        }

        CompletableFuture<?> fut = partitionReplicaListener.invoke(request, LOCAL_NODE_ID);

        await(fut);

        assertThat(
                locks(),
                allOf(
                        hasItem(lockThat(
                                arg.expectedLockOnSortedPk + " on sorted pk index",
                                lock -> Objects.equals(PK_INDEX_ID, lock.lockKey().contextId())
                                        && row2HashKeyConverter.extractColumns(testBinaryRow).byteBuffer().equals(lock.lockKey().key())
                                        && lock.lockMode() == arg.expectedLockOnSortedPk
                        ))
                )
        );
    }

    /** Verifies the mode in which the lock was acquired on the index key for a particular operation. */
    @ParameterizedTest
    @MethodSource("readWriteMultiTestArguments")
    void testReadWriteMulti(ReadWriteTestArg arg) {
        var pks = new ArrayList<BinaryRow>();
        var rows = new ArrayList<BinaryRow>();

        for (int i = 1; i <= 3; i++) {
            pks.add(kvMarshaller.marshal(i));
            rows.add(kvMarshaller.marshal(i, i));
        }

        if (arg.type != RW_INSERT_ALL) {
            for (BinaryRow row : rows) {
                var rowId = new RowId(PART_ID);
                insertRows(List.of(new Pair<>(row, rowId)), TestTransactionIds.newTransactionId());
            }
        }

        InternalClusterNode localNode = DummyInternalTableImpl.LOCAL_NODE;

        ReplicaRequest request;

        switch (arg.type) {
            case RW_DELETE_ALL:
                request = TABLE_MESSAGES_FACTORY.readWriteMultiRowPkReplicaRequest()
                        .groupId(zonePartitionIdMessage(ZONE_PARTITION_ID))
                        .tableId(TABLE_ID)
                        .enlistmentConsistencyToken(1L)
                        .commitPartitionId(zonePartitionIdMessage(ZONE_PARTITION_ID))
                        .transactionId(TRANSACTION_ID)
                        .schemaVersion(pks.iterator().next().schemaVersion())
                        .primaryKeys(pks.stream().map(BinaryRow::tupleSlice).collect(toList()))
                        .requestType(arg.type)
                        .coordinatorId(localNode.id())
                        .timestamp(CLOCK.now())
                        .build();

                break;

            case RW_DELETE_EXACT_ALL:
            case RW_INSERT_ALL:
            case RW_UPSERT_ALL:
                request = TABLE_MESSAGES_FACTORY.readWriteMultiRowReplicaRequest()
                        .groupId(zonePartitionIdMessage(ZONE_PARTITION_ID))
                        .tableId(TABLE_ID)
                        .enlistmentConsistencyToken(1L)
                        .commitPartitionId(zonePartitionIdMessage(ZONE_PARTITION_ID))
                        .transactionId(TRANSACTION_ID)
                        .schemaVersion(rows.iterator().next().schemaVersion())
                        .binaryTuples(binaryRowsToBuffers(rows))
                        .requestType(arg.type)
                        .coordinatorId(localNode.id())
                        .timestamp(CLOCK.now())
                        .build();

                break;

            default:
                throw new AssertionError("Unexpected operation type: " + arg.type);
        }

        CompletableFuture<?> fut = partitionReplicaListener.invoke(request, LOCAL_NODE_ID);

        await(fut);

        for (BinaryRow row : rows) {
            assertThat(
                    locks(),
                    allOf(
                            hasItem(lockThat(
                                    arg.expectedLockOnSortedPk + " on sorted pk index",
                                    lock -> Objects.equals(PK_INDEX_ID, lock.lockKey().contextId())
                                            && row2HashKeyConverter.extractColumns(row).byteBuffer().equals(lock.lockKey().key())
                                            && lock.lockMode() == arg.expectedLockOnSortedPk
                            ))
                    )
            );
        }
    }

    private static Iterable<ReadWriteTestArg> readWriteSingleTestArguments() {
        return List.of(
                new ReadWriteTestArg(RW_DELETE, LockMode.SIX),
                new ReadWriteTestArg(RW_DELETE_EXACT, LockMode.SIX),
                new ReadWriteTestArg(RW_INSERT, LockMode.X),
                new ReadWriteTestArg(RW_UPSERT, LockMode.X),
                new ReadWriteTestArg(RW_REPLACE_IF_EXIST, LockMode.X),

                new ReadWriteTestArg(RW_GET_AND_DELETE, LockMode.SIX),
                new ReadWriteTestArg(RW_GET_AND_REPLACE, LockMode.X),
                new ReadWriteTestArg(RW_GET_AND_UPSERT, LockMode.X)
        );
    }

    private static Iterable<ReadWriteTestArg> readWriteMultiTestArguments() {
        return List.of(
                new ReadWriteTestArg(RW_DELETE_ALL, LockMode.SIX),
                new ReadWriteTestArg(RW_DELETE_EXACT_ALL, LockMode.SIX),
                new ReadWriteTestArg(RW_INSERT_ALL, LockMode.X),
                new ReadWriteTestArg(RW_UPSERT_ALL, LockMode.X)
        );
    }

    private List<Lock> locks() {
        List<Lock> locks = new ArrayList<>();

        Iterator<Lock> it = LOCK_MANAGER.locks(TRANSACTION_ID);

        while (it.hasNext()) {
            locks.add(it.next());
        }

        return locks;
    }

    private void insertRows(List<Pair<BinaryRow, RowId>> rows, UUID txId) {
        HybridTimestamp commitTs = CLOCK.now();

        for (Pair<BinaryRow, RowId> row : rows) {
            BinaryRow binaryRow = row.getFirst();
            RowId rowId = row.getSecond();

            pkStorage.get().put(binaryRow, rowId);
            TEST_MV_PARTITION_STORAGE.addWrite(rowId, binaryRow, txId, TABLE_ID, PART_ID);
            TEST_MV_PARTITION_STORAGE.commitWrite(rowId, commitTs, txId);
        }
    }

    private static Matcher<Lock> lockThat(String description, Function<Lock, Boolean> checker) {
        return new CustomMatcher<>(description) {
            @Override
            public boolean matches(Object actual) {
                return actual instanceof Lock && checker.apply((Lock) actual) == Boolean.TRUE;
            }
        };
    }

    static class ReadWriteTestArg {
        private final RequestType type;
        private final LockMode expectedLockOnSortedPk;

        public ReadWriteTestArg(
                RequestType type,
                LockMode expectedLockOnSortedPk
        ) {
            this.type = type;
            this.expectedLockOnSortedPk = expectedLockOnSortedPk;
        }

        @Override
        public String toString() {
            return type.toString();
        }
    }
}
