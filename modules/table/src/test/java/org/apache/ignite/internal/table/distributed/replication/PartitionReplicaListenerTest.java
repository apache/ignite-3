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

import static java.util.Collections.singletonList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.catalog.CatalogManagerImpl.INITIAL_CAUSALITY_TOKEN;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PARTITION_COUNT;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestampToLong;
import static org.apache.ignite.internal.schema.BinaryRowMatcher.equalToRow;
import static org.apache.ignite.internal.testframework.asserts.CompletableFutureAssert.assertWillThrowFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.apache.ignite.internal.util.ArrayUtils.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.AdditionalMatchers.gt;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.distributed.TestPartitionDataStorage;
import org.apache.ignite.distributed.replicator.action.RequestTypes;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTuplePrefixBuilder;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.marshaller.MarshallerException;
import org.apache.ignite.internal.placementdriver.TestPlacementDriver;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.service.LeaderWithTerm;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowConverter;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.ColumnsExtractor;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.configuration.GcConfiguration;
import org.apache.ignite.internal.schema.marshaller.KvMarshaller;
import org.apache.ignite.internal.schema.marshaller.MarshallerFactory;
import org.apache.ignite.internal.schema.marshaller.reflection.ReflectionMarshallerFactory;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.impl.TestMvPartitionStorage;
import org.apache.ignite.internal.storage.impl.TestMvTableStorage;
import org.apache.ignite.internal.storage.index.IndexRowImpl;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor.StorageHashIndexColumnDescriptor;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor.StorageSortedIndexColumnDescriptor;
import org.apache.ignite.internal.storage.index.impl.TestHashIndexStorage;
import org.apache.ignite.internal.storage.index.impl.TestSortedIndexStorage;
import org.apache.ignite.internal.table.distributed.HashIndexLocker;
import org.apache.ignite.internal.table.distributed.IndexLocker;
import org.apache.ignite.internal.table.distributed.LowWatermark;
import org.apache.ignite.internal.table.distributed.SortedIndexLocker;
import org.apache.ignite.internal.table.distributed.StorageUpdateHandler;
import org.apache.ignite.internal.table.distributed.TableMessagesFactory;
import org.apache.ignite.internal.table.distributed.TableSchemaAwareIndexStorage;
import org.apache.ignite.internal.table.distributed.command.CatalogVersionAware;
import org.apache.ignite.internal.table.distributed.command.FinishTxCommand;
import org.apache.ignite.internal.table.distributed.command.PartitionCommand;
import org.apache.ignite.internal.table.distributed.command.TablePartitionIdMessage;
import org.apache.ignite.internal.table.distributed.command.TxCleanupCommand;
import org.apache.ignite.internal.table.distributed.command.UpdateCommand;
import org.apache.ignite.internal.table.distributed.gc.GcUpdateHandler;
import org.apache.ignite.internal.table.distributed.index.IndexBuilder;
import org.apache.ignite.internal.table.distributed.index.IndexUpdateHandler;
import org.apache.ignite.internal.table.distributed.raft.PartitionDataStorage;
import org.apache.ignite.internal.table.distributed.replication.request.BinaryRowMessage;
import org.apache.ignite.internal.table.distributed.replication.request.BinaryTupleMessage;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteReplicaRequest;
import org.apache.ignite.internal.table.distributed.replicator.IncompatibleSchemaAbortException;
import org.apache.ignite.internal.table.distributed.replicator.IncompatibleSchemaException;
import org.apache.ignite.internal.table.distributed.replicator.LeaderOrTxState;
import org.apache.ignite.internal.table.distributed.replicator.PartitionReplicaListener;
import org.apache.ignite.internal.table.distributed.replicator.TransactionStateResolver;
import org.apache.ignite.internal.table.distributed.replicator.action.RequestType;
import org.apache.ignite.internal.table.distributed.schema.AlwaysSyncedSchemaSyncService;
import org.apache.ignite.internal.table.distributed.schema.FullTableSchema;
import org.apache.ignite.internal.table.distributed.schema.SchemaSyncService;
import org.apache.ignite.internal.table.distributed.schema.Schemas;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.TransactionIds;
import org.apache.ignite.internal.tx.TransactionMeta;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.message.TxFinishReplicaRequest;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.internal.tx.storage.state.test.TestTxStateStorage;
import org.apache.ignite.internal.tx.test.TestTransactionIds;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.Lazy;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.lang.ErrorGroups.Transactions;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterNodeImpl;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.tx.TransactionException;
import org.hamcrest.Matcher;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junitpioneer.jupiter.cartesian.ArgumentSets;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/** Tests for partition replica listener. */
@ExtendWith(ConfigurationExtension.class)
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class PartitionReplicaListenerTest extends IgniteAbstractTest {
    private static final int PART_ID = 0;

    private static final int CURRENT_SCHEMA_VERSION = 1;

    private static final int NEXT_SCHEMA_VERSION = 2;

    private static final int FUTURE_SCHEMA_VERSION = NEXT_SCHEMA_VERSION;

    private static final int FUTURE_SCHEMA_ROW_INDEXED_VALUE = 0;

    private static final int TABLE_ID = 1;

    private final Map<UUID, Set<RowId>> pendingRows = new ConcurrentHashMap<>();

    /** The storage stores partition data. */
    private final TestMvPartitionStorage testMvPartitionStorage = new TestMvPartitionStorage(PART_ID);

    private final LockManager lockManager = new HeapLockManager();

    private final Function<PartitionCommand, CompletableFuture<?>> defaultMockRaftFutureClosure = cmd -> {
        if (cmd instanceof TxCleanupCommand) {
            Set<RowId> rows = pendingRows.remove(cmd.txId());

            HybridTimestamp commitTimestamp = ((TxCleanupCommand) cmd).commitTimestamp();
            assertNotNull(commitTimestamp);

            if (rows != null) {
                for (RowId row : rows) {
                    testMvPartitionStorage.commitWrite(row, commitTimestamp);
                }
            }

            lockManager.locks(cmd.txId()).forEachRemaining(lockManager::release);
        } else if (cmd instanceof UpdateCommand) {
            pendingRows.compute(cmd.txId(), (txId, v) -> {
                if (v == null) {
                    v = new HashSet<>();
                }

                RowId rowId = new RowId(PART_ID, ((UpdateCommand) cmd).rowUuid());
                v.add(rowId);

                return v;
            });
        }

        return completedFuture(null);
    };

    /** Tx messages factory. */
    private static final TxMessagesFactory TX_MESSAGES_FACTORY = new TxMessagesFactory();

    /** Table messages factory. */
    private static final TableMessagesFactory TABLE_MESSAGES_FACTORY = new TableMessagesFactory();

    /** Partition group id. */
    private final TablePartitionId grpId = new TablePartitionId(TABLE_ID, PART_ID);

    /** Hybrid clock. */
    private final HybridClock clock = new HybridClockImpl();

    /** The storage stores transaction states. */
    private final TestTxStateStorage txStateStorage = new TestTxStateStorage();

    /** Local cluster node. */
    private final ClusterNode localNode = new ClusterNodeImpl("node1", "node1", NetworkAddress.from("127.0.0.1:127"));

    /** Another (not local) cluster node. */
    private final ClusterNode anotherNode = new ClusterNodeImpl("node2", "node2", NetworkAddress.from("127.0.0.2:127"));

    private TransactionStateResolver transactionStateResolver;

    private final PartitionDataStorage partitionDataStorage = new TestPartitionDataStorage(TABLE_ID, PART_ID, testMvPartitionStorage);

    @Mock
    private RaftGroupService mockRaftClient;

    @Mock
    private TxManager txManager;

    @Mock
    private TopologyService topologySrv;

    @Mock
    private PendingComparableValuesTracker<HybridTimestamp, Void> safeTimeClock;

    @Mock
    private Schemas schemas;

    @Spy
    private final SchemaSyncService schemaSyncService = new AlwaysSyncedSchemaSyncService();

    @Mock
    private CatalogService catalogService;

    @Mock
    private MessagingService messagingService;

    /** Schema descriptor for tests. */
    private SchemaDescriptor schemaDescriptor;

    /** Schema descriptor, version 2. */
    private SchemaDescriptor schemaDescriptorVersion2;

    /** Key-value marshaller for tests. */
    private KvMarshaller<TestKey, TestValue> kvMarshaller;

    /** Key-value marshaller using schema version 2. */
    private KvMarshaller<TestKey, TestValue> kvMarshallerVersion2;

    private final CatalogTableDescriptor tableDescriptor = new CatalogTableDescriptor(
            TABLE_ID, 1, "table", 1, CURRENT_SCHEMA_VERSION,
            List.of(
                    new CatalogTableColumnDescriptor("intKey", ColumnType.INT32, false, 0, 0, 0, null),
                    new CatalogTableColumnDescriptor("strKey", ColumnType.STRING, false, 0, 0, 0, null),
                    new CatalogTableColumnDescriptor("intVal", ColumnType.INT32, false, 0, 0, 0, null),
                    new CatalogTableColumnDescriptor("strVal", ColumnType.STRING, false, 0, 0, 0, null)
            ),
            List.of("intKey", "strKey"),
            null,
            INITIAL_CAUSALITY_TOKEN
    );

    /** Partition replication listener to test. */
    private PartitionReplicaListener partitionReplicaListener;

    /** Primary index. */
    private Lazy<TableSchemaAwareIndexStorage> pkStorageSupplier;

    /** If true the local replica is considered leader, false otherwise. */
    private boolean localLeader;

    /** The state is used to resolve write intent. */
    @Nullable
    private TxState txState;
    private TxStateMeta txStateMeta;

    /** Secondary sorted index. */
    private TableSchemaAwareIndexStorage sortedIndexStorage;

    /** Secondary hash index. */
    private TableSchemaAwareIndexStorage hashIndexStorage;

    private Function<PartitionCommand, CompletableFuture<?>> raftClientFutureClosure = defaultMockRaftFutureClosure;

    private static final AtomicInteger nextMonotonicInt = new AtomicInteger(1);

    @Captor
    private ArgumentCaptor<HybridTimestamp> timestampCaptor;

    @Captor
    private ArgumentCaptor<Command> commandCaptor;

    private final TestValue someValue = new TestValue(1, "v1");

    @BeforeEach
    public void beforeTest(
            @InjectConfiguration GcConfiguration gcConfig
    ) {
        when(mockRaftClient.refreshAndGetLeaderWithTerm()).thenAnswer(invocationOnMock -> {
            if (!localLeader) {
                return completedFuture(new LeaderWithTerm(new Peer(anotherNode.name()), 1L));
            }

            return completedFuture(new LeaderWithTerm(new Peer(localNode.name()), 1L));
        });

        when(mockRaftClient.run(any()))
                .thenAnswer(invocationOnMock -> raftClientFutureClosure.apply(invocationOnMock.getArgument(0)));

        when(topologySrv.getByConsistentId(any())).thenAnswer(invocationOnMock -> {
            String consistentId = invocationOnMock.getArgument(0);
            if (consistentId.equals(anotherNode.name())) {
                return anotherNode;
            } else if (consistentId.equals(localNode.name())) {
                return localNode;
            } else {
                return null;
            }
        });

        when(topologySrv.localMember()).thenReturn(localNode);

        when(safeTimeClock.waitFor(any())).thenReturn(completedFuture(null));

        when(schemas.waitForSchemasAvailability(any())).thenReturn(completedFuture(null));
        when(schemas.waitForSchemaAvailability(anyInt(), anyInt())).thenReturn(completedFuture(null));

        lenient().when(catalogService.table(anyInt(), anyLong())).thenReturn(tableDescriptor);

        int pkIndexId = 1;
        int sortedIndexId = 2;
        int hashIndexId = 3;

        schemaDescriptor = schemaDescriptorWith(CURRENT_SCHEMA_VERSION);
        schemaDescriptorVersion2 = schemaDescriptorWith(NEXT_SCHEMA_VERSION);

        ColumnsExtractor row2Tuple = BinaryRowConverter.keyExtractor(schemaDescriptor);

        pkStorageSupplier = new Lazy<>(() -> new TableSchemaAwareIndexStorage(
                pkIndexId,
                new TestHashIndexStorage(PART_ID, mock(StorageHashIndexDescriptor.class)),
                row2Tuple
        ));

        SortedIndexStorage indexStorage = new TestSortedIndexStorage(PART_ID, new StorageSortedIndexDescriptor(sortedIndexId, List.of(
                new StorageSortedIndexColumnDescriptor("intVal", NativeTypes.INT32, false, true)
        )));

        // 2 is the index of "intVal" in the list of all columns.
        ColumnsExtractor columnsExtractor = BinaryRowConverter.columnsExtractor(schemaDescriptor, 2);

        sortedIndexStorage = new TableSchemaAwareIndexStorage(sortedIndexId, indexStorage, columnsExtractor);

        hashIndexStorage = new TableSchemaAwareIndexStorage(
                hashIndexId,
                new TestHashIndexStorage(PART_ID, new StorageHashIndexDescriptor(hashIndexId, List.of(
                        new StorageHashIndexColumnDescriptor("intVal", NativeTypes.INT32, false)
                ))),
                columnsExtractor
        );

        IndexLocker pkLocker = new HashIndexLocker(pkIndexId, true, lockManager, row2Tuple);
        IndexLocker sortedIndexLocker = new SortedIndexLocker(sortedIndexId, PART_ID, lockManager, indexStorage, row2Tuple);
        IndexLocker hashIndexLocker = new HashIndexLocker(hashIndexId, false, lockManager, row2Tuple);

        IndexUpdateHandler indexUpdateHandler = new IndexUpdateHandler(
                DummyInternalTableImpl.createTableIndexStoragesSupplier(Map.of(pkStorage().id(), pkStorage()))
        );

        doAnswer(invocation -> {
            Function<TxStateMeta, TxStateMeta> updater = invocation.getArgument(1);
            txStateMeta = updater.apply(txStateMeta);
            return null;
        }).when(txManager).updateTxMeta(any(), any());

        doAnswer(invocation -> txStateMeta).when(txManager).stateMeta(any());

        doAnswer(invocation -> completedFuture(null)).when(txManager).executeCleanupAsync(any());

        doAnswer(invocation -> {
            var resp = new TxMessagesFactory().txStateResponse().txStateMeta(txStateMeta).build();
            return completedFuture(resp);
        }).when(messagingService).invoke(any(ClusterNode.class), any(), anyLong());

        transactionStateResolver = new TransactionStateResolver(
                mock(ReplicaService.class),
                txManager,
                clock,
                consistentId -> consistentId.equals(localNode.name()) ? localNode : anotherNode,
                id -> id.equals(localNode.id()) ? localNode : anotherNode,
                messagingService
        );

        transactionStateResolver.start();

        partitionReplicaListener = new PartitionReplicaListener(
                testMvPartitionStorage,
                mockRaftClient,
                txManager,
                lockManager,
                Runnable::run,
                PART_ID,
                TABLE_ID,
                () -> Map.of(pkLocker.id(), pkLocker, sortedIndexId, sortedIndexLocker, hashIndexId, hashIndexLocker),
                pkStorageSupplier,
                () -> Map.of(sortedIndexId, sortedIndexStorage, hashIndexId, hashIndexStorage),
                clock,
                safeTimeClock,
                txStateStorage,
                transactionStateResolver,
                new StorageUpdateHandler(
                        PART_ID,
                        partitionDataStorage,
                        gcConfig,
                        mock(LowWatermark.class),
                        indexUpdateHandler,
                        new GcUpdateHandler(partitionDataStorage, safeTimeClock, indexUpdateHandler)
                ),
                schemas,
                localNode,
                new TestMvTableStorage(TABLE_ID, DEFAULT_PARTITION_COUNT),
                mock(IndexBuilder.class),
                schemaSyncService,
                catalogService,
                new TestPlacementDriver(localNode.name())
        );

        kvMarshaller = marshallerFor(schemaDescriptor);
        kvMarshallerVersion2 = marshallerFor(schemaDescriptorVersion2);

        reset();
    }

    private static SchemaDescriptor schemaDescriptorWith(int ver) {
        return new SchemaDescriptor(ver, new Column[]{
                new Column("intKey".toUpperCase(Locale.ROOT), NativeTypes.INT32, false),
                new Column("strKey".toUpperCase(Locale.ROOT), NativeTypes.STRING, false),
        }, new Column[]{
                new Column("intVal".toUpperCase(Locale.ROOT), NativeTypes.INT32, false),
                new Column("strVal".toUpperCase(Locale.ROOT), NativeTypes.STRING, false),
        });
    }

    private static KvMarshaller<TestKey, TestValue> marshallerFor(SchemaDescriptor descriptor) {
        MarshallerFactory marshallerFactory = new ReflectionMarshallerFactory();

        return marshallerFactory.create(descriptor, TestKey.class, TestValue.class);
    }

    private TableSchemaAwareIndexStorage pkStorage() {
        return Objects.requireNonNull(pkStorageSupplier.get());
    }

    private void reset() {
        localLeader = true;
        txState = null;
        ((TestHashIndexStorage) pkStorage().storage()).clear();
        ((TestHashIndexStorage) hashIndexStorage.storage()).clear();
        ((TestSortedIndexStorage) sortedIndexStorage.storage()).clear();
        testMvPartitionStorage.clear();
        pendingRows.clear();
        //lockManager.locks(txId).forEachRemaining(lock -> lockManager.release(lock));
    }

    @Test
    public void testTxStateReplicaRequestEmptyState() throws Exception {
        CompletableFuture<?> fut = partitionReplicaListener.invoke(TX_MESSAGES_FACTORY.txStateCommitPartitionRequest()
                .groupId(grpId)
                .txId(newTxId())
                .build(), "senderId");

        LeaderOrTxState tuple = (LeaderOrTxState) fut.get(1, TimeUnit.SECONDS);

        assertNull(tuple.leaderName());
        assertNull(tuple.txMeta());
    }

    @Test
    public void testTxStateReplicaRequestCommitState() throws Exception {
        UUID txId = newTxId();

        txStateStorage.put(txId, new TxMeta(TxState.COMMITED, singletonList(grpId), clock.now()));

        HybridTimestamp readTimestamp = clock.now();

        CompletableFuture<?> fut = partitionReplicaListener.invoke(TX_MESSAGES_FACTORY.txStateCommitPartitionRequest()
                .groupId(grpId)
                .txId(txId)
                .build(), localNode.id());

        LeaderOrTxState tuple = (LeaderOrTxState) fut.get(1, TimeUnit.SECONDS);

        TransactionMeta txMeta = tuple.txMeta();
        assertNotNull(txMeta);
        assertEquals(TxState.COMMITED, txMeta.txState());
        assertNotNull(txMeta.commitTimestamp());
        assertTrue(readTimestamp.compareTo(txMeta.commitTimestamp()) > 0);
        assertNull(tuple.leaderName());
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-20365")
    public void testTxStateReplicaRequestMissLeaderMiss() throws Exception {
        localLeader = false;

        CompletableFuture<?> fut = partitionReplicaListener.invoke(TX_MESSAGES_FACTORY.txStateCommitPartitionRequest()
                .groupId(grpId)
                .txId(newTxId())
                .build(), localNode.id());

        LeaderOrTxState tuple = (LeaderOrTxState) fut.get(1, TimeUnit.SECONDS);

        assertNull(tuple.txMeta());
        assertNotNull(tuple.leaderName());
    }

    @Test
    public void testReadOnlySingleRowReplicaRequestEmptyResult() throws Exception {
        BinaryRow testBinaryKey = nextBinaryKey();

        CompletableFuture<?> fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readOnlySingleRowPkReplicaRequest()
                .groupId(grpId)
                .readTimestampLong(clock.nowLong())
                .primaryKey(testBinaryKey.tupleSlice())
                .requestType(RequestType.RO_GET)
                .build(), localNode.id());

        BinaryRow binaryRow = (BinaryRow) fut.get(1, TimeUnit.SECONDS);

        assertNull(binaryRow);
    }

    @Test
    public void testReadOnlySingleRowReplicaRequestCommittedResult() throws Exception {
        UUID txId = newTxId();
        BinaryRow testBinaryKey = nextBinaryKey();
        BinaryRow testBinaryRow = binaryRow(key(testBinaryKey), new TestValue(1, "v1"));
        var rowId = new RowId(PART_ID);

        pkStorage().put(testBinaryRow, rowId);
        testMvPartitionStorage.addWrite(rowId, testBinaryRow, txId, TABLE_ID, PART_ID);
        testMvPartitionStorage.commitWrite(rowId, clock.now());

        CompletableFuture<?> fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readOnlySingleRowPkReplicaRequest()
                .groupId(grpId)
                .readTimestampLong(clock.nowLong())
                .primaryKey(testBinaryKey.tupleSlice())
                .requestType(RequestType.RO_GET)
                .build(), localNode.id());

        BinaryRow binaryRow = (BinaryRow) fut.get(1, TimeUnit.SECONDS);

        assertNotNull(binaryRow);
    }

    @Test
    public void testReadOnlySingleRowReplicaRequestResolveWriteIntentCommitted() throws Exception {
        UUID txId = newTxId();
        BinaryRow testBinaryKey = nextBinaryKey();
        BinaryRow testBinaryRow = binaryRow(key(testBinaryKey), new TestValue(1, "v1"));
        var rowId = new RowId(PART_ID);
        txState = TxState.COMMITED;

        pkStorage().put(testBinaryRow, rowId);
        testMvPartitionStorage.addWrite(rowId, testBinaryRow, txId, TABLE_ID, PART_ID);
        txManager.updateTxMeta(txId, old -> new TxStateMeta(TxState.COMMITED, localNode.id(), clock.now()));

        CompletableFuture<?> fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readOnlySingleRowPkReplicaRequest()
                .groupId(grpId)
                .readTimestampLong(clock.nowLong())
                .primaryKey(testBinaryKey.tupleSlice())
                .requestType(RequestType.RO_GET)
                .build(), localNode.id());

        BinaryRow binaryRow = (BinaryRow) fut.get(1, TimeUnit.SECONDS);

        assertNotNull(binaryRow);
    }

    @Test
    public void testReadOnlySingleRowReplicaRequestResolveWriteIntentPending() throws Exception {
        UUID txId = newTxId();
        BinaryRow testBinaryKey = nextBinaryKey();
        BinaryRow testBinaryRow = binaryRow(key(testBinaryKey), new TestValue(1, "v1"));
        var rowId = new RowId(PART_ID);

        pkStorage().put(testBinaryRow, rowId);
        testMvPartitionStorage.addWrite(rowId, testBinaryRow, txId, TABLE_ID, PART_ID);
        txManager.updateTxMeta(txId, old -> new TxStateMeta(TxState.PENDING, localNode.id(), null));

        CompletableFuture<?> fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readOnlySingleRowPkReplicaRequest()
                .groupId(grpId)
                .readTimestampLong(clock.nowLong())
                .primaryKey(testBinaryKey.tupleSlice())
                .requestType(RequestType.RO_GET)
                .build(), localNode.id());

        BinaryRow binaryRow = (BinaryRow) fut.get(1, TimeUnit.SECONDS);

        assertNull(binaryRow);
    }

    @Test
    public void testReadOnlySingleRowReplicaRequestResolveWriteIntentAborted() throws Exception {
        UUID txId = newTxId();
        BinaryRow testBinaryKey = nextBinaryKey();
        BinaryRow testBinaryRow = binaryRow(key(testBinaryKey), new TestValue(1, "v1"));
        var rowId = new RowId(PART_ID);
        txState = TxState.ABORTED;

        pkStorage().put(testBinaryRow, rowId);
        testMvPartitionStorage.addWrite(rowId, testBinaryRow, txId, TABLE_ID, PART_ID);
        txManager.updateTxMeta(txId, old -> new TxStateMeta(TxState.ABORTED, localNode.id(), null));

        CompletableFuture<?> fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readOnlySingleRowPkReplicaRequest()
                .groupId(grpId)
                .readTimestampLong(clock.nowLong())
                .primaryKey(testBinaryKey.tupleSlice())
                .requestType(RequestType.RO_GET)
                .build(), localNode.id());

        BinaryRow binaryRow = (BinaryRow) fut.get(1, TimeUnit.SECONDS);

        assertNull(binaryRow);
    }

    @Test
    public void testWriteScanRetrieveBatchReplicaRequestWithSortedIndex() throws Exception {
        UUID txId = newTxId();
        int sortedIndexId = sortedIndexStorage.id();

        IntStream.range(0, 6).forEach(i -> {
            RowId rowId = new RowId(PART_ID);
            int indexedVal = i % 5; // Non-uniq index.
            TestValue testValue = new TestValue(indexedVal, "val" + i);

            BinaryTuple indexedValue = new BinaryTuple(1,
                    new BinaryTupleBuilder(1).appendInt(indexedVal).build());
            BinaryRow storeRow = binaryRow(key(nextBinaryKey()), testValue);

            testMvPartitionStorage.addWrite(rowId, storeRow, txId, TABLE_ID, PART_ID);
            sortedIndexStorage.storage().put(new IndexRowImpl(indexedValue, rowId));
            testMvPartitionStorage.commitWrite(rowId, clock.now());
        });

        UUID scanTxId = newTxId();

        // Request first batch
        CompletableFuture<?> fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readWriteScanRetrieveBatchReplicaRequest()
                .groupId(grpId)
                .transactionId(scanTxId)
                .timestampLong(clock.nowLong())
                .term(1L)
                .scanId(1L)
                .indexToUse(sortedIndexId)
                .batchSize(4)
                .build(), localNode.id());

        List<BinaryRow> rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS);

        assertNotNull(rows);
        assertEquals(4, rows.size());

        // Request second batch
        fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readWriteScanRetrieveBatchReplicaRequest()
                .groupId(grpId)
                .transactionId(scanTxId)
                .timestampLong(clock.nowLong())
                .term(1L)
                .scanId(1L)
                .indexToUse(sortedIndexId)
                .batchSize(4)
                .build(), localNode.id());

        rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS);

        assertNotNull(rows);
        assertEquals(2, rows.size());

        // Request bounded.
        fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readWriteScanRetrieveBatchReplicaRequest()
                .groupId(grpId)
                .transactionId(newTxId())
                .timestampLong(clock.nowLong())
                .term(1L)
                .scanId(2L)
                .indexToUse(sortedIndexId)
                .lowerBoundPrefix(toIndexBound(1))
                .upperBoundPrefix(toIndexBound(3))
                .flags(SortedIndexStorage.LESS_OR_EQUAL)
                .batchSize(5)
                .build(), localNode.id());

        rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS);

        assertNotNull(rows);
        assertEquals(2, rows.size());

        // Empty result.
        fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readWriteScanRetrieveBatchReplicaRequest()
                .groupId(grpId)
                .transactionId(newTxId())
                .timestampLong(clock.nowLong())
                .term(1L)
                .scanId(2L)
                .indexToUse(sortedIndexId)
                .lowerBoundPrefix(toIndexBound(5))
                .batchSize(5)
                .build(), localNode.id());

        rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS);

        assertNotNull(rows);
        assertEquals(0, rows.size());

        // Lookup.
        fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readWriteScanRetrieveBatchReplicaRequest()
                .groupId(grpId)
                .transactionId(newTxId())
                .timestampLong(clock.nowLong())
                .term(1L)
                .scanId(2L)
                .indexToUse(sortedIndexId)
                .exactKey(toIndexKey(0))
                .batchSize(5)
                .build(), localNode.id());

        rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS);

        assertNotNull(rows);
        assertEquals(2, rows.size());
    }

    @Test
    public void testReadOnlyScanRetrieveBatchReplicaRequestSortedIndex() throws Exception {
        UUID txId = newTxId();
        int sortedIndexId = sortedIndexStorage.id();

        IntStream.range(0, 6).forEach(i -> {
            RowId rowId = new RowId(PART_ID);
            int indexedVal = i % 5; // Non-uniq index.
            TestValue testValue = new TestValue(indexedVal, "val" + i);

            BinaryTuple indexedValue = new BinaryTuple(1,
                    new BinaryTupleBuilder(1).appendInt(indexedVal).build());
            BinaryRow storeRow = binaryRow(key(nextBinaryKey()), testValue);

            testMvPartitionStorage.addWrite(rowId, storeRow, txId, TABLE_ID, PART_ID);
            sortedIndexStorage.storage().put(new IndexRowImpl(indexedValue, rowId));
            testMvPartitionStorage.commitWrite(rowId, clock.now());
        });

        UUID scanTxId = newTxId();

        // Request first batch
        CompletableFuture<?> fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readOnlyScanRetrieveBatchReplicaRequest()
                .groupId(grpId)
                .transactionId(scanTxId)
                .readTimestampLong(clock.nowLong())
                .scanId(1L)
                .indexToUse(sortedIndexId)
                .batchSize(4)
                .build(), localNode.id());

        List<BinaryRow> rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS);

        assertNotNull(rows);
        assertEquals(4, rows.size());

        // Request second batch
        fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readOnlyScanRetrieveBatchReplicaRequest()
                .groupId(grpId)
                .transactionId(scanTxId)
                .readTimestampLong(clock.nowLong())
                .scanId(1L)
                .indexToUse(sortedIndexId)
                .batchSize(4)
                .build(), localNode.id());

        rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS);

        assertNotNull(rows);
        assertEquals(2, rows.size());

        // Request bounded.
        fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readOnlyScanRetrieveBatchReplicaRequest()
                .groupId(grpId)
                .transactionId(newTxId())
                .readTimestampLong(clock.nowLong())
                .scanId(2L)
                .indexToUse(sortedIndexId)
                .lowerBoundPrefix(toIndexBound(1))
                .upperBoundPrefix(toIndexBound(3))
                .flags(SortedIndexStorage.LESS_OR_EQUAL)
                .batchSize(5)
                .build(), localNode.id());

        rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS);

        assertNotNull(rows);
        assertEquals(2, rows.size());

        // Empty result.
        fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readOnlyScanRetrieveBatchReplicaRequest()
                .groupId(grpId)
                .transactionId(newTxId())
                .readTimestampLong(clock.nowLong())
                .scanId(2L)
                .indexToUse(sortedIndexId)
                .lowerBoundPrefix(toIndexBound(5))
                .batchSize(5)
                .build(), localNode.id());

        rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS);

        assertNotNull(rows);
        assertEquals(0, rows.size());

        // Lookup.
        fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readOnlyScanRetrieveBatchReplicaRequest()
                .groupId(grpId)
                .transactionId(newTxId())
                .readTimestampLong(clock.nowLong())
                .scanId(2L)
                .indexToUse(sortedIndexId)
                .exactKey(toIndexKey(0))
                .batchSize(5)
                .build(), localNode.id());

        rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS);

        assertNotNull(rows);
        assertEquals(2, rows.size());
    }

    @Test
    public void testReadOnlyScanRetrieveBatchReplicaRequstHashIndex() throws Exception {
        UUID txId = newTxId();
        int hashIndexId = hashIndexStorage.id();

        IntStream.range(0, 7).forEach(i -> {
            RowId rowId = new RowId(PART_ID);
            int indexedVal = i % 2; // Non-uniq index.
            TestValue testValue = new TestValue(indexedVal, "val" + i);

            BinaryTuple indexedValue = new BinaryTuple(1,
                    new BinaryTupleBuilder(1).appendInt(indexedVal).build());
            BinaryRow storeRow = binaryRow(key(nextBinaryKey()), testValue);

            testMvPartitionStorage.addWrite(rowId, storeRow, txId, TABLE_ID, PART_ID);
            hashIndexStorage.storage().put(new IndexRowImpl(indexedValue, rowId));
            testMvPartitionStorage.commitWrite(rowId, clock.now());
        });

        UUID scanTxId = newTxId();

        // Request first batch
        CompletableFuture<?> fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readOnlyScanRetrieveBatchReplicaRequest()
                .groupId(grpId)
                .transactionId(scanTxId)
                .readTimestampLong(clock.nowLong())
                .scanId(1L)
                .indexToUse(hashIndexId)
                .exactKey(toIndexKey(0))
                .batchSize(3)
                .build(), localNode.id());

        List<BinaryRow> rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS);

        assertNotNull(rows);
        assertEquals(3, rows.size());

        // Request second batch
        fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readOnlyScanRetrieveBatchReplicaRequest()
                .groupId(grpId)
                .transactionId(scanTxId)
                .readTimestampLong(clock.nowLong())
                .scanId(1L)
                .indexToUse(hashIndexId)
                .exactKey(toIndexKey(0))
                .batchSize(1)
                .build(), localNode.id());

        rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS);

        assertNotNull(rows);
        assertEquals(1, rows.size());

        // Empty result.
        fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readOnlyScanRetrieveBatchReplicaRequest()
                .groupId(grpId)
                .transactionId(newTxId())
                .readTimestampLong(clock.nowLong())
                .scanId(2L)
                .indexToUse(hashIndexId)
                .exactKey(toIndexKey(5))
                .batchSize(5)
                .build(), localNode.id());

        rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS);

        assertNotNull(rows);
        assertEquals(0, rows.size());

        // Lookup.
        fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readOnlyScanRetrieveBatchReplicaRequest()
                .groupId(grpId)
                .transactionId(newTxId())
                .readTimestampLong(clock.nowLong())
                .scanId(2L)
                .indexToUse(hashIndexId)
                .exactKey(toIndexKey(1))
                .batchSize(5)
                .build(), localNode.id());

        rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS);

        assertNotNull(rows);
        assertEquals(3, rows.size());
    }

    @Test
    public void testWriteIntentOnPrimaryReplicaInsertUpdateDelete() throws MarshallerException {
        UUID txId = newTxId();

        BinaryRow testRow = binaryRow(0);
        BinaryRow testRowPk = kvMarshaller.marshal(new TestKey(0, "k0"));

        assertThat(doSingleRowRequest(txId, testRow, RequestType.RW_INSERT), willCompleteSuccessfully());

        checkRowInMvStorage(testRow, true);

        BinaryRow br = binaryRow(new TestKey(0, "k0"), new TestValue(1, "v1"));

        assertThat(doSingleRowRequest(txId, br, RequestType.RW_UPSERT), willCompleteSuccessfully());

        checkRowInMvStorage(br, true);

        assertThat(doSingleRowPkRequest(txId, testRowPk, RequestType.RW_DELETE), willCompleteSuccessfully());

        checkNoRowInIndex(testRow);

        assertThat(doSingleRowRequest(txId, testRow, RequestType.RW_INSERT), willCompleteSuccessfully());

        checkRowInMvStorage(testRow, true);

        br = binaryRow(new TestKey(0, "k0"), new TestValue(1, "v2"));

        assertThat(doSingleRowRequest(txId, br, RequestType.RW_GET_AND_REPLACE), willCompleteSuccessfully());

        checkRowInMvStorage(br, true);

        br = binaryRow(new TestKey(0, "k0"), new TestValue(1, "v3"));

        assertThat(doSingleRowRequest(txId, br, RequestType.RW_GET_AND_UPSERT), willCompleteSuccessfully());

        checkRowInMvStorage(br, true);

        assertThat(doSingleRowPkRequest(txId, testRowPk, RequestType.RW_GET_AND_DELETE), willCompleteSuccessfully());

        checkNoRowInIndex(br);

        assertThat(doSingleRowRequest(txId, testRow, RequestType.RW_INSERT), willCompleteSuccessfully());

        checkRowInMvStorage(testRow, true);

        assertThat(doSingleRowRequest(txId, testRow, RequestType.RW_DELETE_EXACT), willCompleteSuccessfully());

        checkNoRowInIndex(testRow);

        cleanup(txId);
    }

    @Test
    public void testWriteIntentOnPrimaryReplicaMultiRowOps() throws MarshallerException {
        UUID txId = newTxId();
        BinaryRow row0 = binaryRow(0);
        BinaryRow row1 = binaryRow(1);
        Collection<BinaryRow> rows = asList(row0, row1);

        assertThat(doMultiRowRequest(txId, rows, RequestType.RW_INSERT_ALL), willCompleteSuccessfully());

        checkRowInMvStorage(row0, true);
        checkRowInMvStorage(row1, true);

        BinaryRow newRow0 = binaryRow(new TestKey(0, "k0"), new TestValue(2, "v2"));
        BinaryRow newRow1 = binaryRow(new TestKey(1, "k1"), new TestValue(3, "v3"));
        Collection<BinaryRow> newRows = asList(newRow0, newRow1);

        assertThat(doMultiRowRequest(txId, newRows, RequestType.RW_UPSERT_ALL), willCompleteSuccessfully());

        checkRowInMvStorage(row0, false);
        checkRowInMvStorage(row1, false);
        checkRowInMvStorage(newRow0, true);
        checkRowInMvStorage(newRow1, true);

        Collection<BinaryRow> newRowPks = List.of(
                kvMarshaller.marshal(new TestKey(0, "k0")),
                kvMarshaller.marshal(new TestKey(1, "k1"))
        );

        assertThat(doMultiRowPkRequest(txId, newRowPks, RequestType.RW_DELETE_ALL), willCompleteSuccessfully());

        checkNoRowInIndex(row0);
        checkNoRowInIndex(row1);
        checkNoRowInIndex(newRow0);
        checkNoRowInIndex(newRow1);

        assertThat(doMultiRowRequest(txId, rows, RequestType.RW_INSERT_ALL), willCompleteSuccessfully());

        checkRowInMvStorage(row0, true);
        checkRowInMvStorage(row1, true);

        assertThat(doMultiRowRequest(txId, rows, RequestType.RW_DELETE_EXACT_ALL), willCompleteSuccessfully());

        checkNoRowInIndex(row0);
        checkNoRowInIndex(row1);

        cleanup(txId);
    }

    private CompletableFuture<?> doSingleRowRequest(UUID txId, BinaryRow binaryRow, RequestType requestType) {
        return doSingleRowRequest(txId, binaryRow, requestType, false);
    }

    private CompletableFuture<?> doSingleRowRequest(UUID txId, BinaryRow binaryRow, RequestType requestType, boolean full) {
        return partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readWriteSingleRowReplicaRequest()
                    .groupId(grpId)
                    .transactionId(txId)
                    .requestType(requestType)
                    .binaryRowMessage(binaryRowMessage(binaryRow))
                    .term(1L)
                    .commitPartitionId(commitPartitionId())
                    .full(full)
                    .build(),
                localNode.id()
        );
    }

    private CompletableFuture<?> doSingleRowPkRequest(UUID txId, BinaryRow binaryRow, RequestType requestType) {
        return partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readWriteSingleRowPkReplicaRequest()
                        .groupId(grpId)
                        .transactionId(txId)
                        .requestType(requestType)
                        .primaryKey(binaryRow.tupleSlice())
                        .term(1L)
                        .commitPartitionId(commitPartitionId())
                        .build(),
                localNode.id()
        );
    }

    private TablePartitionIdMessage commitPartitionId() {
        return TABLE_MESSAGES_FACTORY.tablePartitionIdMessage()
                .partitionId(PART_ID)
                .tableId(TABLE_ID)
                .build();
    }

    private CompletableFuture<?> doMultiRowRequest(UUID txId, Collection<BinaryRow> binaryRows, RequestType requestType) {
        return doMultiRowRequest(txId, binaryRows, requestType, false);
    }

    private CompletableFuture<?> doMultiRowRequest(UUID txId, Collection<BinaryRow> binaryRows, RequestType requestType, boolean full) {
        return partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readWriteMultiRowReplicaRequest()
                    .groupId(grpId)
                    .transactionId(txId)
                    .requestType(requestType)
                    .binaryRowMessages(binaryRows.stream().map(PartitionReplicaListenerTest::binaryRowMessage).collect(toList()))
                    .term(1L)
                    .commitPartitionId(commitPartitionId())
                    .full(full)
                    .build(),
                localNode.id()
        );
    }

    private CompletableFuture<?> doMultiRowPkRequest(UUID txId, Collection<BinaryRow> binaryRows, RequestType requestType) {
        return doMultiRowPkRequest(txId, binaryRows, requestType, false);
    }

    private CompletableFuture<?> doMultiRowPkRequest(UUID txId, Collection<BinaryRow> binaryRows, RequestType requestType, boolean full) {
        return partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readWriteMultiRowPkReplicaRequest()
                        .groupId(grpId)
                        .transactionId(txId)
                        .requestType(requestType)
                        .primaryKeys(binaryRows.stream().map(BinaryRow::tupleSlice).collect(toList()))
                        .term(1L)
                        .commitPartitionId(commitPartitionId())
                        .full(full)
                        .build(),
                localNode.id()
        );
    }

    @Test
    public void testWriteIntentOnPrimaryReplicaSingleUpdate() {
        UUID txId = newTxId();
        AtomicInteger counter = new AtomicInteger();

        testWriteIntentOnPrimaryReplica(
                txId,
                () -> {
                    BinaryRow binaryRow = binaryRow(counter.getAndIncrement());

                    return TABLE_MESSAGES_FACTORY.readWriteSingleRowReplicaRequest()
                            .groupId(grpId)
                            .transactionId(txId)
                            .requestType(RequestType.RW_INSERT)
                            .binaryRowMessage(binaryRowMessage(binaryRow))
                            .term(1L)
                            .commitPartitionId(commitPartitionId())
                            .build();
                },
                () -> checkRowInMvStorage(binaryRow(0), true)
        );

        cleanup(txId);
    }

    @Test
    public void testWriteIntentOnPrimaryReplicaUpdateAll() {
        UUID txId = newTxId();
        AtomicInteger counter = new AtomicInteger();

        testWriteIntentOnPrimaryReplica(
                txId,
                () -> {
                    int cntr = counter.getAndIncrement();
                    BinaryRow binaryRow0 = binaryRow(cntr * 2);
                    BinaryRow binaryRow1 = binaryRow(cntr * 2 + 1);

                    return TABLE_MESSAGES_FACTORY.readWriteMultiRowReplicaRequest()
                            .groupId(grpId)
                            .transactionId(txId)
                            .requestType(RequestType.RW_UPSERT_ALL)
                            .binaryRowMessages(asList(binaryRowMessage(binaryRow0), binaryRowMessage(binaryRow1)))
                            .term(1L)
                            .commitPartitionId(commitPartitionId())
                            .build();
                },
                () -> checkRowInMvStorage(binaryRow(0), true)
        );

        cleanup(txId);
    }

    private void checkRowInMvStorage(BinaryRow binaryRow, boolean shouldBePresent) {
        Cursor<RowId> cursor = pkStorage().get(binaryRow);

        if (shouldBePresent) {
            boolean found = false;

            // There can be write intents for deletion.
            while (cursor.hasNext()) {
                RowId rowId = cursor.next();

                BinaryRow row = testMvPartitionStorage.read(rowId, HybridTimestamp.MAX_VALUE).binaryRow();

                if (equalToRow(binaryRow).matches(row)) {
                    found = true;
                }
            }

            assertTrue(found);
        } else {
            RowId rowId = cursor.next();

            BinaryRow row = testMvPartitionStorage.read(rowId, HybridTimestamp.MAX_VALUE).binaryRow();

            assertTrue(row == null || !row.equals(binaryRow));
        }
    }

    private void checkNoRowInIndex(BinaryRow binaryRow) {
        try (Cursor<RowId> cursor = pkStorage().get(binaryRow)) {
            assertFalse(cursor.hasNext());
        }
    }

    private void testWriteIntentOnPrimaryReplica(
            UUID txId,
            Supplier<ReadWriteReplicaRequest> updatingRequestSupplier,
            Runnable checkAfterFirstOperation
    ) {
        partitionReplicaListener.invoke(updatingRequestSupplier.get(), localNode.id());
        checkAfterFirstOperation.run();

        // Check that cleanup request processing awaits all write requests.
        CompletableFuture<?> writeFut = new CompletableFuture<>();

        raftClientFutureClosure = cmd -> writeFut;

        try {
            CompletableFuture<?> replicaWriteFut = partitionReplicaListener.invoke(updatingRequestSupplier.get(), localNode.id());

            assertFalse(replicaWriteFut.isDone());

            raftClientFutureClosure = defaultMockRaftFutureClosure;

            HybridTimestamp now = clock.now();

            // Imitation of tx commit.
            txStateStorage.put(txId, new TxMeta(TxState.COMMITED, new ArrayList<>(), now));
            txManager.updateTxMeta(txId, old -> new TxStateMeta(TxState.COMMITED, UUID.randomUUID().toString(), now));

            CompletableFuture<?> replicaCleanupFut = partitionReplicaListener.invoke(
                    TX_MESSAGES_FACTORY.txCleanupReplicaRequest()
                        .groupId(grpId)
                        .txId(txId)
                        .commit(true)
                        .commitTimestampLong(now.longValue())
                        .build(),
                    localNode.id()
            );

            assertFalse(replicaCleanupFut.isDone());

            writeFut.complete(null);

            assertThat(replicaCleanupFut, willSucceedFast());
        } finally {
            raftClientFutureClosure = defaultMockRaftFutureClosure;
        }

        // Check that one more write after cleanup is discarded.
        CompletableFuture<?> writeAfterCleanupFuture = partitionReplicaListener.invoke(updatingRequestSupplier.get(), localNode.id());
        assertThat(writeAfterCleanupFuture, willThrowFast(TransactionException.class));
    }

    /**
     * Puts several records into the storage, optionally leaving them as write intents, alternately deleting and upserting the same row
     * within the same RW transaction, then checking read correctness via read only request.
     *
     * @param insertFirst Whether to insert some values before RW transaction.
     * @param upsertAfterDelete Whether to insert value after delete in RW transaction, so that it would present as non-null write
     *         intent.
     * @param committed Whether to commit RW transaction before doing RO request.
     * @param multiple Whether to check multiple rows via getAll request.
     */
    @CartesianTest
    void testReadOnlyGetAfterRowRewrite(
            @Values(booleans = {false, true}) boolean insertFirst,
            @Values(booleans = {false, true}) boolean upsertAfterDelete,
            @Values(booleans = {false, true}) boolean committed,
            @Values(booleans = {false, true}) boolean multiple
    ) throws MarshallerException {
        BinaryRow br1 = binaryRow(1);

        BinaryRow br1Pk = kvMarshaller.marshal(new TestKey(1, "k" + 1));

        BinaryRow br2 = binaryRow(2);

        BinaryRow br2Pk = kvMarshaller.marshal(new TestKey(2, "k" + 2));

        if (insertFirst) {
            UUID tx0 = newTxId();
            upsert(tx0, br1);
            upsert(tx0, br2);
            cleanup(tx0);
        }

        txState = null;

        UUID tx1 = newTxId();
        delete(tx1, br1Pk);
        upsert(tx1, br1);

        while (true) {
            delete(tx1, br1Pk);

            if (upsertAfterDelete) {
                upsert(tx1, br1);
            }

            Cursor<RowId> cursor = pkStorage().get(br1);

            if (!insertFirst) {
                if (!upsertAfterDelete) {
                    assertFalse(cursor.hasNext());
                }

                // If there were no other entries in index, break after first iteration.
                break;
            } else {
                // This check is only for cases when new rows generation mess the index contents and some rows there have no value.
                // We try to reach the point when the first row in cursor have no value, to test that this row will be skipped by RO tx.
                // TODO https://issues.apache.org/jira/browse/IGNITE-18767 after this, the following check may be not needed.
                RowId rowId = cursor.next();

                BinaryRow row = testMvPartitionStorage.read(rowId, HybridTimestamp.MAX_VALUE).binaryRow();

                if (row == null) {
                    break;
                }
            }
        }

        if (committed) {
            cleanup(tx1);
        }

        if (multiple) {
            List<BinaryRow> allRowsPks = insertFirst ? List.of(br1Pk, br2Pk) : List.of(br1Pk);
            List<BinaryRow> allRows = insertFirst ? List.of(br1, br2) : List.of(br1);
            List<BinaryRow> allRowsButModified = insertFirst ? Arrays.asList(null, br2) : singletonList((BinaryRow) null);
            List<BinaryRow> expected = committed
                    ? (upsertAfterDelete ? allRows : allRowsButModified)
                    : (insertFirst ? allRows : singletonList((BinaryRow) null));
            List<BinaryRow> res = roGetAll(allRowsPks, clock.nowLong());

            assertEquals(allRows.size(), res.size());

            List<Matcher<? super BinaryRow>> matchers = expected.stream()
                    .map(row -> row == null ? nullValue(BinaryRow.class) : equalToRow(row))
                    .collect(toList());

            assertThat(res, contains(matchers));
        } else {
            BinaryRow res = roGet(br1Pk, clock.nowLong());
            BinaryRow expected = committed
                    ? (upsertAfterDelete ? br1 : null)
                    : (insertFirst ? br1 : null);

            assertThat(res, is(expected == null ? nullValue(BinaryRow.class) : equalToRow(expected)));
        }

        cleanup(tx1);
    }

    @Test
    public void abortsSuccessfully() {
        AtomicReference<Boolean> committed = interceptFinishTxCommand();

        CompletableFuture<?> future = beginAndAbortTx();

        assertThat(future, willSucceedFast());

        assertThat(committed.get(), is(false));
    }

    private CompletableFuture<?> beginAndAbortTx() {
        when(txManager.cleanup(any(), any(), any(), anyBoolean(), any())).thenReturn(completedFuture(null));

        HybridTimestamp beginTimestamp = clock.now();
        UUID txId = transactionIdFor(beginTimestamp);

        TxFinishReplicaRequest commitRequest = TX_MESSAGES_FACTORY.txFinishReplicaRequest()
                .groupId(grpId)
                .txId(txId)
                .groups(Map.of(localNode, List.of(new IgniteBiTuple<>(grpId, 1L))))
                .commit(false)
                .term(1L)
                .build();

        return partitionReplicaListener.invoke(commitRequest, localNode.id());
    }

    private static UUID transactionIdFor(HybridTimestamp beginTimestamp) {
        return TestTransactionIds.TRANSACTION_ID_GENERATOR.transactionIdFor(beginTimestamp);
    }

    @Test
    public void commitsOnSameSchemaSuccessfully() {
        when(schemas.tableSchemaVersionsBetween(anyInt(), any(), any(HybridTimestamp.class)))
                .thenReturn(List.of(
                        tableSchema(CURRENT_SCHEMA_VERSION, List.of(nullableColumn("col")))
                ));

        AtomicReference<Boolean> committed = interceptFinishTxCommand();

        CompletableFuture<?> future = beginAndCommitTx();

        assertThat(future, willSucceedFast());

        assertThat(committed.get(), is(true));
    }

    private static CatalogTableColumnDescriptor nullableColumn(String colName) {
        return new CatalogTableColumnDescriptor(colName, ColumnType.INT32, true, 0, 0, 0, DefaultValue.constant(null));
    }

    private static CatalogTableColumnDescriptor defaultedColumn(String colName, int defaultValue) {
        return new CatalogTableColumnDescriptor(colName, ColumnType.INT32, false, 0, 0, 0, DefaultValue.constant(defaultValue));
    }

    private static FullTableSchema tableSchema(int schemaVersion, List<CatalogTableColumnDescriptor> columns) {
        return new FullTableSchema(schemaVersion, 1, columns, List.of());
    }

    private AtomicReference<Boolean> interceptFinishTxCommand() {
        AtomicReference<Boolean> committed = new AtomicReference<>();

        raftClientFutureClosure = command -> {
            if (command instanceof FinishTxCommand) {
                committed.set(((FinishTxCommand) command).commit());
            }
            return defaultMockRaftFutureClosure.apply(command);
        };

        return committed;
    }

    private CompletableFuture<?> beginAndCommitTx() {
        when(txManager.cleanup(any(), any(), any(), anyBoolean(), any())).thenReturn(completedFuture(null));

        HybridTimestamp beginTimestamp = clock.now();
        UUID txId = transactionIdFor(beginTimestamp);

        HybridTimestamp commitTimestamp = clock.now();

        TxFinishReplicaRequest commitRequest = TX_MESSAGES_FACTORY.txFinishReplicaRequest()
                .groupId(grpId)
                .txId(txId)
                .groups(Map.of(localNode, List.of(new IgniteBiTuple<>(grpId, 1L))))
                .commit(true)
                .commitTimestampLong(hybridTimestampToLong(commitTimestamp))
                .term(1L)
                .build();

        return partitionReplicaListener.invoke(commitRequest, localNode.id());
    }

    @Test
    @Disabled("IGNITE-19229")
    public void commitsOnCompatibleSchemaChangeSuccessfully() {
        when(schemas.tableSchemaVersionsBetween(anyInt(), any(), any(HybridTimestamp.class)))
                .thenReturn(List.of(
                        tableSchema(CURRENT_SCHEMA_VERSION, List.of(nullableColumn("col1"))),
                        tableSchema(FUTURE_SCHEMA_VERSION, List.of(nullableColumn("col1"), nullableColumn("col2")))
                ));

        AtomicReference<Boolean> committed = interceptFinishTxCommand();

        CompletableFuture<?> future = beginAndCommitTx();

        assertThat(future, willSucceedFast());

        assertThat(committed.get(), is(true));
    }

    @Test
    public void abortsCommitOnIncompatibleSchema() {
        simulateForwardIncompatibleSchemaChange(CURRENT_SCHEMA_VERSION, FUTURE_SCHEMA_VERSION);

        AtomicReference<Boolean> committed = interceptFinishTxCommand();

        CompletableFuture<?> future = beginAndCommitTx();

        IncompatibleSchemaAbortException ex = assertWillThrowFast(future,
                IncompatibleSchemaAbortException.class);
        assertThat(ex.code(), is(Transactions.TX_COMMIT_ERR));
        assertThat(ex.getMessage(), containsString("Commit failed because schema 1 is not forward-compatible with 2"));

        assertThat(committed.get(), is(false));
    }

    private void simulateForwardIncompatibleSchemaChange(int fromSchemaVersion, int toSchemaVersion) {
        when(schemas.tableSchemaVersionsBetween(anyInt(), any(), any(HybridTimestamp.class)))
                .thenReturn(incompatibleSchemaVersions(fromSchemaVersion, toSchemaVersion));
    }

    private void simulateBackwardIncompatibleSchemaChange(int fromSchemaVersion, int toSchemaVersion) {
        when(schemas.tableSchemaVersionsBetween(anyInt(), any(), anyInt()))
                .thenReturn(incompatibleSchemaVersions(fromSchemaVersion, toSchemaVersion));
    }

    private static List<FullTableSchema> incompatibleSchemaVersions(int fromSchemaVersion, int toSchemaVersion) {
        return List.of(
                tableSchema(fromSchemaVersion, List.of(defaultedColumn("col", 4))),
                tableSchema(toSchemaVersion, List.of(defaultedColumn("col", 5)))
        );
    }

    @ParameterizedTest
    @MethodSource("singleRowRequestTypes")
    public void failsWhenReadingSingleRowFromFutureIncompatibleSchema(RequestType requestType) {
        switch (requestType) {
            case RW_GET:
            case RW_DELETE:
            case RW_GET_AND_DELETE:
                testFailsWhenReadingFromFutureIncompatibleSchema(
                        (targetTxId, key) -> doSingleRowPkRequest(targetTxId, marshalKeyOrKeyValue(requestType, key), requestType)
                );

                break;
            default:
                testFailsWhenReadingFromFutureIncompatibleSchema(
                        (targetTxId, key) -> doSingleRowRequest(targetTxId, marshalKeyOrKeyValue(requestType, key), requestType)
                );
        }

    }

    private BinaryRow marshalKeyOrKeyValue(RequestType requestType, TestKey key) {
        try {
            return RequestTypes.isKeyOnly(requestType) ? kvMarshaller.marshal(key) : kvMarshaller.marshal(key, someValue);
        } catch (MarshallerException e) {
            throw new AssertionError(e);
        }
    }

    private void testFailsWhenReadingFromFutureIncompatibleSchema(ListenerInvocation listenerInvocation) {
        UUID targetTxId = transactionIdFor(clock.now());

        TestKey key = simulateWriteWithSchemaVersionFromFuture();

        simulateBackwardIncompatibleSchemaChange(CURRENT_SCHEMA_VERSION, FUTURE_SCHEMA_VERSION);

        AtomicReference<Boolean> committed = interceptFinishTxCommand();

        CompletableFuture<?> future = listenerInvocation.invoke(targetTxId, key);

        assertFailureDueToBackwardIncompatibleSchemaChange(future, committed);
    }

    private static Stream<Arguments> singleRowRequestTypes() {
        return Arrays.stream(RequestType.values())
                .filter(RequestTypes::isSingleRowRw)
                .map(Arguments::of);
    }

    private TestKey simulateWriteWithSchemaVersionFromFuture() {
        UUID futureSchemaVersionTxId = transactionIdFor(clock.now());

        TestKey key = nextKey();
        BinaryRow futureSchemaVersionRow = binaryRow(key, new TestValue(2, "v2"), kvMarshallerVersion2);
        var rowId = new RowId(PART_ID);

        BinaryTuple indexedValue = new BinaryTuple(1,
                new BinaryTupleBuilder(1).appendInt(FUTURE_SCHEMA_ROW_INDEXED_VALUE).build()
        );

        pkStorage().put(futureSchemaVersionRow, rowId);
        testMvPartitionStorage.addWrite(rowId, futureSchemaVersionRow, futureSchemaVersionTxId, TABLE_ID, PART_ID);
        sortedIndexStorage.storage().put(new IndexRowImpl(indexedValue, rowId));
        testMvPartitionStorage.commitWrite(rowId, clock.now());

        return key;
    }

    private static void assertFailureDueToBackwardIncompatibleSchemaChange(
            CompletableFuture<?> future,
            AtomicReference<Boolean> committed
    ) {
        IncompatibleSchemaException ex = assertWillThrowFast(future,
                IncompatibleSchemaException.class);
        assertThat(ex.code(), is(Transactions.TX_INCOMPATIBLE_SCHEMA_ERR));
        assertThat(ex.getMessage(), containsString("Operation failed because schema 1 is not backward-compatible with 2"));

        // Tx should not be finished.
        assertThat(committed.get(), is(nullValue()));
    }

    @ParameterizedTest
    @MethodSource("multiRowsRequestTypes")
    public void failsWhenReadingMultiRowsFromFutureIncompatibleSchema(RequestType requestType) {
        if (requestType == RequestType.RW_GET_ALL || requestType == RequestType.RW_DELETE_ALL) {
            testFailsWhenReadingFromFutureIncompatibleSchema(
                    (targetTxId, key) -> doMultiRowPkRequest(targetTxId, List.of(marshalKeyOrKeyValue(requestType, key)), requestType)
            );
        } else {
            testFailsWhenReadingFromFutureIncompatibleSchema(
                    (targetTxId, key) -> doMultiRowRequest(targetTxId, List.of(marshalKeyOrKeyValue(requestType, key)), requestType)
            );
        }
    }

    private static Stream<Arguments> multiRowsRequestTypes() {
        return Arrays.stream(RequestType.values())
                .filter(RequestTypes::isMultipleRowsRw)
                .map(Arguments::of);
    }

    @Test
    public void failsWhenReplacingOnTupleWithIncompatibleSchemaFromFuture() {
        testFailsWhenReadingFromFutureIncompatibleSchema(
                (targetTxId, key) -> doReplaceRequest(
                        targetTxId,
                        binaryRow(key, new TestValue(1, "v1")),
                        binaryRow(key, new TestValue(3, "v3"))
                )
        );
    }

    private CompletableFuture<?> doReplaceRequest(UUID targetTxId, BinaryRow oldRow, BinaryRow newRow) {
        return doReplaceRequest(targetTxId, oldRow, newRow, false);
    }

    private CompletableFuture<?> doReplaceRequest(UUID targetTxId, BinaryRow oldRow, BinaryRow newRow, boolean full) {
        return partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readWriteSwapRowReplicaRequest()
                    .groupId(grpId)
                    .transactionId(targetTxId)
                    .requestType(RequestType.RW_REPLACE)
                    .oldBinaryRowMessage(binaryRowMessage(oldRow))
                    .binaryRowMessage(binaryRowMessage(newRow))
                    .term(1L)
                    .commitPartitionId(commitPartitionId())
                    .full(full)
                    .build(),
                localNode.id()
        );
    }

    @Test
    public void failsWhenScanByExactMatchReadsTupleWithIncompatibleSchemaFromFuture() {
        testFailsWhenReadingFromFutureIncompatibleSchema(
                (targetTxId, key) -> partitionReplicaListener.invoke(
                        TABLE_MESSAGES_FACTORY.readWriteScanRetrieveBatchReplicaRequest()
                            .groupId(grpId)
                            .transactionId(targetTxId)
                            .indexToUse(sortedIndexStorage.id())
                            .exactKey(toIndexKey(FUTURE_SCHEMA_ROW_INDEXED_VALUE))
                            .term(1L)
                            .scanId(1)
                            .batchSize(100)
                            .build(),
                        localNode.id()
                )
        );
    }

    @Test
    public void failsWhenScanByIndexReadsTupleWithIncompatibleSchemaFromFuture() {
        testFailsWhenReadingFromFutureIncompatibleSchema(
                (targetTxId, key) -> partitionReplicaListener.invoke(
                        TABLE_MESSAGES_FACTORY.readWriteScanRetrieveBatchReplicaRequest()
                            .groupId(grpId)
                            .transactionId(targetTxId)
                            .indexToUse(sortedIndexStorage.id())
                            .term(1L)
                            .scanId(1)
                            .batchSize(100)
                            .build(),
                        localNode.id()
                )
        );
    }

    @Test
    public void failsWhenFullScanReadsTupleWithIncompatibleSchemaFromFuture() {
        testFailsWhenReadingFromFutureIncompatibleSchema(
                (targetTxId, key) -> doRwFullScanRetrieveBatchRequest(targetTxId, false)
        );
    }

    private CompletableFuture<?> doRwFullScanRetrieveBatchRequest(UUID targetTxId, boolean full) {
        return partitionReplicaListener.invoke(
                TABLE_MESSAGES_FACTORY.readWriteScanRetrieveBatchReplicaRequest()
                        .groupId(grpId)
                        .transactionId(targetTxId)
                        .term(1L)
                        .scanId(1)
                        .batchSize(100)
                        .full(full)
                        .build(),
                localNode.id()
        );
    }

    @ParameterizedTest
    @MethodSource("singleRowWriteRequestTypes")
    public void singleRowWritesAreSuppliedWithRequiredCatalogVersion(RequestType requestType) {
        if (requestType == RequestType.RW_DELETE || requestType == RequestType.RW_GET_AND_DELETE) {
            testWritesAreSuppliedWithRequiredCatalogVersion(
                    requestType,
                    (targetTxId, key) -> doSingleRowPkRequest(targetTxId, marshalKeyOrKeyValue(requestType, key), requestType)
            );
        } else {
            testWritesAreSuppliedWithRequiredCatalogVersion(
                    requestType,
                    (targetTxId, key) -> doSingleRowRequest(targetTxId, marshalKeyOrKeyValue(requestType, key), requestType)
            );
        }
    }

    private static Stream<Arguments> singleRowWriteRequestTypes() {
        return Arrays.stream(RequestType.values())
                .filter(RequestTypes::isSingleRowWrite)
                .map(Arguments::of);
    }

    private void testWritesAreSuppliedWithRequiredCatalogVersion(RequestType requestType, ListenerInvocation listenerInvocation) {
        TestKey key = nextKey();

        if (RequestTypes.looksUpFirst(requestType)) {
            upsertInNewTxFor(key);

            // While handling the upsert, our mocks were touched, let's reset them to prevent false-positives during verification.
            Mockito.reset(schemaSyncService);
        }

        when(catalogService.activeCatalogVersion(anyLong())).thenReturn(42);

        UUID targetTxId = newTxId();

        CompletableFuture<?> future = listenerInvocation.invoke(targetTxId, key);

        assertThat(future, willCompleteSuccessfully());

        // Make sure metadata completeness is awaited for.
        InOrder inOrder = inOrder(schemaSyncService, catalogService);
        inOrder.verify(schemaSyncService).waitForMetadataCompleteness(timestampCaptor.capture());
        inOrder.verify(catalogService).activeCatalogVersion(timestampCaptor.getValue().longValue());

        // Make sure catalog required version is filled in the executed update command.
        verify(mockRaftClient, atLeast(1)).run(commandCaptor.capture());

        List<Command> commands = commandCaptor.getAllValues();
        Command updateCommand = commands.get(commands.size() - 1);

        assertThat(updateCommand, is(instanceOf(CatalogVersionAware.class)));
        CatalogVersionAware catalogVersionAware = (CatalogVersionAware) updateCommand;
        assertThat(catalogVersionAware.requiredCatalogVersion(), is(42));
    }

    private void upsertInNewTxFor(TestKey key) {
        UUID tx0 = newTxId();
        upsert(tx0, binaryRow(key, someValue));
        cleanup(tx0);
    }

    @Test
    public void replaceRequestIsSuppliedWithRequiredCatalogVersion() {
        testWritesAreSuppliedWithRequiredCatalogVersion(RequestType.RW_REPLACE, (targetTxId, key) -> {
            return doReplaceRequest(
                    targetTxId,
                    marshalKeyOrKeyValue(RequestType.RW_REPLACE, key),
                    marshalKeyOrKeyValue(RequestType.RW_REPLACE, key)
            );
        });
    }

    @ParameterizedTest
    @MethodSource("multiRowsWriteRequestTypes")
    public void multiRowWritesAreSuppliedWithRequiredCatalogVersion(RequestType requestType) {
        if (requestType == RequestType.RW_DELETE_ALL) {
            testWritesAreSuppliedWithRequiredCatalogVersion(
                    requestType,
                    (targetTxId, key) -> doMultiRowPkRequest(targetTxId, List.of(marshalKeyOrKeyValue(requestType, key)), requestType)
            );
        } else {
            testWritesAreSuppliedWithRequiredCatalogVersion(
                    requestType,
                    (targetTxId, key) -> doMultiRowRequest(targetTxId, List.of(marshalKeyOrKeyValue(requestType, key)), requestType)
            );
        }
    }

    private static Stream<Arguments> multiRowsWriteRequestTypes() {
        return Arrays.stream(RequestType.values())
                .filter(RequestTypes::isMultipleRowsWrite)
                .map(Arguments::of);
    }

    @CartesianTest
    @CartesianTest.MethodFactory("singleRowRwOperationTypesFactory")
    void singleRowRwOperationsFailIfTableAlteredAfterTxStart(
            RequestType requestType,
            boolean onExistingRow,
            boolean full
    ) {
        ListenerInvocation invocation = null;

        if (RequestTypes.isSingleRowRwPkOnly(requestType)) {
            invocation = (targetTxId, key) -> {
                return doSingleRowPkRequest(targetTxId, marshalKeyOrKeyValue(requestType, key), requestType);
            };
        } else if (RequestTypes.isSingleRowRwFullRow(requestType)) {
            invocation = (targetTxId, key) -> {
                return doSingleRowRequest(targetTxId, marshalKeyOrKeyValue(requestType, key), requestType);
            };
        } else {
            fail("Uncovered type: " + requestType);
        }

        testRwOperationsFailIfTableAlteredAfterTxStart(requestType, onExistingRow, invocation);
    }

    @SuppressWarnings("unused")
    private static ArgumentSets singleRowRwOperationTypesFactory() {
        return ArgumentSets.argumentsForFirstParameter(singleRowRwOperationTypes())
                .argumentsForNextParameter(false, true)
                .argumentsForNextParameter(false, true);
    }

    private static Stream<RequestType> singleRowRwOperationTypes() {
        return Arrays.stream(RequestType.values())
                .filter(RequestTypes::isSingleRowRw);
    }

    private void testRwOperationsFailIfTableAlteredAfterTxStart(
            RequestType requestType,
            boolean onExistingRow,
            ListenerInvocation listenerInvocation
    ) {
        TestKey key = nextKey();

        if (onExistingRow) {
            upsertInNewTxFor(key);
        }

        UUID txId = newTxId();
        HybridTimestamp txBeginTs = TransactionIds.beginTimestamp(txId);

        CatalogTableDescriptor tableVersion1 = mock(CatalogTableDescriptor.class);
        CatalogTableDescriptor tableVersion2 = mock(CatalogTableDescriptor.class);
        when(tableVersion1.tableVersion()).thenReturn(CURRENT_SCHEMA_VERSION);
        when(tableVersion2.tableVersion()).thenReturn(NEXT_SCHEMA_VERSION);

        when(catalogService.table(TABLE_ID, txBeginTs.longValue())).thenReturn(tableVersion1);
        when(catalogService.table(eq(TABLE_ID), gt(txBeginTs.longValue()))).thenReturn(tableVersion2);

        CompletableFuture<?> future = listenerInvocation.invoke(txId, key);

        boolean expectValidationFailure;
        if (RequestTypes.neverMisses(requestType)) {
            expectValidationFailure = true;
        } else {
            expectValidationFailure = onExistingRow == RequestTypes.writesIfKeyDoesNotExist(requestType);
        }

        if (expectValidationFailure) {
            IncompatibleSchemaException ex = assertWillThrowFast(future, IncompatibleSchemaException.class);
            assertThat(ex.code(), is(Transactions.TX_INCOMPATIBLE_SCHEMA_ERR));
            assertThat(
                    ex.getMessage(),
                    is("Table schema was updated after the transaction was started [table=1, startSchema=1, operationSchema=2]")
            );
        } else {
            assertThat(future, willCompleteSuccessfully());
        }
    }

    @CartesianTest
    @CartesianTest.MethodFactory("multiRowRwOperationTypesFactory")
    void multiRowRwOperationsFailIfTableAlteredAfterTxStart(
            RequestType requestType, boolean onExistingRow, boolean full
    ) {
        ListenerInvocation invocation = null;

        if (RequestTypes.isMultipleRowsRwPkOnly(requestType)) {
            invocation = (targetTxId, key) -> {
                return doMultiRowPkRequest(targetTxId, List.of(marshalKeyOrKeyValue(requestType, key)), requestType, full);
            };
        } else if (RequestTypes.isMultipleRowsRwFullRows(requestType)) {
            invocation = (targetTxId, key) -> {
                return doMultiRowRequest(targetTxId, List.of(marshalKeyOrKeyValue(requestType, key)), requestType, full);
            };
        } else {
            fail("Uncovered type: " + requestType);
        }

        testRwOperationsFailIfTableAlteredAfterTxStart(requestType, onExistingRow, invocation);
    }

    @SuppressWarnings("unused")
    private static ArgumentSets multiRowRwOperationTypesFactory() {
        return ArgumentSets.argumentsForFirstParameter(multiRowRwOperationTypes())
                .argumentsForNextParameter(false, true)
                .argumentsForNextParameter(false, true);
    }

    private static Stream<RequestType> multiRowRwOperationTypes() {
        return Arrays.stream(RequestType.values())
                .filter(RequestTypes::isMultipleRowsRw);
    }

    @CartesianTest
    void replaceRequestFailsIfTableAlteredAfterTxStart(
            @Values(booleans = {false, true}) boolean onExistingRow,
            @Values(booleans = {false, true}) boolean full
    ) {
        testRwOperationsFailIfTableAlteredAfterTxStart(RequestType.RW_REPLACE, onExistingRow, (targetTxId, key) -> {
            return doReplaceRequest(
                    targetTxId,
                    marshalKeyOrKeyValue(RequestType.RW_REPLACE, key),
                    marshalKeyOrKeyValue(RequestType.RW_REPLACE, key),
                    full
            );
        });
    }

    @CartesianTest
    void rwScanRequestFailsIfTableAlteredAfterTxStart(
            @Values(booleans = {false, true}) boolean onExistingRow,
            @Values(booleans = {false, true}) boolean full
    ) {
        testRwOperationsFailIfTableAlteredAfterTxStart(RequestType.RW_SCAN, onExistingRow, (targetTxId, key) -> {
            return doRwFullScanRetrieveBatchRequest(targetTxId, full);
        });
    }

    private UUID newTxId() {
        return transactionIdFor(clock.now());
    }

    private void upsert(UUID txId, BinaryRow row) {
        partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readWriteSingleRowReplicaRequest()
                    .groupId(grpId)
                    .requestType(RequestType.RW_UPSERT)
                    .transactionId(txId)
                    .binaryRowMessage(binaryRowMessage(row))
                    .term(1L)
                    .commitPartitionId(commitPartitionId())
                    .build(),
                localNode.id()
        ).join();
    }

    private void delete(UUID txId, BinaryRow row) {
        partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readWriteSingleRowPkReplicaRequest()
                    .groupId(grpId)
                    .requestType(RequestType.RW_DELETE)
                    .transactionId(txId)
                    .primaryKey(row.tupleSlice())
                    .term(1L)
                    .commitPartitionId(commitPartitionId())
                    .build(),
                localNode.id()
        ).join();
    }

    private BinaryRow roGet(BinaryRow row, long readTimestamp) {
        CompletableFuture<?> future = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readOnlySingleRowPkReplicaRequest()
                    .groupId(grpId)
                    .requestType(RequestType.RO_GET)
                    .readTimestampLong(readTimestamp)
                    .primaryKey(row.tupleSlice())
                    .build(),
                localNode.id()
        );

        return (BinaryRow) future.join();
    }

    private List<BinaryRow> roGetAll(Collection<BinaryRow> rows, long readTimestamp) {
        CompletableFuture<?> future = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readOnlyMultiRowPkReplicaRequest()
                    .groupId(grpId)
                    .requestType(RequestType.RO_GET_ALL)
                    .readTimestampLong(readTimestamp)
                    .primaryKeys(rows.stream().map(BinaryRow::tupleSlice).collect(toList()))
                    .build(),
                localNode.id()
        );

        return (List<BinaryRow>) future.join();
    }

    private void cleanup(UUID txId) {
        HybridTimestamp commitTs = clock.now();

        txManager.updateTxMeta(txId, old -> new TxStateMeta(TxState.COMMITED, UUID.randomUUID().toString(), commitTs));

        partitionReplicaListener.invoke(
                TX_MESSAGES_FACTORY.txCleanupReplicaRequest()
                    .groupId(grpId)
                    .txId(txId)
                    .commit(true)
                    .commitTimestampLong(commitTs.longValue())
                    .build(),
                localNode.id()
        ).join();

        txState = TxState.COMMITED;
    }

    private BinaryTupleMessage toIndexBound(int val) {
        ByteBuffer tuple = new BinaryTuplePrefixBuilder(1, 1).appendInt(val).build();

        return TABLE_MESSAGES_FACTORY.binaryTupleMessage()
                .tuple(tuple)
                .elementCount(1)
                .build();
    }

    private BinaryTupleMessage toIndexKey(int val) {
        ByteBuffer tuple = new BinaryTupleBuilder(1).appendInt(val).build();

        return TABLE_MESSAGES_FACTORY.binaryTupleMessage()
                .tuple(tuple)
                .elementCount(1)
                .build();
    }

    private BinaryRow nextBinaryKey() {
        try {
            return kvMarshaller.marshal(nextKey());
        } catch (MarshallerException e) {
            throw new IgniteException(e);
        }
    }

    private static TestKey nextKey() {
        return new TestKey(monotonicInt(), "key " + monotonicInt());
    }

    private static int monotonicInt() {
        return nextMonotonicInt.getAndIncrement();
    }

    protected BinaryRow binaryRow(int i) {
        return binaryRow(new TestKey(i, "k" + i), new TestValue(i, "v" + i));
    }

    private BinaryRow binaryRow(TestKey key, TestValue value) {
        return binaryRow(key, value, kvMarshaller);
    }

    private static BinaryRow binaryRow(TestKey key, TestValue value, KvMarshaller<TestKey, TestValue> marshaller) {
        try {
            return marshaller.marshal(key, value);
        } catch (MarshallerException e) {
            throw new AssertionError(e);
        }
    }

    private TestKey key(BinaryRow binaryRow) {
        try {
            return kvMarshaller.unmarshalKey(Row.wrapKeyOnlyBinaryRow(schemaDescriptor, binaryRow));
        } catch (MarshallerException e) {
            throw new AssertionError(e);
        }
    }

    private TestValue value(BinaryRow binaryRow) {
        try {
            return kvMarshaller.unmarshalValue(Row.wrapBinaryRow(schemaDescriptor, binaryRow));
        } catch (MarshallerException e) {
            throw new IgniteException(e);
        }
    }

    private static BinaryRowMessage binaryRowMessage(BinaryRow binaryRow) {
        return TABLE_MESSAGES_FACTORY.binaryRowMessage()
                .binaryTuple(binaryRow.tupleSlice())
                .schemaVersion(binaryRow.schemaVersion())
                .build();
    }

    /**
     * Test pojo key.
     */
    private static class TestKey {
        @IgniteToStringInclude
        public int intKey;

        @IgniteToStringInclude
        public String strKey;

        public TestKey() {
        }

        public TestKey(int intKey, String strKey) {
            this.intKey = intKey;
            this.strKey = strKey;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestKey testKey = (TestKey) o;
            return intKey == testKey.intKey && Objects.equals(strKey, testKey.strKey);
        }

        @Override
        public int hashCode() {
            return Objects.hash(intKey, strKey);
        }

        @Override
        public String toString() {
            return S.toString(TestKey.class, this);
        }
    }

    /**
     * Test pojo value.
     */
    private static class TestValue implements Comparable<TestValue> {
        @IgniteToStringInclude
        public Integer intVal;

        @IgniteToStringInclude
        public String strVal;

        public TestValue() {
        }

        public TestValue(Integer intVal, String strVal) {
            this.intVal = intVal;
            this.strVal = strVal;
        }

        @Override
        public int compareTo(TestValue o) {
            int cmp = Integer.compare(intVal, o.intVal);

            return cmp != 0 ? cmp : strVal.compareTo(o.strVal);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestValue testValue = (TestValue) o;
            return Objects.equals(intVal, testValue.intVal) && Objects.equals(strVal, testValue.strVal);
        }

        @Override
        public int hashCode() {
            return Objects.hash(intVal, strVal);
        }

        @Override
        public String toString() {
            return S.toString(TestValue.class, this);
        }
    }

    @FunctionalInterface
    private interface ListenerInvocation {
        CompletableFuture<?> invoke(UUID targetTxId, TestKey key);
    }
}
