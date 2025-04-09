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
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.enabledColocation;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RO_GET;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RO_GET_ALL;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_REPLACE;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_UPSERT;
import static org.apache.ignite.internal.replicator.message.ReplicaMessageUtils.toTablePartitionIdMessage;
import static org.apache.ignite.internal.testframework.asserts.CompletableFutureAssert.assertWillThrowFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.tx.TransactionIds.beginTimestamp;
import static org.apache.ignite.internal.tx.TxState.ABORTED;
import static org.apache.ignite.internal.tx.TxState.COMMITTED;
import static org.apache.ignite.internal.tx.TxState.FINISHING;
import static org.apache.ignite.internal.tx.TxState.checkTransitionCorrectness;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.AdditionalMatchers.gt;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.ignite.distributed.TestPartitionDataStorage;
import org.apache.ignite.distributed.replicator.action.RequestTypes;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.NoOpFailureManager;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.ClusterNodeResolver;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.SingleClusterNodeResolver;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.partition.replicator.ZonePartitionReplicaListener;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.command.FinishTxCommand;
import org.apache.ignite.internal.partition.replicator.network.command.UpdateAllCommand;
import org.apache.ignite.internal.partition.replicator.network.command.UpdateCommand;
import org.apache.ignite.internal.partition.replicator.network.command.WriteIntentSwitchCommand;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadOnlyDirectMultiRowReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadOnlyDirectSingleRowReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadOnlyMultiRowPkReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadOnlyScanRetrieveBatchReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadOnlySingleRowPkReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadWriteSingleRowReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.RequestType;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionDataStorage;
import org.apache.ignite.internal.partition.replicator.schema.ValidationSchemasSource;
import org.apache.ignite.internal.partition.replicator.schemacompat.IncompatibleSchemaVersionException;
import org.apache.ignite.internal.partition.replicator.schemacompat.InternalSchemaVersionMismatchException;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.TestPlacementDriver;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.service.LeaderWithTerm;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.ReplicaResult;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.replicator.message.TablePartitionIdMessage;
import org.apache.ignite.internal.schema.AlwaysSyncedSchemaSyncService;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowConverter;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.ColumnsExtractor;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaSyncService;
import org.apache.ignite.internal.schema.marshaller.KvMarshaller;
import org.apache.ignite.internal.schema.marshaller.MarshallerFactory;
import org.apache.ignite.internal.schema.marshaller.reflection.ReflectionMarshallerFactory;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.TestStorageUtils;
import org.apache.ignite.internal.storage.impl.TestMvPartitionStorage;
import org.apache.ignite.internal.storage.index.HashIndexStorage;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor.StorageHashIndexColumnDescriptor;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor.StorageSortedIndexColumnDescriptor;
import org.apache.ignite.internal.storage.index.impl.TestHashIndexStorage;
import org.apache.ignite.internal.storage.index.impl.TestSortedIndexStorage;
import org.apache.ignite.internal.table.distributed.HashIndexLocker;
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
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.TransactionMeta;
import org.apache.ignite.internal.tx.TransactionResult;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.UpdateCommandResult;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.RemotelyTriggeredResourceRegistry;
import org.apache.ignite.internal.tx.impl.TxMessageSender;
import org.apache.ignite.internal.tx.impl.WaitDieDeadlockPreventionPolicy;
import org.apache.ignite.internal.tx.message.TransactionMetaMessage;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.internal.tx.message.TxStateCoordinatorRequest;
import org.apache.ignite.internal.tx.message.TxStateResponse;
import org.apache.ignite.internal.tx.message.WriteIntentSwitchReplicaRequest;
import org.apache.ignite.internal.tx.storage.state.test.TestTxStatePartitionStorage;
import org.apache.ignite.internal.tx.test.TestTransactionIds;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.Lazy;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.lang.ErrorGroups.Transactions;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junitpioneer.jupiter.cartesian.ArgumentSets;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/** Tests for zone partition replica listener. */
@ExtendWith(MockitoExtension.class)
@ExtendWith(ConfigurationExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class ZonePartitionReplicaListenerTest extends IgniteAbstractTest {
    private static final int PART_ID = 0;

    private static final int CURRENT_SCHEMA_VERSION = 1;

    private static final int NEXT_SCHEMA_VERSION = 2;

    private static final int TABLE_ID = 1;

    private static final TablePartitionId commitPartitionId = new TablePartitionId(TABLE_ID, PART_ID);

    private static final long ANY_ENLISTMENT_CONSISTENCY_TOKEN = 1L;
    private static final String TABLE_NAME = "test";
    private static final String TABLE_NAME_2 = "second_test";

    private final Map<UUID, Set<RowId>> pendingRows = new ConcurrentHashMap<>();

    /** The storage stores partition data. */
    private final TestMvPartitionStorage testMvPartitionStorage = spy(new TestMvPartitionStorage(PART_ID));

    private final LockManager lockManager = lockManager();

    private final Function<Command, CompletableFuture<?>> defaultMockRaftFutureClosure = cmd -> {
        if (cmd instanceof WriteIntentSwitchCommand) {
            WriteIntentSwitchCommand switchCommand = (WriteIntentSwitchCommand) cmd;
            UUID txId = switchCommand.txId();

            Set<RowId> rows = pendingRows.remove(txId);

            HybridTimestamp commitTimestamp = switchCommand.commitTimestamp();
            if (switchCommand.commit()) {
                assertNotNull(commitTimestamp);
            }

            if (rows != null) {
                for (RowId row : rows) {
                    testMvPartitionStorage.commitWrite(row, commitTimestamp);
                }
            }

            lockManager.releaseAll(txId);
        } else if (cmd instanceof UpdateCommand) {
            UUID txId = ((UpdateCommand) cmd).txId();

            pendingRows.compute(txId, (txId0, v) -> {
                if (v == null) {
                    v = new HashSet<>();
                }

                RowId rowId = new RowId(PART_ID, ((UpdateCommand) cmd).rowUuid());
                v.add(rowId);

                return v;
            });

            return completedFuture(new UpdateCommandResult(true, true, 100));
        } else if (cmd instanceof UpdateAllCommand) {
            return completedFuture(new UpdateCommandResult(true, true, 100));
        } else if (cmd instanceof FinishTxCommand) {
            FinishTxCommand command = (FinishTxCommand) cmd;

            return completedFuture(new TransactionResult(command.commit() ? COMMITTED : ABORTED, command.commitTimestamp()));
        }

        return nullCompletedFuture();
    };

    /** Tx messages factory. */
    private static final TxMessagesFactory TX_MESSAGES_FACTORY = new TxMessagesFactory();

    /** Table messages factory. */
    private static final PartitionReplicationMessagesFactory TABLE_MESSAGES_FACTORY = new PartitionReplicationMessagesFactory();

    /** Replica messages factory. */
    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    /** Partition group id. */
    private final TablePartitionId grpId = new TablePartitionId(TABLE_ID, PART_ID);

    /** Hybrid clock. */
    private final HybridClock clock = new HybridClockImpl();

    private final ClockService clockService = new TestClockService(clock);

    /** The storage stores transaction states. */
    private final TestTxStatePartitionStorage txStateStorage = new TestTxStatePartitionStorage();

    /** Local cluster node. */
    private final ClusterNode localNode = new ClusterNodeImpl(nodeId(1), "node1", NetworkAddress.from("127.0.0.1:127"));

    /** Another (not local) cluster node. */
    private final ClusterNode anotherNode = new ClusterNodeImpl(nodeId(2), "node2", NetworkAddress.from("127.0.0.2:127"));

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
    private ValidationSchemasSource validationSchemasSource;

    @Spy
    private final SchemaSyncService schemaSyncService = new AlwaysSyncedSchemaSyncService();

    @Mock
    private CatalogService catalogService;

    @Mock
    private Catalog catalog;

    private final TestCatalogServiceEventProducer catalogServiceEventProducer = new TestCatalogServiceEventProducer();

    @Mock
    private MessagingService messagingService;

    @Mock
    private LowWatermark lowWatermark;

    @InjectConfiguration
    private ReplicationConfiguration replicationConfiguration;

    /** Schema descriptor for tests. */
    private SchemaDescriptor schemaDescriptor;

    /** Schema descriptor, version 2. */
    private SchemaDescriptor schemaDescriptorVersion2;

    /** Key-value marshaller for tests. */
    private KvMarshaller<TestKey, TestValue> kvMarshaller;

    private final CatalogTableDescriptor tableDescriptor = new CatalogTableDescriptor(
            TABLE_ID, 1, 2, TABLE_NAME, 1,
            List.of(
                    new CatalogTableColumnDescriptor("intKey", ColumnType.INT32, false, 0, 0, 0, null),
                    new CatalogTableColumnDescriptor("strKey", ColumnType.STRING, false, 0, 0, 0, null),
                    new CatalogTableColumnDescriptor("intVal", ColumnType.INT32, false, 0, 0, 0, null),
                    new CatalogTableColumnDescriptor("strVal", ColumnType.STRING, false, 0, 0, 0, null)
            ),
            List.of("intKey", "strKey"),
            null,
            DEFAULT_STORAGE_PROFILE
    );

    /** Placement driver. */
    private TestPlacementDriver placementDriver;

    private ZonePartitionReplicaListener zonePartitionReplicaListener;

    /** Partition replication listener to test. */
    private PartitionReplicaListener partitionReplicaListener;

    private HashIndexStorage pkIndexStorage;

    /** Primary index. */
    private Lazy<TableSchemaAwareIndexStorage> pkStorageSupplier;

    /** If true the local replica is considered leader, false otherwise. */
    private boolean localLeader;

    /** Secondary sorted index. */
    private TableSchemaAwareIndexStorage sortedIndexStorage;

    /** Secondary hash index. */
    private TableSchemaAwareIndexStorage hashIndexStorage;

    private Function<Command, CompletableFuture<?>> raftClientFutureClosure = defaultMockRaftFutureClosure;

    private static final AtomicInteger nextMonotonicInt = new AtomicInteger(1);

    private final TestValue someValue = new TestValue(1, "v1");

    @Mock
    private IndexMetaStorage indexMetaStorage;

    private static UUID nodeId(int id) {
        return new UUID(0, id);
    }

    @BeforeEach
    public void beforeTest() {
        doAnswer(invocation -> {
            catalogServiceEventProducer.listen(invocation.getArgument(0), invocation.getArgument(1));

            return null;
        }).when(catalogService).listen(any(), any());

        doAnswer(invocation -> {
            catalogServiceEventProducer.removeListener(invocation.getArgument(0), invocation.getArgument(1));

            return null;
        }).when(catalogService).removeListener(any(), any());

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

        when(safeTimeClock.waitFor(any())).thenReturn(nullCompletedFuture());
        when(safeTimeClock.current()).thenReturn(HybridTimestamp.MIN_VALUE);

        when(validationSchemasSource.waitForSchemaAvailability(anyInt(), anyInt())).thenReturn(nullCompletedFuture());

        lenient().when(catalogService.catalog(anyInt())).thenReturn(catalog);
        lenient().when(catalogService.activeCatalog(anyLong())).thenReturn(catalog);

        lenient().when(catalog.table(anyInt())).thenReturn(tableDescriptor);
        lenient().when(catalog.table(anyInt())).thenReturn(tableDescriptor);

        int pkIndexId = 1;
        int sortedIndexId = 2;
        int hashIndexId = 3;

        schemaDescriptor = schemaDescriptorWith(CURRENT_SCHEMA_VERSION);
        schemaDescriptorVersion2 = schemaDescriptorWith(NEXT_SCHEMA_VERSION);

        ColumnsExtractor row2Tuple = BinaryRowConverter.keyExtractor(schemaDescriptor);

        pkIndexStorage = spy(new TestHashIndexStorage(
                PART_ID,
                new StorageHashIndexDescriptor(pkIndexId, List.of(), false)
        ));
        pkStorageSupplier = new Lazy<>(() -> new TableSchemaAwareIndexStorage(pkIndexId, pkIndexStorage, row2Tuple));

        SortedIndexStorage indexStorage = new TestSortedIndexStorage(
                PART_ID,
                new StorageSortedIndexDescriptor(
                        sortedIndexId,
                        List.of(new StorageSortedIndexColumnDescriptor("intVal", NativeTypes.INT32, false, true)),
                        false
                )
        );

        // 2 is the index of "intVal" in the list of all columns.
        ColumnsExtractor columnsExtractor = BinaryRowConverter.columnsExtractor(schemaDescriptor, 2);

        sortedIndexStorage = new TableSchemaAwareIndexStorage(sortedIndexId, indexStorage, columnsExtractor);

        hashIndexStorage = new TableSchemaAwareIndexStorage(
                hashIndexId,
                new TestHashIndexStorage(
                        PART_ID,
                        new StorageHashIndexDescriptor(
                                hashIndexId,
                                List.of(new StorageHashIndexColumnDescriptor("intVal", NativeTypes.INT32, false)),
                                false
                        )
                ),
                columnsExtractor
        );

        completeBuiltIndexes(sortedIndexStorage.storage(), hashIndexStorage.storage());

        IndexLocker pkLocker = new HashIndexLocker(pkIndexId, true, lockManager, row2Tuple);
        IndexLocker sortedIndexLocker = new SortedIndexLocker(sortedIndexId, PART_ID, lockManager, indexStorage, row2Tuple, false);
        IndexLocker hashIndexLocker = new HashIndexLocker(hashIndexId, false, lockManager, row2Tuple);

        IndexUpdateHandler indexUpdateHandler = new IndexUpdateHandler(
                DummyInternalTableImpl.createTableIndexStoragesSupplier(Map.of(pkStorage().id(), pkStorage()))
        );

        CatalogIndexDescriptor indexDescriptor = mock(CatalogIndexDescriptor.class);
        when(indexDescriptor.id()).thenReturn(pkIndexId);

        when(catalog.indexes(anyInt())).thenReturn(List.of(indexDescriptor));

        configureTxManager(txManager);

        doAnswer(invocation -> {
            Object argument = invocation.getArgument(1);

            if (argument instanceof TxStateCoordinatorRequest) {
                TxStateCoordinatorRequest req = (TxStateCoordinatorRequest) argument;

                return completedFuture(toTxStateResponse(txManager.stateMeta(req.txId())));
            }

            return failedFuture(new Exception("Test exception"));
        }).when(messagingService).invoke(any(ClusterNode.class), any(), anyLong());

        doAnswer(invocation -> {
            Object argument = invocation.getArgument(1);

            if (argument instanceof TxStateCoordinatorRequest) {
                TxStateCoordinatorRequest req = (TxStateCoordinatorRequest) argument;

                return completedFuture(toTxStateResponse(txManager.stateMeta(req.txId())));
            }

            return failedFuture(new Exception("Test exception"));
        }).when(messagingService).invoke(anyString(), any(), anyLong());

        ClusterNodeResolver clusterNodeResolver = new ClusterNodeResolver() {
            @Override
            public ClusterNode getById(UUID id) {
                return id.equals(localNode.id()) ? localNode : anotherNode;
            }

            @Override
            public ClusterNode getByConsistentId(String consistentId) {
                return consistentId.equals(localNode.name()) ? localNode : anotherNode;
            }
        };

        transactionStateResolver = new TransactionStateResolver(
                txManager,
                clockService,
                clusterNodeResolver,
                messagingService,
                mock(PlacementDriver.class),
                new TxMessageSender(
                        messagingService,
                        mock(ReplicaService.class),
                        clockService
                )
        );

        transactionStateResolver.start();

        placementDriver = spy(new TestPlacementDriver(localNode));

        zonePartitionReplicaListener = new ZonePartitionReplicaListener(
                txStateStorage,
                clockService,
                txManager,
                validationSchemasSource,
                schemaSyncService,
                catalogService,
                placementDriver,
                clusterNodeResolver,
                mockRaftClient,
                new NoOpFailureManager(),
                localNode,
                new ZonePartitionId(tableDescriptor.zoneId(), PART_ID)
        );

        partitionReplicaListener = new PartitionReplicaListener(
                testMvPartitionStorage,
                mockRaftClient,
                txManager,
                lockManager,
                Runnable::run,
                enabledColocation() ? new ZonePartitionId(tableDescriptor.zoneId(), PART_ID) : new TablePartitionId(TABLE_ID, PART_ID),
                TABLE_ID,
                () -> Map.of(pkLocker.id(), pkLocker, sortedIndexId, sortedIndexLocker, hashIndexId, hashIndexLocker),
                pkStorageSupplier,
                () -> Map.of(sortedIndexId, sortedIndexStorage, hashIndexId, hashIndexStorage),
                clockService,
                safeTimeClock,
                txStateStorage,
                transactionStateResolver,
                new StorageUpdateHandler(
                        PART_ID,
                        partitionDataStorage,
                        indexUpdateHandler,
                        replicationConfiguration
                ),
                validationSchemasSource,
                localNode,
                schemaSyncService,
                catalogService,
                placementDriver,
                new SingleClusterNodeResolver(localNode),
                new RemotelyTriggeredResourceRegistry(),
                new DummySchemaManagerImpl(schemaDescriptor, schemaDescriptorVersion2),
                indexMetaStorage,
                lowWatermark,
                new NoOpFailureManager()
        );

        kvMarshaller = marshallerFor(schemaDescriptor);

        when(lowWatermark.tryLock(any(), any())).thenReturn(true);

        reset();
    }

    @AfterEach
    public void clearMocks() {
        Mockito.framework().clearInlineMocks();
    }

    private static LockManager lockManager() {
        HeapLockManager lockManager = HeapLockManager.smallInstance();
        lockManager.start(new WaitDieDeadlockPreventionPolicy());
        return lockManager;
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
        ((TestHashIndexStorage) pkStorage().storage()).clear();
        ((TestHashIndexStorage) hashIndexStorage.storage()).clear();
        ((TestSortedIndexStorage) sortedIndexStorage.storage()).clear();
        testMvPartitionStorage.clear();
        pendingRows.clear();

        completeBuiltIndexes(hashIndexStorage.storage(), sortedIndexStorage.storage());
    }

    private CompletableFuture<ReplicaResult> doReadOnlySingleGet(BinaryRow pk, HybridTimestamp readTimestamp) {
        ReadOnlySingleRowPkReplicaRequest request = readOnlySingleRowPkReplicaRequest(pk, readTimestamp);

        return invokeListener(request);
    }

    private CompletableFuture<ReplicaResult> invokeListener(ReplicaRequest request) {
        return zonePartitionReplicaListener.invoke(request, localNode.id());
    }

    private ReadOnlySingleRowPkReplicaRequest readOnlySingleRowPkReplicaRequest(BinaryRow pk, HybridTimestamp readTimestamp) {
        return readOnlySingleRowPkReplicaRequest(grpId, newTxId(), localNode.id(), pk, readTimestamp);
    }

    private static ReadOnlySingleRowPkReplicaRequest readOnlySingleRowPkReplicaRequest(
            TablePartitionId grpId,
            UUID txId,
            UUID coordinatorId,
            BinaryRow pk,
            HybridTimestamp readTimestamp
    ) {
        return TABLE_MESSAGES_FACTORY.readOnlySingleRowPkReplicaRequest()
                .groupId(tablePartitionIdMessage(grpId))
                .tableId(TABLE_ID)
                .readTimestamp(readTimestamp)
                .schemaVersion(pk.schemaVersion())
                .primaryKey(pk.tupleSlice())
                .transactionId(txId)
                .coordinatorId(coordinatorId)
                .requestType(RO_GET)
                .build();
    }

    private CompletableFuture<ReplicaResult> doReadOnlyDirectSingleGet(BinaryRow pk) {
        ReadOnlyDirectSingleRowReplicaRequest request = readOnlyDirectSingleRowReplicaRequest(grpId, pk);

        return invokeListener(request);
    }

    private static ReadOnlyDirectSingleRowReplicaRequest readOnlyDirectSingleRowReplicaRequest(TablePartitionId grpId, BinaryRow pk) {
        return TABLE_MESSAGES_FACTORY.readOnlyDirectSingleRowReplicaRequest()
                .groupId(tablePartitionIdMessage(grpId))
                .tableId(TABLE_ID)
                .schemaVersion(pk.schemaVersion())
                .primaryKey(pk.tupleSlice())
                .requestType(RO_GET)
                .enlistmentConsistencyToken(ANY_ENLISTMENT_CONSISTENCY_TOKEN)
                .build();
    }

    private static <K, V> Row marshalQuietly(K key, KvMarshaller<K, V> marshaller) {
        return marshaller.marshal(key);
    }

    private CompletableFuture<?> doSingleRowRequest(UUID txId, BinaryRow binaryRow, RequestType requestType, boolean full) {
        return zonePartitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readWriteSingleRowReplicaRequest()
                        .groupId(tablePartitionIdMessage(grpId))
                        .tableId(TABLE_ID)
                        .transactionId(txId)
                        .requestType(requestType)
                        .schemaVersion(binaryRow.schemaVersion())
                        .binaryTuple(binaryRow.tupleSlice())
                        .enlistmentConsistencyToken(ANY_ENLISTMENT_CONSISTENCY_TOKEN)
                        .commitPartitionId(commitPartitionId())
                        .coordinatorId(localNode.id())
                        .full(full)
                        .timestamp(clock.now())
                        .build(),
                localNode.id()
        );
    }

    private CompletableFuture<?> doSingleRowPkRequest(UUID txId, BinaryRow binaryRow, RequestType requestType, boolean full) {
        return zonePartitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readWriteSingleRowPkReplicaRequest()
                        .groupId(tablePartitionIdMessage(grpId))
                        .tableId(TABLE_ID)
                        .transactionId(txId)
                        .requestType(requestType)
                        .schemaVersion(binaryRow.schemaVersion())
                        .primaryKey(binaryRow.tupleSlice())
                        .enlistmentConsistencyToken(ANY_ENLISTMENT_CONSISTENCY_TOKEN)
                        .commitPartitionId(commitPartitionId())
                        .coordinatorId(localNode.id())
                        .full(full)
                        .timestamp(clock.now())
                        .build(),
                localNode.id()
        );
    }

    private static TablePartitionIdMessage commitPartitionId() {
        return REPLICA_MESSAGES_FACTORY.tablePartitionIdMessage()
                .partitionId(PART_ID)
                .tableId(TABLE_ID)
                .build();
    }

    private static TablePartitionIdMessage tablePartitionIdMessage(TablePartitionId tablePartitionId) {
        return toTablePartitionIdMessage(REPLICA_MESSAGES_FACTORY, tablePartitionId);
    }

    private CompletableFuture<?> doMultiRowRequest(UUID txId, Collection<BinaryRow> binaryRows, RequestType requestType, boolean full) {
        return zonePartitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readWriteMultiRowReplicaRequest()
                        .groupId(tablePartitionIdMessage(grpId))
                        .tableId(TABLE_ID)
                        .transactionId(txId)
                        .requestType(requestType)
                        .schemaVersion(binaryRows.iterator().next().schemaVersion())
                        .binaryTuples(binaryRowsToBuffers(binaryRows))
                        .enlistmentConsistencyToken(ANY_ENLISTMENT_CONSISTENCY_TOKEN)
                        .commitPartitionId(commitPartitionId())
                        .coordinatorId(localNode.id())
                        .full(full)
                        .timestamp(clock.now())
                        .build(),
                localNode.id()
        );
    }

    static List<ByteBuffer> binaryRowsToBuffers(Collection<BinaryRow> binaryRows) {
        return binaryRows.stream().map(BinaryRow::tupleSlice).collect(toList());
    }

    private CompletableFuture<?> doMultiRowPkRequest(UUID txId, Collection<BinaryRow> binaryRows, RequestType requestType, boolean full) {
        return zonePartitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readWriteMultiRowPkReplicaRequest()
                        .groupId(tablePartitionIdMessage(grpId))
                        .tableId(TABLE_ID)
                        .transactionId(txId)
                        .requestType(requestType)
                        .schemaVersion(binaryRows.iterator().next().schemaVersion())
                        .primaryKeys(binaryRowsToBuffers(binaryRows))
                        .enlistmentConsistencyToken(ANY_ENLISTMENT_CONSISTENCY_TOKEN)
                        .commitPartitionId(commitPartitionId())
                        .coordinatorId(localNode.id())
                        .full(full)
                        .timestamp(clock.now())
                        .build(),
                localNode.id()
        );
    }

    private static UUID transactionIdFor(HybridTimestamp beginTimestamp) {
        return TestTransactionIds.TRANSACTION_ID_GENERATOR.transactionIdFor(beginTimestamp);
    }

    private BinaryRow marshalKeyOrKeyValue(RequestType requestType, TestKey key) {
        return RequestTypes.isKeyOnly(requestType) ? marshalQuietly(key, kvMarshaller) : kvMarshaller.marshal(key, someValue);
    }

    private CompletableFuture<?> doReplaceRequest(UUID targetTxId, BinaryRow oldRow, BinaryRow newRow, boolean full) {
        return zonePartitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readWriteSwapRowReplicaRequest()
                        .groupId(tablePartitionIdMessage(grpId))
                        .tableId(TABLE_ID)
                        .transactionId(targetTxId)
                        .requestType(RW_REPLACE)
                        .schemaVersion(oldRow.schemaVersion())
                        .oldBinaryTuple(oldRow.tupleSlice())
                        .newBinaryTuple(newRow.tupleSlice())
                        .enlistmentConsistencyToken(ANY_ENLISTMENT_CONSISTENCY_TOKEN)
                        .commitPartitionId(commitPartitionId())
                        .coordinatorId(localNode.id())
                        .full(full)
                        .timestamp(clock.now())
                        .build(),
                localNode.id()
        );
    }

    private CompletableFuture<?> doRwScanRetrieveBatchRequest(UUID targetTxId) {
        return zonePartitionReplicaListener.invoke(
                TABLE_MESSAGES_FACTORY.readWriteScanRetrieveBatchReplicaRequest()
                        .groupId(tablePartitionIdMessage(grpId))
                        .tableId(TABLE_ID)
                        .transactionId(targetTxId)
                        .enlistmentConsistencyToken(ANY_ENLISTMENT_CONSISTENCY_TOKEN)
                        .scanId(1)
                        .batchSize(100)
                        .full(false)
                        .commitPartitionId(commitPartitionId())
                        .coordinatorId(localNode.id())
                        .timestamp(clock.now())
                        .build(),
                localNode.id()
        );
    }

    private CompletableFuture<?> doRoScanRetrieveBatchRequest(UUID targetTxId, HybridTimestamp readTimestamp) {
        return zonePartitionReplicaListener.invoke(
                readOnlyScanRetrieveBatchReplicaRequest(grpId, targetTxId, readTimestamp, localNode.id()),
                localNode.id()
        );
    }

    private static ReadOnlyScanRetrieveBatchReplicaRequest readOnlyScanRetrieveBatchReplicaRequest(
            TablePartitionId grpId,
            UUID txId,
            HybridTimestamp readTimestamp,
            UUID coordinatorId
    ) {
        return TABLE_MESSAGES_FACTORY.readOnlyScanRetrieveBatchReplicaRequest()
                .groupId(tablePartitionIdMessage(grpId))
                .tableId(TABLE_ID)
                .transactionId(txId)
                .scanId(1)
                .batchSize(100)
                .readTimestamp(readTimestamp)
                .coordinatorId(coordinatorId)
                .build();
    }

    private static void configureTxManager(TxManager txManager) {
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

        doAnswer(invocation -> nullCompletedFuture()).when(txManager).executeWriteIntentSwitchAsync(any(Runnable.class));

        doAnswer(invocation -> nullCompletedFuture())
                .when(txManager).finish(any(), any(), anyBoolean(), anyBoolean(), any(), any());
        doAnswer(invocation -> nullCompletedFuture())
                .when(txManager).cleanup(any(), anyString(), any());
    }

    private void upsertInNewTxFor(TestKey key) {
        UUID tx0 = newTxId();
        upsert(tx0, binaryRow(key, someValue));
        cleanup(tx0);
    }

    @SuppressWarnings("unused")
    private static ArgumentSets singleRowRwOperationTypesFactory() {
        return ArgumentSets.argumentsForFirstParameter(singleRowRwOperationTypes())
                .argumentsForNextParameter(false, true)
                .argumentsForNextParameter(false, true);
    }

    @SuppressWarnings("unused")
    private static ArgumentSets finishedTxTypesFactory() {
        return ArgumentSets.argumentsForFirstParameter(FINISHING, ABORTED, COMMITTED)
                .argumentsForNextParameter(singleRowRwOperationTypes());
    }

    private static Stream<RequestType> singleRowRwOperationTypes() {
        return Arrays.stream(RequestType.values())
                .filter(RequestTypes::isSingleRowRw);
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
    @CartesianTest.MethodFactory("singleRowRwOperationTypesFactory")
    void singleRowRwOperationsFailIfTableWasDropped(RequestType requestType, boolean onExistingRow, boolean full) {
        RwListenerInvocation invocation = null;

        if (RequestTypes.isSingleRowRwPkOnly(requestType)) {
            invocation = (targetTxId, key) -> doSingleRowPkRequest(targetTxId, marshalKeyOrKeyValue(requestType, key), requestType, full);
        } else if (RequestTypes.isSingleRowRwFullRow(requestType)) {
            invocation = (targetTxId, key) -> doSingleRowRequest(targetTxId, marshalKeyOrKeyValue(requestType, key), requestType, full);
        } else {
            fail("Uncovered type: " + requestType);
        }

        testRwOperationFailsIfTableWasDropped(onExistingRow, invocation);
    }

    private void testRwOperationFailsIfTableWasDropped(boolean onExistingRow, RwListenerInvocation listenerInvocation) {
        TestKey key = nextKey();

        if (onExistingRow) {
            upsertInNewTxFor(key);
        }

        UUID txId = newTxId();
        HybridTimestamp txBeginTs = beginTimestamp(txId);

        makeTableBeDroppedAfter(txBeginTs);

        CompletableFuture<?> future = listenerInvocation.invoke(txId, key);

        IncompatibleSchemaVersionException ex = assertWillThrowFast(future, IncompatibleSchemaVersionException.class);
        assertThat(ex.code(), is(Transactions.TX_INCOMPATIBLE_SCHEMA_ERR));
        assertThat(ex.getMessage(), is("Table was dropped [tableId=1]"));
    }

    private void makeTableBeDroppedAfter(HybridTimestamp txBeginTs) {
        makeTableBeDroppedAfter(txBeginTs, TABLE_ID);
    }

    private void makeTableBeDroppedAfter(HybridTimestamp txBeginTs, int tableId) {
        CatalogTableDescriptor tableVersion1 = mock(CatalogTableDescriptor.class);
        when(tableVersion1.tableVersion()).thenReturn(CURRENT_SCHEMA_VERSION);
        when(tableVersion1.name()).thenReturn(TABLE_NAME);

        when(catalog.table(tableId)).thenReturn(tableVersion1);

        when(catalogService.activeCatalog(txBeginTs.longValue())).thenReturn(catalog);
        when(catalogService.activeCatalog(gt(txBeginTs.longValue()))).thenReturn(mock(Catalog.class));
    }

    @CartesianTest
    @CartesianTest.MethodFactory("multiRowRwOperationTypesFactory")
    void multiRowRwOperationsFailIfTableWasDropped(RequestType requestType, boolean onExistingRow, boolean full) {
        RwListenerInvocation invocation = null;

        if (RequestTypes.isMultipleRowsRwPkOnly(requestType)) {
            invocation = (targetTxId, key)
                    -> doMultiRowPkRequest(targetTxId, List.of(marshalKeyOrKeyValue(requestType, key)), requestType, full);
        } else if (RequestTypes.isMultipleRowsRwFullRows(requestType)) {
            invocation = (targetTxId, key)
                    -> doMultiRowRequest(targetTxId, List.of(marshalKeyOrKeyValue(requestType, key)), requestType, full);
        } else {
            fail("Uncovered type: " + requestType);
        }

        testRwOperationFailsIfTableWasDropped(onExistingRow, invocation);
    }

    @CartesianTest
    void replaceRequestFailsIfTableWasDropped(
            @Values(booleans = {false, true}) boolean onExistingRow,
            @Values(booleans = {false, true}) boolean full
    ) {
        testRwOperationFailsIfTableWasDropped(onExistingRow, (targetTxId, key) -> {
            return doReplaceRequest(
                    targetTxId,
                    marshalKeyOrKeyValue(RW_REPLACE, key),
                    marshalKeyOrKeyValue(RW_REPLACE, key),
                    full
            );
        });
    }

    @CartesianTest
    void rwScanRequestFailsIfTableWasDropped(@Values(booleans = {false, true}) boolean onExistingRow) {
        testRwOperationFailsIfTableWasDropped(onExistingRow, (targetTxId, key) -> {
            return doRwScanRetrieveBatchRequest(targetTxId);
        });
    }

    @CartesianTest
    void singleRowRoGetFailsIfTableWasDropped(
            @Values(booleans = {false, true}) boolean direct,
            @Values(booleans = {false, true}) boolean onExistingRow
    ) {
        testRoOperationFailsIfTableWasDropped(onExistingRow, (targetTxId, readTimestamp, key) -> {
            if (direct) {
                return doReadOnlyDirectSingleGet(marshalQuietly(key, kvMarshaller));
            } else {
                return doReadOnlySingleGet(marshalQuietly(key, kvMarshaller), readTimestamp);
            }
        });
    }

    private void testRoOperationFailsIfTableWasDropped(boolean onExistingRow, RoListenerInvocation listenerInvocation) {
        TestKey key = nextKey();

        if (onExistingRow) {
            upsertInNewTxFor(key);
        }

        UUID txId = newTxId();
        HybridTimestamp readTs = clock.now();

        when(catalog.table(eq(TABLE_ID))).thenReturn(null);

        CompletableFuture<?> future = listenerInvocation.invoke(txId, readTs, key);

        IncompatibleSchemaVersionException ex = assertWillThrowFast(future, IncompatibleSchemaVersionException.class);
        assertThat(ex.code(), is(Transactions.TX_INCOMPATIBLE_SCHEMA_ERR));
        assertThat(ex.getMessage(), is("Table was dropped [tableId=1]"));
    }

    @CartesianTest
    void multiRowRoGetFailsIfTableWasDropped(
            @Values(booleans = {false, true}) boolean direct,
            @Values(booleans = {false, true}) boolean onExistingRow
    ) {
        testRoOperationFailsIfTableWasDropped(onExistingRow, (targetTxId, readTimestamp, key) -> {
            if (direct) {
                return doReadOnlyDirectMultiGet(List.of(marshalQuietly(key, kvMarshaller)));
            } else {
                return doReadOnlyMultiGet(List.of(marshalQuietly(key, kvMarshaller)), readTimestamp);
            }
        });
    }

    @CartesianTest
    void roScanRequestFailsIfTableWasDropped(@Values(booleans = {false, true}) boolean onExistingRow) {
        testRoOperationFailsIfTableWasDropped(onExistingRow, (targetTxId, readTimestamp, key) -> {
            return doRoScanRetrieveBatchRequest(targetTxId, readTimestamp);
        });
    }

    @CartesianTest
    @CartesianTest.MethodFactory("singleRowRwOperationTypesFactory")
    void singleRowRwOperationsFailIfSchemaVersionMismatchesTx(RequestType requestType, boolean onExistingRow, boolean full) {
        RwListenerInvocation invocation = null;

        if (RequestTypes.isSingleRowRwPkOnly(requestType)) {
            invocation = (targetTxId, key) -> doSingleRowPkRequest(targetTxId, marshalKeyOrKeyValue(requestType, key), requestType, full);
        } else if (RequestTypes.isSingleRowRwFullRow(requestType)) {
            invocation = (targetTxId, key) -> doSingleRowRequest(targetTxId, marshalKeyOrKeyValue(requestType, key), requestType, full);
        } else {
            fail("Uncovered type: " + requestType);
        }

        testRwOperationFailsIfSchemaVersionMismatchesTx(onExistingRow, invocation);
    }

    private void testRwOperationFailsIfSchemaVersionMismatchesTx(boolean onExistingRow, RwListenerInvocation listenerInvocation) {
        TestKey key = nextKey();

        if (onExistingRow) {
            upsertInNewTxFor(key);
        }

        UUID txId = newTxId();

        makeSchemaBeNextVersion();

        CompletableFuture<?> future = listenerInvocation.invoke(txId, key);

        assertThat(future, willThrow(InternalSchemaVersionMismatchException.class));
    }

    private void makeSchemaBeNextVersion() {
        CatalogTableDescriptor tableVersion2 = mock(CatalogTableDescriptor.class);
        when(tableVersion2.tableVersion()).thenReturn(NEXT_SCHEMA_VERSION);
        when(tableVersion2.name()).thenReturn(TABLE_NAME_2);

        when(catalog.table(eq(TABLE_ID))).thenReturn(tableVersion2);
    }

    @CartesianTest
    @CartesianTest.MethodFactory("multiRowRwOperationTypesFactory")
    void multiRowRwOperationsFailIfSchemaVersionMismatchesTx(RequestType requestType, boolean onExistingRow, boolean full) {
        RwListenerInvocation invocation = null;

        if (RequestTypes.isMultipleRowsRwPkOnly(requestType)) {
            invocation = (targetTxId, key)
                    -> doMultiRowPkRequest(targetTxId, List.of(marshalKeyOrKeyValue(requestType, key)), requestType, full);
        } else if (RequestTypes.isMultipleRowsRwFullRows(requestType)) {
            invocation = (targetTxId, key)
                    -> doMultiRowRequest(targetTxId, List.of(marshalKeyOrKeyValue(requestType, key)), requestType, full);
        } else {
            fail("Uncovered type: " + requestType);
        }

        testRwOperationFailsIfSchemaVersionMismatchesTx(onExistingRow, invocation);
    }

    @CartesianTest
    void replaceRequestFailsIfSchemaVersionMismatchesTx(
            @Values(booleans = {false, true}) boolean onExistingRow,
            @Values(booleans = {false, true}) boolean full
    ) {
        testRwOperationFailsIfSchemaVersionMismatchesTx(onExistingRow, (targetTxId, key) -> {
            return doReplaceRequest(
                    targetTxId,
                    marshalKeyOrKeyValue(RW_REPLACE, key),
                    marshalKeyOrKeyValue(RW_REPLACE, key),
                    full
            );
        });
    }

    @CartesianTest
    void singleRowRoGetFailsIfSchemaVersionMismatchesTx(
            @Values(booleans = {false, true}) boolean direct,
            @Values(booleans = {false, true}) boolean onExistingRow
    ) {
        testRoOperationFailsIfSchemaVersionMismatchesTx(onExistingRow, (targetTxId, readTimestamp, key) -> {
            if (direct) {
                return doReadOnlyDirectSingleGet(marshalQuietly(key, kvMarshaller));
            } else {
                return doReadOnlySingleGet(marshalQuietly(key, kvMarshaller), readTimestamp);
            }
        });
    }

    private void testRoOperationFailsIfSchemaVersionMismatchesTx(boolean onExistingRow, RoListenerInvocation listenerInvocation) {
        TestKey key = nextKey();

        if (onExistingRow) {
            upsertInNewTxFor(key);
        }

        UUID txId = newTxId();
        HybridTimestamp readTs = clock.now();

        makeSchemaBeNextVersion();

        CompletableFuture<?> future = listenerInvocation.invoke(txId, readTs, key);

        assertThat(future, willThrow(InternalSchemaVersionMismatchException.class));
    }

    @CartesianTest
    void multiRowRoGetFailsIfSchemaVersionMismatchesTx(
            @Values(booleans = {false, true}) boolean direct,
            @Values(booleans = {false, true}) boolean onExistingRow
    ) {
        testRoOperationFailsIfSchemaVersionMismatchesTx(onExistingRow, (targetTxId, readTimestamp, key) -> {
            if (direct) {
                return doReadOnlyDirectMultiGet(List.of(marshalQuietly(key, kvMarshaller)));
            } else {
                return doReadOnlyMultiGet(List.of(marshalQuietly(key, kvMarshaller)), readTimestamp);
            }
        });
    }

    private UUID newTxId() {
        return transactionIdFor(clock.now());
    }

    private void upsert(UUID txId, BinaryRow row) {
        assertThat(upsertAsync(txId, row), willCompleteSuccessfully());
    }

    private CompletableFuture<ReplicaResult> upsertAsync(UUID txId, BinaryRow row) {
        return upsertAsync(txId, row, false);
    }

    private CompletableFuture<ReplicaResult> upsertAsync(UUID txId, BinaryRow row, boolean full) {
        ReadWriteSingleRowReplicaRequest message = TABLE_MESSAGES_FACTORY.readWriteSingleRowReplicaRequest()
                .groupId(tablePartitionIdMessage(grpId))
                .tableId(TABLE_ID)
                .requestType(RW_UPSERT)
                .transactionId(txId)
                .schemaVersion(row.schemaVersion())
                .binaryTuple(row.tupleSlice())
                .enlistmentConsistencyToken(ANY_ENLISTMENT_CONSISTENCY_TOKEN)
                .commitPartitionId(commitPartitionId())
                .coordinatorId(localNode.id())
                .full(full)
                .timestamp(clock.now())
                .build();

        return partitionReplicaListener.invoke(message, localNode.id());
    }

    private CompletableFuture<ReplicaResult> doReadOnlyMultiGet(Collection<BinaryRow> rows, HybridTimestamp readTimestamp) {
        ReadOnlyMultiRowPkReplicaRequest request = readOnlyMultiRowPkReplicaRequest(grpId, newTxId(), localNode.id(), rows, readTimestamp);

        return invokeListener(request);
    }

    private static ReadOnlyMultiRowPkReplicaRequest readOnlyMultiRowPkReplicaRequest(
            TablePartitionId grpId,
            UUID txId,
            UUID coordinatorId,
            Collection<BinaryRow> rows,
            HybridTimestamp readTimestamp
    ) {
        return TABLE_MESSAGES_FACTORY.readOnlyMultiRowPkReplicaRequest()
                .groupId(tablePartitionIdMessage(grpId))
                .tableId(TABLE_ID)
                .requestType(RO_GET_ALL)
                .readTimestamp(readTimestamp)
                .schemaVersion(rows.iterator().next().schemaVersion())
                .primaryKeys(binaryRowsToBuffers(rows))
                .transactionId(txId)
                .coordinatorId(coordinatorId)
                .build();
    }

    private CompletableFuture<ReplicaResult> doReadOnlyDirectMultiGet(Collection<BinaryRow> rows) {
        ReadOnlyDirectMultiRowReplicaRequest request = readOnlyDirectMultiRowReplicaRequest(grpId, rows);

        return invokeListener(request);
    }

    private static ReadOnlyDirectMultiRowReplicaRequest readOnlyDirectMultiRowReplicaRequest(
            TablePartitionId grpId,
            Collection<BinaryRow> rows
    ) {
        return TABLE_MESSAGES_FACTORY.readOnlyDirectMultiRowReplicaRequest()
                .groupId(tablePartitionIdMessage(grpId))
                .tableId(TABLE_ID)
                .requestType(RO_GET_ALL)
                .schemaVersion(rows.iterator().next().schemaVersion())
                .primaryKeys(binaryRowsToBuffers(rows))
                .enlistmentConsistencyToken(ANY_ENLISTMENT_CONSISTENCY_TOKEN)
                .build();
    }

    private void cleanup(UUID txId) {
        cleanup(txId, true);
    }

    private void cleanup(UUID txId, boolean commit) {
        TxState newTxState = commit ? COMMITTED : ABORTED;

        HybridTimestamp commitTs = clock.now();
        HybridTimestamp commitTsOrNull = commit ? commitTs : null;

        txManager.updateTxMeta(txId, old -> new TxStateMeta(newTxState, UUID.randomUUID(), commitPartitionId, commitTsOrNull, null, null));

        if (enabledColocation()) {
            lockManager.releaseAll(txId);
            partitionReplicaListener.cleanupLocally(txId, commit, commitTs);
        } else {
            // Our mocks are tricky: when a WriteIntentSwitchCommand is handled by the raft client mock, it additionally releases all locks
            // of the corresponding transaction.

            WriteIntentSwitchReplicaRequest message = TX_MESSAGES_FACTORY.writeIntentSwitchReplicaRequest()
                    .groupId(tablePartitionIdMessage(grpId))
                    .tableIds(Set.of(grpId.tableId()))
                    .txId(txId)
                    .commit(commit)
                    .commitTimestamp(commitTs)
                    .build();

            assertThat(partitionReplicaListener.invoke(message, localNode.id()), willCompleteSuccessfully());
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
        return marshaller.marshal(key, value);
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
    private interface RwListenerInvocation {
        CompletableFuture<?> invoke(UUID targetTxId, TestKey key);
    }

    @FunctionalInterface
    private interface RoListenerInvocation {
        CompletableFuture<?> invoke(UUID targetTxId, HybridTimestamp readTimestamp, TestKey key);
    }

    private void completeBuiltIndexes(IndexStorage... indexStorages) {
        TestStorageUtils.completeBuiltIndexes(testMvPartitionStorage, indexStorages);
    }

    private static @Nullable TxStateResponse toTxStateResponse(@Nullable TransactionMeta transactionMeta) {
        TransactionMetaMessage transactionMetaMessage =
                transactionMeta == null ? null : transactionMeta.toTransactionMetaMessage(REPLICA_MESSAGES_FACTORY, TX_MESSAGES_FACTORY);

        return TX_MESSAGES_FACTORY.txStateResponse()
                .txStateMeta(transactionMetaMessage)
                .build();
    }
}
