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
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.INDEX_BUILDING;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestampToLong;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RO_GET;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RO_GET_ALL;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_DELETE;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_DELETE_ALL;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_DELETE_EXACT;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_DELETE_EXACT_ALL;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_GET_ALL;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_GET_AND_DELETE;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_GET_AND_REPLACE;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_GET_AND_UPSERT;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_INSERT;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_INSERT_ALL;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_REPLACE;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_SCAN;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_UPSERT;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_UPSERT_ALL;
import static org.apache.ignite.internal.schema.BinaryRowMatcher.equalToRow;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.asserts.CompletableFutureAssert.assertWillThrowFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.apache.ignite.internal.tx.TransactionIds.beginTimestamp;
import static org.apache.ignite.internal.tx.TxState.ABORTED;
import static org.apache.ignite.internal.tx.TxState.COMMITTED;
import static org.apache.ignite.internal.tx.TxState.FINISHING;
import static org.apache.ignite.internal.tx.TxState.checkTransitionCorrectness;
import static org.apache.ignite.internal.util.ArrayUtils.asList;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
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
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.events.StartBuildingIndexEventParameters;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.ClusterNodeResolver;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.SingleClusterNodeResolver;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.command.BuildIndexCommand;
import org.apache.ignite.internal.partition.replicator.network.command.CatalogVersionAware;
import org.apache.ignite.internal.partition.replicator.network.command.FinishTxCommand;
import org.apache.ignite.internal.partition.replicator.network.command.TablePartitionIdMessage;
import org.apache.ignite.internal.partition.replicator.network.command.UpdateAllCommand;
import org.apache.ignite.internal.partition.replicator.network.command.UpdateCommand;
import org.apache.ignite.internal.partition.replicator.network.command.UpdateCommandImpl;
import org.apache.ignite.internal.partition.replicator.network.command.WriteIntentSwitchCommand;
import org.apache.ignite.internal.partition.replicator.network.replication.BinaryRowMessage;
import org.apache.ignite.internal.partition.replicator.network.replication.BinaryTupleMessage;
import org.apache.ignite.internal.partition.replicator.network.replication.BuildIndexReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadOnlyDirectMultiRowReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadOnlyDirectSingleRowReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadOnlyMultiRowPkReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadOnlySingleRowPkReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadWriteReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadWriteSingleRowPkReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadWriteSingleRowReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.RequestType;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.TestPlacementDriver;
import org.apache.ignite.internal.placementdriver.TestReplicaMetaImpl;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.service.LeaderWithTerm;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.ReplicaResult;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.exception.PrimaryReplicaMissException;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowConverter;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.ColumnsExtractor;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.configuration.StorageUpdateConfiguration;
import org.apache.ignite.internal.schema.marshaller.KvMarshaller;
import org.apache.ignite.internal.schema.marshaller.MarshallerFactory;
import org.apache.ignite.internal.schema.marshaller.reflection.ReflectionMarshallerFactory;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.TestStorageUtils;
import org.apache.ignite.internal.storage.impl.TestMvPartitionStorage;
import org.apache.ignite.internal.storage.index.IndexRowImpl;
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
import org.apache.ignite.internal.table.distributed.index.IndexMeta;
import org.apache.ignite.internal.table.distributed.index.IndexMetaStorage;
import org.apache.ignite.internal.table.distributed.index.IndexUpdateHandler;
import org.apache.ignite.internal.table.distributed.index.MetaIndexStatus;
import org.apache.ignite.internal.table.distributed.index.MetaIndexStatusChange;
import org.apache.ignite.internal.table.distributed.raft.PartitionDataStorage;
import org.apache.ignite.internal.table.distributed.replicator.IncompatibleSchemaException;
import org.apache.ignite.internal.table.distributed.replicator.InternalSchemaVersionMismatchException;
import org.apache.ignite.internal.table.distributed.replicator.PartitionReplicaListener;
import org.apache.ignite.internal.table.distributed.replicator.StaleTransactionOperationException;
import org.apache.ignite.internal.table.distributed.replicator.TransactionStateResolver;
import org.apache.ignite.internal.table.distributed.schema.AlwaysSyncedSchemaSyncService;
import org.apache.ignite.internal.table.distributed.schema.FullTableSchema;
import org.apache.ignite.internal.table.distributed.schema.SchemaSyncService;
import org.apache.ignite.internal.table.distributed.schema.ValidationSchemasSource;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.internal.table.impl.DummySchemaManagerImpl;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.MismatchingTransactionOutcomeException;
import org.apache.ignite.internal.tx.TransactionMeta;
import org.apache.ignite.internal.tx.TransactionResult;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.UpdateCommandResult;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.RemotelyTriggeredResourceRegistry;
import org.apache.ignite.internal.tx.impl.TxMessageSender;
import org.apache.ignite.internal.tx.message.TxFinishReplicaRequest;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.internal.tx.message.TxStateCoordinatorRequest;
import org.apache.ignite.internal.tx.message.WriteIntentSwitchReplicaRequest;
import org.apache.ignite.internal.tx.storage.state.test.TestTxStateStorage;
import org.apache.ignite.internal.tx.test.TestTransactionIds;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.Lazy;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.lang.ErrorGroups.Transactions;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.tx.TransactionException;
import org.hamcrest.Matcher;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junitpioneer.jupiter.cartesian.ArgumentSets;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/** Tests for partition replica listener. */
@ExtendWith(MockitoExtension.class)
@ExtendWith(ConfigurationExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class PartitionReplicaListenerTest extends IgniteAbstractTest {
    private static final int PART_ID = 0;

    private static final int CURRENT_SCHEMA_VERSION = 1;

    private static final int NEXT_SCHEMA_VERSION = 2;

    private static final int FUTURE_SCHEMA_VERSION = NEXT_SCHEMA_VERSION;

    private static final int FUTURE_SCHEMA_ROW_INDEXED_VALUE = 0;

    private static final int TABLE_ID = 1;

    private static final TablePartitionId commitPartitionId = new TablePartitionId(TABLE_ID, PART_ID);

    private static final int ANOTHER_TABLE_ID = 2;

    private static final long ANY_ENLISTMENT_CONSISTENCY_TOKEN = 1L;
    private static final String TABLE_NAME = "test";
    private static final String TABLE_NAME_2 = "second_test";

    private final Map<UUID, Set<RowId>> pendingRows = new ConcurrentHashMap<>();

    /** The storage stores partition data. */
    private final TestMvPartitionStorage testMvPartitionStorage = new TestMvPartitionStorage(PART_ID);

    private final LockManager lockManager = new HeapLockManager();

    private final Function<Command, CompletableFuture<?>> defaultMockRaftFutureClosure = cmd -> {
        if (cmd instanceof WriteIntentSwitchCommand) {
            UUID txId = ((WriteIntentSwitchCommand) cmd).txId();

            Set<RowId> rows = pendingRows.remove(txId);

            HybridTimestamp commitTimestamp = ((WriteIntentSwitchCommand) cmd).commitTimestamp();
            assertNotNull(commitTimestamp);

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

            return completedFuture(new UpdateCommandResult(true));
        } else if (cmd instanceof UpdateAllCommand) {
            return completedFuture(new UpdateCommandResult(true));
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

    /** Partition group id. */
    private final TablePartitionId grpId = new TablePartitionId(TABLE_ID, PART_ID);

    /** Hybrid clock. */
    private final HybridClock clock = new HybridClockImpl();

    private final ClockService clockService = new TestClockService(clock);

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
    private ValidationSchemasSource validationSchemasSource;

    @Spy
    private final SchemaSyncService schemaSyncService = new AlwaysSyncedSchemaSyncService();

    @Mock
    private CatalogService catalogService;

    private final TestCatalogServiceEventProducer catalogServiceEventProducer = new TestCatalogServiceEventProducer();

    @Mock
    private MessagingService messagingService;

    @InjectConfiguration
    private StorageUpdateConfiguration storageUpdateConfiguration;

    @InjectConfiguration
    private TransactionConfiguration transactionConfiguration;

    /** Schema descriptor for tests. */
    private SchemaDescriptor schemaDescriptor;

    /** Schema descriptor, version 2. */
    private SchemaDescriptor schemaDescriptorVersion2;

    /** Key-value marshaller for tests. */
    private KvMarshaller<TestKey, TestValue> kvMarshaller;

    /** Key-value marshaller using schema version 2. */
    private KvMarshaller<TestKey, TestValue> kvMarshallerVersion2;

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
    private PlacementDriver placementDriver;

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

    private Function<Command, CompletableFuture<?>> raftClientFutureClosure = defaultMockRaftFutureClosure;

    private static final AtomicInteger nextMonotonicInt = new AtomicInteger(1);

    @Captor
    private ArgumentCaptor<Command> commandCaptor;

    private final TestValue someValue = new TestValue(1, "v1");

    @Mock
    private IndexMetaStorage indexMetaStorage;

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

        lenient().when(catalogService.table(anyInt(), anyLong())).thenReturn(tableDescriptor);
        lenient().when(catalogService.table(anyInt(), anyInt())).thenReturn(tableDescriptor);

        int pkIndexId = 1;
        int sortedIndexId = 2;
        int hashIndexId = 3;

        schemaDescriptor = schemaDescriptorWith(CURRENT_SCHEMA_VERSION);
        schemaDescriptorVersion2 = schemaDescriptorWith(NEXT_SCHEMA_VERSION);

        ColumnsExtractor row2Tuple = BinaryRowConverter.keyExtractor(schemaDescriptor);

        pkStorageSupplier = new Lazy<>(() -> new TableSchemaAwareIndexStorage(
                pkIndexId,
                new TestHashIndexStorage(
                        PART_ID,
                        new StorageHashIndexDescriptor(pkIndexId, List.of(), true)
                ),
                row2Tuple
        ));

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
        IndexLocker sortedIndexLocker = new SortedIndexLocker(sortedIndexId, PART_ID, lockManager, indexStorage, row2Tuple);
        IndexLocker hashIndexLocker = new HashIndexLocker(hashIndexId, false, lockManager, row2Tuple);

        IndexUpdateHandler indexUpdateHandler = new IndexUpdateHandler(
                DummyInternalTableImpl.createTableIndexStoragesSupplier(Map.of(pkStorage().id(), pkStorage()))
        );

        CatalogIndexDescriptor indexDescriptor = mock(CatalogIndexDescriptor.class);
        when(indexDescriptor.id()).thenReturn(pkIndexId);

        when(catalogService.indexes(anyInt(), anyInt())).thenReturn(List.of(indexDescriptor));

        configureTxManager(txManager);

        doAnswer(invocation -> {
            Object argument = invocation.getArgument(1);

            if (argument instanceof TxStateCoordinatorRequest) {
                TxStateCoordinatorRequest req = (TxStateCoordinatorRequest) argument;

                var resp = new TxMessagesFactory().txStateResponse().txStateMeta(txManager.stateMeta(req.txId())).build();

                return completedFuture(resp);
            }

            return CompletableFuture.failedFuture(new Exception("Test exception"));
        }).when(messagingService).invoke(any(ClusterNode.class), any(), anyLong());

        doAnswer(invocation -> {
            Object argument = invocation.getArgument(1);

            if (argument instanceof TxStateCoordinatorRequest) {
                TxStateCoordinatorRequest req = (TxStateCoordinatorRequest) argument;

                var resp = new TxMessagesFactory().txStateResponse().txStateMeta(txManager.stateMeta(req.txId())).build();

                return completedFuture(resp);
            }

            return CompletableFuture.failedFuture(new Exception("Test exception"));
        }).when(messagingService).invoke(anyString(), any(), anyLong());

        ClusterNodeResolver clusterNodeResolver = new ClusterNodeResolver() {
            @Override
            public ClusterNode getById(String id) {
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
                        clockService,
                        transactionConfiguration
                )
        );

        transactionStateResolver.start();

        placementDriver = new TestPlacementDriver(localNode);

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
                clockService,
                safeTimeClock,
                txStateStorage,
                transactionStateResolver,
                new StorageUpdateHandler(
                        PART_ID,
                        partitionDataStorage,
                        indexUpdateHandler,
                        storageUpdateConfiguration
                ),
                validationSchemasSource,
                localNode,
                schemaSyncService,
                catalogService,
                placementDriver,
                new SingleClusterNodeResolver(localNode),
                new RemotelyTriggeredResourceRegistry(),
                new DummySchemaManagerImpl(schemaDescriptor, schemaDescriptorVersion2),
                indexMetaStorage
        );

        kvMarshaller = marshallerFor(schemaDescriptor);
        kvMarshallerVersion2 = marshallerFor(schemaDescriptorVersion2);

        reset();
    }

    @AfterEach
    public void clearMocks() {
        Mockito.framework().clearInlineMocks();
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

        completeBuiltIndexes(hashIndexStorage.storage(), sortedIndexStorage.storage());
    }

    @Test
    public void testTxStateReplicaRequestEmptyState() throws Exception {
        doAnswer(invocation -> {
            UUID txId = invocation.getArgument(4);

            txManager.updateTxMeta(txId, old -> new TxStateMeta(ABORTED, localNode.id(), commitPartitionId, null));

            return nullCompletedFuture();
        }).when(txManager).finish(any(), any(), anyBoolean(), any(), any());

        CompletableFuture<ReplicaResult> fut = partitionReplicaListener.invoke(TX_MESSAGES_FACTORY.txStateCommitPartitionRequest()
                .groupId(grpId)
                .txId(newTxId())
                .enlistmentConsistencyToken(ANY_ENLISTMENT_CONSISTENCY_TOKEN)
                .build(), "senderId");

        TransactionMeta txMeta = (TransactionMeta) fut.get(1, TimeUnit.SECONDS).result();

        assertNotNull(txMeta);

        assertEquals(ABORTED, txMeta.txState());
    }

    @Test
    public void testTxStateReplicaRequestCommitState() throws Exception {
        UUID txId = newTxId();

        txStateStorage.putForRebalance(txId, new TxMeta(COMMITTED, singletonList(grpId), clock.now()));

        HybridTimestamp readTimestamp = clock.now();

        CompletableFuture<ReplicaResult> fut = partitionReplicaListener.invoke(TX_MESSAGES_FACTORY.txStateCommitPartitionRequest()
                .groupId(grpId)
                .txId(txId)
                .enlistmentConsistencyToken(ANY_ENLISTMENT_CONSISTENCY_TOKEN)
                .build(), localNode.id());

        TransactionMeta txMeta = (TransactionMeta) fut.get(1, TimeUnit.SECONDS).result();

        assertNotNull(txMeta);
        assertEquals(COMMITTED, txMeta.txState());
        assertNotNull(txMeta.commitTimestamp());
        assertTrue(readTimestamp.compareTo(txMeta.commitTimestamp()) > 0);
    }

    @CartesianTest
    @CartesianTest.MethodFactory("finishedTxTypesFactory")
    void testExecuteRequestOnFinishedTx(TxState txState, RequestType requestType) {
        UUID txId = newTxId();

        txStateStorage.putForRebalance(txId, new TxMeta(txState, singletonList(grpId), null));
        txManager.updateTxMeta(txId, old -> new TxStateMeta(txState, null, null, null));

        BinaryRow testRow = binaryRow(0);

        assertThat(
                doSingleRowRequest(txId, testRow, requestType),
                willThrowFast(TransactionException.class, "Transaction is already finished")
        );
    }

    @Test
    public void testEnsureReplicaIsPrimaryThrowsPrimaryReplicaMissIfEnlistmentConsistencyTokenDoesNotMatchTheOneInLease() {
        localLeader = false;

        CompletableFuture<ReplicaResult> fut = partitionReplicaListener.invoke(TX_MESSAGES_FACTORY.txStateCommitPartitionRequest()
                .groupId(grpId)
                .txId(newTxId())
                .enlistmentConsistencyToken(10L)
                .build(), localNode.id());

        assertThrowsWithCause(
                () -> fut.get(1, TimeUnit.SECONDS).result(),
                PrimaryReplicaMissException.class);
    }

    @Test
    public void testEnsureReplicaIsPrimaryThrowsPrimaryReplicaMissIfNodeIdDoesNotMatchTheLeaseholder() {
        localLeader = false;

        ((TestPlacementDriver) placementDriver).setPrimaryReplicaSupplier(() -> new TestReplicaMetaImpl("node3", "node3"));

        CompletableFuture<ReplicaResult> fut = partitionReplicaListener.invoke(TX_MESSAGES_FACTORY.txStateCommitPartitionRequest()
                .groupId(grpId)
                .txId(newTxId())
                .enlistmentConsistencyToken(ANY_ENLISTMENT_CONSISTENCY_TOKEN)
                .build(), localNode.id());

        assertThrowsWithCause(
                () -> fut.get(1, TimeUnit.SECONDS).result(),
                PrimaryReplicaMissException.class);
    }

    @Test
    public void testReadOnlySingleRowReplicaRequestEmptyResult() throws Exception {
        BinaryRow testBinaryKey = nextBinaryKey();

        CompletableFuture<ReplicaResult> fut = doReadOnlySingleGet(testBinaryKey);

        BinaryRow binaryRow = (BinaryRow) fut.get(1, TimeUnit.SECONDS).result();

        assertNull(binaryRow);
    }

    private CompletableFuture<ReplicaResult> doReadOnlySingleGet(BinaryRow pk) {
        return doReadOnlySingleGet(pk, clock.now());
    }

    private CompletableFuture<ReplicaResult> doReadOnlySingleGet(BinaryRow pk, HybridTimestamp readTimestamp) {
        ReadOnlySingleRowPkReplicaRequest request = TABLE_MESSAGES_FACTORY.readOnlySingleRowPkReplicaRequest()
                .groupId(grpId)
                .tableId(TABLE_ID)
                .readTimestampLong(readTimestamp.longValue())
                .schemaVersion(pk.schemaVersion())
                .primaryKey(pk.tupleSlice())
                .requestTypeInt(RO_GET.ordinal())
                .build();

        return partitionReplicaListener.invoke(request, localNode.id());
    }

    private CompletableFuture<ReplicaResult> doReadOnlyDirectSingleGet(BinaryRow pk) {
        ReadOnlyDirectSingleRowReplicaRequest request = TABLE_MESSAGES_FACTORY.readOnlyDirectSingleRowReplicaRequest()
                .groupId(grpId)
                .tableId(TABLE_ID)
                .schemaVersion(pk.schemaVersion())
                .primaryKey(pk.tupleSlice())
                .requestTypeInt(RO_GET.ordinal())
                .enlistmentConsistencyToken(ANY_ENLISTMENT_CONSISTENCY_TOKEN)
                .build();

        return partitionReplicaListener.invoke(request, localNode.id());
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

        CompletableFuture<ReplicaResult> fut = doReadOnlySingleGet(testBinaryKey);

        BinaryRow binaryRow = (BinaryRow) fut.get(1, TimeUnit.SECONDS).result();

        assertNotNull(binaryRow);
    }

    @Test
    public void testReadOnlySingleRowReplicaRequestResolveWriteIntentCommitted() throws Exception {
        UUID txId = newTxId();
        BinaryRow testBinaryKey = nextBinaryKey();
        BinaryRow testBinaryRow = binaryRow(key(testBinaryKey), new TestValue(1, "v1"));
        var rowId = new RowId(PART_ID);
        txState = COMMITTED;

        pkStorage().put(testBinaryRow, rowId);
        testMvPartitionStorage.addWrite(rowId, testBinaryRow, txId, TABLE_ID, PART_ID);
        txManager.updateTxMeta(txId, old -> new TxStateMeta(COMMITTED, localNode.id(), commitPartitionId, clock.now()));

        CompletableFuture<ReplicaResult> fut = doReadOnlySingleGet(testBinaryKey);

        BinaryRow binaryRow = (BinaryRow) fut.get(1, TimeUnit.SECONDS).result();

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
        txManager.updateTxMeta(txId, old -> new TxStateMeta(TxState.PENDING, localNode.id(), commitPartitionId, null));

        CompletableFuture<ReplicaResult> fut = doReadOnlySingleGet(testBinaryKey);

        BinaryRow binaryRow = (BinaryRow) fut.get(1, TimeUnit.SECONDS).result();

        assertNull(binaryRow);
    }

    @Test
    public void testReadOnlySingleRowReplicaRequestResolveWriteIntentAborted() throws Exception {
        UUID txId = newTxId();
        BinaryRow testBinaryKey = nextBinaryKey();
        BinaryRow testBinaryRow = binaryRow(key(testBinaryKey), new TestValue(1, "v1"));
        var rowId = new RowId(PART_ID);
        txState = ABORTED;

        pkStorage().put(testBinaryRow, rowId);
        testMvPartitionStorage.addWrite(rowId, testBinaryRow, txId, TABLE_ID, PART_ID);
        txManager.updateTxMeta(txId, old -> new TxStateMeta(ABORTED, localNode.id(), commitPartitionId, null));

        CompletableFuture<ReplicaResult> fut = doReadOnlySingleGet(testBinaryKey);

        BinaryRow binaryRow = (BinaryRow) fut.get(1, TimeUnit.SECONDS).result();

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
        CompletableFuture<ReplicaResult> fut = partitionReplicaListener.invoke(
                TABLE_MESSAGES_FACTORY.readWriteScanRetrieveBatchReplicaRequest()
                        .groupId(grpId)
                        .tableId(TABLE_ID)
                        .transactionId(scanTxId)
                        .timestampLong(clock.nowLong())
                        .enlistmentConsistencyToken(ANY_ENLISTMENT_CONSISTENCY_TOKEN)
                        .scanId(1L)
                        .indexToUse(sortedIndexId)
                        .batchSize(4)
                        .commitPartitionId(commitPartitionId())
                        .coordinatorId(localNode.id())
                        .build(), localNode.id());

        List<BinaryRow> rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS).result();

        assertNotNull(rows);
        assertEquals(4, rows.size());

        // Request second batch
        fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readWriteScanRetrieveBatchReplicaRequest()
                .groupId(grpId)
                .tableId(TABLE_ID)
                .transactionId(scanTxId)
                .timestampLong(clock.nowLong())
                .enlistmentConsistencyToken(ANY_ENLISTMENT_CONSISTENCY_TOKEN)
                .scanId(1L)
                .indexToUse(sortedIndexId)
                .batchSize(4)
                .commitPartitionId(commitPartitionId())
                .coordinatorId(localNode.id())
                .build(), localNode.id());

        rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS).result();

        assertNotNull(rows);
        assertEquals(2, rows.size());

        // Request bounded.
        fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readWriteScanRetrieveBatchReplicaRequest()
                .groupId(grpId)
                .tableId(TABLE_ID)
                .transactionId(newTxId())
                .timestampLong(clock.nowLong())
                .enlistmentConsistencyToken(ANY_ENLISTMENT_CONSISTENCY_TOKEN)
                .scanId(2L)
                .indexToUse(sortedIndexId)
                .lowerBoundPrefix(toIndexBound(1))
                .upperBoundPrefix(toIndexBound(3))
                .flags(SortedIndexStorage.LESS_OR_EQUAL)
                .batchSize(5)
                .commitPartitionId(commitPartitionId())
                .coordinatorId(localNode.id())
                .build(), localNode.id());

        rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS).result();

        assertNotNull(rows);
        assertEquals(2, rows.size());

        // Empty result.
        fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readWriteScanRetrieveBatchReplicaRequest()
                .groupId(grpId)
                .tableId(TABLE_ID)
                .transactionId(newTxId())
                .timestampLong(clock.nowLong())
                .enlistmentConsistencyToken(ANY_ENLISTMENT_CONSISTENCY_TOKEN)
                .scanId(2L)
                .indexToUse(sortedIndexId)
                .lowerBoundPrefix(toIndexBound(5))
                .batchSize(5)
                .commitPartitionId(commitPartitionId())
                .coordinatorId(localNode.id())
                .build(), localNode.id());

        rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS).result();

        assertNotNull(rows);
        assertEquals(0, rows.size());

        // Lookup.
        fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readWriteScanRetrieveBatchReplicaRequest()
                .groupId(grpId)
                .tableId(TABLE_ID)
                .transactionId(newTxId())
                .timestampLong(clock.nowLong())
                .enlistmentConsistencyToken(ANY_ENLISTMENT_CONSISTENCY_TOKEN)
                .scanId(2L)
                .indexToUse(sortedIndexId)
                .exactKey(toIndexKey(0))
                .batchSize(5)
                .commitPartitionId(commitPartitionId())
                .coordinatorId(localNode.id())
                .build(), localNode.id());

        rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS).result();

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
        CompletableFuture<ReplicaResult> fut = partitionReplicaListener.invoke(
                TABLE_MESSAGES_FACTORY.readOnlyScanRetrieveBatchReplicaRequest()
                        .groupId(grpId)
                        .tableId(TABLE_ID)
                        .transactionId(scanTxId)
                        .readTimestampLong(clock.nowLong())
                        .scanId(1L)
                        .indexToUse(sortedIndexId)
                        .batchSize(4)
                        .coordinatorId(localNode.id())
                        .build(), localNode.id());

        List<BinaryRow> rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS).result();

        assertNotNull(rows);
        assertEquals(4, rows.size());

        // Request second batch
        fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readOnlyScanRetrieveBatchReplicaRequest()
                .groupId(grpId)
                .tableId(TABLE_ID)
                .transactionId(scanTxId)
                .readTimestampLong(clock.nowLong())
                .scanId(1L)
                .indexToUse(sortedIndexId)
                .batchSize(4)
                .coordinatorId(localNode.id())
                .build(), localNode.id());

        rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS).result();

        assertNotNull(rows);
        assertEquals(2, rows.size());

        // Request bounded.
        fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readOnlyScanRetrieveBatchReplicaRequest()
                .groupId(grpId)
                .tableId(TABLE_ID)
                .transactionId(newTxId())
                .readTimestampLong(clock.nowLong())
                .scanId(2L)
                .indexToUse(sortedIndexId)
                .lowerBoundPrefix(toIndexBound(1))
                .upperBoundPrefix(toIndexBound(3))
                .flags(SortedIndexStorage.LESS_OR_EQUAL)
                .batchSize(5)
                .coordinatorId(localNode.id())
                .build(), localNode.id());

        rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS).result();

        assertNotNull(rows);
        assertEquals(2, rows.size());

        // Empty result.
        fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readOnlyScanRetrieveBatchReplicaRequest()
                .groupId(grpId)
                .tableId(TABLE_ID)
                .transactionId(newTxId())
                .readTimestampLong(clock.nowLong())
                .scanId(2L)
                .indexToUse(sortedIndexId)
                .lowerBoundPrefix(toIndexBound(5))
                .batchSize(5)
                .coordinatorId(localNode.id())
                .build(), localNode.id());

        rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS).result();

        assertNotNull(rows);
        assertEquals(0, rows.size());

        // Lookup.
        fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readOnlyScanRetrieveBatchReplicaRequest()
                .groupId(grpId)
                .tableId(TABLE_ID)
                .transactionId(newTxId())
                .readTimestampLong(clock.nowLong())
                .scanId(2L)
                .indexToUse(sortedIndexId)
                .exactKey(toIndexKey(0))
                .batchSize(5)
                .coordinatorId(localNode.id())
                .build(), localNode.id());

        rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS).result();

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
        CompletableFuture<ReplicaResult> fut = partitionReplicaListener.invoke(
                TABLE_MESSAGES_FACTORY.readOnlyScanRetrieveBatchReplicaRequest()
                        .groupId(grpId)
                        .tableId(TABLE_ID)
                        .transactionId(scanTxId)
                        .readTimestampLong(clock.nowLong())
                        .scanId(1L)
                        .indexToUse(hashIndexId)
                        .exactKey(toIndexKey(0))
                        .batchSize(3)
                        .coordinatorId(localNode.id())
                        .build(), localNode.id());

        List<BinaryRow> rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS).result();

        assertNotNull(rows);
        assertEquals(3, rows.size());

        // Request second batch
        fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readOnlyScanRetrieveBatchReplicaRequest()
                .groupId(grpId)
                .tableId(TABLE_ID)
                .transactionId(scanTxId)
                .readTimestampLong(clock.nowLong())
                .scanId(1L)
                .indexToUse(hashIndexId)
                .exactKey(toIndexKey(0))
                .batchSize(1)
                .coordinatorId(localNode.id())
                .build(), localNode.id());

        rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS).result();

        assertNotNull(rows);
        assertEquals(1, rows.size());

        // Empty result.
        fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readOnlyScanRetrieveBatchReplicaRequest()
                .groupId(grpId)
                .tableId(TABLE_ID)
                .transactionId(newTxId())
                .readTimestampLong(clock.nowLong())
                .scanId(2L)
                .indexToUse(hashIndexId)
                .exactKey(toIndexKey(5))
                .batchSize(5)
                .coordinatorId(localNode.id())
                .build(), localNode.id());

        rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS).result();

        assertNotNull(rows);
        assertEquals(0, rows.size());

        // Lookup.
        fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readOnlyScanRetrieveBatchReplicaRequest()
                .groupId(grpId)
                .tableId(TABLE_ID)
                .transactionId(newTxId())
                .readTimestampLong(clock.nowLong())
                .scanId(2L)
                .indexToUse(hashIndexId)
                .exactKey(toIndexKey(1))
                .batchSize(5)
                .coordinatorId(localNode.id())
                .build(), localNode.id());

        rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS).result();

        assertNotNull(rows);
        assertEquals(3, rows.size());
    }

    @Test
    public void testWriteIntentOnPrimaryReplicaInsertUpdateDelete() {
        UUID txId = newTxId();

        BinaryRow testRow = binaryRow(0);
        BinaryRow testRowPk = marshalQuietly(new TestKey(0, "k0"), kvMarshaller);

        assertThat(doSingleRowRequest(txId, testRow, RW_INSERT), willCompleteSuccessfully());

        checkRowInMvStorage(testRow, true);

        BinaryRow br = binaryRow(new TestKey(0, "k0"), new TestValue(1, "v1"));

        assertThat(doSingleRowRequest(txId, br, RW_UPSERT), willCompleteSuccessfully());

        checkRowInMvStorage(br, true);

        assertThat(doSingleRowPkRequest(txId, testRowPk, RW_DELETE), willCompleteSuccessfully());

        checkNoRowInIndex(testRow);

        assertThat(doSingleRowRequest(txId, testRow, RW_INSERT), willCompleteSuccessfully());

        checkRowInMvStorage(testRow, true);

        br = binaryRow(new TestKey(0, "k0"), new TestValue(1, "v2"));

        assertThat(doSingleRowRequest(txId, br, RW_GET_AND_REPLACE), willCompleteSuccessfully());

        checkRowInMvStorage(br, true);

        br = binaryRow(new TestKey(0, "k0"), new TestValue(1, "v3"));

        assertThat(doSingleRowRequest(txId, br, RW_GET_AND_UPSERT), willCompleteSuccessfully());

        checkRowInMvStorage(br, true);

        assertThat(doSingleRowPkRequest(txId, testRowPk, RW_GET_AND_DELETE), willCompleteSuccessfully());

        checkNoRowInIndex(br);

        assertThat(doSingleRowRequest(txId, testRow, RW_INSERT), willCompleteSuccessfully());

        checkRowInMvStorage(testRow, true);

        assertThat(doSingleRowRequest(txId, testRow, RW_DELETE_EXACT), willCompleteSuccessfully());

        checkNoRowInIndex(testRow);

        cleanup(txId);
    }

    private static <K, V> Row marshalQuietly(K key, KvMarshaller<K, V> marshaller) {
        return marshaller.marshal(key);
    }

    @Test
    public void testWriteIntentOnPrimaryReplicaMultiRowOps() {
        UUID txId = newTxId();
        BinaryRow row0 = binaryRow(0);
        BinaryRow row1 = binaryRow(1);
        Collection<BinaryRow> rows = asList(row0, row1);

        assertThat(doMultiRowRequest(txId, rows, RW_INSERT_ALL), willCompleteSuccessfully());

        checkRowInMvStorage(row0, true);
        checkRowInMvStorage(row1, true);

        BinaryRow newRow0 = binaryRow(new TestKey(0, "k0"), new TestValue(2, "v2"));
        BinaryRow newRow1 = binaryRow(new TestKey(1, "k1"), new TestValue(3, "v3"));
        Collection<BinaryRow> newRows = asList(newRow0, newRow1);

        assertThat(doMultiRowRequest(txId, newRows, RW_UPSERT_ALL), willCompleteSuccessfully());

        checkRowInMvStorage(row0, false);
        checkRowInMvStorage(row1, false);
        checkRowInMvStorage(newRow0, true);
        checkRowInMvStorage(newRow1, true);

        Collection<BinaryRow> newRowPks = List.of(
                marshalQuietly(new TestKey(0, "k0"), kvMarshaller),
                marshalQuietly(new TestKey(1, "k1"), kvMarshaller)
        );

        assertThat(doMultiRowPkRequest(txId, newRowPks, RW_DELETE_ALL), willCompleteSuccessfully());

        checkNoRowInIndex(row0);
        checkNoRowInIndex(row1);
        checkNoRowInIndex(newRow0);
        checkNoRowInIndex(newRow1);

        assertThat(doMultiRowRequest(txId, rows, RW_INSERT_ALL), willCompleteSuccessfully());

        checkRowInMvStorage(row0, true);
        checkRowInMvStorage(row1, true);

        assertThat(doMultiRowRequest(txId, rows, RW_DELETE_EXACT_ALL), willCompleteSuccessfully());

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
                        .tableId(TABLE_ID)
                        .transactionId(txId)
                        .requestTypeInt(requestType.ordinal())
                        .schemaVersion(binaryRow.schemaVersion())
                        .binaryTuple(binaryRow.tupleSlice())
                        .enlistmentConsistencyToken(ANY_ENLISTMENT_CONSISTENCY_TOKEN)
                        .commitPartitionId(commitPartitionId())
                        .coordinatorId(localNode.id())
                        .full(full)
                        .build(),
                localNode.id()
        );
    }

    private CompletableFuture<?> doSingleRowPkRequest(UUID txId, BinaryRow binaryRow, RequestType requestType) {
        return doSingleRowPkRequest(txId, binaryRow, requestType, false);
    }

    private CompletableFuture<?> doSingleRowPkRequest(UUID txId, BinaryRow binaryRow, RequestType requestType, boolean full) {
        return partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readWriteSingleRowPkReplicaRequest()
                        .groupId(grpId)
                        .tableId(TABLE_ID)
                        .transactionId(txId)
                        .requestTypeInt(requestType.ordinal())
                        .schemaVersion(binaryRow.schemaVersion())
                        .primaryKey(binaryRow.tupleSlice())
                        .enlistmentConsistencyToken(ANY_ENLISTMENT_CONSISTENCY_TOKEN)
                        .commitPartitionId(commitPartitionId())
                        .coordinatorId(localNode.id())
                        .full(full)
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
                        .tableId(TABLE_ID)
                        .transactionId(txId)
                        .requestTypeInt(requestType.ordinal())
                        .schemaVersion(binaryRows.iterator().next().schemaVersion())
                        .binaryTuples(binaryRowsToBuffers(binaryRows))
                        .enlistmentConsistencyToken(ANY_ENLISTMENT_CONSISTENCY_TOKEN)
                        .commitPartitionId(commitPartitionId())
                        .coordinatorId(localNode.id())
                        .full(full)
                        .build(),
                localNode.id()
        );
    }

    static List<ByteBuffer> binaryRowsToBuffers(Collection<BinaryRow> binaryRows) {
        return binaryRows.stream().map(BinaryRow::tupleSlice).collect(toList());
    }

    private CompletableFuture<?> doMultiRowPkRequest(UUID txId, Collection<BinaryRow> binaryRows, RequestType requestType) {
        return doMultiRowPkRequest(txId, binaryRows, requestType, false);
    }

    private CompletableFuture<?> doMultiRowPkRequest(UUID txId, Collection<BinaryRow> binaryRows, RequestType requestType, boolean full) {
        return partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readWriteMultiRowPkReplicaRequest()
                        .groupId(grpId)
                        .tableId(TABLE_ID)
                        .transactionId(txId)
                        .requestTypeInt(requestType.ordinal())
                        .schemaVersion(binaryRows.iterator().next().schemaVersion())
                        .primaryKeys(binaryRowsToBuffers(binaryRows))
                        .enlistmentConsistencyToken(ANY_ENLISTMENT_CONSISTENCY_TOKEN)
                        .commitPartitionId(commitPartitionId())
                        .coordinatorId(localNode.id())
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
                            .tableId(TABLE_ID)
                            .transactionId(txId)
                            .requestTypeInt(RW_INSERT.ordinal())
                            .schemaVersion(binaryRow.schemaVersion())
                            .binaryTuple(binaryRow.tupleSlice())
                            .enlistmentConsistencyToken(ANY_ENLISTMENT_CONSISTENCY_TOKEN)
                            .commitPartitionId(commitPartitionId())
                            .coordinatorId(localNode.id())
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
                            .tableId(TABLE_ID)
                            .transactionId(txId)
                            .requestTypeInt(RW_UPSERT_ALL.ordinal())
                            .schemaVersion(binaryRow0.schemaVersion())
                            .binaryTuples(asList(binaryRow0.tupleSlice(), binaryRow1.tupleSlice()))
                            .enlistmentConsistencyToken(ANY_ENLISTMENT_CONSISTENCY_TOKEN)
                            .commitPartitionId(commitPartitionId())
                            .coordinatorId(localNode.id())
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
        CompletableFuture<UpdateCommandResult> writeFut = new CompletableFuture<>();

        raftClientFutureClosure = cmd -> writeFut;

        try {
            CompletableFuture<ReplicaResult> replicaWriteFut = partitionReplicaListener.invoke(updatingRequestSupplier.get(),
                    localNode.id());

            assertTrue(replicaWriteFut.isDone());

            raftClientFutureClosure = defaultMockRaftFutureClosure;

            HybridTimestamp now = clock.now();

            // Imitation of tx commit.
            txStateStorage.putForRebalance(txId, new TxMeta(COMMITTED, new ArrayList<>(), now));
            txManager.updateTxMeta(txId, old -> new TxStateMeta(COMMITTED, UUID.randomUUID().toString(), commitPartitionId, now));

            CompletableFuture<?> replicaCleanupFut = partitionReplicaListener.invoke(
                    TX_MESSAGES_FACTORY.writeIntentSwitchReplicaRequest()
                            .groupId(grpId)
                            .txId(txId)
                            .commit(true)
                            .commitTimestampLong(now.longValue())
                            .build(),
                    localNode.id()
            );

            assertFalse(replicaCleanupFut.isDone());

            writeFut.complete(new UpdateCommandResult(true));

            assertThat(replicaCleanupFut, willSucceedFast());
        } finally {
            raftClientFutureClosure = defaultMockRaftFutureClosure;
        }

        // Check that one more write after cleanup is discarded.
        CompletableFuture<?> writeAfterCleanupFuture = partitionReplicaListener.invoke(updatingRequestSupplier.get(), localNode.id());
        assertThat(writeAfterCleanupFuture, willThrowFast(TransactionException.class));
    }

    @Test
    void testWriteIntentBearsLastCommitTimestamp() {
        BinaryRow br1 = binaryRow(1);

        BinaryRow br2 = binaryRow(2);

        // First insert a row
        UUID tx0 = newTxId();
        upsert(tx0, br1);
        upsert(tx0, br2);

        cleanup(tx0);

        raftClientFutureClosure = partitionCommand -> {
            assertTrue(partitionCommand instanceof UpdateCommandImpl);

            UpdateCommandImpl impl = (UpdateCommandImpl) partitionCommand;

            assertNotNull(impl.messageRowToUpdate());
            assertNotNull(impl.messageRowToUpdate().binaryRow());
            assertNotNull(impl.messageRowToUpdate().timestamp());

            return defaultMockRaftFutureClosure.apply(partitionCommand);
        };

        UUID tx1 = newTxId();
        upsert(tx1, br1);
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
    ) {
        BinaryRow br1 = binaryRow(1);

        BinaryRow br1Pk = marshalQuietly(new TestKey(1, "k" + 1), kvMarshaller);

        BinaryRow br2 = binaryRow(2);

        BinaryRow br2Pk = marshalQuietly(new TestKey(2, "k" + 2), kvMarshaller);

        // Preloading the data if needed.
        if (insertFirst) {
            UUID tx0 = newTxId();
            upsert(tx0, br1);
            upsert(tx0, br2);
            cleanup(tx0);
        }

        txState = null;

        // Delete the same row 2 times within the same transaction to generate garbage rows in storage.
        // If the data was not preloaded, there will be one deletion actually.
        UUID tx1 = newTxId();
        delete(tx1, br1Pk);
        upsert(tx1, br1);
        delete(tx1, br1Pk);

        if (upsertAfterDelete) {
            upsert(tx1, br1);
        }

        if (!insertFirst && !upsertAfterDelete) {
            Cursor<RowId> cursor = pkStorage().get(br1);

            // Data was not preloaded or inserted after deletion.
            assertFalse(cursor.hasNext());
        } else {
            // We create a null row with a row id having minimum possible value to ensure this row would be the first in cursor.
            // This is needed to check that this row will be skipped by RO tx and it will see the data anyway.
            // TODO https://issues.apache.org/jira/browse/IGNITE-18767 after this, the following check may be not needed.
            RowId emptyRowId = new RowId(PART_ID, new UUID(Long.MIN_VALUE, Long.MIN_VALUE));
            testMvPartitionStorage.addWrite(emptyRowId, null, tx1, TABLE_ID, PART_ID);

            if (committed) {
                testMvPartitionStorage.commitWrite(emptyRowId, clock.now());
            }

            pkStorage().put(br1, emptyRowId);
        }

        // If committed, there will be actual values in storage, otherwise write intents.
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
            List<BinaryRow> res = roGetAll(allRowsPks, clock.now());

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
        when(txManager.cleanup(any(), any(Map.class), anyBoolean(), any(), any())).thenReturn(nullCompletedFuture());

        HybridTimestamp beginTimestamp = clock.now();
        UUID txId = transactionIdFor(beginTimestamp);

        TxFinishReplicaRequest commitRequest = TX_MESSAGES_FACTORY.txFinishReplicaRequest()
                .groupId(grpId)
                .txId(txId)
                .groups(Map.of(grpId, localNode.name()))
                .commit(false)
                .enlistmentConsistencyToken(ANY_ENLISTMENT_CONSISTENCY_TOKEN)
                .build();

        return partitionReplicaListener.invoke(commitRequest, localNode.id());
    }

    private static UUID transactionIdFor(HybridTimestamp beginTimestamp) {
        return TestTransactionIds.TRANSACTION_ID_GENERATOR.transactionIdFor(beginTimestamp);
    }

    @Test
    public void commitsOnSameSchemaSuccessfully() {
        when(validationSchemasSource.tableSchemaVersionsBetween(anyInt(), any(), any(HybridTimestamp.class)))
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
        return new FullTableSchema(schemaVersion, TABLE_ID, TABLE_NAME, columns);
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
        when(txManager.cleanup(any(), any(Map.class), anyBoolean(), any(), any())).thenReturn(nullCompletedFuture());

        HybridTimestamp beginTimestamp = clock.now();
        UUID txId = transactionIdFor(beginTimestamp);

        HybridTimestamp commitTimestamp = clock.now();

        TxFinishReplicaRequest commitRequest = TX_MESSAGES_FACTORY.txFinishReplicaRequest()
                .groupId(grpId)
                .txId(txId)
                .groups(Map.of(grpId, localNode.name()))
                .commit(true)
                .commitTimestampLong(hybridTimestampToLong(commitTimestamp))
                .enlistmentConsistencyToken(ANY_ENLISTMENT_CONSISTENCY_TOKEN)
                .build();

        return partitionReplicaListener.invoke(commitRequest, localNode.id());
    }

    @Test
    public void commitsOnCompatibleSchemaChangeSuccessfully() {
        when(validationSchemasSource.tableSchemaVersionsBetween(anyInt(), any(), any(HybridTimestamp.class)))
                .thenReturn(List.of(
                        tableSchema(CURRENT_SCHEMA_VERSION, List.of(nullableColumn("col1"))),
                        // Addition of a nullable column is forward-compatible.
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

        MismatchingTransactionOutcomeException ex = assertWillThrowFast(future,
                MismatchingTransactionOutcomeException.class);

        assertThat(ex.getMessage(), containsString("Commit failed because schema is not forward-compatible [fromSchemaVersion=1, "
                + "toSchemaVersion=2, table=test, details=Column default value changed]"));

        assertThat(committed.get(), is(false));
    }

    private void simulateForwardIncompatibleSchemaChange(int fromSchemaVersion, int toSchemaVersion) {
        when(validationSchemasSource.tableSchemaVersionsBetween(anyInt(), any(), any(HybridTimestamp.class)))
                .thenReturn(incompatibleSchemaVersions(fromSchemaVersion, toSchemaVersion));
    }

    private void simulateBackwardIncompatibleSchemaChange(int fromSchemaVersion, int toSchemaVersion) {
        when(validationSchemasSource.tableSchemaVersionsBetween(anyInt(), any(), anyInt()))
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
        return RequestTypes.isKeyOnly(requestType) ? marshalQuietly(key, kvMarshaller) : kvMarshaller.marshal(key, someValue);
    }

    private void testFailsWhenReadingFromFutureIncompatibleSchema(RwListenerInvocation listenerInvocation) {
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
        assertThat(ex.getMessage(), containsString(
                "Operation failed because it tried to access a row with newer schema version than transaction's [table=test, "
                        + "txSchemaVersion=1, rowSchemaVersion=2]"
        ));

        // Tx should not be finished.
        assertThat(committed.get(), is(nullValue()));
    }

    @ParameterizedTest
    @MethodSource("multiRowsRequestTypes")
    public void failsWhenReadingMultiRowsFromFutureIncompatibleSchema(RequestType requestType) {
        if (requestType == RW_GET_ALL || requestType == RW_DELETE_ALL) {
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
                        .tableId(TABLE_ID)
                        .transactionId(targetTxId)
                        .requestTypeInt(RW_REPLACE.ordinal())
                        .schemaVersion(oldRow.schemaVersion())
                        .oldBinaryTuple(oldRow.tupleSlice())
                        .newBinaryTuple(newRow.tupleSlice())
                        .enlistmentConsistencyToken(ANY_ENLISTMENT_CONSISTENCY_TOKEN)
                        .commitPartitionId(commitPartitionId())
                        .coordinatorId(localNode.id())
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
                                .tableId(TABLE_ID)
                                .transactionId(targetTxId)
                                .indexToUse(sortedIndexStorage.id())
                                .exactKey(toIndexKey(FUTURE_SCHEMA_ROW_INDEXED_VALUE))
                                .enlistmentConsistencyToken(ANY_ENLISTMENT_CONSISTENCY_TOKEN)
                                .scanId(1)
                                .batchSize(100)
                                .commitPartitionId(commitPartitionId())
                                .coordinatorId(localNode.id())
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
                                .tableId(TABLE_ID)
                                .transactionId(targetTxId)
                                .indexToUse(sortedIndexStorage.id())
                                .enlistmentConsistencyToken(ANY_ENLISTMENT_CONSISTENCY_TOKEN)
                                .scanId(1)
                                .batchSize(100)
                                .commitPartitionId(commitPartitionId())
                                .coordinatorId(localNode.id())
                                .build(),
                        localNode.id()
                )
        );
    }

    @Test
    public void failsWhenScanReadsTupleWithIncompatibleSchemaFromFuture() {
        testFailsWhenReadingFromFutureIncompatibleSchema(
                (targetTxId, key) -> doRwScanRetrieveBatchRequest(targetTxId)
        );
    }

    private CompletableFuture<?> doRwScanRetrieveBatchRequest(UUID targetTxId) {
        return partitionReplicaListener.invoke(
                TABLE_MESSAGES_FACTORY.readWriteScanRetrieveBatchReplicaRequest()
                        .groupId(grpId)
                        .tableId(TABLE_ID)
                        .transactionId(targetTxId)
                        .enlistmentConsistencyToken(ANY_ENLISTMENT_CONSISTENCY_TOKEN)
                        .scanId(1)
                        .batchSize(100)
                        .full(false)
                        .commitPartitionId(commitPartitionId())
                        .coordinatorId(localNode.id())
                        .build(),
                localNode.id()
        );
    }

    private CompletableFuture<?> doRwScanCloseRequest(UUID targetTxId) {
        return partitionReplicaListener.invoke(
                TABLE_MESSAGES_FACTORY.scanCloseReplicaRequest()
                        .groupId(grpId)
                        .transactionId(targetTxId)
                        .scanId(1)
                        .build(),
                localNode.id()
        );
    }

    private CompletableFuture<?> doRoScanRetrieveBatchRequest(UUID targetTxId, HybridTimestamp readTimestamp) {
        return partitionReplicaListener.invoke(
                TABLE_MESSAGES_FACTORY.readOnlyScanRetrieveBatchReplicaRequest()
                        .groupId(grpId)
                        .tableId(TABLE_ID)
                        .transactionId(targetTxId)
                        .scanId(1)
                        .batchSize(100)
                        .readTimestampLong(readTimestamp.longValue())
                        .coordinatorId(localNode.id())
                        .build(),
                localNode.id()
        );
    }

    @ParameterizedTest
    @MethodSource("singleRowWriteRequestTypes")
    public void singleRowWritesAreSuppliedWithRequiredCatalogVersion(RequestType requestType) {
        if (requestType == RW_DELETE || requestType == RW_GET_AND_DELETE) {
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

        doAnswer(invocation -> nullCompletedFuture()).when(txManager).finish(any(), any(), anyBoolean(), any(), any());
        doAnswer(invocation -> nullCompletedFuture()).when(txManager).cleanup(any(), anyString(), any());
    }

    private void testWritesAreSuppliedWithRequiredCatalogVersion(RequestType requestType, RwListenerInvocation listenerInvocation) {
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
        testWritesAreSuppliedWithRequiredCatalogVersion(RW_REPLACE, (targetTxId, key) -> {
            return doReplaceRequest(
                    targetTxId,
                    marshalKeyOrKeyValue(RW_REPLACE, key),
                    marshalKeyOrKeyValue(RW_REPLACE, key)
            );
        });
    }

    @ParameterizedTest
    @MethodSource("multiRowsWriteRequestTypes")
    public void multiRowWritesAreSuppliedWithRequiredCatalogVersion(RequestType requestType) {
        if (requestType == RW_DELETE_ALL) {
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
        RwListenerInvocation invocation = null;

        if (RequestTypes.isSingleRowRwPkOnly(requestType)) {
            invocation = (targetTxId, key) -> doSingleRowPkRequest(targetTxId, marshalKeyOrKeyValue(requestType, key), requestType, full);
        } else if (RequestTypes.isSingleRowRwFullRow(requestType)) {
            invocation = (targetTxId, key) -> doSingleRowRequest(targetTxId, marshalKeyOrKeyValue(requestType, key), requestType, full);
        } else {
            fail("Uncovered type: " + requestType);
        }

        testRwOperationFailsIfTableWasAlteredAfterTxStart(requestType, onExistingRow, invocation);
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

    private void testRwOperationFailsIfTableWasAlteredAfterTxStart(
            RequestType requestType,
            boolean onExistingRow,
            RwListenerInvocation listenerInvocation
    ) {
        TestKey key = nextKey();

        if (onExistingRow) {
            upsertInNewTxFor(key);
        }

        UUID txId = newTxId();
        HybridTimestamp txBeginTs = beginTimestamp(txId);

        makeSchemaChangeAfter(txBeginTs);

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
                    is("Table schema was updated after the transaction was started [table=test, startSchema=1, operationSchema=2]")
            );
        } else {
            assertThat(future, willCompleteSuccessfully());
        }
    }

    private void makeSchemaChangeAfter(HybridTimestamp txBeginTs) {
        CatalogTableDescriptor tableVersion1 = mock(CatalogTableDescriptor.class);
        CatalogTableDescriptor tableVersion2 = mock(CatalogTableDescriptor.class);

        when(tableVersion1.name()).thenReturn(TABLE_NAME);
        when(tableVersion2.name()).thenReturn(TABLE_NAME_2);
        when(tableVersion1.tableVersion()).thenReturn(CURRENT_SCHEMA_VERSION);
        when(tableVersion2.tableVersion()).thenReturn(NEXT_SCHEMA_VERSION);

        when(catalogService.table(TABLE_ID, txBeginTs.longValue())).thenReturn(tableVersion1);
        when(catalogService.table(eq(TABLE_ID), gt(txBeginTs.longValue()))).thenReturn(tableVersion2);
    }

    @CartesianTest
    @CartesianTest.MethodFactory("multiRowRwOperationTypesFactory")
    void multiRowRwOperationsFailIfTableAlteredAfterTxStart(
            RequestType requestType, boolean onExistingRow, boolean full
    ) {
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

        testRwOperationFailsIfTableWasAlteredAfterTxStart(requestType, onExistingRow, invocation);
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
        testRwOperationFailsIfTableWasAlteredAfterTxStart(RW_REPLACE, onExistingRow, (targetTxId, key) -> {
            return doReplaceRequest(
                    targetTxId,
                    marshalKeyOrKeyValue(RW_REPLACE, key),
                    marshalKeyOrKeyValue(RW_REPLACE, key),
                    full
            );
        });
    }

    @CartesianTest
    void rwScanRequestFailsIfTableAlteredAfterTxStart(@Values(booleans = {false, true}) boolean onExistingRow) {
        testRwOperationFailsIfTableWasAlteredAfterTxStart(RW_SCAN, onExistingRow, (targetTxId, key) -> {
            return doRwScanRetrieveBatchRequest(targetTxId);
        });
    }

    @Test
    void rwScanCloseRequestSucceedsIfTableAlteredAfterTxStart() {
        UUID txId = newTxId();
        HybridTimestamp txBeginTs = beginTimestamp(txId);

        makeSchemaChangeAfter(txBeginTs);

        CompletableFuture<?> future = doRwScanCloseRequest(txId);

        assertThat(future, willCompleteSuccessfully());
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

        IncompatibleSchemaException ex = assertWillThrowFast(future, IncompatibleSchemaException.class);
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

        when(catalogService.table(tableId, txBeginTs.longValue())).thenReturn(tableVersion1);
        when(catalogService.table(eq(tableId), gt(txBeginTs.longValue()))).thenReturn(null);
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

    @Test
    void rwScanCloseRequestSucceedsIfTableWasDropped() {
        UUID txId = newTxId();
        HybridTimestamp txBeginTs = beginTimestamp(txId);

        makeTableBeDroppedAfter(txBeginTs);

        CompletableFuture<?> future = doRwScanCloseRequest(txId);

        assertThat(future, willCompleteSuccessfully());
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

        when(catalogService.table(eq(TABLE_ID), anyLong())).thenReturn(null);

        CompletableFuture<?> future = listenerInvocation.invoke(txId, readTs, key);

        IncompatibleSchemaException ex = assertWillThrowFast(future, IncompatibleSchemaException.class);
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

    @Test
    void commitRequestFailsIfCommitPartitionTableWasDropped() {
        testCommitRequestIfTableWasDropped(grpId, Map.of(grpId, localNode.name()), grpId.tableId());
    }

    @Test
    void commitRequestFailsIfNonCommitPartitionTableWasDropped() {
        TablePartitionId anotherPartitionId = new TablePartitionId(ANOTHER_TABLE_ID, 0);

        testCommitRequestIfTableWasDropped(grpId, Map.of(grpId, localNode.name(), anotherPartitionId, localNode.name()),
                anotherPartitionId.tableId());
    }

    private void testCommitRequestIfTableWasDropped(
            TablePartitionId commitPartitionId,
            Map<ReplicationGroupId, String> groups,
            int tableToBeDroppedId
    ) {
        when(validationSchemasSource.tableSchemaVersionsBetween(anyInt(), any(), any(HybridTimestamp.class)))
                .thenReturn(List.of(
                        tableSchema(CURRENT_SCHEMA_VERSION, List.of(nullableColumn("col")))
                ));
        when(txManager.cleanup(any(), any(Map.class), anyBoolean(), any(), any())).thenReturn(nullCompletedFuture());

        AtomicReference<Boolean> committed = interceptFinishTxCommand();

        UUID txId = newTxId();
        HybridTimestamp txBeginTs = beginTimestamp(txId);

        String tableNameToBeDropped = catalogService.table(tableToBeDroppedId, txBeginTs.longValue()).name();

        makeTableBeDroppedAfter(txBeginTs, tableToBeDroppedId);

        CompletableFuture<?> future = partitionReplicaListener.invoke(
                TX_MESSAGES_FACTORY.txFinishReplicaRequest()
                        .groupId(commitPartitionId)
                        .groups(groups)
                        .txId(txId)
                        .enlistmentConsistencyToken(ANY_ENLISTMENT_CONSISTENCY_TOKEN)
                        .commit(true)
                        .commitTimestampLong(clock.nowLong())
                        .build(),
                localNode.id()
        );

        MismatchingTransactionOutcomeException ex = assertWillThrowFast(future, MismatchingTransactionOutcomeException.class);

        assertThat(ex.getMessage(), is("Commit failed because a table was already dropped [table=" + tableNameToBeDropped + "]"));

        assertThat("The transaction must have been aborted", committed.get(), is(false));
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

        when(catalogService.table(eq(TABLE_ID), anyLong())).thenReturn(tableVersion2);
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
                .groupId(grpId)
                .tableId(TABLE_ID)
                .requestTypeInt(RW_UPSERT.ordinal())
                .transactionId(txId)
                .schemaVersion(row.schemaVersion())
                .binaryTuple(row.tupleSlice())
                .enlistmentConsistencyToken(ANY_ENLISTMENT_CONSISTENCY_TOKEN)
                .commitPartitionId(commitPartitionId())
                .coordinatorId(localNode.id())
                .full(full)
                .build();

        return partitionReplicaListener.invoke(message, localNode.id());
    }

    private void delete(UUID txId, BinaryRow row) {
        ReadWriteSingleRowPkReplicaRequest message = TABLE_MESSAGES_FACTORY.readWriteSingleRowPkReplicaRequest()
                .groupId(grpId)
                .tableId(TABLE_ID)
                .requestTypeInt(RW_DELETE.ordinal())
                .transactionId(txId)
                .schemaVersion(row.schemaVersion())
                .primaryKey(row.tupleSlice())
                .enlistmentConsistencyToken(ANY_ENLISTMENT_CONSISTENCY_TOKEN)
                .commitPartitionId(commitPartitionId())
                .coordinatorId(localNode.id())
                .build();

        assertThat(partitionReplicaListener.invoke(message, localNode.id()), willCompleteSuccessfully());
    }

    private BinaryRow roGet(BinaryRow row, long readTimestamp) {
        CompletableFuture<BinaryRow> roGetAsync = roGetAsync(row, readTimestamp);

        assertThat(roGetAsync, willCompleteSuccessfully());

        return roGetAsync.join();
    }

    private CompletableFuture<BinaryRow> roGetAsync(BinaryRow row, long readTimestamp) {
        ReadOnlySingleRowPkReplicaRequest message = TABLE_MESSAGES_FACTORY.readOnlySingleRowPkReplicaRequest()
                .groupId(grpId)
                .tableId(TABLE_ID)
                .requestTypeInt(RO_GET.ordinal())
                .readTimestampLong(readTimestamp)
                .schemaVersion(row.schemaVersion())
                .primaryKey(row.tupleSlice())
                .build();

        return partitionReplicaListener.invoke(message, localNode.id()).thenApply(replicaResult -> (BinaryRow) replicaResult.result());
    }

    private List<BinaryRow> roGetAll(Collection<BinaryRow> rows, HybridTimestamp readTimestamp) {
        CompletableFuture<ReplicaResult> future = doReadOnlyMultiGet(rows, readTimestamp);

        assertThat(future, willCompleteSuccessfully());

        return (List<BinaryRow>) future.join().result();
    }

    private CompletableFuture<ReplicaResult> doReadOnlyMultiGet(Collection<BinaryRow> rows, HybridTimestamp readTimestamp) {
        ReadOnlyMultiRowPkReplicaRequest request = TABLE_MESSAGES_FACTORY.readOnlyMultiRowPkReplicaRequest()
                .groupId(grpId)
                .tableId(TABLE_ID)
                .requestTypeInt(RO_GET_ALL.ordinal())
                .readTimestampLong(readTimestamp.longValue())
                .schemaVersion(rows.iterator().next().schemaVersion())
                .primaryKeys(binaryRowsToBuffers(rows))
                .build();

        return partitionReplicaListener.invoke(request, localNode.id());
    }

    private CompletableFuture<ReplicaResult> doReadOnlyDirectMultiGet(Collection<BinaryRow> rows) {
        ReadOnlyDirectMultiRowReplicaRequest request = TABLE_MESSAGES_FACTORY.readOnlyDirectMultiRowReplicaRequest()
                .groupId(grpId)
                .tableId(TABLE_ID)
                .requestTypeInt(RO_GET_ALL.ordinal())
                .schemaVersion(rows.iterator().next().schemaVersion())
                .primaryKeys(binaryRowsToBuffers(rows))
                .enlistmentConsistencyToken(ANY_ENLISTMENT_CONSISTENCY_TOKEN)
                .build();

        return partitionReplicaListener.invoke(request, localNode.id());
    }

    private void cleanup(UUID txId) {
        HybridTimestamp commitTs = clock.now();

        txManager.updateTxMeta(txId, old -> new TxStateMeta(COMMITTED, UUID.randomUUID().toString(), commitPartitionId, commitTs));

        WriteIntentSwitchReplicaRequest message = TX_MESSAGES_FACTORY.writeIntentSwitchReplicaRequest()
                .groupId(grpId)
                .txId(txId)
                .commit(true)
                .commitTimestampLong(commitTs.longValue())
                .build();

        assertThat(partitionReplicaListener.invoke(message, localNode.id()), willCompleteSuccessfully());

        txState = COMMITTED;
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
        return marshalQuietly(nextKey(), kvMarshaller);
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

    private TestKey key(BinaryRow binaryRow) {
        return kvMarshaller.unmarshalKeyOnly(Row.wrapKeyOnlyBinaryRow(schemaDescriptor, binaryRow));
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
    private interface RwListenerInvocation {
        CompletableFuture<?> invoke(UUID targetTxId, TestKey key);
    }

    @FunctionalInterface
    private interface RoListenerInvocation {
        CompletableFuture<?> invoke(UUID targetTxId, HybridTimestamp readTimestamp, TestKey key);
    }

    @ParameterizedTest(name = "readOnly = {0}")
    @ValueSource(booleans = {true, false})
    void testStaleTxOperationAfterIndexStartBuilding(boolean readOnly) {
        int indexId = hashIndexStorage.id();
        int indexCreationCatalogVersion = 1;
        int startBuildingIndexCatalogVersion = 2;
        long indexCreationActivationTs = clock.now().addPhysicalTime(-100).longValue();
        long startBuildingIndexActivationTs = clock.nowLong();

        setIndexMetaInBuildingStatus(
                indexId,
                indexCreationCatalogVersion,
                indexCreationActivationTs,
                startBuildingIndexCatalogVersion,
                startBuildingIndexActivationTs
        );

        fireHashIndexStartBuildingEventForStaleTxOperation(indexId, startBuildingIndexCatalogVersion);

        UUID txId = newTxId();
        long beginTs = beginTimestamp(txId).longValue();

        when(catalogService.activeCatalogVersion(eq(beginTs))).thenReturn(0);

        BinaryRow row = binaryRow(0);

        if (readOnly) {
            assertThat(roGetAsync(row, clock.nowLong()), willCompleteSuccessfully());
        } else {
            assertThat(upsertAsync(txId, row), willThrow(StaleTransactionOperationException.class));
        }
    }

    @Test
    void testBuildIndexReplicaRequestWithoutRwTxOperations() {
        int indexId = hashIndexStorage.id();
        int indexCreationCatalogVersion = 1;
        int startBuildingIndexCatalogVersion = 2;
        long indexCreationActivationTs = clock.now().addPhysicalTime(-100).longValue();
        long startBuildingIndexActivationTs = clock.nowLong();

        setIndexMetaInBuildingStatus(
                indexId,
                indexCreationCatalogVersion,
                indexCreationActivationTs,
                startBuildingIndexCatalogVersion,
                startBuildingIndexActivationTs
        );

        CompletableFuture<?> invokeBuildIndexReplicaRequestFuture = invokeBuildIndexReplicaRequestAsync(indexId);

        assertFalse(invokeBuildIndexReplicaRequestFuture.isDone());

        fireHashIndexStartBuildingEventForStaleTxOperation(indexId, startBuildingIndexCatalogVersion);

        assertThat(invokeBuildIndexReplicaRequestFuture, willCompleteSuccessfully());
        assertThat(invokeBuildIndexReplicaRequestAsync(indexId), willCompleteSuccessfully());
    }

    @ParameterizedTest(name = "failCmd = {0}")
    @ValueSource(booleans = {false, true})
    void testBuildIndexReplicaRequest(boolean failCmd) {
        var continueNotBuildIndexCmdFuture = new CompletableFuture<Void>();
        var buildIndexCommandFuture = new CompletableFuture<BuildIndexCommand>();

        when(mockRaftClient.run(any())).thenAnswer(invocation -> {
            Command cmd = invocation.getArgument(0);

            if (cmd instanceof BuildIndexCommand) {
                buildIndexCommandFuture.complete((BuildIndexCommand) cmd);

                return raftClientFutureClosure.apply(cmd);
            }

            return continueNotBuildIndexCmdFuture.thenCompose(unused -> raftClientFutureClosure.apply(cmd));
        });

        UUID txId = newTxId();
        long beginTs = beginTimestamp(txId).longValue();

        when(catalogService.activeCatalogVersion(eq(beginTs))).thenReturn(0);

        BinaryRow row = binaryRow(0);

        CompletableFuture<ReplicaResult> upsertFuture = upsertAsync(txId, row, true);

        int indexId = hashIndexStorage.id();
        int indexCreationCatalogVersion = 1;
        int startBuildingIndexCatalogVersion = 2;
        long indexCreationActivationTs = clock.now().addPhysicalTime(-100).longValue();
        long startBuildingIndexActivationTs = clock.nowLong();

        setIndexMetaInBuildingStatus(
                indexId,
                indexCreationCatalogVersion,
                indexCreationActivationTs,
                startBuildingIndexCatalogVersion,
                startBuildingIndexActivationTs
        );

        CompletableFuture<?> invokeBuildIndexReplicaRequestFuture = invokeBuildIndexReplicaRequestAsync(indexId);

        fireHashIndexStartBuildingEventForStaleTxOperation(indexId, startBuildingIndexCatalogVersion);

        assertFalse(upsertFuture.isDone());
        assertFalse(invokeBuildIndexReplicaRequestFuture.isDone());

        if (failCmd) {
            continueNotBuildIndexCmdFuture.completeExceptionally(new RuntimeException("error from test"));

            assertThat(upsertFuture, willThrow(RuntimeException.class));
        } else {
            continueNotBuildIndexCmdFuture.complete(null);

            assertThat(upsertFuture, willCompleteSuccessfully());
        }

        assertThat(invokeBuildIndexReplicaRequestFuture, willCompleteSuccessfully());

        HybridTimestamp startBuildingIndexActivationTs0 = hybridTimestamp(startBuildingIndexActivationTs);

        verify(safeTimeClock).waitFor(eq(startBuildingIndexActivationTs0));

        assertThat(buildIndexCommandFuture, willCompleteSuccessfully());

        BuildIndexCommand buildIndexCommand = buildIndexCommandFuture.join();
        assertThat(buildIndexCommand.indexId(), equalTo(indexId));
        assertThat(buildIndexCommand.requiredCatalogVersion(), equalTo(startBuildingIndexCatalogVersion));
    }

    private void fireHashIndexStartBuildingEventForStaleTxOperation(int indexId, int startBuildingIndexCatalogVersion) {
        assertThat(
                catalogServiceEventProducer.fireEvent(
                        INDEX_BUILDING,
                        new StartBuildingIndexEventParameters(0L, startBuildingIndexCatalogVersion, indexId)
                ),
                willCompleteSuccessfully()
        );
    }

    private CompletableFuture<?> invokeBuildIndexReplicaRequestAsync(int indexId) {
        BuildIndexReplicaRequest request = TABLE_MESSAGES_FACTORY.buildIndexReplicaRequest()
                .groupId(grpId)
                .indexId(indexId)
                .enlistmentConsistencyToken(ANY_ENLISTMENT_CONSISTENCY_TOKEN)
                .rowIds(List.of())
                .build();

        return partitionReplicaListener.invoke(request, localNode.id());
    }

    private void completeBuiltIndexes(IndexStorage... indexStorages) {
        TestStorageUtils.completeBuiltIndexes(testMvPartitionStorage, indexStorages);
    }

    private void setIndexMetaInBuildingStatus(
            int indexId,
            int registeredCatalogVersion,
            long registeredActivationTs,
            int buildingCatalogVersion,
            long buildingActivationTs
    ) {
        IndexMeta indexMeta = mock(IndexMeta.class);

        MetaIndexStatusChange change0 = mock(MetaIndexStatusChange.class);
        MetaIndexStatusChange change1 = mock(MetaIndexStatusChange.class);

        when(change0.catalogVersion()).thenReturn(registeredCatalogVersion);
        when(change0.activationTimestamp()).thenReturn(registeredActivationTs);

        when(change1.catalogVersion()).thenReturn(buildingCatalogVersion);
        when(change1.activationTimestamp()).thenReturn(buildingActivationTs);

        Map<MetaIndexStatus, MetaIndexStatusChange> statusChangeMap = Map.of(
                MetaIndexStatus.REGISTERED, change0,
                MetaIndexStatus.BUILDING, change1
        );

        when(indexMeta.indexId()).thenReturn(indexId);
        when(indexMeta.status()).thenReturn(MetaIndexStatus.BUILDING);
        when(indexMeta.tableId()).thenReturn(TABLE_ID);
        when(indexMeta.statusChanges()).thenReturn(statusChangeMap);
        when(indexMeta.statusChange(eq(MetaIndexStatus.REGISTERED))).thenReturn(change0);
        when(indexMeta.statusChange(eq(MetaIndexStatus.BUILDING))).thenReturn(change1);

        when(indexMetaStorage.indexMeta(eq(indexId))).thenReturn(indexMeta);
    }
}
