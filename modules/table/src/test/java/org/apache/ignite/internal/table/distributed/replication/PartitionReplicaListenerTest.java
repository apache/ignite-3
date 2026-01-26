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
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.INDEX_BUILDING;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;
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
import static org.apache.ignite.internal.replicator.message.ReplicaMessageUtils.toZonePartitionIdMessage;
import static org.apache.ignite.internal.schema.BinaryRowMatcher.equalToRow;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.asserts.CompletableFutureAssert.assertWillThrowFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.tx.TransactionIds.beginTimestamp;
import static org.apache.ignite.internal.tx.TxState.ABORTED;
import static org.apache.ignite.internal.tx.TxState.COMMITTED;
import static org.apache.ignite.internal.tx.TxState.FINISHING;
import static org.apache.ignite.internal.tx.TxState.PENDING;
import static org.apache.ignite.internal.tx.TxState.UNKNOWN;
import static org.apache.ignite.internal.tx.TxState.checkTransitionCorrectness;
import static org.apache.ignite.internal.tx.test.TxStateMetaTestUtils.assertTxStateMetaIsSame;
import static org.apache.ignite.internal.util.ArrayUtils.asList;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.flatArray;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;
import static org.mockito.AdditionalMatchers.gt;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import it.unimi.dsi.fastutil.ints.IntList;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.distributed.TestPartitionDataStorage;
import org.apache.ignite.distributed.replicator.action.RequestTypes;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTuplePrefixBuilder;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogNotFoundException;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.events.StartBuildingIndexEventParameters;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.NoOpFailureManager;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.ClusterNodeResolver;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.SingleClusterNodeResolver;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.partition.replicator.ReplicaPrimacy;
import org.apache.ignite.internal.partition.replicator.ReplicaPrimacyEngine;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.command.CatalogVersionAware;
import org.apache.ignite.internal.partition.replicator.network.command.FinishTxCommand;
import org.apache.ignite.internal.partition.replicator.network.command.UpdateAllCommand;
import org.apache.ignite.internal.partition.replicator.network.command.UpdateCommand;
import org.apache.ignite.internal.partition.replicator.network.command.WriteIntentSwitchCommand;
import org.apache.ignite.internal.partition.replicator.network.replication.BinaryTupleMessage;
import org.apache.ignite.internal.partition.replicator.network.replication.BuildIndexReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadOnlyDirectMultiRowReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadOnlyDirectSingleRowReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadOnlyMultiRowPkReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadOnlyReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadOnlyScanRetrieveBatchReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadOnlySingleRowPkReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadWriteSingleRowPkReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadWriteSingleRowReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.RequestType;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionDataStorage;
import org.apache.ignite.internal.partition.replicator.schema.FullTableSchema;
import org.apache.ignite.internal.partition.replicator.schema.ValidationSchemasSource;
import org.apache.ignite.internal.partition.replicator.schemacompat.IncompatibleSchemaVersionException;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.TestPlacementDriver;
import org.apache.ignite.internal.placementdriver.TestReplicaMetaImpl;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.service.LeaderWithTerm;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.ReplicaResult;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.replicator.exception.PrimaryReplicaMissException;
import org.apache.ignite.internal.replicator.message.PrimaryReplicaRequest;
import org.apache.ignite.internal.replicator.message.ReadOnlyDirectReplicaRequest;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.replicator.message.ZonePartitionIdMessage;
import org.apache.ignite.internal.schema.AlwaysSyncedSchemaSyncService;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowConverter;
import org.apache.ignite.internal.schema.BinaryTuple;
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
import org.apache.ignite.internal.storage.index.IndexRowImpl;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor.StorageHashIndexColumnDescriptor;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor.StorageSortedIndexColumnDescriptor;
import org.apache.ignite.internal.storage.index.impl.TestHashIndexStorage;
import org.apache.ignite.internal.storage.index.impl.TestSortedIndexStorage;
import org.apache.ignite.internal.table.TableTestUtils;
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
import org.apache.ignite.internal.table.distributed.replicator.PartitionReplicaListener;
import org.apache.ignite.internal.table.distributed.replicator.StaleTransactionOperationException;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.internal.table.impl.DummySchemaManagerImpl;
import org.apache.ignite.internal.table.metrics.TableMetricSource;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.OutdatedReadOnlyTransactionInternalException;
import org.apache.ignite.internal.tx.TransactionMeta;
import org.apache.ignite.internal.tx.TransactionResult;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.TxStateMetaAbandoned;
import org.apache.ignite.internal.tx.UpdateCommandResult;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.PlacementDriverHelper;
import org.apache.ignite.internal.tx.impl.RemotelyTriggeredResourceRegistry;
import org.apache.ignite.internal.tx.impl.TransactionStateResolver;
import org.apache.ignite.internal.tx.impl.TxMessageSender;
import org.apache.ignite.internal.tx.impl.WaitDieDeadlockPreventionPolicy;
import org.apache.ignite.internal.tx.message.TransactionMetaMessage;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.internal.tx.message.TxStateCommitPartitionRequest;
import org.apache.ignite.internal.tx.message.TxStateCoordinatorRequest;
import org.apache.ignite.internal.tx.message.TxStatePrimaryReplicaRequest;
import org.apache.ignite.internal.tx.message.TxStateResponse;
import org.apache.ignite.internal.tx.test.TestTransactionIds;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.Lazy;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.lang.ErrorGroups.Transactions;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.table.QualifiedName;
import org.apache.ignite.tx.TransactionException;
import org.hamcrest.Matcher;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
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

    private static final int ZONE_ID = 2;

    private static final ZonePartitionId commitPartitionId = new ZonePartitionId(ZONE_ID, PART_ID);

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
                    testMvPartitionStorage.commitWrite(row, commitTimestamp, txId);
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
    private final ZonePartitionId grpId = new ZonePartitionId(ZONE_ID, PART_ID);

    /** Hybrid clock. */
    private final HybridClock clock = new HybridClockImpl();

    private final ClockService clockService = new TestClockService(clock);

    /** Local cluster node. */
    private final InternalClusterNode localNode = new ClusterNodeImpl(nodeId(1), "node1", NetworkAddress.from("127.0.0.1:127"));

    /** Another (not local) cluster node. */
    private final InternalClusterNode anotherNode = new ClusterNodeImpl(nodeId(2), "node2", NetworkAddress.from("127.0.0.2:127"));

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

    /** Key-value marshaller using schema version 2. */
    private KvMarshaller<TestKey, TestValue> kvMarshallerVersion2;

    private final CatalogTableDescriptor tableDescriptor;

    {
        List<CatalogTableColumnDescriptor> columns = List.of(
                new CatalogTableColumnDescriptor("intKey", ColumnType.INT32, false, 0, 0, 0, null),
                new CatalogTableColumnDescriptor("strKey", ColumnType.STRING, false, 0, 0, 0, null),
                new CatalogTableColumnDescriptor("intVal", ColumnType.INT32, false, 0, 0, 0, null),
                new CatalogTableColumnDescriptor("strVal", ColumnType.STRING, false, 0, 0, 0, null)
        );
        tableDescriptor = CatalogTableDescriptor.builder()
                .id(TABLE_ID)
                .schemaId(1)
                .primaryKeyIndexId(2)
                .name(TABLE_NAME)
                .zoneId(ZONE_ID)
                .newColumns(columns)
                .primaryKeyColumns(IntList.of(0, 1))
                .storageProfile(CatalogService.DEFAULT_STORAGE_PROFILE)
                .build();
    }

    /** Placement driver. */
    private TestPlacementDriver placementDriver;

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

    @Captor
    private ArgumentCaptor<Command> commandCaptor;

    private final TestValue someValue = new TestValue(1, "v1");

    @Mock
    private IndexMetaStorage indexMetaStorage;

    private ReplicaPrimacyEngine primacyEngine;

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
                        List.of(new StorageSortedIndexColumnDescriptor("intVal", NativeTypes.INT32, false, true, false)),
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
        }).when(messagingService).invoke(any(InternalClusterNode.class), any(), anyLong());

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
            public InternalClusterNode getById(UUID id) {
                return id.equals(localNode.id()) ? localNode : anotherNode;
            }

            @Override
            public InternalClusterNode getByConsistentId(String consistentId) {
                return consistentId.equals(localNode.name()) ? localNode : anotherNode;
            }
        };

        transactionStateResolver = new TransactionStateResolver(
                txManager,
                clockService,
                clusterNodeResolver,
                messagingService,
                mock(PlacementDriverHelper.class),
                new TxMessageSender(
                        messagingService,
                        mock(ReplicaService.class),
                        clockService
                )
        );

        transactionStateResolver.start();

        placementDriver = spy(new TestPlacementDriver(localNode));

        partitionReplicaListener = new PartitionReplicaListener(
                testMvPartitionStorage,
                mockRaftClient,
                txManager,
                lockManager,
                Runnable::run,
                new ZonePartitionId(tableDescriptor.zoneId(), PART_ID),
                TABLE_ID,
                () -> Map.of(pkLocker.id(), pkLocker, sortedIndexId, sortedIndexLocker, hashIndexId, hashIndexLocker),
                pkStorageSupplier,
                () -> Map.of(sortedIndexId, sortedIndexStorage, hashIndexId, hashIndexStorage),
                clockService,
                safeTimeClock,
                transactionStateResolver,
                new StorageUpdateHandler(
                        PART_ID,
                        partitionDataStorage,
                        indexUpdateHandler,
                        replicationConfiguration,
                        TableTestUtils.NOOP_PARTITION_MODIFICATION_COUNTER
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
                new NoOpFailureManager(),
                new TableMetricSource(QualifiedName.fromSimple("test_table"))
        );

        kvMarshaller = marshallerFor(schemaDescriptor);
        kvMarshallerVersion2 = marshallerFor(schemaDescriptorVersion2);

        when(lowWatermark.tryLock(any(), any())).thenReturn(true);

        primacyEngine = new ReplicaPrimacyEngine(placementDriver, clockService, grpId, localNode);

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

    @CartesianTest
    @CartesianTest.MethodFactory("finishedTxTypesFactory")
    void testExecuteRequestOnFinishedTx(TxState txState, RequestType requestType) {
        UUID txId = newTxId();

        txManager.updateTxMeta(txId, old -> new TxStateMeta(txState, null, null, null, null, null));

        BinaryRow testRow = binaryRow(0);

        assertThat(
                doSingleRowRequest(txId, testRow, requestType),
                willThrowFast(TransactionException.class, "Transaction is already finished")
        );
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
        ReadOnlySingleRowPkReplicaRequest request = readOnlySingleRowPkReplicaRequest(pk, readTimestamp);

        return invokeListener(request);
    }

    private CompletableFuture<ReplicaResult> invokeListener(ReplicaRequest request) {
        return processWithPrimacy(request);
    }

    private static ReplicaPrimacy validRoPrimacy() {
        return ReplicaPrimacy.forIsPrimary(true);
    }

    private ReadOnlySingleRowPkReplicaRequest readOnlySingleRowPkReplicaRequest(BinaryRow pk, HybridTimestamp readTimestamp) {
        return readOnlySingleRowPkReplicaRequest(grpId, newTxId(), localNode.id(), pk, readTimestamp);
    }

    private static ReadOnlySingleRowPkReplicaRequest readOnlySingleRowPkReplicaRequest(
            ZonePartitionId grpId,
            UUID txId,
            UUID coordinatorId,
            BinaryRow pk,
            HybridTimestamp readTimestamp
    ) {
        return TABLE_MESSAGES_FACTORY.readOnlySingleRowPkReplicaRequest()
                .groupId(zonePartitionIdMessage(grpId))
                .tableId(TABLE_ID)
                .readTimestamp(readTimestamp)
                .schemaVersion(pk.schemaVersion())
                .primaryKey(pk.tupleSlice())
                .transactionId(txId)
                .coordinatorId(coordinatorId)
                .requestType(RO_GET)
                .build();
    }

    private static ReadOnlyDirectSingleRowReplicaRequest readOnlyDirectSingleRowReplicaRequest(ZonePartitionId grpId, BinaryRow pk) {
        return TABLE_MESSAGES_FACTORY.readOnlyDirectSingleRowReplicaRequest()
                .groupId(zonePartitionIdMessage(grpId))
                .tableId(TABLE_ID)
                .schemaVersion(pk.schemaVersion())
                .primaryKey(pk.tupleSlice())
                .requestType(RO_GET)
                .enlistmentConsistencyToken(ANY_ENLISTMENT_CONSISTENCY_TOKEN)
                .build();
    }

    @Test
    public void testReadOnlySingleRowReplicaRequestCommittedResult() throws Exception {
        UUID txId = newTxId();
        BinaryRow testBinaryKey = nextBinaryKey();
        BinaryRow testBinaryRow = binaryRow(key(testBinaryKey), new TestValue(1, "v1"));
        var rowId = new RowId(PART_ID);

        pkStorage().put(testBinaryRow, rowId);
        testMvPartitionStorage.addWrite(rowId, testBinaryRow, txId, ZONE_ID, PART_ID);
        testMvPartitionStorage.commitWrite(rowId, clock.now(), txId);

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

        pkStorage().put(testBinaryRow, rowId);
        testMvPartitionStorage.addWrite(rowId, testBinaryRow, txId, ZONE_ID, PART_ID);
        txManager.updateTxMeta(txId, old -> new TxStateMeta(COMMITTED, localNode.id(), commitPartitionId, clock.now(), null, null));

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
        testMvPartitionStorage.addWrite(rowId, testBinaryRow, txId, ZONE_ID, PART_ID);
        txManager.updateTxMeta(txId, old -> new TxStateMeta(PENDING, localNode.id(), commitPartitionId, null, null, null));

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

        pkStorage().put(testBinaryRow, rowId);
        testMvPartitionStorage.addWrite(rowId, testBinaryRow, txId, ZONE_ID, PART_ID);
        txManager.updateTxMeta(txId, old -> new TxStateMeta(ABORTED, localNode.id(), commitPartitionId, null, null, null));

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

            testMvPartitionStorage.addWrite(rowId, storeRow, txId, ZONE_ID, PART_ID);
            sortedIndexStorage.storage().put(new IndexRowImpl(indexedValue, rowId));
            testMvPartitionStorage.commitWrite(rowId, clock.now(), txId);
        });

        UUID scanTxId = newTxId();

        // Request first batch
        CompletableFuture<ReplicaResult> fut = processWithPrimacy(
                TABLE_MESSAGES_FACTORY.readWriteScanRetrieveBatchReplicaRequest()
                        .groupId(zonePartitionIdMessage(grpId))
                        .tableId(TABLE_ID)
                        .transactionId(scanTxId)
                        .timestamp(clock.now())
                        .enlistmentConsistencyToken(ANY_ENLISTMENT_CONSISTENCY_TOKEN)
                        .scanId(1L)
                        .indexToUse(sortedIndexId)
                        .batchSize(4)
                        .commitPartitionId(commitPartitionId())
                        .coordinatorId(localNode.id())
                        .timestamp(clock.now())
                        .build());

        List<BinaryRow> rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS).result();

        assertNotNull(rows);
        assertEquals(4, rows.size());

        // Request second batch
        fut = processWithPrimacy(TABLE_MESSAGES_FACTORY.readWriteScanRetrieveBatchReplicaRequest()
                .groupId(zonePartitionIdMessage(grpId))
                .tableId(TABLE_ID)
                .transactionId(scanTxId)
                .timestamp(clock.now())
                .enlistmentConsistencyToken(ANY_ENLISTMENT_CONSISTENCY_TOKEN)
                .scanId(1L)
                .indexToUse(sortedIndexId)
                .batchSize(4)
                .commitPartitionId(commitPartitionId())
                .coordinatorId(localNode.id())
                .timestamp(clock.now())
                .build());

        rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS).result();

        assertNotNull(rows);
        assertEquals(2, rows.size());

        // Request bounded.
        fut = processWithPrimacy(TABLE_MESSAGES_FACTORY.readWriteScanRetrieveBatchReplicaRequest()
                .groupId(zonePartitionIdMessage(grpId))
                .tableId(TABLE_ID)
                .transactionId(newTxId())
                .timestamp(clock.now())
                .enlistmentConsistencyToken(ANY_ENLISTMENT_CONSISTENCY_TOKEN)
                .scanId(2L)
                .indexToUse(sortedIndexId)
                .lowerBoundPrefix(toIndexBound(1))
                .upperBoundPrefix(toIndexBound(3))
                .flags(SortedIndexStorage.LESS_OR_EQUAL)
                .batchSize(5)
                .commitPartitionId(commitPartitionId())
                .coordinatorId(localNode.id())
                .timestamp(clock.now())
                .build());

        rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS).result();

        assertNotNull(rows);
        assertEquals(2, rows.size());

        // Empty result.
        fut = processWithPrimacy(TABLE_MESSAGES_FACTORY.readWriteScanRetrieveBatchReplicaRequest()
                .groupId(zonePartitionIdMessage(grpId))
                .tableId(TABLE_ID)
                .transactionId(newTxId())
                .timestamp(clock.now())
                .enlistmentConsistencyToken(ANY_ENLISTMENT_CONSISTENCY_TOKEN)
                .scanId(2L)
                .indexToUse(sortedIndexId)
                .lowerBoundPrefix(toIndexBound(5))
                .batchSize(5)
                .commitPartitionId(commitPartitionId())
                .coordinatorId(localNode.id())
                .timestamp(clock.now())
                .build());

        rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS).result();

        assertNotNull(rows);
        assertEquals(0, rows.size());

        // Lookup.
        fut = processWithPrimacy(TABLE_MESSAGES_FACTORY.readWriteScanRetrieveBatchReplicaRequest()
                .groupId(zonePartitionIdMessage(grpId))
                .tableId(TABLE_ID)
                .transactionId(newTxId())
                .timestamp(clock.now())
                .enlistmentConsistencyToken(ANY_ENLISTMENT_CONSISTENCY_TOKEN)
                .scanId(2L)
                .indexToUse(sortedIndexId)
                .exactKey(toIndexKey(0))
                .batchSize(5)
                .commitPartitionId(commitPartitionId())
                .coordinatorId(localNode.id())
                .timestamp(clock.now())
                .build());

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

            testMvPartitionStorage.addWrite(rowId, storeRow, txId, ZONE_ID, PART_ID);
            sortedIndexStorage.storage().put(new IndexRowImpl(indexedValue, rowId));
            testMvPartitionStorage.commitWrite(rowId, clock.now(), txId);
        });

        UUID scanTxId = newTxId();

        // Request first batch
        CompletableFuture<ReplicaResult> fut = partitionReplicaListener.process(
                TABLE_MESSAGES_FACTORY.readOnlyScanRetrieveBatchReplicaRequest()
                        .groupId(zonePartitionIdMessage(grpId))
                        .tableId(TABLE_ID)
                        .transactionId(scanTxId)
                        .readTimestamp(clock.now())
                        .scanId(1L)
                        .indexToUse(sortedIndexId)
                        .batchSize(4)
                        .coordinatorId(localNode.id())
                        .build(), validRoPrimacy(), localNode.id());

        List<BinaryRow> rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS).result();

        assertNotNull(rows);
        assertEquals(4, rows.size());

        // Request second batch
        fut = partitionReplicaListener.process(TABLE_MESSAGES_FACTORY.readOnlyScanRetrieveBatchReplicaRequest()
                .groupId(zonePartitionIdMessage(grpId))
                .tableId(TABLE_ID)
                .transactionId(scanTxId)
                .readTimestamp(clock.now())
                .scanId(1L)
                .indexToUse(sortedIndexId)
                .batchSize(4)
                .coordinatorId(localNode.id())
                .build(), validRoPrimacy(), localNode.id());

        rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS).result();

        assertNotNull(rows);
        assertEquals(2, rows.size());

        // Request bounded.
        fut = partitionReplicaListener.process(TABLE_MESSAGES_FACTORY.readOnlyScanRetrieveBatchReplicaRequest()
                .groupId(zonePartitionIdMessage(grpId))
                .tableId(TABLE_ID)
                .transactionId(newTxId())
                .readTimestamp(clock.now())
                .scanId(2L)
                .indexToUse(sortedIndexId)
                .lowerBoundPrefix(toIndexBound(1))
                .upperBoundPrefix(toIndexBound(3))
                .flags(SortedIndexStorage.LESS_OR_EQUAL)
                .batchSize(5)
                .coordinatorId(localNode.id())
                .build(), validRoPrimacy(), localNode.id());

        rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS).result();

        assertNotNull(rows);
        assertEquals(2, rows.size());

        // Empty result.
        fut = partitionReplicaListener.process(TABLE_MESSAGES_FACTORY.readOnlyScanRetrieveBatchReplicaRequest()
                .groupId(zonePartitionIdMessage(grpId))
                .tableId(TABLE_ID)
                .transactionId(newTxId())
                .readTimestamp(clock.now())
                .scanId(2L)
                .indexToUse(sortedIndexId)
                .lowerBoundPrefix(toIndexBound(5))
                .batchSize(5)
                .coordinatorId(localNode.id())
                .build(), validRoPrimacy(), localNode.id());

        rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS).result();

        assertNotNull(rows);
        assertEquals(0, rows.size());

        // Lookup.
        fut = partitionReplicaListener.process(TABLE_MESSAGES_FACTORY.readOnlyScanRetrieveBatchReplicaRequest()
                .groupId(zonePartitionIdMessage(grpId))
                .tableId(TABLE_ID)
                .transactionId(newTxId())
                .readTimestamp(clock.now())
                .scanId(2L)
                .indexToUse(sortedIndexId)
                .exactKey(toIndexKey(0))
                .batchSize(5)
                .coordinatorId(localNode.id())
                .build(), validRoPrimacy(), localNode.id());

        rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS).result();

        assertNotNull(rows);
        assertEquals(2, rows.size());
    }

    @Test
    public void testReadOnlyScanRetrieveBatchReplicaRequestHashIndex() throws Exception {
        UUID txId = newTxId();
        int hashIndexId = hashIndexStorage.id();

        IntStream.range(0, 7).forEach(i -> {
            RowId rowId = new RowId(PART_ID);
            int indexedVal = i % 2; // Non-uniq index.
            TestValue testValue = new TestValue(indexedVal, "val" + i);

            BinaryTuple indexedValue = new BinaryTuple(1,
                    new BinaryTupleBuilder(1).appendInt(indexedVal).build());
            BinaryRow storeRow = binaryRow(key(nextBinaryKey()), testValue);

            testMvPartitionStorage.addWrite(rowId, storeRow, txId, ZONE_ID, PART_ID);
            hashIndexStorage.storage().put(new IndexRowImpl(indexedValue, rowId));
            testMvPartitionStorage.commitWrite(rowId, clock.now(), txId);
        });

        UUID scanTxId = newTxId();

        // Request first batch
        CompletableFuture<ReplicaResult> fut = partitionReplicaListener.process(
                TABLE_MESSAGES_FACTORY.readOnlyScanRetrieveBatchReplicaRequest()
                        .groupId(zonePartitionIdMessage(grpId))
                        .tableId(TABLE_ID)
                        .transactionId(scanTxId)
                        .readTimestamp(clock.now())
                        .scanId(1L)
                        .indexToUse(hashIndexId)
                        .exactKey(toIndexKey(0))
                        .batchSize(3)
                        .coordinatorId(localNode.id())
                        .build(), validRoPrimacy(), localNode.id());

        List<BinaryRow> rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS).result();

        assertNotNull(rows);
        assertEquals(3, rows.size());

        // Request second batch
        fut = partitionReplicaListener.process(TABLE_MESSAGES_FACTORY.readOnlyScanRetrieveBatchReplicaRequest()
                .groupId(zonePartitionIdMessage(grpId))
                .tableId(TABLE_ID)
                .transactionId(scanTxId)
                .readTimestamp(clock.now())
                .scanId(1L)
                .indexToUse(hashIndexId)
                .exactKey(toIndexKey(0))
                .batchSize(1)
                .coordinatorId(localNode.id())
                .build(), validRoPrimacy(), localNode.id());

        rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS).result();

        assertNotNull(rows);
        assertEquals(1, rows.size());

        // Empty result.
        fut = partitionReplicaListener.process(TABLE_MESSAGES_FACTORY.readOnlyScanRetrieveBatchReplicaRequest()
                .groupId(zonePartitionIdMessage(grpId))
                .tableId(TABLE_ID)
                .transactionId(newTxId())
                .readTimestamp(clock.now())
                .scanId(2L)
                .indexToUse(hashIndexId)
                .exactKey(toIndexKey(5))
                .batchSize(5)
                .coordinatorId(localNode.id())
                .build(), validRoPrimacy(), localNode.id());

        rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS).result();

        assertNotNull(rows);
        assertEquals(0, rows.size());

        // Lookup.
        fut = partitionReplicaListener.process(TABLE_MESSAGES_FACTORY.readOnlyScanRetrieveBatchReplicaRequest()
                .groupId(zonePartitionIdMessage(grpId))
                .tableId(TABLE_ID)
                .transactionId(newTxId())
                .readTimestamp(clock.now())
                .scanId(2L)
                .indexToUse(hashIndexId)
                .exactKey(toIndexKey(1))
                .batchSize(5)
                .coordinatorId(localNode.id())
                .build(), validRoPrimacy(), localNode.id());

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

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testCleanupOnCompactedCatalogVersion(boolean commit) {
        UUID txId = newTxId();

        BinaryRow testRow = binaryRow(0);

        assertThat(doSingleRowRequest(txId, testRow, RW_INSERT), willCompleteSuccessfully());

        when(catalogService.activeCatalog(anyLong())).thenThrow(new CatalogNotFoundException("Catalog not found"));

        assertDoesNotThrow(() -> cleanup(txId, commit));
        if (commit) {
            verify(testMvPartitionStorage, atLeastOnce()).commitWrite(any(), any(), eq(txId));
        } else {
            verify(testMvPartitionStorage, atLeastOnce()).abortWrite(any(), eq(txId));
        }
    }

    private CompletableFuture<?> doSingleRowRequest(UUID txId, BinaryRow binaryRow, RequestType requestType) {
        return doSingleRowRequest(txId, binaryRow, requestType, false);
    }

    private CompletableFuture<?> doSingleRowRequest(UUID txId, BinaryRow binaryRow, RequestType requestType, boolean full) {
        return doSingleRowRequest(txId, binaryRow, requestType, full, null);
    }

    private CompletableFuture<?> doSingleRowRequest(
            UUID txId,
            BinaryRow binaryRow,
            RequestType requestType,
            boolean full,
            @Nullable String txLabel
    ) {
        return processWithPrimacy(TABLE_MESSAGES_FACTORY.readWriteSingleRowReplicaRequest()
                        .groupId(zonePartitionIdMessage(grpId))
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
                        .txLabel(txLabel)
                        .build()
        );
    }

    private CompletableFuture<?> doSingleRowPkRequest(UUID txId, BinaryRow binaryRow, RequestType requestType) {
        return doSingleRowPkRequest(txId, binaryRow, requestType, false);
    }

    private CompletableFuture<?> doSingleRowPkRequest(UUID txId, BinaryRow binaryRow, RequestType requestType, boolean full) {
        return doSingleRowPkRequest(txId, binaryRow, requestType, full, null);
    }

    private CompletableFuture<?> doSingleRowPkRequest(
            UUID txId,
            BinaryRow binaryRow,
            RequestType requestType,
            boolean full,
            @Nullable String txLabel
    ) {
        return processWithPrimacy(TABLE_MESSAGES_FACTORY.readWriteSingleRowPkReplicaRequest()
                        .groupId(zonePartitionIdMessage(grpId))
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
                        .txLabel(txLabel)
                        .build()
        );
    }

    private static ZonePartitionIdMessage commitPartitionId() {
        return REPLICA_MESSAGES_FACTORY.zonePartitionIdMessage()
                .partitionId(PART_ID)
                .zoneId(ZONE_ID)
                .build();
    }

    static ZonePartitionIdMessage zonePartitionIdMessage(ZonePartitionId zonePartitionId) {
        return toZonePartitionIdMessage(REPLICA_MESSAGES_FACTORY, zonePartitionId);
    }

    private CompletableFuture<?> doMultiRowRequest(UUID txId, Collection<BinaryRow> binaryRows, RequestType requestType) {
        return doMultiRowRequest(txId, binaryRows, requestType, false);
    }

    private CompletableFuture<?> doMultiRowRequest(UUID txId, Collection<BinaryRow> binaryRows, RequestType requestType, boolean full) {
        return doMultiRowRequest(txId, binaryRows, requestType, full, null);
    }

    private CompletableFuture<?> doMultiRowRequest(
            UUID txId,
            Collection<BinaryRow> binaryRows,
            RequestType requestType,
            boolean full,
            @Nullable String txLabel
    ) {
        return processWithPrimacy(TABLE_MESSAGES_FACTORY.readWriteMultiRowReplicaRequest()
                        .groupId(zonePartitionIdMessage(grpId))
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
                        .txLabel(txLabel)
                        .build()
        );
    }

    static List<ByteBuffer> binaryRowsToBuffers(Collection<BinaryRow> binaryRows) {
        return binaryRows.stream().map(BinaryRow::tupleSlice).collect(toList());
    }

    private CompletableFuture<?> doMultiRowPkRequest(UUID txId, Collection<BinaryRow> binaryRows, RequestType requestType) {
        return doMultiRowPkRequest(txId, binaryRows, requestType, false);
    }

    private CompletableFuture<?> doMultiRowPkRequest(UUID txId, Collection<BinaryRow> binaryRows, RequestType requestType, boolean full) {
        return doMultiRowPkRequest(txId, binaryRows, requestType, full, null);
    }

    private CompletableFuture<?> doMultiRowPkRequest(
            UUID txId,
            Collection<BinaryRow> binaryRows,
            RequestType requestType,
            boolean full,
            @Nullable String txLabel
    ) {
        return processWithPrimacy(TABLE_MESSAGES_FACTORY.readWriteMultiRowPkReplicaRequest()
                        .groupId(zonePartitionIdMessage(grpId))
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
                        .txLabel(txLabel)
                        .build()
        );
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
            assertInstanceOf(UpdateCommand.class, partitionCommand);

            UpdateCommand impl = (UpdateCommand) partitionCommand;

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
            testMvPartitionStorage.addWrite(emptyRowId, null, tx1, ZONE_ID, PART_ID);

            if (committed) {
                testMvPartitionStorage.commitWrite(emptyRowId, clock.now(), tx1);
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
            BinaryRow res = roGet(br1Pk, clock.now());
            BinaryRow expected = committed
                    ? (upsertAfterDelete ? br1 : null)
                    : (insertFirst ? br1 : null);

            assertThat(res, is(expected == null ? nullValue(BinaryRow.class) : equalToRow(expected)));
        }

        cleanup(tx1);
    }

    private static UUID transactionIdFor(HybridTimestamp beginTimestamp) {
        return TestTransactionIds.TRANSACTION_ID_GENERATOR.transactionIdFor(beginTimestamp);
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
        testMvPartitionStorage.addWrite(rowId, futureSchemaVersionRow, futureSchemaVersionTxId, ZONE_ID, PART_ID);
        sortedIndexStorage.storage().put(new IndexRowImpl(indexedValue, rowId));
        testMvPartitionStorage.commitWrite(rowId, clock.now(), futureSchemaVersionTxId);

        return key;
    }

    private static void assertFailureDueToBackwardIncompatibleSchemaChange(
            CompletableFuture<?> future,
            AtomicReference<Boolean> committed
    ) {
        IncompatibleSchemaVersionException ex = assertWillThrowFast(future,
                IncompatibleSchemaVersionException.class);
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
        return processWithPrimacy(TABLE_MESSAGES_FACTORY.readWriteSwapRowReplicaRequest()
                        .groupId(zonePartitionIdMessage(grpId))
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
                        .build()
        );
    }

    @Test
    public void failsWhenScanByExactMatchReadsTupleWithIncompatibleSchemaFromFuture() {
        testFailsWhenReadingFromFutureIncompatibleSchema(
                (targetTxId, key) -> processWithPrimacy(
                        TABLE_MESSAGES_FACTORY.readWriteScanRetrieveBatchReplicaRequest()
                                .groupId(zonePartitionIdMessage(grpId))
                                .tableId(TABLE_ID)
                                .transactionId(targetTxId)
                                .indexToUse(sortedIndexStorage.id())
                                .exactKey(toIndexKey(FUTURE_SCHEMA_ROW_INDEXED_VALUE))
                                .enlistmentConsistencyToken(ANY_ENLISTMENT_CONSISTENCY_TOKEN)
                                .scanId(1)
                                .batchSize(100)
                                .commitPartitionId(commitPartitionId())
                                .coordinatorId(localNode.id())
                                .timestamp(clock.now())
                                .build()
                )
        );
    }

    @Test
    public void failsWhenScanByIndexReadsTupleWithIncompatibleSchemaFromFuture() {
        testFailsWhenReadingFromFutureIncompatibleSchema(
                (targetTxId, key) -> processWithPrimacy(
                        TABLE_MESSAGES_FACTORY.readWriteScanRetrieveBatchReplicaRequest()
                                .groupId(zonePartitionIdMessage(grpId))
                                .tableId(TABLE_ID)
                                .transactionId(targetTxId)
                                .indexToUse(sortedIndexStorage.id())
                                .enlistmentConsistencyToken(ANY_ENLISTMENT_CONSISTENCY_TOKEN)
                                .scanId(1)
                                .batchSize(100)
                                .commitPartitionId(commitPartitionId())
                                .coordinatorId(localNode.id())
                                .timestamp(clock.now())
                                .build()
                )
        );
    }

    @Test
    public void failsWhenScanReadsTupleWithIncompatibleSchemaFromFuture() {
        testFailsWhenReadingFromFutureIncompatibleSchema(
                (targetTxId, key) -> doRwScanRetrieveBatchRequest(targetTxId)
        );
    }

    @ParameterizedTest
    @MethodSource("txLabelParameters")
    public void testTxLabelAfterRequest(boolean readOnly, boolean pk, boolean full, boolean multiRow, boolean txLabelShouldBePresent) {
        UUID txId = newTxId();
        BinaryRow br = binaryRow(1);
        String txLabel = "my-tx-label";

        CompletableFuture<?> rrFut;

        if (readOnly) {
            rrFut = doReadOnlySingleGet(br);
        } else if (pk) {
            if (multiRow) {
                rrFut = doMultiRowPkRequest(txId, List.of(br), RW_DELETE_ALL, full, txLabel);
            } else {
                rrFut = doSingleRowPkRequest(txId, br, RW_DELETE, full, txLabel);
            }
        } else {
            if (multiRow) {
                rrFut = doMultiRowRequest(txId, List.of(br), RW_UPSERT_ALL, full, txLabel);
            } else {
                rrFut = doSingleRowRequest(txId, br, RW_UPSERT, full, txLabel);
            }
        }

        assertThat(rrFut, willCompleteSuccessfully());

        TxStateMeta txMeta = txManager.stateMeta(txId);

        if (txLabelShouldBePresent) {
            assertEquals(txLabel, txMeta.txLabel());
        } else {
            if (txMeta != null) {
                assertNull(txMeta.txLabel());
            }
        }
    }

    private static Stream<Arguments> txLabelParameters() {
        return Stream.of(
                argumentSet("read-only", true, false, false, false, false),
                argumentSet("rw pk full multi-row", false, true, true, true, false),
                argumentSet("rw pk full single-row", false, true, true, false, false),
                argumentSet("rw pk non-full multi-row", false, true, false, true, true),
                argumentSet("rw pk non-full single-row", false, true, false, false, true),
                argumentSet("rw non-pk full multi-row", false, false, true, true, false),
                argumentSet("rw non-pk full single-row", false, false, true, false, false),
                argumentSet("rw non-pk non-full multi-row", false, false, false, true, true),
                argumentSet("rw non-pk non-full single-row", false, false, false, false, true)
        );
    }

    @Test
    public void txStateMessageConversion() {
        TxStateMeta txStateMeta = TxStateMeta.builder(PENDING)
                .txCoordinatorId(UUID.randomUUID())
                .commitPartitionId(new ZonePartitionId(1, 1))
                .commitTimestamp(hybridTimestamp(1))
                .txLabel("test-tx-label")
                .build();

        assertTxStateMetaIsSame(
                txStateMeta,
                txStateMeta.toTransactionMetaMessage(REPLICA_MESSAGES_FACTORY, TX_MESSAGES_FACTORY).asTxStateMeta()
        );

        TxStateMetaAbandoned txStateMetaAbandoned = txStateMeta.abandoned();

        assertTxStateMetaIsSame(
                txStateMetaAbandoned,
                txStateMetaAbandoned.toTransactionMetaMessage(REPLICA_MESSAGES_FACTORY, TX_MESSAGES_FACTORY).asTxStateMeta()
        );
    }

    private CompletableFuture<?> doRwScanRetrieveBatchRequest(UUID targetTxId) {
        return processWithPrimacy(
                TABLE_MESSAGES_FACTORY.readWriteScanRetrieveBatchReplicaRequest()
                        .groupId(zonePartitionIdMessage(grpId))
                        .tableId(TABLE_ID)
                        .transactionId(targetTxId)
                        .enlistmentConsistencyToken(ANY_ENLISTMENT_CONSISTENCY_TOKEN)
                        .scanId(1)
                        .batchSize(100)
                        .full(false)
                        .commitPartitionId(commitPartitionId())
                        .coordinatorId(localNode.id())
                        .timestamp(clock.now())
                        .build()
        );
    }

    private CompletableFuture<?> doRwScanCloseRequest(UUID targetTxId) {
        return processWithPrimacy(
                TABLE_MESSAGES_FACTORY.scanCloseReplicaRequest()
                        .groupId(zonePartitionIdMessage(new ZonePartitionId(tableDescriptor.zoneId(), grpId.partitionId())))
                        .tableId(grpId.zoneId())
                        .transactionId(targetTxId)
                        .timestamp(beginTimestamp(targetTxId))
                        .scanId(1)
                        .build()
        );
    }

    private static ReadOnlyScanRetrieveBatchReplicaRequest readOnlyScanRetrieveBatchReplicaRequest(
            ZonePartitionId grpId,
            UUID txId,
            HybridTimestamp readTimestamp,
            UUID coordinatorId
    ) {
        return TABLE_MESSAGES_FACTORY.readOnlyScanRetrieveBatchReplicaRequest()
                .groupId(zonePartitionIdMessage(grpId))
                .tableId(TABLE_ID)
                .transactionId(txId)
                .scanId(1)
                .batchSize(100)
                .readTimestamp(readTimestamp)
                .coordinatorId(coordinatorId)
                .build();
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

        doAnswer(invocation -> nullCompletedFuture())
                .when(txManager).finish(any(), any(), anyBoolean(), anyBoolean(), anyBoolean(), anyBoolean(), any(), any());
        doAnswer(invocation -> nullCompletedFuture())
                .when(txManager).cleanup(any(), anyString(), any());
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
            IncompatibleSchemaVersionException ex = assertWillThrowFast(future, IncompatibleSchemaVersionException.class);
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
        when(tableVersion1.latestSchemaVersion()).thenReturn(CURRENT_SCHEMA_VERSION);
        when(tableVersion2.latestSchemaVersion()).thenReturn(NEXT_SCHEMA_VERSION);

        Catalog catalog1 = mock(Catalog.class);
        when(catalog1.table(TABLE_ID)).thenReturn(tableVersion1);

        Catalog catalog2 = mock(Catalog.class);
        when(catalog2.table(TABLE_ID)).thenReturn(tableVersion2);

        when(catalogService.activeCatalog(txBeginTs.longValue())).thenReturn(catalog1);
        when(catalogService.activeCatalog(gt(txBeginTs.longValue()))).thenReturn(catalog2);
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

    private void makeTableBeDroppedAfter(HybridTimestamp txBeginTs) {
        makeTableBeDroppedAfter(txBeginTs, TABLE_ID);
    }

    private void makeTableBeDroppedAfter(HybridTimestamp txBeginTs, int tableId) {
        CatalogTableDescriptor tableVersion1 = mock(CatalogTableDescriptor.class);
        when(tableVersion1.latestSchemaVersion()).thenReturn(CURRENT_SCHEMA_VERSION);
        when(tableVersion1.name()).thenReturn(TABLE_NAME);

        when(catalog.table(tableId)).thenReturn(tableVersion1);

        when(catalogService.activeCatalog(txBeginTs.longValue())).thenReturn(catalog);
        when(catalogService.activeCatalog(gt(txBeginTs.longValue()))).thenReturn(mock(Catalog.class));
    }

    @Test
    void rwScanCloseRequestSucceedsIfTableWasDropped() {
        UUID txId = newTxId();
        HybridTimestamp txBeginTs = beginTimestamp(txId);

        makeTableBeDroppedAfter(txBeginTs);

        CompletableFuture<?> future = doRwScanCloseRequest(txId);

        assertThat(future, willCompleteSuccessfully());
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
                .groupId(zonePartitionIdMessage(grpId))
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

        return processWithPrimacy(message);
    }

    private void delete(UUID txId, BinaryRow row) {
        ReadWriteSingleRowPkReplicaRequest message = TABLE_MESSAGES_FACTORY.readWriteSingleRowPkReplicaRequest()
                .groupId(zonePartitionIdMessage(grpId))
                .tableId(TABLE_ID)
                .requestType(RW_DELETE)
                .transactionId(txId)
                .schemaVersion(row.schemaVersion())
                .primaryKey(row.tupleSlice())
                .enlistmentConsistencyToken(ANY_ENLISTMENT_CONSISTENCY_TOKEN)
                .commitPartitionId(commitPartitionId())
                .coordinatorId(localNode.id())
                .timestamp(clock.now())
                .build();

        assertThat(processWithPrimacy(message), willCompleteSuccessfully());
    }

    private BinaryRow roGet(BinaryRow row, HybridTimestamp readTimestamp) {
        CompletableFuture<BinaryRow> roGetAsync = roGetAsync(row, readTimestamp);

        assertThat(roGetAsync, willCompleteSuccessfully());

        return roGetAsync.join();
    }

    private CompletableFuture<BinaryRow> roGetAsync(BinaryRow row, HybridTimestamp readTimestamp) {
        ReadOnlySingleRowPkReplicaRequest message = readOnlySingleRowPkReplicaRequest(row, readTimestamp);

        return partitionReplicaListener.process(message, validRoPrimacy(), localNode.id())
                .thenApply(replicaResult -> (BinaryRow) replicaResult.result());
    }

    private List<BinaryRow> roGetAll(Collection<BinaryRow> rows, HybridTimestamp readTimestamp) {
        CompletableFuture<ReplicaResult> future = doReadOnlyMultiGet(rows, readTimestamp);

        assertThat(future, willCompleteSuccessfully());

        return (List<BinaryRow>) future.join().result();
    }

    private CompletableFuture<ReplicaResult> doReadOnlyMultiGet(Collection<BinaryRow> rows, HybridTimestamp readTimestamp) {
        ReadOnlyMultiRowPkReplicaRequest request = readOnlyMultiRowPkReplicaRequest(grpId, newTxId(), localNode.id(), rows, readTimestamp);

        return invokeListener(request);
    }

    private static ReadOnlyMultiRowPkReplicaRequest readOnlyMultiRowPkReplicaRequest(
            ZonePartitionId grpId,
            UUID txId,
            UUID coordinatorId,
            Collection<BinaryRow> rows,
            HybridTimestamp readTimestamp
    ) {
        return TABLE_MESSAGES_FACTORY.readOnlyMultiRowPkReplicaRequest()
                .groupId(zonePartitionIdMessage(grpId))
                .tableId(TABLE_ID)
                .requestType(RO_GET_ALL)
                .readTimestamp(readTimestamp)
                .schemaVersion(rows.iterator().next().schemaVersion())
                .primaryKeys(binaryRowsToBuffers(rows))
                .transactionId(txId)
                .coordinatorId(coordinatorId)
                .build();
    }

    private static ReadOnlyDirectMultiRowReplicaRequest readOnlyDirectMultiRowReplicaRequest(
            ZonePartitionId grpId,
            Collection<BinaryRow> rows
    ) {
        return TABLE_MESSAGES_FACTORY.readOnlyDirectMultiRowReplicaRequest()
                .groupId(zonePartitionIdMessage(grpId))
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

        lockManager.releaseAll(txId);
        partitionReplicaListener.cleanupLocally(txId, commit, commitTs);
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

    /**
     * Test pojo key.
     */
    private static class TestKey {
        @IgniteToStringInclude
        int intKey;

        @IgniteToStringInclude
        String strKey;

        TestKey() {
        }

        TestKey(int intKey, String strKey) {
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
        Integer intVal;

        @IgniteToStringInclude
        String strVal;

        TestValue() {
        }

        TestValue(Integer intVal, String strVal) {
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
            assertThat(roGetAsync(row, clock.now()), willCompleteSuccessfully());
        } else {
            assertThat(upsertAsync(txId, row), willThrow(StaleTransactionOperationException.class));
        }
    }

    @Test
    void testBuildIndexReplicaRequest() {
        int indexId = hashIndexStorage.id();
        int indexCreationCatalogVersion = 1;
        int startBuildingIndexCatalogVersion = 2;
        long indexCreationActivationTs = clock.now().subtractPhysicalTime(100).longValue();
        long startBuildingIndexActivationTs = clock.nowLong();

        setIndexMetaInBuildingStatus(
                indexId,
                indexCreationCatalogVersion,
                indexCreationActivationTs,
                startBuildingIndexCatalogVersion,
                startBuildingIndexActivationTs
        );

        fireHashIndexStartBuildingEventForStaleTxOperation(indexId, startBuildingIndexCatalogVersion);

        CompletableFuture<?> invokeBuildIndexReplicaRequestFuture = invokeBuildIndexReplicaRequestAsync(indexId);

        assertThat(invokeBuildIndexReplicaRequestFuture, willCompleteSuccessfully());
        assertThat(invokeBuildIndexReplicaRequestAsync(indexId), willCompleteSuccessfully());
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
                .groupId(zonePartitionIdMessage(grpId))
                .indexId(indexId)
                .enlistmentConsistencyToken(ANY_ENLISTMENT_CONSISTENCY_TOKEN)
                .rowIds(List.of())
                .abortedTransactionIds(Set.of())
                .timestamp(clock.current())
                .build();

        return invokeListener(request);
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

    private static @Nullable TxStateResponse toTxStateResponse(@Nullable TransactionMeta transactionMeta) {
        TransactionMetaMessage transactionMetaMessage =
                transactionMeta == null ? null : transactionMeta.toTransactionMetaMessage(REPLICA_MESSAGES_FACTORY, TX_MESSAGES_FACTORY);

        return TX_MESSAGES_FACTORY.txStateResponse()
                .txStateMeta(transactionMetaMessage)
                .build();
    }

    @ParameterizedTest
    @EnumSource(NonDirectReadOnlyRequestFactory.class)
    void nonDirectReadOnlyRequestsLockLwmAndDoNotUnlockIt(NonDirectReadOnlyRequestFactory requestFactory) {
        RequestContext context = new RequestContext(grpId, newTxId(), clock, nextBinaryKey(), localNode.id());
        ReadOnlyReplicaRequest request = requestFactory.create(context);

        assertThat(invokeListener(request), willCompleteSuccessfully());

        verify(lowWatermark).tryLock(any(), eq(request.readTimestamp()));
        verify(lowWatermark, never()).unlock(any());
    }

    @Test
    void directReadOnlySingleRowRequestDoesNotLockLwm() {
        ReadOnlyDirectReplicaRequest request = readOnlyDirectSingleRowReplicaRequest(grpId, nextBinaryKey());

        assertThat(invokeListener(request), willCompleteSuccessfully());

        verify(lowWatermark, never()).tryLock(any(), any());
    }

    @Test
    void directReadOnlyMultiRowRequestWithOneKeyDoesNotLockLwm() {
        ReadOnlyDirectReplicaRequest request = readOnlyDirectMultiRowReplicaRequest(grpId, List.of(nextBinaryKey()));

        assertThat(invokeListener(request), willCompleteSuccessfully());

        verify(lowWatermark, never()).tryLock(any(), any());
    }

    @Test
    void directReadOnlyMultiRowRequestWithMultipleKeysLockAndUnlockLwm() {
        ReadOnlyDirectReplicaRequest request = readOnlyDirectMultiRowReplicaRequest(grpId, List.of(nextBinaryKey(), nextBinaryKey()));

        assertThat(invokeListener(request), willCompleteSuccessfully());

        InOrder orderVerifier = inOrder(lowWatermark, pkIndexStorage);

        ArgumentCaptor<UUID> lockTxIdCaptor = ArgumentCaptor.forClass(UUID.class);
        ArgumentCaptor<UUID> unlockTxIdCaptor = ArgumentCaptor.forClass(UUID.class);

        orderVerifier.verify(lowWatermark).tryLock(lockTxIdCaptor.capture(), any());
        orderVerifier.verify(pkIndexStorage).get(any());
        orderVerifier.verify(lowWatermark).unlock(unlockTxIdCaptor.capture());

        assertThat(unlockTxIdCaptor.getValue(), is(lockTxIdCaptor.getValue()));
    }

    @Test
    void directReadOnlyMultiRowRequestWithMultipleKeysUnlockLwmEvenWhenExceptionHappens() {
        doThrow(new RuntimeException("Oops")).when(pkIndexStorage).get(any());

        ReadOnlyDirectReplicaRequest request = readOnlyDirectMultiRowReplicaRequest(grpId, List.of(nextBinaryKey(), nextBinaryKey()));

        assertThat(invokeListener(request), willThrowFast(RuntimeException.class, "Oops"));

        verify(lowWatermark).unlock(any());
    }

    @ParameterizedTest
    @ValueSource(classes = {PrimaryReplicaRequest.class, TxStateCommitPartitionRequest.class})
    void primaryReplicaRequestsAreRejectedWhenPrimaryIsNotKnown(Class<? extends PrimaryReplicaRequest> requestClass) {
        doReturn(null).when(placementDriver).getCurrentPrimaryReplica(any(), any());
        doReturn(nullCompletedFuture()).when(placementDriver).getPrimaryReplica(any(), any());

        PrimaryReplicaRequest request = mock(requestClass);

        assertThat(processWithPrimacy(request), willThrow(PrimaryReplicaMissException.class));
    }

    @ParameterizedTest
    @ValueSource(classes = {PrimaryReplicaRequest.class, TxStateCommitPartitionRequest.class})
    void primaryReplicaRequestsAreRejectedWhenPrimaryDoesNotMatchLeaseStartTime(Class<? extends PrimaryReplicaRequest> requestClass) {
        long leaseStartTime = clock.nowLong();
        placementDriver.setPrimaryReplicaSupplier(
                () -> new TestReplicaMetaImpl(localNode, hybridTimestamp(leaseStartTime), HybridTimestamp.MAX_VALUE)
        );

        PrimaryReplicaRequest request = mock(requestClass);
        when(request.enlistmentConsistencyToken()).thenReturn(leaseStartTime - 1000);

        assertThat(processWithPrimacy(request), willThrow(PrimaryReplicaMissException.class));
    }

    @ParameterizedTest
    @ValueSource(classes = {PrimaryReplicaRequest.class, TxStateCommitPartitionRequest.class})
    void primaryReplicaRequestsAreRejectedWhenLeaseIsExpired(Class<? extends PrimaryReplicaRequest> requestClass) {
        long leaseStartTime = clock.nowLong();
        placementDriver.setPrimaryReplicaSupplier(
                () -> new TestReplicaMetaImpl(localNode, hybridTimestamp(leaseStartTime), HybridTimestamp.MIN_VALUE)
        );

        PrimaryReplicaRequest request = mock(requestClass);
        when(request.enlistmentConsistencyToken()).thenReturn(leaseStartTime);

        assertThat(processWithPrimacy(request), willThrow(PrimaryReplicaMissException.class));
    }

    @ParameterizedTest
    @MethodSource("prepareVersionsParameters")
    void processTxStatePrimaryReplicaRequestTest(
            boolean addVersionBeforeNewestCommitTs,
            boolean addVersionAfterNewestCommitTsBeforeReadTs,
            boolean addVersionEqualToReadTs,
            boolean addVersionAfterReadTs,
            boolean addWriteIntent,
            boolean writeIntentHasThisTxId,
            boolean outdatedReadTs,
            boolean newestCommitTsIsPresent,
            IgniteBiTuple<Class<Exception>, TransactionMeta> expected
    ) {
        UUID thisTxId = newTxId();
        UUID otherTxId = newTxId();

        RowId rowId = new RowId(PART_ID, 1, 1);
        BinaryRow row = binaryRow(1);

        prepareVersions(
                rowId,
                row,
                thisTxId,
                otherTxId,
                addVersionBeforeNewestCommitTs,
                addVersionAfterNewestCommitTsBeforeReadTs,
                addVersionEqualToReadTs,
                addVersionAfterReadTs,
                addWriteIntent,
                writeIntentHasThisTxId,
                newestCommitTsIsPresent
        );

        HybridTimestamp readTs = outdatedReadTs
                ? new HybridTimestamp(clockService.maxClockSkewMillis() - 1, 0)
                : new HybridTimestamp(100, 0);
        HybridTimestamp newestCommitTs = newestCommitTsIsPresent ? new HybridTimestamp(50, 0) : null;

        TxStatePrimaryReplicaRequest request = TX_MESSAGES_FACTORY.txStatePrimaryReplicaRequest()
                .groupId(zonePartitionIdMessage(grpId))
                .enlistmentConsistencyToken(ANY_ENLISTMENT_CONSISTENCY_TOKEN)
                .tableId(TABLE_ID)
                .rowId(TX_MESSAGES_FACTORY.rowIdMessage().partitionId(rowId.partitionId()).uuid(rowId.uuid()).build())
                .txId(thisTxId)
                .newestCommitTimestamp(newestCommitTs)
                .readTimestamp(readTs)
                .build();

        CompletableFuture<ReplicaResult> fut = processWithPrimacy(request);

        if (expected.get1() != null) {
            try {
                assertThrowsWithCause(fut::join, expected.get1());
            } catch (AssertionError e) {
                log.info("Actual state: " + fut.join().result());
                throw e;
            }
        } else {
            assertThat(fut, willCompleteSuccessfully());
            TransactionMeta expectedMeta = expected.get2();
            ReplicaResult res = fut.join();

            TransactionMeta meta = (TransactionMeta) res.result();
            assertEquals(expectedMeta.txState(), meta.txState());

            if (expectedMeta.txState() == COMMITTED) {
                assertEquals(expectedMeta.commitTimestamp(), meta.commitTimestamp());
            }
        }
    }

    private void prepareVersions(
            RowId rowId,
            BinaryRow row,
            UUID thisTxId,
            UUID otherTxId,
            boolean addVersionBeforeNewestCommitTs,
            boolean addVersionAfterNewestCommitTsBeforeReadTs,
            boolean addVersionEqualToReadTs,
            boolean addVersionAfterReadTs,
            boolean addWriteIntent,
            boolean writeIntentHasThisTxId,
            boolean newestCommitTsIsPresent
    ) {
        if (newestCommitTsIsPresent) {
            if (addVersionBeforeNewestCommitTs) {
                addWriteAndCommit(rowId, row, otherTxId, new HybridTimestamp(20, 0));
            }

            addWriteAndCommit(rowId, row, otherTxId, new HybridTimestamp(50, 0));
        }

        if (addVersionAfterNewestCommitTsBeforeReadTs) {
            addWriteAndCommit(rowId, row, otherTxId, new HybridTimestamp(70, 0));
        }

        if (addVersionEqualToReadTs) {
            addWriteAndCommit(rowId, row, otherTxId, new HybridTimestamp(100, 0));
        }

        if (addVersionAfterReadTs) {
            addWriteAndCommit(rowId, row, otherTxId, new HybridTimestamp(150, 0));
        }

        if (addWriteIntent) {
            UUID txId = writeIntentHasThisTxId ? thisTxId : otherTxId;
            testMvPartitionStorage.addWrite(rowId, row, txId, 1, 1);
        }
    }

    private void addWriteAndCommit(RowId rowId, BinaryRow row, UUID txId, HybridTimestamp commitTs) {
        testMvPartitionStorage.addWrite(rowId, row, txId, ZONE_ID, PART_ID);
        testMvPartitionStorage.commitWrite(rowId, commitTs, txId);
    }

    private static Stream<Arguments> prepareVersionsParameters() {
        boolean[] noCommittedVersions = { false, false, false, false };
        boolean[] committedVersions = { true, true, true, true };
        boolean[] commitTsBeforeReadTs = { false, true, false, false };
        boolean[] commitTsBeforeReadTsWithEarlyHistory = { true, true, false, false };
        boolean[] commitTsEqualToReadTs = { false, false, true, false };
        boolean[] commitTsAfterReadTs = { false, false, false, true };
        boolean[] commitTsBeforeAndAfterReadTs = { false, true, false, true };

        boolean[] noWriteIntent = { false, false };
        boolean[] writeIntentThisTx = { true, true };
        boolean[] writeIntentAnotherTx = { true, false };

        boolean roOk = false;
        boolean roOutdated = true;

        boolean newestAbsent = false;
        boolean newestPresent = true;

        var outdatedError = new IgniteBiTuple<>(OutdatedReadOnlyTransactionInternalException.class, null);
        var pending = expectedTxStateArg(PENDING, null);
        var aborted = expectedTxStateArg(ABORTED, null);
        var committedBeforeReadTs = expectedTxStateArg(COMMITTED, new HybridTimestamp(70, 0));
        var committedEqualToReadTs = expectedTxStateArg(COMMITTED, new HybridTimestamp(100, 0));
        var unknown = expectedTxStateArg(UNKNOWN, null);

        return Stream.of(
                argumentSet(
                        "outdated ro txn, no committed versions",
                        flatArray(noCommittedVersions, noWriteIntent, roOutdated, newestAbsent, outdatedError)
                ),
                argumentSet(
                        "outdated ro txn, no committed versions, write intent of another tx",
                        flatArray(noCommittedVersions, writeIntentAnotherTx, roOutdated, newestAbsent, outdatedError)
                ),
                argumentSet(
                        "outdated ro txn, committed versions",
                        flatArray(committedVersions, noWriteIntent, roOutdated, newestAbsent, outdatedError)
                ),
                argumentSet(
                        "empty result",
                        flatArray(noCommittedVersions, noWriteIntent, roOk, newestAbsent, aborted)
                ),
                argumentSet(
                        "write intent of this tx, no committed versions",
                        flatArray(noCommittedVersions, writeIntentThisTx, roOk, newestAbsent, pending)
                ),
                argumentSet(
                        "write intent of another tx, no committed versions",
                        flatArray(noCommittedVersions, writeIntentAnotherTx, roOk, newestAbsent, aborted)
                ),
                argumentSet(
                        "committed write intent, commit ts before read ts",
                        flatArray(commitTsBeforeReadTs, noWriteIntent, roOk, newestAbsent, committedBeforeReadTs)
                ),
                argumentSet(
                        "committed write intent, commit ts before read ts, early history",
                        flatArray(commitTsBeforeReadTsWithEarlyHistory, noWriteIntent, roOk, newestAbsent, committedBeforeReadTs)
                ),
                argumentSet(
                        "committed write intent, commit ts equal to read ts",
                        flatArray(commitTsEqualToReadTs, noWriteIntent, roOk, newestAbsent, committedEqualToReadTs)
                ),
                argumentSet(
                        "committed write intent, commit ts after read ts",
                        flatArray(commitTsAfterReadTs, noWriteIntent, roOk, newestAbsent, unknown)
                ),
                argumentSet(
                        "committed write intent, commit ts before and after read ts",
                        flatArray(commitTsBeforeAndAfterReadTs, noWriteIntent, roOk, newestAbsent, committedBeforeReadTs)
                ),
                argumentSet(
                        "newest commitTs present, committed write intent, commit ts before read ts",
                        flatArray(commitTsBeforeReadTs, noWriteIntent, roOk, newestPresent, committedBeforeReadTs)
                ),
                argumentSet(
                        "newest commitTs present, early history, committed write intent, commit ts before read ts",
                        flatArray(commitTsBeforeReadTsWithEarlyHistory, noWriteIntent, roOk, newestPresent, committedBeforeReadTs)
                ),
                argumentSet(
                        "newest commitTs present, committed write intent, commit ts equal to read ts",
                        flatArray(commitTsEqualToReadTs, noWriteIntent, roOk, newestPresent, committedEqualToReadTs)
                ),
                argumentSet(
                        "newest commitTs present, committed write intent, commit ts after read ts",
                        flatArray(commitTsAfterReadTs, noWriteIntent, roOk, newestPresent, unknown)
                ),
                argumentSet(
                        "newest commitTs present, committed write intent, commit ts before and after read ts",
                        flatArray(commitTsBeforeAndAfterReadTs, noWriteIntent, roOk, newestPresent, committedBeforeReadTs)
                ),
                argumentSet(
                        "newest commitTs present, write intent of this tx",
                        flatArray(noCommittedVersions, writeIntentThisTx, roOk, newestPresent, pending)
                ),
                argumentSet(
                        "newest commitTs present, write intent of another tx",
                        flatArray(noCommittedVersions, writeIntentAnotherTx, roOk, newestPresent, aborted)
                ),
                argumentSet(
                        "newest commitTs present, write intent of another tx, commit ts before read ts",
                        flatArray(commitTsBeforeReadTs, writeIntentAnotherTx, roOk, newestPresent, committedBeforeReadTs)
                ),
                argumentSet(
                        "newest commitTs present, early history, write intent of another tx, commit ts before read ts",
                        flatArray(commitTsBeforeReadTsWithEarlyHistory, writeIntentAnotherTx, roOk, newestPresent, committedBeforeReadTs)
                ),
                argumentSet(
                        "newest commitTs present, write intent of another tx, commit ts equal to read ts",
                        flatArray(commitTsEqualToReadTs, writeIntentAnotherTx, roOk, newestPresent, committedEqualToReadTs)
                ),
                argumentSet(
                        "newest commitTs present, write intent of another tx, commit ts after read ts",
                        flatArray(commitTsAfterReadTs, writeIntentAnotherTx, roOk, newestPresent, unknown)
                ),
                argumentSet(
                        "newest commitTs present, write intent of another tx, commit ts after before and after read ts",
                        flatArray(commitTsBeforeAndAfterReadTs, writeIntentAnotherTx, roOk, newestPresent,
                                committedBeforeReadTs)
                )
        );
    }

    private static IgniteBiTuple<Class<Exception>, TransactionMeta> expectedTxStateArg(
            TxState txState,
            @Nullable HybridTimestamp commitTs
    ) {
        return new IgniteBiTuple<>(null, TxStateMeta.builder(txState).commitTimestamp(commitTs).build());
    }

    private CompletableFuture<ReplicaResult> processWithPrimacy(ReplicaRequest request) {
        return primacyEngine.validatePrimacy(request)
                .thenCompose(primacy -> partitionReplicaListener.process(request, primacy, localNode.id()));
    }

    private static class RequestContext {
        private final ZonePartitionId groupId;
        private final UUID txId;
        private final HybridClock clock;
        private final BinaryRow key;
        private final UUID coordinatorId;

        private RequestContext(ZonePartitionId groupId, UUID txId, HybridClock clock, BinaryRow key, UUID coordinatorId) {
            this.groupId = groupId;
            this.txId = txId;
            this.clock = clock;
            this.key = key;
            this.coordinatorId = coordinatorId;
        }
    }

    private enum NonDirectReadOnlyRequestFactory {
        SINGLE_GET(context -> readOnlySingleRowPkReplicaRequest(
                context.groupId,
                context.txId,
                context.coordinatorId,
                context.key,
                context.clock.now())
        ),
        MULTI_GET(context -> readOnlyMultiRowPkReplicaRequest(
                context.groupId,
                context.txId,
                context.coordinatorId,
                singletonList(context.key),
                context.clock.now()
        )),
        SCAN(context -> readOnlyScanRetrieveBatchReplicaRequest(context.groupId, context.txId, context.clock.now(), context.coordinatorId));

        private final Function<RequestContext, ReadOnlyReplicaRequest> requestFactory;

        NonDirectReadOnlyRequestFactory(Function<RequestContext, ReadOnlyReplicaRequest> requestFactory) {
            this.requestFactory = requestFactory;
        }

        ReadOnlyReplicaRequest create(RequestContext context) {
            return requestFactory.apply(context);
        }
    }
}
