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

package org.apache.ignite.internal.table.distributed.raft;

import static java.util.Collections.emptySet;
import static java.util.Collections.singletonMap;
import static java.util.UUID.randomUUID;
import static org.apache.ignite.internal.replicator.message.ReplicaMessageUtils.toTablePartitionIdMessage;
import static org.apache.ignite.internal.table.distributed.index.MetaIndexStatus.BUILDING;
import static org.apache.ignite.internal.table.distributed.index.MetaIndexStatus.REGISTERED;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.deriveUuidFrom;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.ignite.distributed.TestPartitionDataStorage;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.command.BuildIndexCommand;
import org.apache.ignite.internal.partition.replicator.network.command.BuildIndexCommandV3;
import org.apache.ignite.internal.partition.replicator.network.command.TimedBinaryRowMessage;
import org.apache.ignite.internal.partition.replicator.network.command.UpdateAllCommandV2;
import org.apache.ignite.internal.partition.replicator.network.command.UpdateCommandV2;
import org.apache.ignite.internal.partition.replicator.network.command.WriteIntentSwitchCommandV2;
import org.apache.ignite.internal.partition.replicator.network.replication.BinaryRowMessage;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionDataStorage;
import org.apache.ignite.internal.placementdriver.LeasePlacementDriver;
import org.apache.ignite.internal.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.raft.RaftGroupConfigurationConverter;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.replicator.message.PrimaryReplicaChangeCommand;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.TablePartitionIdMessage;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowConverter;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.impl.TestMvPartitionStorage;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor.StorageHashIndexColumnDescriptor;
import org.apache.ignite.internal.storage.index.impl.TestHashIndexStorage;
import org.apache.ignite.internal.storage.util.LockByRowId;
import org.apache.ignite.internal.table.TableTestUtils;
import org.apache.ignite.internal.table.distributed.StorageUpdateHandler;
import org.apache.ignite.internal.table.distributed.TableSchemaAwareIndexStorage;
import org.apache.ignite.internal.table.distributed.index.IndexMeta;
import org.apache.ignite.internal.table.distributed.index.IndexMetaStorage;
import org.apache.ignite.internal.table.distributed.index.IndexUpdateHandler;
import org.apache.ignite.internal.table.distributed.index.MetaIndexStatus;
import org.apache.ignite.internal.table.distributed.index.MetaIndexStatusChange;
import org.apache.ignite.internal.table.distributed.raft.handlers.BuildIndexCommandHandler;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.internal.table.impl.DummySchemaManagerImpl;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.test.TestTransactionIds;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for the table command listener.
 */
@ExtendWith(WorkDirectoryExtension.class)
@ExtendWith(MockitoExtension.class)
@ExtendWith(ConfigurationExtension.class)
@ExtendWith(ExecutorServiceExtension.class)
public class PartitionCommandListenerTest extends BaseIgniteAbstractTest {
    private static final int KEY_COUNT = 100;

    private static final int TABLE_ID = 1;

    private static final int PARTITION_ID = 0;

    private static final int ZONE_ID = 2;

    private static final SchemaDescriptor SCHEMA = new SchemaDescriptor(
            1,
            new Column[]{new Column("key", NativeTypes.INT32, false)},
            new Column[]{new Column("value", NativeTypes.INT32, false)}
    );

    private static final SchemaRegistry SCHEMA_REGISTRY = new DummySchemaManagerImpl(SCHEMA);

    private TablePartitionProcessor commandListener;

    private final AtomicLong raftIndex = new AtomicLong();

    private final TableSchemaAwareIndexStorage pkStorage = new TableSchemaAwareIndexStorage(
            1,
            new TestHashIndexStorage(
                    PARTITION_ID,
                    new StorageHashIndexDescriptor(
                            TABLE_ID,
                            List.of(new StorageHashIndexColumnDescriptor("key", NativeTypes.INT32, false)),
                            false
                    )
            ),
            BinaryRowConverter.keyExtractor(SCHEMA)
    );

    private final BooleanSupplier shouldRelease = mock(BooleanSupplier.class, "shouldRelease");

    private final MvPartitionStorage mvPartitionStorage = spy(new TestMvPartitionStorage(PARTITION_ID, new LockByRowId(), shouldRelease));

    private final PartitionDataStorage partitionDataStorage = spy(new TestPartitionDataStorage(TABLE_ID, PARTITION_ID, mvPartitionStorage));

    private static final PartitionReplicationMessagesFactory PARTITION_REPLICATION_MESSAGES_FACTORY =
            new PartitionReplicationMessagesFactory();

    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    private final HybridClock hybridClock = new HybridClockImpl();

    private final RaftGroupConfigurationConverter raftGroupConfigurationConverter = new RaftGroupConfigurationConverter();

    private IndexUpdateHandler indexUpdateHandler;

    private StorageUpdateHandler storageUpdateHandler;

    @InjectConfiguration
    private ReplicationConfiguration replicationConfiguration;

    private CatalogService catalogService;

    private IndexMetaStorage indexMetaStorage;

    private ClusterService clusterService;

    /**
     * Initializes a table listener before tests.
     */
    @BeforeEach
    public void before() {
        NetworkAddress addr = new NetworkAddress("127.0.0.1", 5003);

        clusterService = mock(ClusterService.class, RETURNS_DEEP_STUBS);

        when(clusterService.topologyService().localMember().address()).thenReturn(addr);
        when(clusterService.topologyService().localMember().id()).thenReturn(deriveUuidFrom(addr.toString()));
        when(clusterService.nodeName()).thenReturn(addr.toString());

        int indexId = pkStorage.id();

        indexUpdateHandler = spy(new IndexUpdateHandler(
                DummyInternalTableImpl.createTableIndexStoragesSupplier(Map.of(indexId, pkStorage))
        ));

        storageUpdateHandler = spy(new StorageUpdateHandler(
                PARTITION_ID,
                partitionDataStorage,
                indexUpdateHandler,
                replicationConfiguration,
                TableTestUtils.NOOP_PARTITION_MODIFICATION_COUNTER
        ));

        catalogService = mock(CatalogService.class);

        Catalog catalog = mock(Catalog.class);

        CatalogIndexDescriptor indexDescriptor = mock(CatalogIndexDescriptor.class);

        lenient().when(catalog.index(indexId)).thenReturn(indexDescriptor);
        lenient().when(catalogService.catalog(anyInt())).thenReturn(catalog);
        lenient().when(catalogService.activeCatalog(anyLong())).thenReturn(catalog);

        indexDescriptor = mock(CatalogIndexDescriptor.class);

        lenient().when(indexDescriptor.id()).thenReturn(indexId);
        lenient().when(catalog.indexes(anyInt())).thenReturn(List.of(indexDescriptor));
        lenient().when(catalog.index(anyInt())).thenReturn(indexDescriptor);

        CatalogTableDescriptor tableDescriptor = mock(CatalogTableDescriptor.class);

        int tableVersion = SCHEMA.version();

        lenient().when(tableDescriptor.latestSchemaVersion()).thenReturn(tableVersion);
        lenient().when(catalog.table(anyInt())).thenReturn(tableDescriptor);

        indexMetaStorage = mock(IndexMetaStorage.class);

        IndexMeta indexMeta = createIndexMeta(indexId, tableVersion);

        lenient().when(indexMetaStorage.indexMeta(eq(indexId))).thenReturn(indexMeta);

        LeasePlacementDriver placementDriver = mock(LeasePlacementDriver.class);
        lenient().when(placementDriver.getCurrentPrimaryReplica(any(), any())).thenReturn(null);

        ClockService clockService = mock(ClockService.class);
        lenient().when(clockService.current()).thenReturn(hybridClock.current());

        commandListener = new TablePartitionProcessor(
                mock(TxManager.class),
                partitionDataStorage,
                storageUpdateHandler,
                catalogService,
                SCHEMA_REGISTRY,
                indexMetaStorage,
                clusterService.topologyService().localMember().id(),
                mock(MinimumRequiredTimeCollectorService.class),
                placementDriver,
                clockService,
                new ZonePartitionId(ZONE_ID, PARTITION_ID)
        );

        // Update(All)Command handling requires both information about raft group topology and the primary replica,
        // thus onConfigurationCommited and primaryReplicaChangeCommand are called.
        {
            long index = raftIndex.incrementAndGet();
            commandListener.onConfigurationCommitted(
                    new RaftGroupConfiguration(
                            index,
                            1,
                            111L,
                            110L,
                            List.of(clusterService.nodeName()),
                            Collections.emptyList(),
                            null,
                            null
                    ),
                    index,
                    1
            );

            PrimaryReplicaChangeCommand command = REPLICA_MESSAGES_FACTORY.primaryReplicaChangeCommand()
                    .primaryReplicaNodeName("primary")
                    .primaryReplicaNodeId(randomUUID())
                    .leaseStartTime(HybridTimestamp.MIN_VALUE.addPhysicalTime(1).longValue())
                    .build();

            commandListener.processCommand(command, raftIndex.incrementAndGet(), 1, null);
        }

        // Do it so that in actual tests we can verify only interactions that are done in test methods.
        clearInvocations(partitionDataStorage);
    }

    /**
     * Inserts rows and checks them.
     */
    @Test
    public void testInsertCommands() {
        readAndCheck(false);

        insert();

        readAndCheck(true);

        delete();
    }

    /**
     * Upserts rows and checks them.
     */
    @Test
    public void testUpdateValues() {
        readAndCheck(false);

        insert();

        readAndCheck(true);

        update(integer -> integer + 1);

        readAndCheck(true, integer -> integer + 1);

        delete();

        readAndCheck(false);
    }

    /**
     * The test checks a batch upsert command.
     */
    @Test
    public void testUpsertRowsBatchedAndCheck() {
        readAndCheck(false);

        insertAll();

        readAndCheck(true);

        updateAll(integer -> integer + 2);

        readAndCheck(true, integer -> integer + 2);

        deleteAll();

        readAndCheck(false);
    }

    /**
     * The test checks a batch insert command.
     */
    @Test
    public void testInsertRowsBatchedAndCheck() {
        readAndCheck(false);

        insertAll();

        readAndCheck(true);

        deleteAll();

        readAndCheck(false);
    }

    @ParameterizedTest
    @MethodSource("allCommandsUpdatingLastAppliedIndex")
    void updatesLastAppliedForCommandsUpdatingLastAppliedIndex(WriteCommand command) {
        commandListener.processCommand(command, 3, 2, hybridClock.now());

        verify(mvPartitionStorage).lastApplied(3, 2);
    }

    private static UpdateCommandV2 updateCommand() {
        return PARTITION_REPLICATION_MESSAGES_FACTORY.updateCommandV2()
                .rowUuid(randomUUID())
                .tableId(TABLE_ID)
                .commitPartitionId(defaultPartitionIdMessage())
                .txCoordinatorId(randomUUID())
                .txId(TestTransactionIds.newTransactionId())
                .initiatorTime(anyTime())
                .build();
    }

    private static UpdateAllCommandV2 updateAllCommand() {
        return PARTITION_REPLICATION_MESSAGES_FACTORY.updateAllCommandV2()
                .messageRowsToUpdate(singletonMap(
                        randomUUID(),
                        PARTITION_REPLICATION_MESSAGES_FACTORY.timedBinaryRowMessage().build())
                )
                .tableId(TABLE_ID)
                .commitPartitionId(defaultPartitionIdMessage())
                .txCoordinatorId(randomUUID())
                .txId(TestTransactionIds.newTransactionId())
                .initiatorTime(anyTime())
                .build();
    }

    private static Stream<Arguments> allCommandsUpdatingLastAppliedIndex() {
        return Stream.of(
                updateCommand(),
                updateAllCommand(),
                writeIntentSwitchCommand(),
                primaryReplicaChangeCommand(),
                buildIndexCommand()
        ).map(Arguments::of);
    }

    private static PrimaryReplicaChangeCommand primaryReplicaChangeCommand() {
        return new ReplicaMessagesFactory().primaryReplicaChangeCommand()
                .primaryReplicaNodeId(randomUUID())
                .primaryReplicaNodeName("test-node")
                .build();
    }

    private static BuildIndexCommandV3 buildIndexCommand() {
        return PARTITION_REPLICATION_MESSAGES_FACTORY.buildIndexCommandV3()
                .tableId(TABLE_ID)
                .indexId(1)
                .rowIds(List.of(randomUUID()))
                .abortedTransactionIds(emptySet())
                .build();
    }

    private static HybridTimestamp anyTime() {
        return HybridTimestamp.MIN_VALUE.addPhysicalTime(1000);
    }

    @Test
    void updatesGroupConfigurationOnConfigCommit() {
        long index = raftIndex.incrementAndGet();

        commandListener.onConfigurationCommitted(
                new RaftGroupConfiguration(
                        index,
                        2,
                        111L,
                        110L,
                        List.of("peer"),
                        List.of("learner"),
                        List.of("old-peer"),
                        List.of("old-learner")
                ),
                index,
                2
        );

        RaftGroupConfiguration expectedConfig = new RaftGroupConfiguration(
                index,
                2,
                111L,
                110L,
                List.of("peer"),
                List.of("learner"),
                List.of("old-peer"),
                List.of("old-learner")
        );
        verify(mvPartitionStorage).committedGroupConfiguration(
                raftGroupConfigurationConverter.toBytes(expectedConfig)
        );
    }

    @Test
    void updatesLastAppliedIndexAndTermOnConfigCommit() {
        commandListener.onConfigurationCommitted(
                new RaftGroupConfiguration(
                        3,
                        2,
                        111L,
                        110L,
                        List.of("peer"),
                        List.of("learner"),
                        List.of("old-peer"),
                        List.of("old-learner")
                ),
                3,
                2
        );

        verify(mvPartitionStorage).lastApplied(3, 2);
    }

    @Test
    void skipsUpdatesOnConfigCommitIfIndexIsStale() {
        mvPartitionStorage.lastApplied(10, 3);

        commandListener.onConfigurationCommitted(
                new RaftGroupConfiguration(
                    1, 2, 111L, 110L, List.of("peer"), List.of("learner"), List.of("old-peer"), List.of("old-learner")
                ),
                1,
                2
        );

        // Exact one call is expected because it's done in @BeforeEach in order to prepare initial configuration.
        verify(mvPartitionStorage, times(1)).committedGroupConfiguration(any());
        verify(mvPartitionStorage, times(1)).lastApplied(eq(1L), anyLong());
    }

    @Test
    void testBuildIndexCommand() {
        int indexId = pkStorage.id();

        when(indexUpdateHandler.getNextRowIdToBuildIndex(anyInt())).thenReturn(RowId.lowestRowId(PARTITION_ID));
        doNothing().when(indexUpdateHandler).buildIndex(eq(indexId), any(Stream.class), any());

        RowId row0 = new RowId(PARTITION_ID);
        RowId row1 = new RowId(PARTITION_ID);
        RowId row2 = new RowId(PARTITION_ID);

        InOrder inOrder = inOrder(partitionDataStorage, indexUpdateHandler);

        commandListener.processCommand(createBuildIndexCommand(indexId, List.of(row0.uuid()), false), 10, 1, null);

        inOrder.verify(indexUpdateHandler).buildIndex(eq(indexId), any(Stream.class), eq(row0.increment()));
        inOrder.verify(partitionDataStorage).lastApplied(10, 1);

        commandListener.processCommand(createBuildIndexCommand(indexId, List.of(row1.uuid()), true), 20, 2, null);

        inOrder.verify(indexUpdateHandler).buildIndex(eq(indexId), any(Stream.class), eq(null));
        inOrder.verify(partitionDataStorage).lastApplied(20, 2);

        // Let's check that the command with a lower commandIndex than in the storage will not be executed.
        commandListener.processCommand(createBuildIndexCommand(indexId, List.of(row2.uuid()), false), 5, 1, null);

        inOrder.verify(indexUpdateHandler, never()).buildIndex(eq(indexId), any(Stream.class), eq(row2.increment()));
        inOrder.verify(partitionDataStorage, never()).lastApplied(5, 1);
    }

    /**
     * Tests that {@link BuildIndexCommandHandler} exits early when storage engine needs resources.
     *
     * <p>This test verifies that the index building process can be interrupted when {@code shouldRelease()}
     * returns {@code true}, allowing the storage engine to perform critical operations like checkpoints.
     */
    @Test
    void testBuildIndexCommandExitsEarlyOnShouldRelease() {
        int indexId = pkStorage.id();

        // Create a new command listener with the custom storage
        LeasePlacementDriver placementDriver = mock(LeasePlacementDriver.class);
        lenient().when(placementDriver.getCurrentPrimaryReplica(any(), any())).thenReturn(null);

        ClockService clockService = mock(ClockService.class);
        lenient().when(clockService.current()).thenReturn(hybridClock.current());

        when(indexUpdateHandler.getNextRowIdToBuildIndex(anyInt())).thenReturn(RowId.lowestRowId(PARTITION_ID));
        doNothing().when(indexUpdateHandler).buildIndex(eq(indexId), any(Stream.class), any());

        RowId row0 = new RowId(PARTITION_ID);
        RowId row1 = new RowId(PARTITION_ID);
        RowId row2 = new RowId(PARTITION_ID);

        // Add some rows to the storage
        mvPartitionStorage.runConsistently(locker -> {
            mvPartitionStorage.addWriteCommitted(row0, null, hybridClock.now());
            mvPartitionStorage.addWriteCommitted(row1, null, hybridClock.now());
            mvPartitionStorage.addWriteCommitted(row2, null, hybridClock.now());
            return null;
        });

        // When: Command is processed with shouldRelease returning true before processing
        when(shouldRelease.getAsBoolean()).thenReturn(true, false);
        commandListener.processCommand(
                createBuildIndexCommand(indexId, List.of(row0.uuid(), row1.uuid(), row2.uuid()), true),
                20,
                2,
                null
        );

        // Then: Command checks should release and finishes with null nextRowIdToBuild
        verify(shouldRelease, atLeastOnce()).getAsBoolean();
        verify(indexUpdateHandler, atLeast(2)).buildIndex(eq(indexId), any(Stream.class), any());
        verify(indexUpdateHandler, times(1)).buildIndex(eq(indexId), any(Stream.class), Mockito.isNull(RowId.class));
        verify(mvPartitionStorage, times(1)).lastApplied(20, 2);
    }

    private BuildIndexCommand createBuildIndexCommand(int indexId, List<UUID> rowUuids, boolean finish) {
        return PARTITION_REPLICATION_MESSAGES_FACTORY.buildIndexCommandV2()
                .indexId(indexId)
                .tableId(TABLE_ID)
                .rowIds(rowUuids)
                .finish(finish)
                .build();
    }

    private static WriteIntentSwitchCommandV2 writeIntentSwitchCommand() {
        return PARTITION_REPLICATION_MESSAGES_FACTORY.writeIntentSwitchCommandV2()
                .txId(TestTransactionIds.newTransactionId())
                .tableIds(Set.of(1))
                .initiatorTime(anyTime())
                .safeTime(anyTime())
                .build();
    }

    /**
     * Inserts all rows.
     */
    private void insertAll() {
        Map<UUID, TimedBinaryRowMessage> rows = new HashMap<>(KEY_COUNT);
        UUID txId = TestTransactionIds.newTransactionId();
        var commitPartId = new TablePartitionId(TABLE_ID, PARTITION_ID);

        for (int i = 0; i < KEY_COUNT; i++) {
            rows.put(
                    TestTransactionIds.newTransactionId(),
                    PARTITION_REPLICATION_MESSAGES_FACTORY.timedBinaryRowMessage()
                            .binaryRowMessage(getTestRow(i, i))
                            .build()
            );
        }

        HybridTimestamp commitTimestamp = hybridClock.now();

        invokeBatchedCommand(PARTITION_REPLICATION_MESSAGES_FACTORY.updateAllCommandV2()
                .tableId(TABLE_ID)
                .commitPartitionId(toTablePartitionIdMessage(REPLICA_MESSAGES_FACTORY, commitPartId))
                .messageRowsToUpdate(rows)
                .txId(txId)
                .initiatorTime(hybridClock.now())
                .safeTime(hybridClock.now())
                .txCoordinatorId(randomUUID())
                .build());

        invokeBatchedCommand(PARTITION_REPLICATION_MESSAGES_FACTORY.writeIntentSwitchCommandV2()
                .txId(txId)
                .commit(true)
                .commitTimestamp(commitTimestamp)
                .tableIds(Set.of(commitPartId.tableId()))
                .initiatorTime(hybridClock.now())
                .safeTime(hybridClock.now())
                .build()
        );
    }

    /**
     * Update values from the listener in the batch operation.
     *
     * @param keyValueMapper Map a value to update to the iter number.
     */
    private void updateAll(Function<Integer, Integer> keyValueMapper) {
        UUID txId = TestTransactionIds.newTransactionId();
        var commitPartId = new TablePartitionId(TABLE_ID, PARTITION_ID);
        Map<UUID, TimedBinaryRowMessage> rows = new HashMap<>(KEY_COUNT);

        for (int i = 0; i < KEY_COUNT; i++) {
            ReadResult readResult = readRow(getTestKey(i));

            rows.put(readResult.rowId().uuid(),
                    PARTITION_REPLICATION_MESSAGES_FACTORY.timedBinaryRowMessage()
                            .binaryRowMessage(getTestRow(i, keyValueMapper.apply(i)))
                            .build()
            );
        }

        HybridTimestamp commitTimestamp = hybridClock.now();

        invokeBatchedCommand(PARTITION_REPLICATION_MESSAGES_FACTORY.updateAllCommandV2()
                .tableId(TABLE_ID)
                .commitPartitionId(toTablePartitionIdMessage(REPLICA_MESSAGES_FACTORY, commitPartId))
                .messageRowsToUpdate(rows)
                .txId(txId)
                .initiatorTime(hybridClock.now())
                .safeTime(hybridClock.now())
                .txCoordinatorId(randomUUID())
                .build());

        invokeBatchedCommand(PARTITION_REPLICATION_MESSAGES_FACTORY.writeIntentSwitchCommandV2()
                .txId(txId)
                .commit(true)
                .commitTimestamp(commitTimestamp)
                .tableIds(Set.of(commitPartId.tableId()))
                .initiatorTime(hybridClock.now())
                .safeTime(hybridClock.now())
                .build()
        );
    }

    /**
     * Deletes all rows.
     */
    private void deleteAll() {
        UUID txId = TestTransactionIds.newTransactionId();
        var commitPartId = new TablePartitionId(TABLE_ID, PARTITION_ID);
        Map<UUID, TimedBinaryRowMessage> keyRows = new HashMap<>(KEY_COUNT);

        for (int i = 0; i < KEY_COUNT; i++) {
            ReadResult readResult = readRow(getTestKey(i));

            keyRows.put(readResult.rowId().uuid(), PARTITION_REPLICATION_MESSAGES_FACTORY.timedBinaryRowMessage()
                    .build());
        }

        HybridTimestamp commitTimestamp = hybridClock.now();

        invokeBatchedCommand(PARTITION_REPLICATION_MESSAGES_FACTORY.updateAllCommandV2()
                .tableId(TABLE_ID)
                .commitPartitionId(toTablePartitionIdMessage(REPLICA_MESSAGES_FACTORY, commitPartId))
                .messageRowsToUpdate(keyRows)
                .txId(txId)
                .initiatorTime(hybridClock.now())
                .safeTime(hybridClock.now())
                .txCoordinatorId(randomUUID())
                .build());

        invokeBatchedCommand(PARTITION_REPLICATION_MESSAGES_FACTORY.writeIntentSwitchCommandV2()
                .txId(txId)
                .commit(true)
                .commitTimestamp(commitTimestamp)
                .tableIds(Set.of(commitPartId.tableId()))
                .initiatorTime(hybridClock.now())
                .safeTime(hybridClock.now())
                .build()
        );
    }

    /**
     * Update rows.
     *
     * @param keyValueMapper Map a value to update to the iter number.
     */
    private void update(Function<Integer, Integer> keyValueMapper) {
        List<UUID> txIds = new ArrayList<>();

        for (int i = 0; i < KEY_COUNT; i++) {
            UUID txId = TestTransactionIds.newTransactionId();
            BinaryRowMessage row = getTestRow(i, keyValueMapper.apply(i));
            ReadResult readResult = readRow(getTestKey(i));

            txIds.add(txId);

            UpdateCommandV2 command = PARTITION_REPLICATION_MESSAGES_FACTORY.updateCommandV2()
                    .tableId(TABLE_ID)
                    .commitPartitionId(defaultPartitionIdMessage())
                    .rowUuid(readResult.rowId().uuid())
                    .messageRowToUpdate(PARTITION_REPLICATION_MESSAGES_FACTORY.timedBinaryRowMessage()
                            .binaryRowMessage(row)
                            .build())
                    .txId(txId)
                    .initiatorTime(hybridClock.now())
                    .safeTime(hybridClock.now())
                    .txCoordinatorId(randomUUID())
                    .build();

            commandListener.processCommand(command, raftIndex.incrementAndGet(), 1, hybridClock.now());
        }

        HybridTimestamp commitTimestamp = hybridClock.now();

        txIds.forEach(txId -> invokeBatchedCommand(PARTITION_REPLICATION_MESSAGES_FACTORY.writeIntentSwitchCommandV2()
                .txId(txId)
                .commit(true)
                .commitTimestamp(commitTimestamp)
                .tableIds(Set.of(defaultPartitionIdMessage().tableId()))
                .initiatorTime(hybridClock.now())
                .safeTime(hybridClock.now())
                .build()));
    }

    private static TablePartitionIdMessage defaultPartitionIdMessage() {
        return REPLICA_MESSAGES_FACTORY.tablePartitionIdMessage()
                .tableId(TABLE_ID)
                .partitionId(PARTITION_ID)
                .build();
    }

    /**
     * Deletes row.
     */
    private void delete() {
        List<UUID> txIds = new ArrayList<>();

        for (int i = 0; i < KEY_COUNT; i++) {
            UUID txId = TestTransactionIds.newTransactionId();
            ReadResult readResult = readRow(getTestKey(i));

            txIds.add(txId);

            UpdateCommandV2 command = PARTITION_REPLICATION_MESSAGES_FACTORY.updateCommandV2()
                    .tableId(TABLE_ID)
                    .commitPartitionId(defaultPartitionIdMessage())
                    .rowUuid(readResult.rowId().uuid())
                    .txId(txId)
                    .initiatorTime(hybridClock.now())
                    .safeTime(hybridClock.now())
                    .txCoordinatorId(randomUUID())
                    .build();

            commandListener.processCommand(command, raftIndex.incrementAndGet(), 1, hybridClock.now());
        }

        HybridTimestamp commitTimestamp = hybridClock.now();

        txIds.forEach(txId -> invokeBatchedCommand(PARTITION_REPLICATION_MESSAGES_FACTORY.writeIntentSwitchCommandV2()
                .txId(txId)
                .commit(true)
                .tableIds(Set.of(defaultPartitionIdMessage().tableId()))
                .commitTimestamp(commitTimestamp)
                .initiatorTime(hybridClock.now())
                .safeTime(hybridClock.now())
                .build()
        ));
    }

    /**
     * Reads rows from the listener and checks them.
     *
     * @param existed True if rows are existed, false otherwise.
     */
    private void readAndCheck(boolean existed) {
        readAndCheck(existed, i -> i);
    }

    /**
     * Reads rows from the listener and checks values as expected by a mapper.
     *
     * @param existed True if rows are existed, false otherwise.
     * @param keyValueMapper Mapper a key to the value which will be expected.
     */
    private void readAndCheck(boolean existed, Function<Integer, Integer> keyValueMapper) {
        for (int i = 0; i < KEY_COUNT; i++) {
            ReadResult readResult = readRow(getTestKey(i));

            if (existed) {
                assertNotNull(readResult);

                Row row = Row.wrapBinaryRow(SCHEMA, readResult.binaryRow());

                assertEquals(i, row.intValue(0));
                assertEquals(keyValueMapper.apply(i), row.intValue(1));
            } else {
                assertNull(readResult);
            }
        }
    }

    /**
     * Inserts row.
     */
    private void insert() {
        List<UUID> txIds = new ArrayList<>();

        for (int i = 0; i < KEY_COUNT; i++) {
            UUID txId0 = TestTransactionIds.newTransactionId();
            txIds.add(txId0);

            UpdateCommandV2 command = PARTITION_REPLICATION_MESSAGES_FACTORY.updateCommandV2()
                    .tableId(TABLE_ID)
                    .commitPartitionId(defaultPartitionIdMessage())
                    .rowUuid(randomUUID())
                    .messageRowToUpdate(PARTITION_REPLICATION_MESSAGES_FACTORY.timedBinaryRowMessage()
                            .binaryRowMessage(getTestRow(i, i))
                            .build())
                    .txId(txId0)
                    .initiatorTime(hybridClock.now())
                    .safeTime(hybridClock.now())
                    .txCoordinatorId(randomUUID())
                    .build();

            commandListener.processCommand(command, raftIndex.incrementAndGet(), 1, hybridClock.now());
        }

        HybridTimestamp commitTimestamp = hybridClock.now();

        txIds.forEach(txId -> invokeBatchedCommand(
                PARTITION_REPLICATION_MESSAGES_FACTORY.writeIntentSwitchCommandV2()
                        .txId(txId)
                        .commit(true)
                        .commitTimestamp(commitTimestamp)
                        .tableIds(Set.of(defaultPartitionIdMessage().tableId()))
                        .initiatorTime(hybridClock.now())
                        .safeTime(hybridClock.now())
                        .build()));
    }

    /**
     * Prepares a test row which contains only key field.
     *
     * @return Row.
     */
    private static BinaryTuple getTestKey(int key) {
        ByteBuffer buf = new BinaryTupleBuilder(1)
                .appendInt(key)
                .build();

        return new BinaryTuple(1, buf);
    }

    /**
     * Prepares a test binary row which contains key and value fields.
     *
     * @return Row.
     */
    private BinaryRowMessage getTestRow(int key, int val) {
        RowAssembler rowBuilder = new RowAssembler(SCHEMA, -1);

        rowBuilder.appendInt(key);
        rowBuilder.appendInt(val);

        BinaryRow row = rowBuilder.build();

        return PARTITION_REPLICATION_MESSAGES_FACTORY.binaryRowMessage()
                .binaryTuple(row.tupleSlice())
                .schemaVersion(row.schemaVersion())
                .build();
    }

    private void invokeBatchedCommand(WriteCommand cmd) {
        commandListener.processCommand(cmd, raftIndex.incrementAndGet(), 1, hybridClock.now());
    }

    private @Nullable ReadResult readRow(BinaryTuple pk) {
        try (Cursor<RowId> cursor = pkStorage.storage().get(pk)) {
            return cursor.stream()
                    .map(rowId -> mvPartitionStorage.read(rowId, HybridTimestamp.MAX_VALUE))
                    .filter(readResult -> !readResult.isEmpty())
                    .findAny()
                    .orElse(null);
        }
    }

    private static IndexMeta createIndexMeta(int indexId, int tableVersion) {
        IndexMeta indexMeta = mock(IndexMeta.class);

        MetaIndexStatusChange change0 = mock(MetaIndexStatusChange.class);
        MetaIndexStatusChange change1 = mock(MetaIndexStatusChange.class);

        Map<MetaIndexStatus, MetaIndexStatusChange> changeMap = Map.of(REGISTERED, change0, BUILDING, change1);

        lenient().when(indexMeta.indexId()).thenReturn(indexId);
        lenient().when(indexMeta.status()).thenReturn(BUILDING);
        lenient().when(indexMeta.tableVersion()).thenReturn(tableVersion);
        lenient().when(indexMeta.statusChanges()).thenReturn(changeMap);
        lenient().when(indexMeta.statusChange(eq(REGISTERED))).thenReturn(change0);
        lenient().when(indexMeta.statusChange(eq(BUILDING))).thenReturn(change1);

        return indexMeta;
    }
}
