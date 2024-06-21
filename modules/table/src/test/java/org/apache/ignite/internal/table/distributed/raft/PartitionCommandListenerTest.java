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

import static java.util.Collections.singletonMap;
import static org.apache.ignite.internal.table.distributed.index.MetaIndexStatus.BUILDING;
import static org.apache.ignite.internal.table.distributed.index.MetaIndexStatus.REGISTERED;
import static org.apache.ignite.internal.util.ArrayUtils.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.ignite.distributed.TestPartitionDataStorage;
import org.apache.ignite.internal.TestHybridClock;
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
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.CommittedConfiguration;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.command.SafeTimePropagatingCommand;
import org.apache.ignite.internal.replicator.command.SafeTimeSyncCommand;
import org.apache.ignite.internal.replicator.command.SafeTimeSyncCommandBuilder;
import org.apache.ignite.internal.replicator.message.PrimaryReplicaChangeCommand;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowConverter;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.configuration.StorageUpdateConfiguration;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.MvPartitionStorage.WriteClosure;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.impl.TestMvPartitionStorage;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor.StorageHashIndexColumnDescriptor;
import org.apache.ignite.internal.storage.index.impl.TestHashIndexStorage;
import org.apache.ignite.internal.table.distributed.StorageUpdateHandler;
import org.apache.ignite.internal.table.distributed.TableSchemaAwareIndexStorage;
import org.apache.ignite.internal.table.distributed.command.BuildIndexCommand;
import org.apache.ignite.internal.table.distributed.command.FinishTxCommand;
import org.apache.ignite.internal.table.distributed.command.TablePartitionIdMessage;
import org.apache.ignite.internal.table.distributed.command.TimedBinaryRowMessage;
import org.apache.ignite.internal.table.distributed.command.UpdateAllCommand;
import org.apache.ignite.internal.table.distributed.command.UpdateCommand;
import org.apache.ignite.internal.table.distributed.command.WriteIntentSwitchCommand;
import org.apache.ignite.internal.table.distributed.index.IndexMeta;
import org.apache.ignite.internal.table.distributed.index.IndexMetaStorage;
import org.apache.ignite.internal.table.distributed.index.IndexUpdateHandler;
import org.apache.ignite.internal.table.distributed.index.MetaIndexStatus;
import org.apache.ignite.internal.table.distributed.index.MetaIndexStatusChange;
import org.apache.ignite.internal.table.distributed.replication.request.BinaryRowMessage;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.internal.table.impl.DummySchemaManagerImpl;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.UpdateCommandResult;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.tx.storage.state.test.TestTxStateStorage;
import org.apache.ignite.internal.tx.test.TestTransactionIds;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for the table command listener.
 */
@ExtendWith(WorkDirectoryExtension.class)
@ExtendWith(MockitoExtension.class)
@ExtendWith(ConfigurationExtension.class)
public class PartitionCommandListenerTest extends BaseIgniteAbstractTest {
    private static final int KEY_COUNT = 100;

    private static final int TABLE_ID = 1;

    private static final int PARTITION_ID = 0;

    private static final SchemaDescriptor SCHEMA = new SchemaDescriptor(
            1,
            new Column[]{new Column("key", NativeTypes.INT32, false)},
            new Column[]{new Column("value", NativeTypes.INT32, false)}
    );

    private static final SchemaRegistry SCHEMA_REGISTRY = new DummySchemaManagerImpl(SCHEMA);

    private PartitionListener commandListener;

    private final AtomicLong raftIndex = new AtomicLong();

    private final TableSchemaAwareIndexStorage pkStorage = new TableSchemaAwareIndexStorage(
            1,
            new TestHashIndexStorage(
                    PARTITION_ID,
                    new StorageHashIndexDescriptor(
                            TABLE_ID,
                            List.of(new StorageHashIndexColumnDescriptor("key", NativeTypes.INT32, false)),
                            true
                    )
            ),
            BinaryRowConverter.keyExtractor(SCHEMA)
    );

    private final MvPartitionStorage mvPartitionStorage = spy(new TestMvPartitionStorage(PARTITION_ID));

    private final PartitionDataStorage partitionDataStorage = spy(new TestPartitionDataStorage(TABLE_ID, PARTITION_ID, mvPartitionStorage));

    private final TxStateStorage txStateStorage = spy(new TestTxStateStorage());

    @WorkDirectory
    private Path workDir;

    private final PartitionReplicationMessagesFactory msgFactory = new PartitionReplicationMessagesFactory();

    private final HybridClock hybridClock = new HybridClockImpl();

    private PendingComparableValuesTracker<HybridTimestamp, Void> safeTimeTracker;

    @Captor
    private ArgumentCaptor<Throwable> commandClosureResultCaptor;

    @Captor
    private ArgumentCaptor<UpdateCommandResult> updateCommandClosureResultCaptor;

    private final RaftGroupConfigurationConverter raftGroupConfigurationConverter = new RaftGroupConfigurationConverter();

    private IndexUpdateHandler indexUpdateHandler;

    private StorageUpdateHandler storageUpdateHandler;

    @InjectConfiguration
    private StorageUpdateConfiguration storageUpdateConfiguration;

    private CatalogService catalogService;

    private final ClockService clockService = new TestClockService(new HybridClockImpl());

    private IndexMetaStorage indexMetaStorage;

    /**
     * Initializes a table listener before tests.
     */
    @BeforeEach
    public void before() {
        NetworkAddress addr = new NetworkAddress("127.0.0.1", 5003);

        ClusterService clusterService = mock(ClusterService.class, RETURNS_DEEP_STUBS);

        when(clusterService.topologyService().localMember().address()).thenReturn(addr);

        safeTimeTracker = new PendingComparableValuesTracker<>(new HybridTimestamp(1, 0));

        int indexId = pkStorage.id();

        indexUpdateHandler = spy(new IndexUpdateHandler(
                DummyInternalTableImpl.createTableIndexStoragesSupplier(Map.of(indexId, pkStorage))
        ));

        storageUpdateHandler = spy(new StorageUpdateHandler(
                PARTITION_ID,
                partitionDataStorage,
                indexUpdateHandler,
                storageUpdateConfiguration
        ));

        catalogService = mock(CatalogService.class);

        Catalog catalog = mock(Catalog.class);

        CatalogIndexDescriptor indexDescriptor = mock(CatalogIndexDescriptor.class);

        lenient().when(catalog.index(indexId)).thenReturn(indexDescriptor);
        lenient().when(catalogService.catalog(anyInt())).thenReturn(catalog);

        indexDescriptor = mock(CatalogIndexDescriptor.class);

        lenient().when(indexDescriptor.id()).thenReturn(indexId);
        lenient().when(catalogService.indexes(anyInt(), anyInt())).thenReturn(List.of(indexDescriptor));
        lenient().when(catalogService.index(anyInt(), anyInt())).thenReturn(indexDescriptor);

        CatalogTableDescriptor tableDescriptor = mock(CatalogTableDescriptor.class);

        int tableVersion = SCHEMA.version();

        lenient().when(tableDescriptor.tableVersion()).thenReturn(tableVersion);
        lenient().when(catalogService.table(anyInt(), anyInt())).thenReturn(tableDescriptor);

        indexMetaStorage = mock(IndexMetaStorage.class);

        IndexMeta indexMeta = createIndexMeta(indexId, tableVersion);

        lenient().when(indexMetaStorage.indexMeta(eq(indexId))).thenReturn(indexMeta);

        commandListener = new PartitionListener(
                mock(TxManager.class),
                partitionDataStorage,
                storageUpdateHandler,
                txStateStorage,
                safeTimeTracker,
                new PendingComparableValuesTracker<>(0L),
                catalogService,
                SCHEMA_REGISTRY,
                clockService,
                indexMetaStorage
        );
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

    @Test
    void testSkipWriteCommandByAppliedIndex() {
        mvPartitionStorage.lastApplied(10L, 1L);

        UpdateCommand updateCommand = mock(UpdateCommand.class);
        when(updateCommand.safeTime()).thenAnswer(v -> hybridClock.now());

        WriteIntentSwitchCommand writeIntentSwitchCommand = mock(WriteIntentSwitchCommand.class);
        when(writeIntentSwitchCommand.safeTime()).thenAnswer(v -> hybridClock.now());

        SafeTimeSyncCommand safeTimeSyncCommand = mock(SafeTimeSyncCommand.class);
        when(safeTimeSyncCommand.safeTime()).thenAnswer(v -> hybridClock.now());

        FinishTxCommand finishTxCommand = mock(FinishTxCommand.class);
        when(finishTxCommand.safeTime()).thenAnswer(v -> hybridClock.now());

        PrimaryReplicaChangeCommand primaryReplicaChangeCommand = mock(PrimaryReplicaChangeCommand.class);

        // Checks for MvPartitionStorage.
        commandListener.onWrite(List.of(
                writeCommandCommandClosure(3, 1, updateCommand, updateCommandClosureResultCaptor),
                writeCommandCommandClosure(10, 1, updateCommand, updateCommandClosureResultCaptor),
                writeCommandCommandClosure(4, 1, writeIntentSwitchCommand, commandClosureResultCaptor),
                writeCommandCommandClosure(5, 1, safeTimeSyncCommand, commandClosureResultCaptor),
                writeCommandCommandClosure(6, 1, primaryReplicaChangeCommand, commandClosureResultCaptor)
        ).iterator());

        verify(mvPartitionStorage, never()).runConsistently(any(WriteClosure.class));
        verify(mvPartitionStorage, times(1)).lastApplied(anyLong(), anyLong());

        assertThat(updateCommandClosureResultCaptor.getAllValues(), containsInAnyOrder(new UpdateCommandResult(true),
                new UpdateCommandResult(true)));
        assertThat(commandClosureResultCaptor.getAllValues(), containsInAnyOrder(new Throwable[]{null, null, null}));

        // Checks for TxStateStorage.
        mvPartitionStorage.lastApplied(1L, 1L);
        txStateStorage.lastApplied(10L, 2L);

        commandClosureResultCaptor = ArgumentCaptor.forClass(Throwable.class);

        commandListener.onWrite(List.of(
                writeCommandCommandClosure(2, 1, finishTxCommand, commandClosureResultCaptor),
                writeCommandCommandClosure(10, 1, finishTxCommand, commandClosureResultCaptor)
        ).iterator());

        verify(txStateStorage, never()).compareAndSet(any(UUID.class), any(TxState.class), any(TxMeta.class), anyLong(), anyLong());
        verify(txStateStorage, times(1)).lastApplied(anyLong(), anyLong());

        assertThat(commandClosureResultCaptor.getAllValues(), containsInAnyOrder(new Throwable[]{null, null}));
    }

    private static CommandClosure<WriteCommand> writeCommandCommandClosure(
            long index,
            long term,
            WriteCommand writeCommand
    ) {
        return writeCommandCommandClosure(index, term, writeCommand, null);
    }

    /**
     * Create a command closure.
     *
     * @param index Index of the RAFT command.
     * @param writeCommand Write command.
     * @param resultClosureCaptor Captor for {@link CommandClosure#result(Serializable)}
     */
    private static CommandClosure<WriteCommand> writeCommandCommandClosure(
            long index,
            long term,
            WriteCommand writeCommand,
            @Nullable ArgumentCaptor<? extends Serializable> resultClosureCaptor
    ) {
        CommandClosure<WriteCommand> commandClosure = mock(CommandClosure.class);

        when(commandClosure.index()).thenReturn(index);
        when(commandClosure.term()).thenReturn(term);
        when(commandClosure.command()).thenReturn(writeCommand);

        if (resultClosureCaptor != null) {
            doNothing().when(commandClosure).result(resultClosureCaptor.capture());
        }

        return commandClosure;
    }

    /**
     * The test checks that {@link PartitionListener#onSnapshotSave(Path, Consumer)} propagates the maximal last applied index among
     * storages to all storages.
     */
    @Test
    public void testOnSnapshotSavePropagateLastAppliedIndexAndTerm() {
        TestPartitionDataStorage partitionDataStorage = new TestPartitionDataStorage(TABLE_ID, PARTITION_ID, mvPartitionStorage);

        IndexUpdateHandler indexUpdateHandler = new IndexUpdateHandler(
                DummyInternalTableImpl.createTableIndexStoragesSupplier(Map.of(pkStorage.id(), pkStorage))
        );

        StorageUpdateHandler storageUpdateHandler = new StorageUpdateHandler(
                PARTITION_ID,
                partitionDataStorage,
                indexUpdateHandler,
                storageUpdateConfiguration
        );

        PartitionListener testCommandListener = new PartitionListener(
                mock(TxManager.class),
                partitionDataStorage,
                storageUpdateHandler,
                txStateStorage,
                safeTimeTracker,
                new PendingComparableValuesTracker<>(0L),
                catalogService,
                SCHEMA_REGISTRY,
                clockService,
                indexMetaStorage
        );

        txStateStorage.lastApplied(3L, 1L);

        partitionDataStorage.lastApplied(5L, 2L);

        AtomicLong counter = new AtomicLong(0);

        testCommandListener.onSnapshotSave(workDir, (throwable) -> counter.incrementAndGet());

        assertEquals(1L, counter.get());

        assertEquals(5L, partitionDataStorage.lastAppliedIndex());
        assertEquals(2L, partitionDataStorage.lastAppliedTerm());

        assertEquals(5L, txStateStorage.lastAppliedIndex());
        assertEquals(2L, txStateStorage.lastAppliedTerm());

        txStateStorage.lastApplied(10L, 2L);

        partitionDataStorage.lastApplied(7L, 1L);

        testCommandListener.onSnapshotSave(workDir, (throwable) -> counter.incrementAndGet());

        assertEquals(2L, counter.get());

        assertEquals(10L, partitionDataStorage.lastAppliedIndex());
        assertEquals(2L, partitionDataStorage.lastAppliedTerm());

        assertEquals(10L, txStateStorage.lastAppliedIndex());
        assertEquals(2L, txStateStorage.lastAppliedTerm());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void updatesLastAppliedForUpdateCommands(boolean stale) {
        safeTimeTracker.update(hybridClock.now(), null);

        UpdateCommand command = msgFactory.updateCommand()
                .rowUuid(UUID.randomUUID())
                .tablePartitionId(defaultPartitionIdMessage())
                .txCoordinatorId(UUID.randomUUID().toString())
                .txId(TestTransactionIds.newTransactionId())
                .safeTimeLong(staleOrFreshSafeTime(stale))
                .build();

        commandListener.onWrite(List.of(
                writeCommandCommandClosure(3, 2, command)
        ).iterator());

        verify(mvPartitionStorage).lastApplied(3, 2);
    }

    private long staleOrFreshSafeTime(boolean stale) {
        return stale ? safeTimeTracker.current().subtractPhysicalTime(1).longValue() : hybridClock.nowLong();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void updatesLastAppliedForUpdateAllCommands(boolean stale) {
        safeTimeTracker.update(hybridClock.now(), null);

        UpdateAllCommand command = msgFactory.updateAllCommand()
                .messageRowsToUpdate(singletonMap(UUID.randomUUID(), msgFactory.timedBinaryRowMessage().build()))
                .tablePartitionId(defaultPartitionIdMessage())
                .txCoordinatorId(UUID.randomUUID().toString())
                .txId(TestTransactionIds.newTransactionId())
                .safeTimeLong(staleOrFreshSafeTime(stale))
                .build();

        commandListener.onWrite(List.of(
                writeCommandCommandClosure(3, 2, command)
        ).iterator());

        verify(mvPartitionStorage).lastApplied(3, 2);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void updatesLastAppliedForFinishTxCommands(boolean stale) {
        safeTimeTracker.update(hybridClock.now(), null);

        FinishTxCommand command = msgFactory.finishTxCommand()
                .txId(TestTransactionIds.newTransactionId())
                .safeTimeLong(staleOrFreshSafeTime(stale))
                .partitionIds(List.of())
                .build();

        commandListener.onWrite(List.of(
                writeCommandCommandClosure(3, 2, command)
        ).iterator());

        assertThat(txStateStorage.lastAppliedIndex(), is(3L));
        assertThat(txStateStorage.lastAppliedTerm(), is(2L));
    }

    @Test
    void locksOnCommandApplication() {
        SafeTimeSyncCommandBuilder safeTimeSyncCommand = new ReplicaMessagesFactory()
                .safeTimeSyncCommand()
                .safeTimeLong(hybridClock.nowLong());

        commandListener.onWrite(List.of(
                writeCommandCommandClosure(3, 2, safeTimeSyncCommand.build(), commandClosureResultCaptor)
        ).iterator());

        InOrder inOrder = inOrder(partitionDataStorage);

        inOrder.verify(partitionDataStorage).acquirePartitionSnapshotsReadLock();
        inOrder.verify(partitionDataStorage).lastApplied(3, 2);
        inOrder.verify(partitionDataStorage).releasePartitionSnapshotsReadLock();
    }

    @Test
    void updatesGroupConfigurationOnConfigCommit() {
        commandListener.onConfigurationCommitted(new CommittedConfiguration(
                1, 2, List.of("peer"), List.of("learner"), List.of("old-peer"), List.of("old-learner")
        ));

        RaftGroupConfiguration expectedConfig = new RaftGroupConfiguration(
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
        commandListener.onConfigurationCommitted(new CommittedConfiguration(
                1, 2, List.of("peer"), List.of("learner"), List.of("old-peer"), List.of("old-learner")
        ));

        verify(mvPartitionStorage).lastApplied(1, 2);
    }

    @Test
    void skipsUpdatesOnConfigCommitIfIndexIsStale() {
        mvPartitionStorage.lastApplied(10, 3);

        commandListener.onConfigurationCommitted(new CommittedConfiguration(
                1, 2, List.of("peer"), List.of("learner"), List.of("old-peer"), List.of("old-learner")
        ));

        verify(mvPartitionStorage, never()).committedGroupConfiguration(any());
        verify(mvPartitionStorage, never()).lastApplied(eq(1L), anyLong());
    }

    @Test
    void locksOnConfigCommit() {
        commandListener.onConfigurationCommitted(new CommittedConfiguration(
                1, 2, List.of("peer"), List.of("learner"), List.of("old-peer"), List.of("old-learner")
        ));

        InOrder inOrder = inOrder(partitionDataStorage);

        inOrder.verify(partitionDataStorage).acquirePartitionSnapshotsReadLock();
        inOrder.verify(partitionDataStorage).lastApplied(1, 2);
        inOrder.verify(partitionDataStorage).releasePartitionSnapshotsReadLock();
    }

    @Test
    public void testSafeTime() {
        HybridClock testClock = new TestHybridClock(() -> 1);

        applySafeTimeCommand(SafeTimeSyncCommand.class, testClock.now());
        applySafeTimeCommand(SafeTimeSyncCommand.class, testClock.now());
    }

    @Test
    void testBuildIndexCommand() {
        int indexId = pkStorage.id();

        doNothing().when(indexUpdateHandler).buildIndex(eq(indexId), any(Stream.class), any());

        RowId row0 = new RowId(PARTITION_ID);
        RowId row1 = new RowId(PARTITION_ID);
        RowId row2 = new RowId(PARTITION_ID);

        InOrder inOrder = inOrder(partitionDataStorage, indexUpdateHandler);

        commandListener.handleBuildIndexCommand(createBuildIndexCommand(indexId, List.of(row0.uuid()), false), 10, 1);

        inOrder.verify(indexUpdateHandler).buildIndex(eq(indexId), any(Stream.class), eq(row0.increment()));
        inOrder.verify(partitionDataStorage).lastApplied(10, 1);

        commandListener.handleBuildIndexCommand(createBuildIndexCommand(indexId, List.of(row1.uuid()), true), 20, 2);

        inOrder.verify(indexUpdateHandler).buildIndex(eq(indexId), any(Stream.class), eq(null));
        inOrder.verify(partitionDataStorage).lastApplied(20, 2);

        // Let's check that the command with a lower commandIndex than in the storage will not be executed.
        commandListener.handleBuildIndexCommand(createBuildIndexCommand(indexId, List.of(row2.uuid()), false), 5, 1);

        inOrder.verify(indexUpdateHandler, never()).buildIndex(eq(indexId), any(Stream.class), eq(row2.increment()));
        inOrder.verify(partitionDataStorage, never()).lastApplied(5, 1);
    }

    private BuildIndexCommand createBuildIndexCommand(int indexId, List<UUID> rowUuids, boolean finish) {
        return msgFactory.buildIndexCommand()
                .indexId(indexId)
                .rowIds(rowUuids)
                .finish(finish)
                .build();
    }

    private void applySafeTimeCommand(Class<? extends SafeTimePropagatingCommand> cls, HybridTimestamp timestamp) {
        SafeTimePropagatingCommand command = mock(cls);
        when(command.safeTime()).thenReturn(timestamp);

        CommandClosure<WriteCommand> closure = writeCommandCommandClosure(1, 1, command, commandClosureResultCaptor);
        commandListener.onWrite(asList(closure).iterator());
        assertEquals(timestamp, safeTimeTracker.current());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void updatesLastAppliedForWriteIntentSwitchCommands(boolean stale) {
        safeTimeTracker.update(hybridClock.now(), null);

        WriteIntentSwitchCommand command = msgFactory.writeIntentSwitchCommand()
                .txId(TestTransactionIds.newTransactionId())
                .safeTimeLong(staleOrFreshSafeTime(stale))
                .build();

        commandListener.onWrite(List.of(
                writeCommandCommandClosure(3, 2, command)
        ).iterator());

        verify(mvPartitionStorage).lastApplied(3, 2);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void updatesLastAppliedForSafeTimeSyncCommands(boolean stale) {
        safeTimeTracker.update(hybridClock.now(), null);

        SafeTimeSyncCommand safeTimeSyncCommand = new ReplicaMessagesFactory()
                .safeTimeSyncCommand()
                .safeTimeLong(staleOrFreshSafeTime(stale))
                .build();

        commandListener.onWrite(List.of(
                writeCommandCommandClosure(3, 2, safeTimeSyncCommand)
        ).iterator());

        verify(mvPartitionStorage).lastApplied(3, 2);
    }

    /**
     * Prepares a closure iterator for a specific batch operation.
     *
     * @param func The function prepare a closure for the operation.
     * @param <T> Type of the operation.
     * @return Closure iterator.
     */
    private <T extends Command> Iterator<CommandClosure<T>> batchIterator(Consumer<CommandClosure<T>> func) {
        return new Iterator<>() {
            boolean moved;

            @Override
            public boolean hasNext() {
                return !moved;
            }

            @Override
            public CommandClosure<T> next() {
                CommandClosure<T> clo = mock(CommandClosure.class);

                func.accept(clo);

                moved = true;

                return clo;
            }
        };
    }

    /**
     * Prepares a closure iterator for a specific operation.
     *
     * @param func The function prepare a closure for the operation.
     * @param <T> Type of the operation.
     * @return Closure iterator.
     */
    private <T extends Command> Iterator<CommandClosure<T>> iterator(BiConsumer<Integer, CommandClosure<T>> func) {
        return new Iterator<>() {
            /** Iteration. */
            private int it = 0;

            /** {@inheritDoc} */
            @Override
            public boolean hasNext() {
                return it < KEY_COUNT;
            }

            /** {@inheritDoc} */
            @Override
            public CommandClosure<T> next() {
                CommandClosure<T> clo = mock(CommandClosure.class);

                func.accept(it, clo);

                it++;

                return clo;
            }
        };
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
                    msgFactory.timedBinaryRowMessage()
                            .binaryRowMessage(getTestRow(i, i))
                            .build()
            );
        }

        HybridTimestamp commitTimestamp = hybridClock.now();

        invokeBatchedCommand(msgFactory.updateAllCommand()
                .tablePartitionId(
                        msgFactory.tablePartitionIdMessage()
                                .tableId(commitPartId.tableId())
                                .partitionId(commitPartId.partitionId())
                                .build())
                .messageRowsToUpdate(rows)
                .txId(txId)
                .safeTimeLong(hybridClock.nowLong())
                .txCoordinatorId(UUID.randomUUID().toString())
                .build());

        invokeBatchedCommand(msgFactory.writeIntentSwitchCommand()
                .txId(txId)
                .commit(true)
                .commitTimestampLong(commitTimestamp.longValue())
                .safeTimeLong(hybridClock.nowLong())
                .build());
    }

    /**
     * Update values from the listener in the batch operation.
     *
     * @param keyValueMapper Mep a value to update to the iter number.
     */
    private void updateAll(Function<Integer, Integer> keyValueMapper) {
        UUID txId = TestTransactionIds.newTransactionId();
        var commitPartId = new TablePartitionId(TABLE_ID, PARTITION_ID);
        Map<UUID, TimedBinaryRowMessage> rows = new HashMap<>(KEY_COUNT);

        for (int i = 0; i < KEY_COUNT; i++) {
            ReadResult readResult = readRow(getTestKey(i));

            rows.put(readResult.rowId().uuid(),
                    msgFactory.timedBinaryRowMessage()
                            .binaryRowMessage(getTestRow(i, keyValueMapper.apply(i)))
                            .build()
            );
        }

        HybridTimestamp commitTimestamp = hybridClock.now();

        invokeBatchedCommand(msgFactory.updateAllCommand()
                .tablePartitionId(
                        msgFactory.tablePartitionIdMessage()
                                .tableId(commitPartId.tableId())
                                .partitionId(commitPartId.partitionId())
                                .build())
                .messageRowsToUpdate(rows)
                .txId(txId)
                .safeTimeLong(hybridClock.nowLong())
                .txCoordinatorId(UUID.randomUUID().toString())
                .build());

        invokeBatchedCommand(msgFactory.writeIntentSwitchCommand()
                .txId(txId)
                .commit(true)
                .commitTimestampLong(commitTimestamp.longValue())
                .safeTimeLong(hybridClock.nowLong())
                .build());
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

            keyRows.put(readResult.rowId().uuid(), msgFactory.timedBinaryRowMessage()
                    .build());
        }

        HybridTimestamp commitTimestamp = hybridClock.now();

        invokeBatchedCommand(msgFactory.updateAllCommand()
                .tablePartitionId(
                        msgFactory.tablePartitionIdMessage()
                                .tableId(commitPartId.tableId())
                                .partitionId(commitPartId.partitionId())
                                .build())
                .messageRowsToUpdate(keyRows)
                .txId(txId)
                .safeTimeLong(hybridClock.nowLong())
                .txCoordinatorId(UUID.randomUUID().toString())
                .build());

        invokeBatchedCommand(msgFactory.writeIntentSwitchCommand()
                .txId(txId)
                .commit(true)
                .commitTimestampLong(commitTimestamp.longValue())
                .safeTimeLong(hybridClock.nowLong())
                .build());
    }

    /**
     * Update rows.
     *
     * @param keyValueMapper Mep a value to update to the iter number.
     */
    private void update(Function<Integer, Integer> keyValueMapper) {
        List<UUID> txIds = new ArrayList<>();

        commandListener.onWrite(iterator((i, clo) -> {
            UUID txId = TestTransactionIds.newTransactionId();
            BinaryRowMessage row = getTestRow(i, keyValueMapper.apply(i));
            ReadResult readResult = readRow(getTestKey(i));

            txIds.add(txId);

            when(clo.index()).thenReturn(raftIndex.incrementAndGet());

            when(clo.command()).thenReturn(
                    msgFactory.updateCommand()
                            .tablePartitionId(defaultPartitionIdMessage())
                            .rowUuid(readResult.rowId().uuid())
                            .messageRowToUpdate(msgFactory.timedBinaryRowMessage()
                                    .binaryRowMessage(row)
                                    .build())
                            .txId(txId)
                            .safeTimeLong(hybridClock.nowLong())
                            .txCoordinatorId(UUID.randomUUID().toString())
                            .build());

            doAnswer(invocation -> {
                assertTrue(invocation.getArgument(0) instanceof UpdateCommandResult);

                return null;
            }).when(clo).result(any());
        }));

        HybridTimestamp commitTimestamp = hybridClock.now();

        txIds.forEach(txId -> invokeBatchedCommand(msgFactory.writeIntentSwitchCommand()
                .txId(txId)
                .commit(true)
                .commitTimestampLong(commitTimestamp.longValue())
                .safeTimeLong(hybridClock.nowLong())
                .build()));
    }

    private TablePartitionIdMessage defaultPartitionIdMessage() {
        return msgFactory.tablePartitionIdMessage()
                .tableId(1)
                .partitionId(PARTITION_ID).build();
    }

    /**
     * Deletes row.
     */
    private void delete() {
        List<UUID> txIds = new ArrayList<>();

        commandListener.onWrite(iterator((i, clo) -> {
            UUID txId = TestTransactionIds.newTransactionId();
            ReadResult readResult = readRow(getTestKey(i));

            txIds.add(txId);

            when(clo.index()).thenReturn(raftIndex.incrementAndGet());

            when(clo.command()).thenReturn(
                    msgFactory.updateCommand()
                            .tablePartitionId(defaultPartitionIdMessage())
                            .rowUuid(readResult.rowId().uuid())
                            .txId(txId)
                            .safeTimeLong(hybridClock.nowLong())
                            .txCoordinatorId(UUID.randomUUID().toString())
                            .build());

            doAnswer(invocation -> {
                assertTrue(invocation.getArgument(0) instanceof UpdateCommandResult);

                return null;
            }).when(clo).result(any());
        }));

        HybridTimestamp commitTimestamp = hybridClock.now();

        txIds.forEach(txId -> invokeBatchedCommand(msgFactory.writeIntentSwitchCommand()
                .txId(txId)
                .commit(true)
                .commitTimestampLong(commitTimestamp.longValue())
                .safeTimeLong(hybridClock.nowLong())
                .build()));
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

        commandListener.onWrite(iterator((i, clo) -> {
            UUID txId = TestTransactionIds.newTransactionId();
            txIds.add(txId);

            when(clo.index()).thenReturn(raftIndex.incrementAndGet());

            when(clo.command()).thenReturn(
                    msgFactory.updateCommand()
                            .tablePartitionId(defaultPartitionIdMessage())
                            .rowUuid(UUID.randomUUID())
                            .messageRowToUpdate(msgFactory.timedBinaryRowMessage()
                                    .binaryRowMessage(getTestRow(i, i))
                                    .build())
                            .txId(txId)
                            .safeTimeLong(hybridClock.nowLong())
                            .txCoordinatorId(UUID.randomUUID().toString())
                            .build());

            doAnswer(invocation -> {
                assertTrue(invocation.getArgument(0) instanceof UpdateCommandResult);

                return null;
            }).when(clo).result(any());
        }));

        long commitTimestamp = hybridClock.nowLong();

        txIds.forEach(txId -> invokeBatchedCommand(
                msgFactory.writeIntentSwitchCommand()
                        .txId(txId)
                        .commit(true)
                        .commitTimestampLong(commitTimestamp)
                        .safeTimeLong(hybridClock.nowLong())
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

        return msgFactory.binaryRowMessage()
                .binaryTuple(row.tupleSlice())
                .schemaVersion(row.schemaVersion())
                .build();
    }

    private void invokeBatchedCommand(WriteCommand cmd) {
        commandListener.onWrite(batchIterator(clo -> {
            when(clo.index()).thenReturn(raftIndex.incrementAndGet());

            doAnswer(invocation -> {
                if (cmd instanceof UpdateCommand || cmd instanceof UpdateAllCommand) {
                    assertTrue(invocation.getArgument(0) instanceof UpdateCommandResult);
                } else {
                    assertNull(invocation.getArgument(0));
                }

                return null;
            }).when(clo).result(any());

            when(clo.command()).thenReturn(cmd);
        }));
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
