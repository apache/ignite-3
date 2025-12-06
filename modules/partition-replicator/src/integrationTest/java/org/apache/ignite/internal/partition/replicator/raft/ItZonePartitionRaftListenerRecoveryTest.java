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

package org.apache.ignite.internal.partition.replicator.raft;

import static java.util.Collections.reverse;
import static org.apache.ignite.internal.configuration.RaftGroupOptionsConfigHelper.configureProperties;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.COLOCATION_FEATURE_FLAG;
import static org.apache.ignite.internal.network.utils.ClusterServiceTestUtils.clusterService;
import static org.apache.ignite.internal.replicator.message.ReplicaMessageUtils.toReplicationGroupIdMessage;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.tx.TransactionIds.transactionId;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.startAsync;
import static org.apache.ignite.internal.util.IgniteUtils.stopAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.mockito.quality.Strictness.LENIENT;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.components.NoOpLogSyncer;
import org.apache.ignite.internal.configuration.ComponentWorkingDir;
import org.apache.ignite.internal.configuration.SystemLocalConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.NoOpFailureManager;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metrics.NoOpMetricManager;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.StaticNodeFinder;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.command.TimedBinaryRowMessage;
import org.apache.ignite.internal.partition.replicator.network.command.UpdateCommand;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.LogStorageAccessImpl;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionMvStorageAccess;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionSnapshotStorage;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionSnapshotStorageFactory;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionTxStateAccessImpl;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.ZonePartitionKey;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.placementdriver.LeasePlacementDriver;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftGroupConfigurationConverter;
import org.apache.ignite.internal.raft.RaftGroupEventsListener;
import org.apache.ignite.internal.raft.RaftGroupOptionsConfigurer;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.server.RaftGroupOptions;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.raft.storage.LogStorageFactory;
import org.apache.ignite.internal.raft.util.SharedLogStorageFactoryUtils;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.schema.BinaryRowImpl;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.MvPartitionStorage.Locker;
import org.apache.ignite.internal.storage.MvPartitionStorage.WriteClosure;
import org.apache.ignite.internal.storage.lease.LeaseInfo;
import org.apache.ignite.internal.table.distributed.StorageUpdateHandler;
import org.apache.ignite.internal.table.distributed.index.IndexMetaStorage;
import org.apache.ignite.internal.table.distributed.raft.MinimumRequiredTimeCollectorService;
import org.apache.ignite.internal.table.distributed.raft.PartitionListener;
import org.apache.ignite.internal.table.distributed.raft.snapshot.SnapshotAwarePartitionDataStorage;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.testframework.SystemPropertiesExtension;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.tx.storage.state.rocksdb.TxStateRocksDbSharedStorage;
import org.apache.ignite.internal.tx.storage.state.rocksdb.TxStateRocksDbStorage;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.internal.util.SafeTimeValuesTracker;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(SystemPropertiesExtension.class)
@ExtendWith(ExecutorServiceExtension.class)
@ExtendWith(ConfigurationExtension.class)
@ExtendWith(MockitoExtension.class)
@WithSystemProperty(key = COLOCATION_FEATURE_FLAG, value = "true")
class ItZonePartitionRaftListenerRecoveryTest extends IgniteAbstractTest {
    private static final ZonePartitionId PARTITION_ID = new ZonePartitionId(1, 3);

    private static final PartitionReplicationMessagesFactory MESSAGE_FACTORY = new PartitionReplicationMessagesFactory();

    private ComponentWorkingDir componentWorkingDir;

    private ClusterService clusterService;

    private Loza raftManager;

    private TxStateStorage txStateStorage;

    private OutgoingSnapshotsManager outgoingSnapshotsManager;

    private LogStorageFactory logStorageFactory;

    private PartitionSnapshotStorage partitionSnapshotStorage;

    private ZonePartitionRaftListener currentRaftListener;

    @Mock
    private TxManager txManager;

    @Mock
    private CatalogService catalogService;

    @Mock
    private SchemaRegistry schemaRegistry;

    @Mock
    private IndexMetaStorage indexMetaStorage;

    @Mock
    private MinimumRequiredTimeCollectorService minimumRequiredTimeCollectorService;

    @Mock
    private StorageUpdateHandler storageUpdateHandler;

    @InjectExecutorService
    private ExecutorService executor;

    private final List<IgniteComponent> components = new ArrayList<>();

    private final HybridClock clock = new HybridClockImpl();

    private final Map<Integer, MockMvPartitionStorage> storagesByTableId = new HashMap<>();

    private static class MockMvPartitionStorage {
        final MvPartitionStorage storage = mock(MvPartitionStorage.class, withSettings().strictness(LENIENT));

        final PartitionMvStorageAccess storageAccess = mock(PartitionMvStorageAccess.class, withSettings().strictness(LENIENT));

        volatile long lastAppliedIndex;

        volatile byte[] raftGroupConfiguration;

        private final RaftGroupConfigurationConverter raftGroupConfigurationConverter = new RaftGroupConfigurationConverter();

        MockMvPartitionStorage(InternalClusterNode node) {
            doAnswer(invocationOnMock -> {
                lastAppliedIndex = invocationOnMock.getArgument(0);

                return null;
            }).when(storage).lastApplied(anyLong(), anyLong());

            doAnswer(invocationOnMock -> {
                raftGroupConfiguration = invocationOnMock.getArgument(0);

                return null;
            }).when(storage).committedGroupConfiguration(any());

            when(storage.lastAppliedIndex()).then(v -> lastAppliedIndex);

            when(storage.committedGroupConfiguration()).then(v -> raftGroupConfiguration);

            when(storage.leaseInfo()).thenReturn(new LeaseInfo(0, node.id(), node.name()));

            when(storage.flush(anyBoolean())).thenReturn(nullCompletedFuture());

            when(storage.runConsistently(any())).thenAnswer(invocationOnMock -> {
                WriteClosure<?> writeClosure = invocationOnMock.getArgument(0);

                return writeClosure.execute(mock(Locker.class));
            });

            when(storageAccess.lastAppliedIndex()).then(v -> lastAppliedIndex);

            when(storageAccess.committedGroupConfiguration()).then(v -> raftGroupConfigurationConverter.fromBytes(raftGroupConfiguration));
        }
    }

    @BeforeEach
    void setUp(
            TestInfo testInfo,
            @InjectConfiguration RaftConfiguration raftConfiguration,
            @InjectConfiguration SystemLocalConfiguration systemLocalConfiguration,
            @InjectExecutorService ScheduledExecutorService scheduledExecutorService,
            @Mock Catalog catalog,
            @Mock CatalogIndexDescriptor catalogIndexDescriptor,
            @Mock ReplicaManager replicaManager
    ) {
        when(catalogService.activeCatalog(anyLong())).thenReturn(catalog);
        when(catalog.indexes(anyInt())).thenReturn(List.of(catalogIndexDescriptor));

        doAnswer(invocation -> {
            // This is needed to bump last applied index on corresponding storages.
            Runnable onApplication = invocation.getArgument(5);

            onApplication.run();

            return null;
        }).when(storageUpdateHandler).handleUpdate(any(), any(), any(), any(), anyBoolean(), any(), any(), any(), any());

        componentWorkingDir = new ComponentWorkingDir(workDir);

        var addr = new NetworkAddress("127.0.0.1", 10_000);

        clusterService = clusterService(testInfo, addr.port(), new StaticNodeFinder(List.of(addr)));

        components.add(clusterService);

        var failureProcessor = new NoOpFailureManager();

        raftManager = new Loza(
                clusterService,
                new NoOpMetricManager(),
                raftConfiguration,
                systemLocalConfiguration,
                clock,
                new RaftGroupEventsClientListener(),
                failureProcessor
        );

        components.add(raftManager);

        var sharedRockDbStorage = new TxStateRocksDbSharedStorage(
                clusterService.nodeName(),
                workDir.resolve("tx"),
                scheduledExecutorService,
                executor,
                new NoOpLogSyncer(),
                failureProcessor
        );

        components.add(sharedRockDbStorage);

        outgoingSnapshotsManager = new OutgoingSnapshotsManager(
                clusterService.nodeName(),
                clusterService.messagingService(),
                failureProcessor
        );

        components.add(outgoingSnapshotsManager);

        logStorageFactory = SharedLogStorageFactoryUtils.create(
                "table data log",
                clusterService.nodeName(),
                componentWorkingDir.raftLogPath(),
                true
        );

        components.add(logStorageFactory);

        assertThat(startAsync(new ComponentContext(), components), willCompleteSuccessfully());

        txStateStorage = new TxStateRocksDbStorage(PARTITION_ID.zoneId(), 10, sharedRockDbStorage);

        partitionSnapshotStorage = new PartitionSnapshotStorage(
                new ZonePartitionKey(PARTITION_ID.zoneId(), PARTITION_ID.partitionId()),
                clusterService.topologyService(),
                outgoingSnapshotsManager,
                new PartitionTxStateAccessImpl(txStateStorage.getOrCreatePartitionStorage(PARTITION_ID.partitionId())),
                catalogService,
                failureProcessor,
                executor,
                new LogStorageAccessImpl(replicaManager)
        );
    }

    @AfterEach
    void tearDown() throws NodeStoppingException {
        stopRaftGroupNode();

        reverse(components);

        components.forEach(IgniteComponent::beforeNodeStop);

        assertThat(stopAsync(new ComponentContext(), components), willCompleteSuccessfully());
    }

    private RaftGroupService startRaftGroupNode(int... tableIds) throws NodeStoppingException {
        currentRaftListener = new ZonePartitionRaftListener(
                PARTITION_ID,
                txStateStorage.getOrCreatePartitionStorage(PARTITION_ID.partitionId()),
                txManager,
                new SafeTimeValuesTracker(HybridTimestamp.MIN_VALUE),
                new PendingComparableValuesTracker<>(0L),
                outgoingSnapshotsManager,
                executor
        );

        for (int tableId : tableIds) {
            // We need to remove the storage, because this method can be called multiple times with the same ids and there's an assertion
            // inside.
            partitionSnapshotStorage.removeMvPartition(tableId);

            partitionSnapshotStorage.addMvPartition(tableId, mockStorage(tableId).storageAccess);

            currentRaftListener.addTableProcessorOnRecovery(tableId, createTableProcessor(tableId));
        }

        var peersAndLearners = PeersAndLearners.fromConsistentIds(Set.of(clusterService.nodeName()));

        RaftGroupOptions options = RaftGroupOptions.forPersistentStores();

        options.snapshotStorageFactory(new PartitionSnapshotStorageFactory(partitionSnapshotStorage));

        RaftGroupOptionsConfigurer raftGroupOptionsConfigurer = configureProperties(
                logStorageFactory,
                componentWorkingDir.metaPath()
        );

        raftGroupOptionsConfigurer.configure(options);

        return raftManager.startRaftGroupNode(
                new RaftNodeId(PARTITION_ID, new Peer(clusterService.nodeName())),
                peersAndLearners,
                currentRaftListener,
                RaftGroupEventsListener.noopLsnr,
                options
        );
    }

    private void addTableProcessor(int tableId) {
        currentRaftListener.addTableProcessor(tableId, createTableProcessor(tableId));
    }

    private void stopRaftGroupNode() throws NodeStoppingException {
        raftManager.stopRaftNodes(PARTITION_ID);
    }

    private RaftTableProcessor createTableProcessor(int tableId) {
        var storage = new SnapshotAwarePartitionDataStorage(
                tableId,
                mockStorage(tableId).storage,
                outgoingSnapshotsManager,
                new ZonePartitionKey(PARTITION_ID.zoneId(), PARTITION_ID.partitionId())
        );

        LeasePlacementDriver placementDriver = mock(LeasePlacementDriver.class);
        lenient().when(placementDriver.getCurrentPrimaryReplica(any(), any())).thenReturn(null);

        ClockService clockService = mock(ClockService.class);
        lenient().when(clockService.current()).thenReturn(clock.current());

        return new PartitionListener(
                txManager,
                storage,
                storageUpdateHandler,
                txStateStorage.getOrCreatePartitionStorage(PARTITION_ID.partitionId()),
                new SafeTimeValuesTracker(HybridTimestamp.MIN_VALUE),
                catalogService,
                schemaRegistry,
                indexMetaStorage,
                clusterService.topologyService().localMember().id(),
                minimumRequiredTimeCollectorService,
                executor,
                placementDriver,
                clockService,
                new ZonePartitionId(PARTITION_ID.zoneId(), PARTITION_ID.partitionId())
        );
    }

    private MockMvPartitionStorage mockStorage(int tableId) {
        return storagesByTableId.computeIfAbsent(tableId, id -> new MockMvPartitionStorage(clusterService.topologyService().localMember()));
    }

    @Test
    void testLogReplayWithoutSnapshotAndWithEmptyStorages() throws NodeStoppingException {
        int[] tableIds = {1, 2, 3};

        RaftGroupService raftGroupService = startRaftGroupNode(tableIds);

        List<UUID> rowIds = applyRandomUpdateCommands(raftGroupService, tableIds);

        stopRaftGroupNode();

        clearStorages();

        clearInvocations(storageUpdateHandler);

        startRaftGroupNode(tableIds);

        rowIds.forEach(this::verifyRowWasUpdated);
    }

    @Test
    void testLogReplayWithoutSnapshotAndWithoutEmptyStorages() throws NodeStoppingException {
        int[] tableIds = {1, 2, 3};

        RaftGroupService raftGroupService = startRaftGroupNode(tableIds);

        List<UUID> rowIds = applyRandomUpdateCommands(raftGroupService, tableIds);

        stopRaftGroupNode();

        clearInvocations(storageUpdateHandler);

        startRaftGroupNode(tableIds);

        rowIds.forEach(this::verifyRowWasNotUpdated);
    }

    @Test
    void testLogReplayWithSnapshotAndWithEmptyStorage() throws NodeStoppingException {
        int[] tableIds = {1, 2};

        RaftGroupService raftGroupService = startRaftGroupNode(tableIds);

        List<UUID> rowIds = applyRandomUpdateCommands(raftGroupService, tableIds);

        // Execute a snapshot and then apply some commands on top of it for a new table.
        // We then expect that the new commands will be re-applied on startup.
        assertThat(
                raftGroupService.snapshot(new Peer(clusterService.nodeName()), true),
                willCompleteSuccessfully()
        );

        currentRaftListener.addTableProcessor(3, createTableProcessor(3));

        List<UUID> rowIdsForTable3 = applyRandomUpdateCommands(raftGroupService, 3);

        stopRaftGroupNode();

        clearInvocations(storageUpdateHandler);

        // Emulate a situation as if the storage wasn't able to flush its data.
        storagesByTableId.remove(3);

        startRaftGroupNode(1, 2, 3);

        rowIds.forEach(this::verifyRowWasNotUpdated);

        rowIdsForTable3.forEach(this::verifyRowWasUpdated);
    }

    /**
     * Test recovery when one of the table processors has been added after a snapshot but was able to persist its data.
     */
    @Test
    void testLogReplayWithSnapshotAndStorageFlush() throws NodeStoppingException {
        int[] tableIds = {1, 2};

        RaftGroupService raftGroupService = startRaftGroupNode(tableIds);

        List<UUID> rowIds = applyRandomUpdateCommands(raftGroupService, tableIds);

        assertThat(
                raftGroupService.snapshot(new Peer(clusterService.nodeName()), true),
                willCompleteSuccessfully()
        );

        addTableProcessor(3);

        List<UUID> rowIdsForTable3 = applyRandomUpdateCommands(raftGroupService, 3);

        stopRaftGroupNode();

        clearInvocations(storageUpdateHandler);

        startRaftGroupNode(1, 2, 3);

        rowIds.forEach(this::verifyRowWasNotUpdated);
        rowIdsForTable3.forEach(this::verifyRowWasNotUpdated);
    }

    @Test
    void throwsInconsistentLogErrorAfterSnapshotWithEmptyStorage() throws NodeStoppingException {
        int[] tableIds = {1};

        RaftGroupService raftGroupService = startRaftGroupNode(tableIds);

        applyRandomUpdateCommands(raftGroupService, tableIds);

        assertThat(
                raftGroupService.snapshot(new Peer(clusterService.nodeName()), true),
                willCompleteSuccessfully()
        );

        stopRaftGroupNode();

        clearStorages();

        // Mock storages are empty on startup, so we should get an exception that part of the log is missing.
        assertThrows(IgniteInternalException.class, () -> startRaftGroupNode(tableIds));
    }

    private List<UUID> applyRandomUpdateCommands(RaftGroupService raftGroupService, int... tableIds) {
        var commands = new ArrayList<UUID>();

        for (int tableId : tableIds) {
            UUID id = UUID.randomUUID();

            assertThat(raftGroupService.run(createUpdateCommand(id, tableId)), willCompleteSuccessfully());

            commands.add(id);
        }

        commands.forEach(this::verifyRowWasUpdated);

        return commands;
    }

    private UpdateCommand createUpdateCommand(UUID id, int tableId) {
        HybridTimestamp now = clock.now();

        var binaryRow = new BinaryRowImpl(0, ByteBuffer.allocate(10));

        TimedBinaryRowMessage row = MESSAGE_FACTORY.timedBinaryRowMessage()
                .timestamp(now)
                .binaryRowMessage(MESSAGE_FACTORY.binaryRowMessage()
                        .binaryTuple(binaryRow.tupleSlice())
                        .schemaVersion(binaryRow.schemaVersion())
                        .build())
                .build();

        return MESSAGE_FACTORY.updateCommandV2()
                .tableId(tableId)
                .commitPartitionId(toReplicationGroupIdMessage(new ReplicaMessagesFactory(), PARTITION_ID))
                .rowUuid(id)
                .messageRowToUpdate(row)
                .txId(transactionId(now, 0))
                .full(true)
                .initiatorTime(now)
                .txCoordinatorId(id)
                .requiredCatalogVersion(0)
                .leaseStartTime(0L)
                .safeTime(now)
                .build();
    }

    private void verifyRowWasUpdated(UUID rowId) {
        verify(storageUpdateHandler, timeout(1000)).handleUpdate(
                any(),
                eq(rowId),
                any(),
                any(),
                anyBoolean(),
                any(),
                any(),
                any(),
                any()
        );
    }

    private void verifyRowWasNotUpdated(UUID rowId) {
        verify(storageUpdateHandler, never()).handleUpdate(
                any(),
                eq(rowId),
                any(),
                any(),
                anyBoolean(),
                any(),
                any(),
                any(),
                any()
        );
    }

    private void clearStorages() {
        storagesByTableId.clear();
    }
}
