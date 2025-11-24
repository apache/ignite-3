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

package org.apache.ignite.internal.partition.replicator;

import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.TEST_DELAY_DURATION;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.stablePartAssignmentsKey;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.COLOCATION_FEATURE_FLAG;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.THREAD_ASSERTIONS_ENABLED;
import static org.apache.ignite.internal.partition.replicator.LocalPartitionReplicaEvent.AFTER_REPLICA_DESTROYED;
import static org.apache.ignite.internal.partition.replicator.LocalPartitionReplicaEvent.AFTER_REPLICA_STOPPED;
import static org.apache.ignite.internal.partition.replicator.LocalPartitionReplicaEvent.BEFORE_REPLICA_DESTROYED;
import static org.apache.ignite.internal.partition.replicator.LocalPartitionReplicaEvent.BEFORE_REPLICA_STOPPED;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.emptySetCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.startAsync;
import static org.apache.ignite.internal.util.IgniteUtils.stopAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogManagerImpl;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.catalog.storage.UpdateLogImpl;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.components.SystemPropertiesNodeProperties;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.partition.replicator.ZoneResourcesManager.ZonePartitionResources;
import org.apache.ignite.internal.partition.replicator.raft.ZonePartitionRaftListener;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionSnapshotStorage;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.RaftGroupOptionsConfigurer;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupService;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.raft.server.RaftGroupOptions;
import org.apache.ignite.internal.raft.storage.impl.LogStorageFactoryCreator;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.SchemaSyncService;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.storage.state.ThreadAssertingTxStateStorage;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.tx.storage.state.rocksdb.TxStateRocksDbPartitionStorage;
import org.apache.ignite.internal.tx.storage.state.rocksdb.TxStateRocksDbSharedStorage;
import org.apache.ignite.internal.tx.storage.state.rocksdb.TxStateRocksDbStorage;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.internal.worker.ThreadAssertions;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@ExtendWith(ExecutorServiceExtension.class)
@ExtendWith(ConfigurationExtension.class)
@WithSystemProperty(key = COLOCATION_FEATURE_FLAG, value = "true")
@WithSystemProperty(key = THREAD_ASSERTIONS_ENABLED, value = "false")
@MockitoSettings(strictness = Strictness.LENIENT)
class PartitionReplicaLifecycleManagerTest extends BaseIgniteAbstractTest {
    private MetaStorageManager metaStorageManager;

    private CatalogManager catalogManager;

    private ReplicaManager replicaManager;

    private PartitionReplicaLifecycleManager partitionReplicaLifecycleManager;

    private final HybridClock clock = new HybridClockImpl();

    private ZoneResourcesManager zoneResourcesManager;

    private ZonePartitionResources commonZonePartitionResources;

    @Mock
    private Loza raftManager;

    @Mock
    TxStateRocksDbPartitionStorage txStatePartitionStorage;

    @BeforeEach
    void setUp(
            TestInfo testInfo,
            @Mock(answer = RETURNS_DEEP_STUBS) ClusterService clusterService,
            @Mock TopologyService topologyService,
            @Mock DistributionZoneManager distributionZoneManager,
            @Mock LowWatermark lowWatermark,
            @Mock ClockService clockService,
            @Mock PlacementDriver placementDriver,
            @Mock TopologyAwareRaftGroupService topologyAwareRaftGroupService,
            @Mock SchemaSyncService schemaSyncService,
            @Mock TxManager txManager,
            @Mock SchemaManager schemaManager,
            @Mock ClusterManagementGroupManager cmgManager,
            @Mock FailureManager failureManager,
            @Mock TopologyAwareRaftGroupServiceFactory topologyAwareRaftGroupServiceFactory,
            @Mock LogStorageFactoryCreator logStorageFactoryCreator,
            @Mock PartitionSnapshotStorage partitionSnapshotStorage,
            @Mock TxStateRocksDbSharedStorage sharedTxStateStorage,
            @Mock ZonePartitionRaftListener raftGroupListener,
            @Mock DataStorageManager dataStorageManager,
            @Mock CatalogService catalogService,
            @Mock OutgoingSnapshotsManager outgoingSnapshotsManager,
            @InjectExecutorService ExecutorService executorService,
            @InjectExecutorService ScheduledExecutorService scheduledExecutorService,
            @InjectConfiguration SystemDistributedConfiguration systemDistributedConfiguration

    ) throws NodeStoppingException {
        String nodeName = testNodeName(testInfo, 0);

        when(topologyService.localMember())
                .thenReturn(new ClusterNodeImpl(randomUUID(), nodeName, new NetworkAddress("localhost", 0)));
        when(clusterService.topologyService()).thenReturn(topologyService);
        when(topologyAwareRaftGroupService.unsubscribeLeader()).thenReturn(nullCompletedFuture());

        lenient().when(placementDriver.getPrimaryReplica(any(), any())).thenReturn(nullCompletedFuture());

        when(cmgManager.metaStorageNodes()).thenReturn(emptySetCompletedFuture());

        when(distributionZoneManager.dataNodes(any(), anyInt(), anyInt())).thenReturn(completedFuture(Set.of(nodeName)));

        commonZonePartitionResources = spy(new ZonePartitionResources(
                txStatePartitionStorage,
                raftGroupListener,
                partitionSnapshotStorage,
                new PendingComparableValuesTracker<>(0L)
        ));

        when(raftManager.startRaftGroupNode(any(), any(), any(), any(), any(RaftGroupOptions.class), any()))
                .thenReturn(topologyAwareRaftGroupService);

        lenient().when(schemaSyncService.waitForMetadataCompleteness(any())).thenReturn(nullCompletedFuture());

        metaStorageManager = StandaloneMetaStorageManager.create();

        catalogManager = new CatalogManagerImpl(
                new UpdateLogImpl(metaStorageManager, failureManager),
                clockService,
                failureManager,
                () -> TEST_DELAY_DURATION
        );

        replicaManager = spy(new ReplicaManager(
                nodeName,
                clusterService,
                cmgManager,
                clockService,
                Set.of(),
                placementDriver,
                executorService,
                () -> Long.MAX_VALUE,
                failureManager,
                null,
                topologyAwareRaftGroupServiceFactory,
                raftManager,
                RaftGroupOptionsConfigurer.EMPTY,
                logStorageFactoryCreator,
                executorService,
                groupId -> nullCompletedFuture(),
                executorService
        ));

        zoneResourcesManager = spy(new ZoneResourcesManager(
                sharedTxStateStorage,
                txManager,
                outgoingSnapshotsManager,
                topologyService,
                catalogService,
                failureManager,
                executorService,
                replicaManager
        ) {
            @Override
            protected TxStateStorage createTxStateStorage(int zoneId, int partitionCount) {
                TxStateStorage txStateStorage = new TxStateRocksDbStorage(zoneId, partitionCount, sharedTxStateStorage) {
                    @Override
                    protected TxStateRocksDbPartitionStorage createPartitionStorage(int partitionId) {
                        return txStatePartitionStorage;
                    }
                };

                if (ThreadAssertions.enabled()) {
                    txStateStorage = new ThreadAssertingTxStateStorage(txStateStorage);
                }

                txStateStorage.start();

                return txStateStorage;
            }
        });

        partitionReplicaLifecycleManager = new PartitionReplicaLifecycleManager(
                catalogManager,
                replicaManager,
                distributionZoneManager,
                metaStorageManager,
                clusterService.topologyService(),
                lowWatermark,
                failureManager,
                new SystemPropertiesNodeProperties(),
                executorService,
                scheduledExecutorService,
                executorService,
                clockService,
                placementDriver,
                schemaSyncService,
                systemDistributedConfiguration,
                txManager,
                schemaManager,
                dataStorageManager,
                zoneResourcesManager
        );

        var componentContext = new ComponentContext();

        CompletableFuture<Void> startFuture = metaStorageManager.startAsync(componentContext)
                .thenCompose(v -> metaStorageManager.recoveryFinishedFuture())
                .thenCompose(v -> startAsync(componentContext, catalogManager, replicaManager, partitionReplicaLifecycleManager))
                .thenCompose(v -> metaStorageManager.deployWatches())
                .thenCompose(v -> catalogManager.catalogInitializationFuture());

        assertThat(startFuture, willCompleteSuccessfully());

        when(clockService.waitFor(any())).thenReturn(nullCompletedFuture());

        DistributionZonesTestUtil.createDefaultZone(catalogManager);
    }

    @AfterEach
    void tearDown() {
        List<IgniteComponent> components = List.of(partitionReplicaLifecycleManager, replicaManager, catalogManager, metaStorageManager);

        components.forEach(IgniteComponent::beforeNodeStop);

        assertThat(stopAsync(new ComponentContext(), components), willCompleteSuccessfully());
    }

    /**
     * Tests that resources of {@link PartitionReplicaLifecycleManager} are stopped in a correct order.
     */
    @Test
    void testStopOrder() throws NodeStoppingException {
        int zoneId = catalogManager.catalog(catalogManager.latestCatalogVersion()).defaultZone().id();

        var zonePartitionId = new ZonePartitionId(zoneId, 0);

        // Put empty stable assignments to force the replica manager to stop the running replicas.
        Assignments assignments = Assignments.of(Set.of(), 1);

        assertThat(
                metaStorageManager.put(stablePartAssignmentsKey(zonePartitionId), assignments.toBytes()),
                willCompleteSuccessfully()
        );

        InOrder inOrder = inOrder(raftManager, zoneResourcesManager, replicaManager);

        inOrder.verify(raftManager, timeout(1_000)).stopRaftNodes(zonePartitionId);
        inOrder.verify(zoneResourcesManager, timeout(1_000)).destroyZonePartitionResources(zonePartitionId);
        inOrder.verify(replicaManager, timeout(1_000)).destroyReplicationProtocolStoragesDurably(zonePartitionId, false);
    }

    @Test
    void producesEventsOnPartitionRestart() {
        var beforeReplicaStoppedFuture = new CompletableFuture<LocalPartitionReplicaEventParameters>();
        var afterReplicaStoppedFuture = new CompletableFuture<LocalPartitionReplicaEventParameters>();

        partitionReplicaLifecycleManager.listen(BEFORE_REPLICA_STOPPED, EventListener.fromConsumer(beforeReplicaStoppedFuture::complete));
        partitionReplicaLifecycleManager.listen(AFTER_REPLICA_STOPPED, EventListener.fromConsumer(params -> {
            assertTrue(beforeReplicaStoppedFuture.isDone(), "AFTER_REPLICA_STOPPED event received before BEFORE_REPLICA_STOPPED");

            afterReplicaStoppedFuture.complete(params);
        }));

        int zoneId = catalogManager.catalog(catalogManager.latestCatalogVersion()).defaultZone().id();

        var zonePartitionId = new ZonePartitionId(zoneId, 0);

        assertThat(
                partitionReplicaLifecycleManager.restartPartition(zonePartitionId, Long.MAX_VALUE, clock.nowLong()),
                willCompleteSuccessfully()
        );

        assertThat(beforeReplicaStoppedFuture.thenApply(LocalPartitionReplicaEventParameters::zonePartitionId), willBe(zonePartitionId));
        assertThat(afterReplicaStoppedFuture.thenApply(LocalPartitionReplicaEventParameters::zonePartitionId), willBe(zonePartitionId));
    }

    @Test
    void producesEventsOnPartitionDestroy() {
        var beforeReplicaDestroyedFuture = new CompletableFuture<LocalPartitionReplicaEventParameters>();
        var afterReplicaDestroyedFuture = new CompletableFuture<LocalPartitionReplicaEventParameters>();

        partitionReplicaLifecycleManager.listen(
                BEFORE_REPLICA_DESTROYED,
                EventListener.fromConsumer(beforeReplicaDestroyedFuture::complete)
        );

        partitionReplicaLifecycleManager.listen(AFTER_REPLICA_DESTROYED, EventListener.fromConsumer(params -> {
            assertTrue(beforeReplicaDestroyedFuture.isDone(), "AFTER_REPLICA_DESTROYED event received before BEFORE_REPLICA_DESTROYED");

            afterReplicaDestroyedFuture.complete(params);
        }));

        int zoneId = catalogManager.catalog(catalogManager.latestCatalogVersion()).defaultZone().id();

        var zonePartitionId = new ZonePartitionId(zoneId, 0);

        // Put empty stable assignments to force the replica manager to stop the running replicas.
        Assignments assignments = Assignments.of(Set.of(), 1);

        assertThat(
                metaStorageManager.put(stablePartAssignmentsKey(zonePartitionId), assignments.toBytes()),
                willCompleteSuccessfully()
        );

        assertThat(beforeReplicaDestroyedFuture.thenApply(LocalPartitionReplicaEventParameters::zonePartitionId), willBe(zonePartitionId));
        assertThat(afterReplicaDestroyedFuture.thenApply(LocalPartitionReplicaEventParameters::zonePartitionId), willBe(zonePartitionId));
    }

    @Test
    public void partitionLifecycleManagerStopsCorrectWhenAllComponentsAreStoppedFine() throws Exception {
        assertDoesNotThrow(() -> partitionReplicaLifecycleManager.beforeNodeStop());

        assertThat(partitionReplicaLifecycleManager.stopAsync(), willCompleteSuccessfully());

        verify(replicaManager, times(CatalogUtils.DEFAULT_PARTITION_COUNT)).stopReplica(any());
    }

    @Test
    public void partitionLifecycleManagerStopsCorrectWhenReplicasAreStoppedExceptionally() throws Exception {
        doThrow(new NodeStoppingException()).when(replicaManager).stopReplica(any());

        assertDoesNotThrow(() -> partitionReplicaLifecycleManager.beforeNodeStop());

        assertThat(partitionReplicaLifecycleManager.stopAsync(), willCompleteSuccessfully());

        verify(replicaManager, times(CatalogUtils.DEFAULT_PARTITION_COUNT)).stopReplica(any());

        // Do reset for correct replica manager stop on tear down.
        reset(replicaManager);
    }

    @Test
    public void partitionLifecycleManagerStopsCorrectWhenTxStatePartitionStoragesAreStoppedExceptionally() throws Exception {
        doReturn(commonZonePartitionResources).when(zoneResourcesManager).getZonePartitionResources(any());

        int defaultZoneId = catalogManager.catalog(catalogManager.latestCatalogVersion()).defaultZone().id();
        List<ZonePartitionResources> defaultZoneResources = IntStream.range(0, CatalogUtils.DEFAULT_PARTITION_COUNT)
                .mapToObj(partId -> new ZonePartitionId(defaultZoneId, partId))
                .map(partitionReplicaLifecycleManager::zonePartitionResources)
                .collect(Collectors.toList());

        defaultZoneResources.forEach(resources -> {
            TxStatePartitionStorage txStorage = resources.txStatePartitionStorage();

            doThrow(new RuntimeException()).when(txStorage).close();
        });

        assertDoesNotThrow(() -> partitionReplicaLifecycleManager.beforeNodeStop());

        assertThat(partitionReplicaLifecycleManager.stopAsync(), willThrow(RuntimeException.class));

        verify(replicaManager, times(CatalogUtils.DEFAULT_PARTITION_COUNT)).stopReplica(any());

        defaultZoneResources.forEach(resources -> verify(resources.txStatePartitionStorage(), atLeastOnce()).close());
    }
}
