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
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.stablePartAssignmentsKey;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.COLOCATION_FEATURE_FLAG;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.THREAD_ASSERTIONS_ENABLED;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.emptySetCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogManagerImpl;
import org.apache.ignite.internal.catalog.storage.UpdateLogImpl;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.RaftGroupOptionsConfigurer;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.raft.storage.impl.LogStorageFactoryCreator;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.SchemaSyncService;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@ExtendWith(ExecutorServiceExtension.class)
@ExtendWith(ConfigurationExtension.class)
@WithSystemProperty(key = COLOCATION_FEATURE_FLAG, value = "true")
@WithSystemProperty(key = THREAD_ASSERTIONS_ENABLED, value = "false")
class PartitionReplicaLifecycleManagerTest extends BaseIgniteAbstractTest {
    private MetaStorageManager metaStorageManager;

    private CatalogManager catalogManager;

    private ReplicaManager replicaManager;

    private PartitionReplicaLifecycleManager partitionReplicaLifecycleManager;

    @Mock
    private Loza raftManager;

    @Mock
    private ZoneResourcesManager zoneResourcesManager;

    @InjectExecutorService
    private ExecutorService executorService;

    @InjectExecutorService
    private ScheduledExecutorService scheduledExecutorService;

    @InjectConfiguration
    private SystemDistributedConfiguration systemDistributedConfiguration;

    @BeforeEach
    void setUp(
            TestInfo testInfo,
            @Mock(answer = Answers.RETURNS_DEEP_STUBS) ClusterService clusterService,
            @Mock DistributionZoneManager distributionZoneManager,
            @Mock LowWatermark lowWatermark,
            @Mock ClockService clockService,
            @Mock PlacementDriver placementDriver,
            @Mock SchemaSyncService schemaSyncService,
            @Mock TxManager txManager,
            @Mock SchemaManager schemaManager,
            @Mock ClusterManagementGroupManager cmgManager,
            @Mock FailureManager failureManager,
            @Mock TopologyAwareRaftGroupServiceFactory topologyAwareRaftGroupServiceFactory,
            @Mock LogStorageFactoryCreator logStorageFactoryCreator,
            @Mock OutgoingSnapshotsManager outgoingSnapshotsManager
    ) {
        String nodeName = testNodeName(testInfo, 0);

        when(clusterService.topologyService().localMember())
                .thenReturn(new ClusterNodeImpl(randomUUID(), nodeName, new NetworkAddress("localhost", 0)));

        when(placementDriver.getPrimaryReplica(any(), any())).thenReturn(nullCompletedFuture());

        when(cmgManager.metaStorageNodes()).thenReturn(emptySetCompletedFuture());

        when(distributionZoneManager.dataNodes(anyLong(), anyInt(), anyInt())).thenReturn(completedFuture(Set.of(nodeName)));

        metaStorageManager = StandaloneMetaStorageManager.create();

        catalogManager = new CatalogManagerImpl(new UpdateLogImpl(metaStorageManager), clockService);

        replicaManager = new ReplicaManager(
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
                groupId -> nullCompletedFuture()
        );

        partitionReplicaLifecycleManager = new PartitionReplicaLifecycleManager(
                catalogManager,
                replicaManager,
                distributionZoneManager,
                metaStorageManager,
                clusterService.topologyService(),
                lowWatermark,
                executorService,
                scheduledExecutorService,
                executorService,
                clockService,
                placementDriver,
                schemaSyncService,
                systemDistributedConfiguration,
                txManager,
                schemaManager,
                outgoingSnapshotsManager,
                zoneResourcesManager
        );

        var componentContext = new ComponentContext();

        assertThat(metaStorageManager.startAsync(componentContext), willCompleteSuccessfully());
        assertThat(metaStorageManager.recoveryFinishedFuture(), willCompleteSuccessfully());

        assertThat(catalogManager.startAsync(componentContext), willCompleteSuccessfully());
        assertThat(replicaManager.startAsync(componentContext), willCompleteSuccessfully());
        assertThat(partitionReplicaLifecycleManager.startAsync(componentContext), willCompleteSuccessfully());

        assertThat(metaStorageManager.deployWatches(), willCompleteSuccessfully());
        assertThat(catalogManager.catalogInitializationFuture(), willCompleteSuccessfully());
    }

    @AfterEach
    void tearDown() {
        List<IgniteComponent> components = List.of(partitionReplicaLifecycleManager, replicaManager, catalogManager, metaStorageManager);

        components.forEach(IgniteComponent::beforeNodeStop);

        IgniteUtils.stopAsync(new ComponentContext(), components);
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

        InOrder inOrder = inOrder(raftManager, zoneResourcesManager);

        inOrder.verify(raftManager, timeout(1_000)).stopRaftNodes(zonePartitionId);
        inOrder.verify(zoneResourcesManager, timeout(1_000)).destroyZonePartitionResources(zoneId, 0);
    }
}
