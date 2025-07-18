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

package org.apache.ignite.internal.table.distributed.storage;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.colocationEnabled;
import static org.apache.ignite.internal.network.utils.ClusterServiceTestUtils.clusterService;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.apache.ignite.internal.util.IgniteUtils.startAsync;
import static org.apache.ignite.internal.util.IgniteUtils.stopAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.components.SystemPropertiesNodeProperties;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.NoOpFailureManager;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.ClockServiceImpl;
import org.apache.ignite.internal.hlc.ClockWaiter;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.lowwatermark.TestLowWatermark;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.network.ClusterNodeResolver;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.StaticNodeFinder;
import org.apache.ignite.internal.partition.replicator.schema.ValidationSchemasSource;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.leases.Lease;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.service.RaftCommandRunner;
import org.apache.ignite.internal.replicator.PartitionGroupId;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.SchemaSyncService;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.StreamerReceiverRunner;
import org.apache.ignite.internal.table.distributed.StorageUpdateHandler;
import org.apache.ignite.internal.table.distributed.index.IndexMetaStorage;
import org.apache.ignite.internal.table.distributed.replicator.PartitionReplicaListener;
import org.apache.ignite.internal.table.distributed.replicator.TransactionStateResolver;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.RemotelyTriggeredResourceRegistry;
import org.apache.ignite.internal.tx.impl.TransactionInflights;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.util.Lazy;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.table.QualifiedNameHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for distributed aspects of the {@link InternalTable#estimatedSize} method.
 */
@ExtendWith(ExecutorServiceExtension.class)
@ExtendWith(MockitoExtension.class)
@ExtendWith(ConfigurationExtension.class)
public class InternalTableEstimatedSizeTest extends BaseIgniteAbstractTest {
    private static final String TABLE_NAME = "TEST";

    private static final int TABLE_ID = 1;

    private static final int ZONE_ID = 2;

    private static final int PARTITIONS_NUM = 3;

    private static final List<ReplicationGroupId> PARTITION_GROUP_IDS = IntStream.range(0, PARTITIONS_NUM)
            .mapToObj(i -> replicationGroupId(i))
            .collect(toList());

    private static final UUID DEAD_NODE_ID = new UUID(-1, -1);

    private ClusterNode node;

    private InternalTableImpl table;

    private MessagingService messagingService;

    private static ReplicationGroupId replicationGroupId(int partId) {
        return colocationEnabled() ? new ZonePartitionId(ZONE_ID, partId) : new TablePartitionId(TABLE_ID, partId);
    }

    @Mock
    private PlacementDriver placementDriver;

    private final HybridClockImpl clock = new HybridClockImpl();

    private final ComponentContext componentContext = new ComponentContext();

    private final List<MvPartitionStorage> partitionStorages = new ArrayList<>(PARTITIONS_NUM);

    private final List<IgniteComponent> components = new ArrayList<>();

    @InjectExecutorService
    private ScheduledExecutorService scheduledExecutor;

    @BeforeEach
    void setUp(
            TestInfo testInfo,
            @Mock TxManager txManager,
            @Mock LockManager lockManager,
            @Mock MvTableStorage tableStorage,
            @Mock TxStateStorage txStateStorage,
            @Mock TxStatePartitionStorage txStatePartitionStorage,
            @Mock TransactionStateResolver transactionStateResolver,
            @Mock StorageUpdateHandler storageUpdateHandler,
            @Mock ValidationSchemasSource validationSchemasSource,
            @Mock SchemaSyncService schemaSyncService,
            @Mock CatalogService catalogService,
            @Mock RemotelyTriggeredResourceRegistry remotelyTriggeredResourceRegistry,
            @Mock SchemaRegistry schemaRegistry,
            @Mock IndexMetaStorage indexMetaStorage,
            @InjectConfiguration ReplicationConfiguration replicationConfiguration
    ) {
        String nodeName = testNodeName(testInfo, 0);

        var addr = new NetworkAddress("localhost", 10_000);

        ClusterService clusterService = spy(clusterService(nodeName, addr.port(), new StaticNodeFinder(List.of(addr))));

        messagingService = spy(clusterService.messagingService());

        when(clusterService.messagingService()).thenReturn(messagingService);

        components.add(clusterService);

        var clockWaiter = new ClockWaiter(nodeName, clock, scheduledExecutor);

        components.add(clockWaiter);

        MetaStorageManager metaStorageManager = StandaloneMetaStorageManager.create(nodeName, clock);

        components.add(metaStorageManager);

        assertThat(startAsync(componentContext, components), willCompleteSuccessfully());

        assertThat(metaStorageManager.deployWatches(), willCompleteSuccessfully());

        node = clusterService.topologyService().localMember();

        var clockService = new ClockServiceImpl(clock, clockWaiter, () -> 0);

        table = new InternalTableImpl(
                QualifiedNameHelper.fromNormalized(SqlCommon.DEFAULT_SCHEMA_NAME, TABLE_NAME),
                ZONE_ID,
                TABLE_ID,
                PARTITIONS_NUM,
                clusterService.topologyService(),
                txManager,
                tableStorage,
                txStateStorage,
                new ReplicaService(clusterService.messagingService(), clock, replicationConfiguration),
                clockService,
                HybridTimestampTracker.atomicTracker(null),
                placementDriver,
                new TransactionInflights(placementDriver, clockService),
                () -> null,
                mock(StreamerReceiverRunner.class),
                () -> 10_000L,
                () -> 10_000L,
                colocationEnabled()
        );

        when(catalogService.catalog(anyInt())).thenReturn(mock(Catalog.class));

        List<PartitionReplicaListener> partitionReplicaListeners = IntStream.range(0, PARTITIONS_NUM)
                .mapToObj(partId -> createPartitionReplicaListener(
                        partId,
                        txManager,
                        lockManager,
                        clockService,
                        txStatePartitionStorage,
                        transactionStateResolver,
                        storageUpdateHandler,
                        validationSchemasSource,
                        node,
                        schemaSyncService,
                        catalogService,
                        placementDriver,
                        clusterService.topologyService(),
                        remotelyTriggeredResourceRegistry,
                        schemaRegistry,
                        indexMetaStorage
                ))
                .collect(toList());

        lenient().doAnswer(invocation -> {
            ReplicaRequest request = invocation.getArgument(1);

            var tablePartitionId = (PartitionGroupId) request.groupId().asReplicationGroupId();

            return partitionReplicaListeners.get(tablePartitionId.partitionId())
                    .invoke(request, node.id())
                    .thenApply(replicaResult -> new ReplicaMessagesFactory()
                            .replicaResponse()
                            .result(replicaResult.result())
                            .build()
                    );
        }).when(messagingService).invoke(eq(nodeName), any(ReplicaRequest.class), anyLong());
    }

    @AfterEach
    void tearDown() throws Exception {
        Collections.reverse(components);

        closeAll(components.stream().map(c -> c::beforeNodeStop));

        assertThat(stopAsync(componentContext, components), willCompleteSuccessfully());
    }

    private PartitionReplicaListener createPartitionReplicaListener(
            int partId,
            TxManager txManager,
            LockManager lockManager,
            ClockService clockService,
            TxStatePartitionStorage txStatePartitionStorage,
            TransactionStateResolver transactionStateResolver,
            StorageUpdateHandler storageUpdateHandler,
            ValidationSchemasSource validationSchemasSource,
            ClusterNode node,
            SchemaSyncService schemaSyncService,
            CatalogService catalogService,
            PlacementDriver placementDriver,
            ClusterNodeResolver clusterNodeResolver,
            RemotelyTriggeredResourceRegistry remotelyTriggeredResourceRegistry,
            SchemaRegistry schemaRegistry,
            IndexMetaStorage indexMetaStorage
    ) {
        MvPartitionStorage partitionStorage = mock(MvPartitionStorage.class);

        partitionStorages.add(partitionStorage);

        return new PartitionReplicaListener(
                partitionStorage,
                new RaftCommandRunner() {
                    @Override
                    public <R> CompletableFuture<R> run(Command cmd) {
                        return nullCompletedFuture();
                    }

                    @Override
                    public <R> CompletableFuture<R> run(Command cmd, long timeoutMillis) {
                        return nullCompletedFuture();
                    }
                },
                txManager,
                lockManager,
                ForkJoinPool.commonPool(),
                colocationEnabled() ? new ZonePartitionId(ZONE_ID, partId) : new TablePartitionId(TABLE_ID, partId),
                TABLE_ID,
                Map::of,
                new Lazy<>(() -> null),
                Map::of,
                clockService,
                new PendingComparableValuesTracker<>(HybridTimestamp.MIN_VALUE),
                txStatePartitionStorage,
                transactionStateResolver,
                storageUpdateHandler,
                validationSchemasSource,
                node,
                schemaSyncService,
                catalogService,
                placementDriver,
                clusterNodeResolver,
                remotelyTriggeredResourceRegistry,
                schemaRegistry,
                indexMetaStorage,
                new TestLowWatermark(),
                new NoOpFailureManager(),
                new SystemPropertiesNodeProperties()
        );
    }

    /**
     * Validates that {@link InternalTable#estimatedSize} works correctly using the test setup.
     */
    @Test
    void testHappyCase() {
        HybridTimestamp startTime = HybridTimestamp.MIN_VALUE;
        HybridTimestamp expireTime = HybridTimestamp.MAX_VALUE;

        PARTITION_GROUP_IDS.forEach(groupId -> {
            var replicaMeta = new Lease(node.name(), node.id(), startTime, expireTime, groupId);

            when(placementDriver.awaitPrimaryReplica(eq(groupId), any(), anyLong(), any()))
                    .thenReturn(completedFuture(replicaMeta));
            when(placementDriver.getPrimaryReplica(eq(groupId), any()))
                    .thenReturn(completedFuture(replicaMeta));
        });

        validateEstimatedSize();

        // One request per partition.
        verify(messagingService, times(3)).invoke(anyString(), any(ReplicaRequest.class), anyLong());
    }

    /**
     * Tests that a retry will happen when a Primary Replica lease expires during the call to {@link InternalTable#estimatedSize}.
     */
    @Test
    void testRetryOnReplicaMiss() {
        HybridTimestamp startTime = HybridTimestamp.MIN_VALUE;
        HybridTimestamp expireTime = clock.now().addPhysicalTime(10_000);

        for (int i = 0; i < PARTITION_GROUP_IDS.size(); i++) {
            ReplicationGroupId groupId = PARTITION_GROUP_IDS.get(i);

            var replicaMeta = new Lease(node.name(), node.id(), startTime, expireTime, groupId);

            // Emulate lease expiration on the first replica (getPrimaryReplica will return null). We then expect that
            // a second request will be sent with a different timestamp.
            if (i == 0) {
                var newReplicaMeta = new Lease(node.name(), node.id(), expireTime, HybridTimestamp.MAX_VALUE, groupId);

                when(placementDriver.awaitPrimaryReplica(eq(groupId), any(), anyLong(), any()))
                        .thenReturn(completedFuture(replicaMeta));

                when(placementDriver.awaitPrimaryReplica(eq(groupId), eq(expireTime.tick()), anyLong(), any()))
                        .thenReturn(completedFuture(newReplicaMeta));

                when(placementDriver.getPrimaryReplica(eq(groupId), any()))
                        .thenReturn(nullCompletedFuture())
                        .thenReturn(completedFuture(newReplicaMeta));
            } else {
                when(placementDriver.awaitPrimaryReplica(eq(groupId), any(), anyLong(), any()))
                        .thenReturn(completedFuture(replicaMeta));
                when(placementDriver.getPrimaryReplica(eq(groupId), any()))
                        .thenReturn(completedFuture(replicaMeta));
            }
        }

        validateEstimatedSize();

        // One request per partition + one retry for the first partition.
        verify(messagingService, times(4)).invoke(anyString(), any(ReplicaRequest.class), anyLong());
    }

    /**
     * Tests that a retry will happen when the Primary Replica dies during the call to {@link InternalTable#estimatedSize}.
     */
    @Test
    void testRetryOnReplicaUnavailable() {
        HybridTimestamp startTime = HybridTimestamp.MIN_VALUE;
        HybridTimestamp expireTime = clock.now().addPhysicalTime(10_000);

        for (int i = 0; i < PARTITION_GROUP_IDS.size(); i++) {
            ReplicationGroupId groupId = PARTITION_GROUP_IDS.get(i);

            var replicaMeta = new Lease(node.name(), node.id(), startTime, expireTime, groupId);

            // Emulate Primary Replica death by issuing a lease for a non-existent node. We then expect that a retry to the correct node
            // will be issued.
            if (i == 0) {
                var fakeReplicaMeta = new Lease("Dead node name", DEAD_NODE_ID, HybridTimestamp.MIN_VALUE, expireTime, groupId);

                var newReplicaMeta = new Lease(node.name(), node.id(), expireTime, HybridTimestamp.MAX_VALUE, groupId);

                when(placementDriver.awaitPrimaryReplica(eq(groupId), any(), anyLong(), any()))
                        .thenReturn(completedFuture(fakeReplicaMeta));

                when(placementDriver.awaitPrimaryReplica(eq(groupId), eq(expireTime.tick()), anyLong(), any()))
                        .thenReturn(completedFuture(newReplicaMeta));

                when(placementDriver.getPrimaryReplica(eq(groupId), any()))
                        .thenReturn(completedFuture(newReplicaMeta));
            } else {
                when(placementDriver.awaitPrimaryReplica(eq(groupId), any(), anyLong(), any()))
                        .thenReturn(completedFuture(replicaMeta));
                when(placementDriver.getPrimaryReplica(eq(groupId), any()))
                        .thenReturn(completedFuture(replicaMeta));
            }
        }

        validateEstimatedSize();

        // One request per partition + one retry for the first partition.
        verify(messagingService, times(4)).invoke(anyString(), any(ReplicaRequest.class), anyLong());
    }

    /**
     * Tests that a retry will happen when the Primary Replica becomes unavailable during the call to {@link InternalTable#estimatedSize}.
     */
    @Test
    void testRetryOnTimeout() {
        HybridTimestamp startTime = HybridTimestamp.MIN_VALUE;
        HybridTimestamp expireTime = clock.now().addPhysicalTime(10_000);

        for (int i = 0; i < PARTITION_GROUP_IDS.size(); i++) {
            ReplicationGroupId groupId = PARTITION_GROUP_IDS.get(i);

            var replicaMeta = new Lease(node.name(), node.id(), startTime, expireTime, groupId);

            // Emulate a network timeout, we use a fake lease to not override an existing mocking on the messagingService.
            if (i == 0) {
                var fakeReplicaMeta = new Lease("Dead node name", DEAD_NODE_ID, HybridTimestamp.MIN_VALUE, expireTime, groupId);

                var newReplicaMeta = new Lease(node.name(), node.id(), expireTime, HybridTimestamp.MAX_VALUE, groupId);

                doReturn(failedFuture(new TimeoutException()))
                        .when(messagingService).invoke(eq(fakeReplicaMeta.getLeaseholder()), any(), anyLong());

                when(placementDriver.awaitPrimaryReplica(eq(groupId), any(), anyLong(), any()))
                        .thenReturn(completedFuture(fakeReplicaMeta));

                when(placementDriver.awaitPrimaryReplica(eq(groupId), eq(expireTime.tick()), anyLong(), any()))
                        .thenReturn(completedFuture(newReplicaMeta));

                when(placementDriver.getPrimaryReplica(eq(groupId), any()))
                        .thenReturn(completedFuture(newReplicaMeta));
            } else {
                when(placementDriver.awaitPrimaryReplica(eq(groupId), any(), anyLong(), any()))
                        .thenReturn(completedFuture(replicaMeta));
                when(placementDriver.getPrimaryReplica(eq(groupId), any()))
                        .thenReturn(completedFuture(replicaMeta));
            }
        }

        validateEstimatedSize();

        // One request per partition + one retry for the first partition.
        verify(messagingService, times(4)).invoke(anyString(), any(ReplicaRequest.class), anyLong());
    }

    private void validateEstimatedSize() {
        long expectedSum = 0;

        for (MvPartitionStorage partitionStorage : partitionStorages) {
            long size = ThreadLocalRandom.current().nextLong(100);

            when(partitionStorage.estimatedSize()).thenReturn(size);

            expectedSum += size;
        }

        assertThat(table.estimatedSize(), willBe(expectedSum));
    }
}
