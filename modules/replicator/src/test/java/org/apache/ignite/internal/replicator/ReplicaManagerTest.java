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

package org.apache.ignite.internal.replicator;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.replicator.LocalReplicaEvent.AFTER_REPLICA_STARTED;
import static org.apache.ignite.internal.replicator.LocalReplicaEvent.BEFORE_REPLICA_STOPPED;
import static org.apache.ignite.internal.replicator.ReplicatorConstants.DEFAULT_IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.emptySetCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.failure.NoOpFailureProcessor;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.raft.Marshaller;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftManager;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupService;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.raft.storage.impl.VolatileLogStorageFactoryCreator;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.TopologyService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for {@link ReplicaManager}.
 */
@ExtendWith(MockitoExtension.class)
public class ReplicaManagerTest extends BaseIgniteAbstractTest {
    private ExecutorService requestsExecutor;

    private ReplicaManager replicaManager;

    @Mock
    private RaftManager raftManager;

    @BeforeEach
    void startReplicaManager(
            TestInfo testInfo,
            @Mock ClusterService clusterService,
            @Mock ClusterManagementGroupManager cmgManager,
            @Mock PlacementDriver placementDriver,
            @Mock MessagingService messagingService,
            @Mock TopologyService topologyService,
            @Mock Marshaller marshaller,
            @Mock TopologyAwareRaftGroupServiceFactory raftGroupServiceFactory,
            @Mock VolatileLogStorageFactoryCreator volatileLogStorageFactoryCreator
    ) {
        String nodeName = testNodeName(testInfo, 0);

        when(clusterService.messagingService()).thenReturn(messagingService);
        when(clusterService.topologyService()).thenReturn(topologyService);

        when(topologyService.localMember()).thenReturn(new ClusterNodeImpl(nodeName, nodeName, new NetworkAddress("foo", 0)));

        when(cmgManager.metaStorageNodes()).thenReturn(emptySetCompletedFuture());

        var clock = new HybridClockImpl();

        requestsExecutor = new ThreadPoolExecutor(
                0, 5,
                0, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                NamedThreadFactory.create(nodeName, "partition-operations", log)
        );

        replicaManager = new ReplicaManager(
                nodeName,
                clusterService,
                cmgManager,
                new TestClockService(clock),
                Set.of(),
                placementDriver,
                requestsExecutor,
                () -> DEFAULT_IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS,
                new NoOpFailureProcessor(),
                marshaller,
                raftGroupServiceFactory,
                raftManager,
                volatileLogStorageFactoryCreator
        );

        assertThat(replicaManager.startAsync(new ComponentContext()), willCompleteSuccessfully());
    }

    @AfterEach
    void stopReplicaManager() {
        CompletableFuture<?>[] replicaStopFutures = replicaManager.startedGroups().stream()
            .map(id -> {
                try {
                    return replicaManager.stopReplica(id);
                } catch (NodeStoppingException e) {
                    throw new AssertionError(e);
                }
            })
            .toArray(CompletableFuture[]::new);

        assertThat(allOf(replicaStopFutures), willCompleteSuccessfully());

        assertThat(replicaManager.stopAsync(new ComponentContext()), willCompleteSuccessfully());

        IgniteUtils.shutdownAndAwaitTermination(requestsExecutor, 10, TimeUnit.SECONDS);
    }

    /**
     * Tests that Replica Manager produces events when a Replica is started or stopped.
     */
    @Test
    void testReplicaEvents(
            TestInfo testInfo,
            @Mock EventListener<LocalReplicaEventParameters> createReplicaListener,
            @Mock EventListener<LocalReplicaEventParameters> removeReplicaListener,
            @Mock ReplicaListener replicaListener,
            @Mock TopologyAwareRaftGroupService raftGroupService
    ) throws NodeStoppingException {
        when(raftGroupService.unsubscribeLeader()).thenReturn(nullCompletedFuture());

        when(createReplicaListener.notify(any())).thenReturn(falseCompletedFuture());
        when(removeReplicaListener.notify(any())).thenReturn(falseCompletedFuture());

        replicaManager.listen(AFTER_REPLICA_STARTED, createReplicaListener);
        replicaManager.listen(BEFORE_REPLICA_STOPPED, removeReplicaListener);

        var groupId = new TablePartitionId(0, 0);
        when(replicaListener.raftClient()).thenReturn(raftGroupService);

        String nodeName = testNodeName(testInfo, 0);
        PeersAndLearners newConfiguration = PeersAndLearners.fromConsistentIds(Set.of(nodeName));

        CompletableFuture<Boolean> startReplicaFuture = replicaManager.startReplica(
                groupId,
                newConfiguration,
                (unused) -> { },
                (unused) -> replicaListener,
                new PendingComparableValuesTracker<>(0L),
                completedFuture(raftGroupService)
        );

        assertThat(startReplicaFuture, willCompleteSuccessfully());

        var expectedCreateParams = new LocalReplicaEventParameters(groupId);

        verify(createReplicaListener).notify(eq(expectedCreateParams));
        verify(removeReplicaListener, never()).notify(any());

        CompletableFuture<Boolean> stopReplicaFuture = replicaManager.stopReplica(groupId);

        assertThat(stopReplicaFuture, willBe(true));

        verify(createReplicaListener).notify(eq(expectedCreateParams));
        verify(removeReplicaListener).notify(eq(expectedCreateParams));
    }
}
