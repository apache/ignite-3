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

package org.apache.ignite.internal.index;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.createTestCatalogManager;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.BUILDING;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.REGISTERED;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.COLUMN_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.INDEX_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.LOCAL_NODE;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.LOGICAL_LOCAL_NODE;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.NODE_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.TABLE_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.createIndex;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.createTable;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.indexDescriptor;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.newPrimaryReplicaMeta;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.startBuildingIndex;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mock.Strictness.LENIENT;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.ClockWaiter;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.index.message.IndexMessagesFactory;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.PrimaryReplicaAwaitException;
import org.apache.ignite.internal.placementdriver.PrimaryReplicaAwaitTimeoutException;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.RecipientLeftException;
import org.apache.ignite.network.TopologyService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/** For {@link IndexBuildingStarterTask} testing. */
@ExtendWith(MockitoExtension.class)
public class IndexBuildingStarterTaskTest extends IgniteAbstractTest {
    private static final IndexMessagesFactory FACTORY = new IndexMessagesFactory();

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final HybridClock clock = new HybridClockImpl();

    private final ClockWaiter clockWaiter = spy(new ClockWaiter(NODE_NAME, clock));

    private final CatalogManager catalogManager = createTestCatalogManager(NODE_NAME, clockWaiter);

    private final ExecutorService executor = spy(newSingleThreadExecutor());

    private final ClusterService clusterService = createClusterService();

    @Mock
    private PlacementDriver placementDriver;

    @Mock(strictness = LENIENT)
    private LogicalTopologyService logicalTopologyService;

    private IndexBuildingStarterTask task;

    private CatalogIndexDescriptor indexDescriptor;

    @BeforeEach
    void setUp() {
        assertThat(allOf(clockWaiter.start(), catalogManager.start()), willCompleteSuccessfully());

        createTable(catalogManager, TABLE_NAME, COLUMN_NAME);
        createIndex(catalogManager, TABLE_NAME, INDEX_NAME, COLUMN_NAME);

        indexDescriptor = indexDescriptor(catalogManager, INDEX_NAME, clock);

        CompletableFuture<ReplicaMeta> localNodeReplicaMetaFuture = completedFuture(
                createLocalNodeReplicaMeta(HybridTimestamp.MIN_VALUE, HybridTimestamp.MAX_VALUE)
        );

        when(placementDriver.awaitPrimaryReplica(any(), any(), anyLong(), any())).thenReturn(localNodeReplicaMetaFuture);

        CompletableFuture<LogicalTopologySnapshot> logicalTopologySnapshotFuture = completedFuture(
                new LogicalTopologySnapshot(1, List.of(LOGICAL_LOCAL_NODE))
        );

        when(logicalTopologyService.logicalTopologyOnLeader()).thenReturn(logicalTopologySnapshotFuture);

        task = new IndexBuildingStarterTask(
                indexDescriptor.id(),
                indexDescriptor.tableId(),
                catalogManager,
                placementDriver,
                clusterService,
                logicalTopologyService,
                clock,
                clockWaiter,
                executor,
                busyLock
        );
    }

    @AfterEach
    void tearDown() throws Exception {
        closeAll(
                catalogManager::beforeNodeStop,
                clockWaiter::beforeNodeStop,
                catalogManager::stop,
                clockWaiter::stop,
                task == null ? null : task::stop
        );

        shutdownAndAwaitTermination(executor, 1, SECONDS);
    }

    @Test
    void testSimpleTaskExecution() {
        clearInvocations(clockWaiter);

        assertThat(task.start(), willCompleteSuccessfully());
        assertEquals(BUILDING, actualIndexStatus());

        verify(executor, atLeast(3)).execute(any());
        verify(clockWaiter, atLeast(2)).waitFor(any());
        verify(placementDriver).awaitPrimaryReplica(any(), any(), anyLong(), any());
        verify(logicalTopologyService).logicalTopologyOnLeader();
        verify(logicalTopologyService).addEventListener(any());
        verify(logicalTopologyService).removeEventListener(any());
        verify(clusterService.messagingService()).invoke(any(ClusterNode.class), any(), anyLong());
    }

    @Test
    void testTimeoutAndSuccessOnAwaitPrimaryReplica() {
        CompletableFuture<ReplicaMeta> awaitPrimaryReplicaFuture0 = failedFuture(mock(PrimaryReplicaAwaitTimeoutException.class));

        CompletableFuture<ReplicaMeta> awaitPrimaryReplicaFuture1 = completedFuture(
                createLocalNodeReplicaMeta(HybridTimestamp.MIN_VALUE, HybridTimestamp.MAX_VALUE)
        );

        when(placementDriver.awaitPrimaryReplica(any(), any(), anyLong(), any())).thenReturn(
                awaitPrimaryReplicaFuture0,
                awaitPrimaryReplicaFuture1
        );

        assertThat(task.start(), willCompleteSuccessfully());
        assertEquals(BUILDING, actualIndexStatus());

        verify(placementDriver, times(2)).awaitPrimaryReplica(any(), any(), anyLong(), any());
    }

    @Test
    void testTimeoutAndExpireOnAwaitPrimaryReplica() {
        CompletableFuture<ReplicaMeta> awaitPrimaryReplicaFuture0 = failedFuture(mock(PrimaryReplicaAwaitTimeoutException.class));

        CompletableFuture<ReplicaMeta> awaitPrimaryReplicaFuture1 = completedFuture(
                createLocalNodeReplicaMeta(HybridTimestamp.MIN_VALUE, HybridTimestamp.MIN_VALUE.addPhysicalTime(1))
        );

        when(placementDriver.awaitPrimaryReplica(any(), any(), anyLong(), any())).thenReturn(
                awaitPrimaryReplicaFuture0,
                awaitPrimaryReplicaFuture1
        );

        assertThat(task.start(), willCompleteSuccessfully());
        assertEquals(REGISTERED, actualIndexStatus());

        verify(placementDriver, times(2)).awaitPrimaryReplica(any(), any(), anyLong(), any());
    }

    @Test
    void testTimeoutAndErrorOnAwaitPrimaryReplica() {
        CompletableFuture<ReplicaMeta> awaitPrimaryReplicaFuture0 = failedFuture(mock(PrimaryReplicaAwaitTimeoutException.class));

        CompletableFuture<ReplicaMeta> awaitPrimaryReplicaFuture1 = failedFuture(mock(PrimaryReplicaAwaitException.class));

        when(placementDriver.awaitPrimaryReplica(any(), any(), anyLong(), any())).thenReturn(
                awaitPrimaryReplicaFuture0,
                awaitPrimaryReplicaFuture1
        );

        assertThat(task.start(), willCompleteSuccessfully());
        assertEquals(REGISTERED, actualIndexStatus());

        verify(placementDriver, times(2)).awaitPrimaryReplica(any(), any(), anyLong(), any());
    }

    @Test
    void testNodeLeftLogicalTopology() {
        doAnswer(invocation -> {
            LogicalTopologyEventListener listener = invocation.getArgument(0);

            listener.onNodeLeft(LOGICAL_LOCAL_NODE, mock(LogicalTopologySnapshot.class));

            return null;
        }).when(logicalTopologyService).addEventListener(any());

        assertThat(task.start(), willCompleteSuccessfully());
        assertEquals(BUILDING, actualIndexStatus());
    }

    @Test
    void testTimeoutAndNodeLeftPhysicalTopologyOnSendMessage() {
        CompletableFuture<NetworkMessage> invokeFuture0 = failedFuture(new TimeoutException());
        CompletableFuture<NetworkMessage> invokeFuture1 = failedFuture(new RecipientLeftException());
        CompletableFuture<NetworkMessage> invokeFuture2 = isNodeFinishedRwTransactionsStartedBeforeResponseFuture(true);

        when(clusterService.messagingService().invoke(any(ClusterNode.class), any(), anyLong()))
                .thenReturn(invokeFuture0, invokeFuture1, invokeFuture2);

        clearInvocations(clockWaiter);

        assertThat(task.start(), willCompleteSuccessfully());
        assertEquals(BUILDING, actualIndexStatus());

        verify(clusterService.messagingService(), times(3)).invoke(any(ClusterNode.class), any(), anyLong());
        verify(clockWaiter, times(4)).waitFor(any());
    }

    @Test
    void testSecondTimesSendIsNodeFinishedRwTransactionsStartedBeforeRequest() {
        CompletableFuture<NetworkMessage> invokeFuture0 = isNodeFinishedRwTransactionsStartedBeforeResponseFuture(false);
        CompletableFuture<NetworkMessage> invokeFuture1 = isNodeFinishedRwTransactionsStartedBeforeResponseFuture(true);

        when(clusterService.messagingService().invoke(any(ClusterNode.class), any(), anyLong())).thenReturn(invokeFuture0, invokeFuture1);

        clearInvocations(clockWaiter);

        assertThat(task.start(), willCompleteSuccessfully());
        assertEquals(BUILDING, actualIndexStatus());

        verify(clusterService.messagingService(), times(2)).invoke(any(ClusterNode.class), any(), anyLong());
        verify(clockWaiter, times(3)).waitFor(any());
    }

    @Test
    void testFailedSendIsNodeFinishedRwTransactionsStartedBeforeRequest() {
        CompletableFuture<NetworkMessage> invokeFuture = failedFuture(new Exception());

        when(clusterService.messagingService().invoke(any(ClusterNode.class), any(), anyLong())).thenReturn(invokeFuture);

        clearInvocations(clockWaiter);

        assertThat(task.start(), willCompleteSuccessfully());
        assertEquals(REGISTERED, actualIndexStatus());

        verify(clusterService.messagingService(), times(1)).invoke(any(ClusterNode.class), any(), anyLong());
        verify(clockWaiter).waitFor(any());
    }

    @Test
    void testIndexAlreadyInBuildingStatus() {
        startBuildingIndex(catalogManager, indexDescriptor.id());

        assertThat(task.start(), willCompleteSuccessfully());
        assertEquals(BUILDING, actualIndexStatus());
    }

    private CatalogIndexStatus actualIndexStatus() {
        return indexDescriptor(catalogManager, INDEX_NAME, clock).status();
    }

    private ReplicaMeta createLocalNodeReplicaMeta(HybridTimestamp startTime, HybridTimestamp expirationTime) {
        return newPrimaryReplicaMeta(LOCAL_NODE, new TablePartitionId(indexDescriptor.tableId(), 0), startTime, expirationTime);
    }

    private static ClusterService createClusterService() {
        ClusterService clusterService = mock(ClusterService.class);
        TopologyService topologyService = mock(TopologyService.class);
        MessagingService messagingService = mock(MessagingService.class);

        CompletableFuture<NetworkMessage> responseFuture = completedFuture(
                FACTORY.isNodeFinishedRwTransactionsStartedBeforeResponse().finished(true).build()
        );

        when(topologyService.localMember()).thenReturn(LOCAL_NODE);
        when(messagingService.invoke(any(ClusterNode.class), any(), anyLong())).thenReturn(responseFuture);

        when(clusterService.topologyService()).thenReturn(topologyService);
        when(clusterService.messagingService()).thenReturn(messagingService);

        return clusterService;
    }

    private static CompletableFuture<NetworkMessage> isNodeFinishedRwTransactionsStartedBeforeResponseFuture(boolean finished) {
        return completedFuture(FACTORY.isNodeFinishedRwTransactionsStartedBeforeResponse().finished(finished).build());
    }
}
