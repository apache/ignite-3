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

package org.apache.ignite.internal.placementdriver;

import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.affinity.Assignment.forPeer;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.stablePartAssignmentsKey;
import static org.apache.ignite.internal.placementdriver.PlacementDriverManager.PLACEMENTDRIVER_LEASES_KEY;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import org.apache.ignite.internal.affinity.Assignments;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.placementdriver.leases.Lease;
import org.apache.ignite.internal.placementdriver.leases.LeaseBatch;
import org.apache.ignite.internal.placementdriver.leases.LeaseTracker;
import org.apache.ignite.internal.placementdriver.message.LeaseGrantedMessage;
import org.apache.ignite.internal.placementdriver.message.LeaseGrantedMessageResponse;
import org.apache.ignite.internal.placementdriver.message.PlacementDriverMessagesFactory;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test checking exceptional situations on lease negotiation.
 */
public class LeaseNegotiationTest extends BaseIgniteAbstractTest {
    private static final PlacementDriverMessagesFactory MSG_FACTORY = new PlacementDriverMessagesFactory();

    private static final TablePartitionId GROUP_ID = new TablePartitionId(0, 0);

    private static final String NODE_0_NAME = "node0";
    private static final LogicalNode CLUSTER_NODE_0 = new LogicalNode(randomUUID().toString(), NODE_0_NAME, mock(NetworkAddress.class));

    private static final String NODE_1_NAME = "node1";

    private static final LogicalNode CLUSTER_NODE_1 = new LogicalNode(randomUUID().toString(), NODE_1_NAME, mock(NetworkAddress.class));

    private LeaseUpdater leaseUpdater;

    private MetaStorageManager metaStorageManager;

    private ClusterService pdClusterService;

    private MessagingService pdMessagingService;

    private LogicalTopologyService pdLogicalTopologyService;

    private LogicalTopologyEventListener pdLogicalTopologyEventListener;

    private BiFunction<String, LeaseGrantedMessage, LeaseGrantedMessageResponse> leaseGrantedMessageHandler;

    @BeforeEach
    public void setUp() {
        metaStorageManager = StandaloneMetaStorageManager.create();
        assertThat(metaStorageManager.startAsync(new ComponentContext()), willCompleteSuccessfully());
        metaStorageManager.deployWatches();

        pdLogicalTopologyService = mock(LogicalTopologyService.class);
        when(pdLogicalTopologyService.logicalTopologyOnLeader())
                .thenAnswer(inv -> completedFuture(new LogicalTopologySnapshot(0, Set.of(CLUSTER_NODE_0, CLUSTER_NODE_1))));
        doAnswer(inv -> {
            pdLogicalTopologyEventListener = inv.getArgument(0);

            return null;
        }).when(pdLogicalTopologyService).addEventListener(any());

        leaseUpdater = createLeaseUpdater();

        leaseUpdater.init();

        leaseUpdater.activate();
    }

    @AfterEach
    public void tearDown() {
        leaseUpdater.deactivate();
    }

    private LeaseUpdater createLeaseUpdater() {
        TopologyService pdTopologyService = mock(TopologyService.class);
        when(pdTopologyService.getById(anyString())).thenAnswer(inv -> CLUSTER_NODE_0);

        pdMessagingService = mock(MessagingService.class);
        when(pdMessagingService.invoke(anyString(), any(), anyLong())).thenAnswer(inv -> {
            String nodeId = inv.getArgument(0);

            LeaseGrantedMessage leaseGrantedMessage = inv.getArgument(1);

            if (leaseGrantedMessageHandler != null) {
                return CompletableFuture.supplyAsync(() -> leaseGrantedMessageHandler.apply(nodeId, leaseGrantedMessage));
            } else {
                return completedFuture(createLeaseGrantedMessageResponse(true));
            }
        });

        pdClusterService = mock(ClusterService.class);
        when(pdClusterService.messagingService()).thenAnswer(inv -> pdMessagingService);
        when(pdClusterService.topologyService()).thenAnswer(inv -> pdTopologyService);

        LeaseTracker leaseTracker = new LeaseTracker(
                metaStorageManager,
                pdClusterService.topologyService(),
                new TestClockService(new HybridClockImpl())
        );

        leaseTracker.startTrack(0L);

        return new LeaseUpdater(
                NODE_0_NAME,
                pdClusterService,
                metaStorageManager,
                pdLogicalTopologyService,
                leaseTracker,
                new TestClockService(new HybridClockImpl())
        );
    }

    private static LeaseGrantedMessageResponse createLeaseGrantedMessageResponse(boolean accept) {
        return MSG_FACTORY.leaseGrantedMessageResponse().accepted(accept).build();
    }

    @Test
    public void testAssignmentChangeOnNegotiation() throws InterruptedException {
        var lgmReceived = new CompletableFuture<>();
        var lgmProcessed = new CompletableFuture<>();

        leaseGrantedMessageHandler = (n, lgm) -> {
            if (n.equals(NODE_0_NAME)) {
                lgmReceived.complete(null);

                lgmProcessed.join();
            }

            return createLeaseGrantedMessageResponse(true);
        };

        metaStorageManager.put(stablePartAssignmentsKey(GROUP_ID), Assignments.toBytes(Set.of(forPeer(NODE_0_NAME))));

        assertThat(lgmReceived, willCompleteSuccessfully());

        metaStorageManager.put(stablePartAssignmentsKey(GROUP_ID), Assignments.toBytes(Set.of(forPeer(NODE_1_NAME))));

        waitForAcceptedLease();

        assertLeaseCorrect(CLUSTER_NODE_1.id());

        lgmProcessed.complete(null);
    }

    @Test
    public void testAssignmentChangeOnNegotiationAndReplicaRejectsLease() throws InterruptedException {
        var lgmReceived = new CompletableFuture<>();

        leaseGrantedMessageHandler = (n, lgm) -> {
            if (n.equals(NODE_0_NAME) && !lgmReceived.isDone()) {
                lgmReceived.complete(null);

                return createLeaseGrantedMessageResponse(false);
            }

            return createLeaseGrantedMessageResponse(true);
        };

        metaStorageManager.put(stablePartAssignmentsKey(GROUP_ID), Assignments.toBytes(Set.of(forPeer(NODE_0_NAME))));

        assertThat(lgmReceived, willCompleteSuccessfully());

        waitForAcceptedLease();

        assertLeaseCorrect(CLUSTER_NODE_0.id());
    }

    @Test
    public void testAssignmentChangeOnNegotiationNodeLeftTopology() throws InterruptedException {
        var lgmReceived = new CompletableFuture<>();
        var lgmProcessed = new CompletableFuture<>();

        leaseGrantedMessageHandler = (n, lgm) -> {
            if (n.equals(NODE_0_NAME)) {
                lgmReceived.complete(null);

                lgmProcessed.join();
            }

            return createLeaseGrantedMessageResponse(true);
        };

        metaStorageManager.put(stablePartAssignmentsKey(GROUP_ID), Assignments.toBytes(Set.of(forPeer(NODE_0_NAME), forPeer(NODE_1_NAME))));

        assertThat(lgmReceived, willCompleteSuccessfully());

        pdLogicalTopologyEventListener.onNodeLeft(CLUSTER_NODE_0, new LogicalTopologySnapshot(1L, Set.of(CLUSTER_NODE_1)));

        waitForAcceptedLease();

        assertLeaseCorrect(CLUSTER_NODE_1.id());

        lgmProcessed.complete(null);
    }

    @Test
    public void testNetworkExceptionOnNegotiation() throws InterruptedException {
        var lgmReceived = new CompletableFuture<>();

        leaseGrantedMessageHandler = (n, lgm) -> {
            if (!lgmReceived.isDone()) {
                lgmReceived.complete(null);

                throw new RuntimeException("test");
            }

            return createLeaseGrantedMessageResponse(true);
        };

        metaStorageManager.put(stablePartAssignmentsKey(GROUP_ID), Assignments.toBytes(Set.of(forPeer(NODE_0_NAME))));

        assertThat(lgmReceived, willCompleteSuccessfully());

        waitForAcceptedLease();

        assertLeaseCorrect(CLUSTER_NODE_0.id());
    }

    private Lease getLeaseFromMs() {
        CompletableFuture<Entry> f = metaStorageManager.get(PLACEMENTDRIVER_LEASES_KEY);

        assertThat(f, willSucceedFast());

        Entry e = f.join();

        LeaseBatch leases = LeaseBatch.fromBytes(ByteBuffer.wrap(e.value()).order(ByteOrder.LITTLE_ENDIAN));

        return leases.leases().stream().findFirst().orElseThrow();
    }

    private void waitForAcceptedLease() throws InterruptedException {
        assertTrue(waitForCondition(() -> {
            Lease lease = getLeaseFromMs();

            return lease.isAccepted();
        }, 10_000));
    }

    private void assertLeaseCorrect(String leaseholderId) {
        Lease lease = getLeaseFromMs();

        assertTrue(lease.isAccepted());
        assertEquals(leaseholderId, lease.getLeaseholderId());
    }
}
