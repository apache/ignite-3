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

package org.apache.ignite.internal.raft.client;

import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.NoRouteToHostException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.raft.ComponentStoppingExceptionFactory;
import org.apache.ignite.internal.raft.Marshaller;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.TestThrottlingContextHolder;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.replicator.TestReplicationGroupId;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.verification.VerificationWithTimeout;

@ExtendWith(ConfigurationExtension.class)
@ExtendWith(ExecutorServiceExtension.class)
@ExtendWith(MockitoExtension.class)
class TopologyAwareRaftGroupServiceRetriesTest extends BaseIgniteAbstractTest {
    @InjectConfiguration
    private RaftConfiguration raftConfiguration;

    @InjectExecutorService
    private ScheduledExecutorService scheduler;

    @Mock
    private LogicalTopologyService logicalTopologyService;

    private final UUID clusterId = randomUUID();

    @Mock
    private ClusterService clusterService;

    @Mock
    private MessagingService messagingService;

    @Mock
    private TopologyService topologyService;

    /**
     * The test is about a situation when some node gets added to the Logical Topology (LT) in v1, then gets removed from it and gets
     * added one more time in v2. First addition is supposed to initiate an attempt to subscribe. The invocation fails as
     * v1 points to a machine that does not accept connections, so the Raft group service under test retries invocation attempts.
     *
     * <p>Now, v1 is removed from the LT and v2 of the same node is added (with another address).
     *
     * <p>It is expected that retries to connect to the old address will stop as v1 has left the LT, and those attempts were
     * initiated exactly by the node joining the LT.
     */
    @Test
    void attemptsToSubscribeToStaleNodeAreStopped() throws Exception {
        // Given a TopologyAwareRaftGroupService that is subscribed to leader changes...
        when(clusterService.messagingService()).thenReturn(messagingService);
        when(clusterService.topologyService()).thenReturn(topologyService);

        AtomicReference<LogicalTopologyEventListener> listenerRef = new AtomicReference<>();

        doAnswer(invocation -> {
            LogicalTopologyEventListener listener = invocation.getArgument(0);
            listenerRef.set(listener);
            return null;
        }).when(logicalTopologyService).addEventListener(any());

        String thisNodeName = "node1";
        String anotherNodeName = "node2";

        TopologyAwareRaftGroupService service = TopologyAwareRaftGroupService.start(
                new TestReplicationGroupId("test"),
                clusterService,
                new RaftMessagesFactory(),
                raftConfiguration,
                PeersAndLearners.fromConsistentIds(Set.of(thisNodeName, anotherNodeName)),
                scheduler,
                logicalTopologyService,
                new RaftGroupEventsClientListener(),
                true,
                mock(Marshaller.class),
                new ComponentStoppingExceptionFactory(),
                TestThrottlingContextHolder.throttlingContextHolder()
        );

        LogicalTopologyEventListener listener = listenerRef.get();
        assertThat(listener, is(notNullValue()));

        service.subscribeLeader((leader, term) -> {});

        LogicalNode nodeV1 = new LogicalNode(randomUUID(), anotherNodeName, new NetworkAddress("old-address", 3345));
        LogicalNode anotherNodeV2 = new LogicalNode(randomUUID(), anotherNodeName, new NetworkAddress("new-address", 3345));

        // And none of the addresses accepts connections...
        when(messagingService.invoke(eq(nodeV1), any(), anyLong()))
                .thenReturn(failedFuture(new NoRouteToHostException()));
        when(messagingService.invoke(eq(anotherNodeV2), any(), anyLong()))
                .thenReturn(failedFuture(new NoRouteToHostException()));

        // When a node v1 joins...
        listener.onNodeJoined(nodeV1, new LogicalTopologySnapshot(2, Set.of(nodeV1), clusterId));

        // Then we attempt to subscribe to it.
        verify(messagingService, inReasonableTime().atLeastOnce()).invoke(eq(nodeV1), any(), anyLong());

        // When v1 is replaced with v2...
        listener.onNodeLeft(nodeV1, new LogicalTopologySnapshot(3, Set.of(), clusterId));
        listener.onNodeJoined(anotherNodeV2, new LogicalTopologySnapshot(4, Set.of(anotherNodeV2), clusterId));

        // Then we attempt to subscribe to v2...
        verify(messagingService, inReasonableTime().atLeastOnce()).invoke(eq(anotherNodeV2), any(), anyLong());

        allowRetriesToStop();

        clearInvocations(messagingService);

        allowRetriesToHappen();

        // But subscription attempts to v1 are stopped.
        verify(messagingService, never()).invoke(eq(nodeV1), any(), anyLong());
    }

    private static VerificationWithTimeout inReasonableTime() {
        return timeout(SECONDS.toMillis(10));
    }

    private static void allowRetriesToStop() throws InterruptedException {
        Thread.sleep(500);
    }

    private static void allowRetriesToHappen() throws InterruptedException {
        Thread.sleep(1000);
    }
}
