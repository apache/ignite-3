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

package org.apache.ignite.internal.disaster.system;

import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willTimeoutIn;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedIn;
import static org.apache.ignite.internal.util.CompletableFutures.emptySetCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessagesFactory;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopology;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.disaster.system.message.BecomeMetastorageLeaderMessage;
import org.apache.ignite.internal.disaster.system.message.StartMetastorageRepairRequest;
import org.apache.ignite.internal.disaster.system.message.StartMetastorageRepairResponse;
import org.apache.ignite.internal.disaster.system.message.SystemDisasterRecoveryMessagesFactory;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MetastorageRepairImplTest extends BaseIgniteAbstractTest {
    @Mock
    private MessagingService messagingService;

    @Mock
    private LogicalTopology logicalTopology;

    @Mock
    private ClusterManagementGroupManager cmgManager;

    @InjectMocks
    private MetastorageRepairImpl repair;

    @Captor
    private ArgumentCaptor<Set<String>> mgNodesCaptor;

    @Captor
    private ArgumentCaptor<Long> mgRepairingConfigIndexCaptor;

    @Captor
    private ArgumentCaptor<BecomeMetastorageLeaderMessage> becomeLeaderMessageCaptor;

    private final SystemDisasterRecoveryMessagesFactory messagesFactory = new SystemDisasterRecoveryMessagesFactory();
    private final CmgMessagesFactory cmgMessagesFactory = new CmgMessagesFactory();

    private final InternalClusterNode node1 = new ClusterNodeImpl(randomUUID(), "node1", new NetworkAddress("host", 1001));
    private final InternalClusterNode node2 = new ClusterNodeImpl(randomUUID(), "node2", new NetworkAddress("host", 1002));
    private final InternalClusterNode node3 = new ClusterNodeImpl(randomUUID(), "node3", new NetworkAddress("host", 1003));
    private final InternalClusterNode node4 = new ClusterNodeImpl(randomUUID(), "node4", new NetworkAddress("host", 1004));

    @BeforeEach
    void configureMocks() {
        lenient().when(messagingService.invoke(any(String.class), any(BecomeMetastorageLeaderMessage.class), anyLong()))
                .thenReturn(completedFuture(cmgMessagesFactory.successResponseMessage().build()));
        lenient().when(cmgManager.changeMetastorageNodes(any(), anyLong()))
                .thenReturn(nullCompletedFuture());
    }

    @Test
    void hangsIfParticipatingNodesNeverAppear() {
        when(cmgManager.validatedNodes()).thenReturn(emptySetCompletedFuture());

        assertThat(repair.repair(Set.of("node1", "node2"), 1), willTimeoutIn(100, MILLISECONDS));
    }

    @Test
    void repairsWithMgFactor1() {
        when(cmgManager.validatedNodes()).thenReturn(completedFuture(Set.of(node1)));

        willRespondWithIndexAndTerm(node1, 10, 1);

        assertThat(repair.repair(Set.of(node1.name()), 1), willSucceedIn(3, SECONDS));

        verify(cmgManager).changeMetastorageNodes(mgNodesCaptor.capture(), mgRepairingConfigIndexCaptor.capture());
        assertThat(mgNodesCaptor.getValue(), contains(node1.name()));
        assertThat(mgRepairingConfigIndexCaptor.getValue(), is(11L));

        verify(messagingService).invoke(eq(node1.name()), becomeLeaderMessageCaptor.capture(), anyLong());
        assertThat(becomeLeaderMessageCaptor.getValue().termBeforeChange(), is(1L));
        assertThat(becomeLeaderMessageCaptor.getValue().targetVotingSet(), is(Set.of(node1.name())));
    }

    private void willRespondWithIndexAndTerm(InternalClusterNode node, int raftIndex, int raftTerm) {
        when(messagingService.invoke(eq(node.name()), any(StartMetastorageRepairRequest.class), anyLong()))
                .thenReturn(completedFuture(indexTermResponse(raftIndex, raftTerm)));
    }

    private StartMetastorageRepairResponse indexTermResponse(int raftIndex, int raftTerm) {
        return messagesFactory.startMetastorageRepairResponse()
                .raftIndex(raftIndex)
                .raftTerm(raftTerm)
                .build();
    }

    @Test
    void repairsWithMgFactor3() {
        when(cmgManager.validatedNodes()).thenReturn(completedFuture(Set.of(node1, node2, node3, node4)));

        willRespondWithIndexAndTerm(node1, 12, 1);
        willRespondWithIndexAndTerm(node2, 11, 1);
        willRespondWithIndexAndTerm(node3, 10, 1);

        Set<String> threeNodes = Set.of(node1.name(), node2.name(), node3.name());
        assertThat(repair.repair(threeNodes, 3), willSucceedIn(3, SECONDS));

        verify(cmgManager).changeMetastorageNodes(mgNodesCaptor.capture(), mgRepairingConfigIndexCaptor.capture());
        assertThat(mgNodesCaptor.getValue(), containsInAnyOrder(node1.name(), node2.name(), node3.name()));
        assertThat(mgRepairingConfigIndexCaptor.getValue(), is(13L));

        verify(messagingService).invoke(eq(node1.name()), becomeLeaderMessageCaptor.capture(), anyLong());
        assertThat(becomeLeaderMessageCaptor.getValue().termBeforeChange(), is(1L));
        assertThat(becomeLeaderMessageCaptor.getValue().targetVotingSet(), is(threeNodes));
    }

    @Test
    void proceedsIfParticipatingNodesAppearAsValidatedLaterThanRepairStarts() {
        when(cmgManager.validatedNodes()).thenReturn(emptySetCompletedFuture());
        doAnswer(invocation -> {
            LogicalTopologyEventListener listener = invocation.getArgument(0);

            listener.onNodeValidated(new LogicalNode(node1));
            listener.onNodeValidated(new LogicalNode(node2));

            return null;
        }).when(logicalTopology).addEventListener(any());

        willRespondWithIndexAndTerm(node1, 10, 1);

        assertThat(repair.repair(Set.of(node1.name()), 1), willSucceedIn(3, SECONDS));
    }

    @Test
    void proceedsIfParticipatingNodesAppearAsJoinedLaterThanRepairStarts() {
        when(cmgManager.validatedNodes()).thenReturn(emptySetCompletedFuture());
        doAnswer(invocation -> {
            LogicalTopologyEventListener listener = invocation.getArgument(0);

            LogicalNode joinedNode1 = new LogicalNode(node1);
            LogicalNode joinedNode2 = new LogicalNode(node2);

            listener.onNodeJoined(joinedNode1, new LogicalTopologySnapshot(1, Set.of(joinedNode1)));
            listener.onNodeJoined(joinedNode2, new LogicalTopologySnapshot(2, Set.of(joinedNode1, joinedNode2)));

            return null;
        }).when(logicalTopology).addEventListener(any());

        willRespondWithIndexAndTerm(node1, 10, 1);

        assertThat(repair.repair(Set.of(node1.name()), 1), willSucceedIn(3, SECONDS));
    }

    @Test
    void waitsTillEveryNodeResponds() {
        when(cmgManager.validatedNodes()).thenReturn(completedFuture(Set.of(node1, node2)));

        willRespondWithIndexAndTerm(node1, 10, 1);
        when(messagingService.invoke(eq(node2.name()), any(), anyLong())).thenReturn(new CompletableFuture<>());

        assertThat(repair.repair(Set.of(node1.name(), node2.name()), 1), willTimeoutIn(100, MILLISECONDS));
    }

    @Test
    void choosesBestMgNodesAndLeader() {
        when(cmgManager.validatedNodes()).thenReturn(completedFuture(Set.of(node1, node2, node3)));

        willRespondWithIndexAndTerm(node1, 10, 1);
        willRespondWithIndexAndTerm(node2, 12, 1);
        willRespondWithIndexAndTerm(node3, 11, 1);

        assertThat(repair.repair(Set.of(node1.name(), node2.name(), node3.name()), 2), willSucceedIn(3, SECONDS));

        verify(cmgManager).changeMetastorageNodes(mgNodesCaptor.capture(), mgRepairingConfigIndexCaptor.capture());
        assertThat(mgNodesCaptor.getValue(), containsInAnyOrder(node2.name(), node3.name()));
        assertThat(mgRepairingConfigIndexCaptor.getValue(), is(13L));

        verify(messagingService).invoke(eq(node2.name()), becomeLeaderMessageCaptor.capture(), anyLong());
    }

    @Test
    void termIsStrongerThanIndexWhenComparing() {
        when(cmgManager.validatedNodes()).thenReturn(completedFuture(Set.of(node1, node2, node3)));

        willRespondWithIndexAndTerm(node1, 10, 3);
        willRespondWithIndexAndTerm(node2, 12, 1);
        willRespondWithIndexAndTerm(node3, 11, 2);

        assertThat(repair.repair(Set.of(node1.name(), node2.name(), node3.name()), 2), willSucceedIn(3, SECONDS));

        verify(cmgManager).changeMetastorageNodes(mgNodesCaptor.capture(), mgRepairingConfigIndexCaptor.capture());
        assertThat(mgNodesCaptor.getValue(), containsInAnyOrder(node1.name(), node3.name()));
        assertThat(mgRepairingConfigIndexCaptor.getValue(), is(11L));

        verify(messagingService).invoke(eq(node1.name()), becomeLeaderMessageCaptor.capture(), anyLong());
    }
}
