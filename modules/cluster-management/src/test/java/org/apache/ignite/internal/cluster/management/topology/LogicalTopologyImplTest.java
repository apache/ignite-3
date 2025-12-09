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

package org.apache.ignite.internal.cluster.management.topology;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.cluster.management.raft.ClusterStateStorage;
import org.apache.ignite.internal.cluster.management.raft.TestClusterStateStorage;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.failure.NoOpFailureManager;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.testframework.failure.FailureManagerExtension;
import org.apache.ignite.internal.testframework.failure.MuteFailureManagerLogging;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(WorkDirectoryExtension.class)
@ExtendWith(MockitoExtension.class)
@ExtendWith(FailureManagerExtension.class)
class LogicalTopologyImplTest extends BaseIgniteAbstractTest {
    private final ClusterStateStorage storage = spy(TestClusterStateStorage.initializedClusterStateStorage());

    private LogicalTopology topology;

    @WorkDirectory
    protected Path workDir;

    @Mock
    private LogicalTopologyEventListener listener;

    @Captor
    private ArgumentCaptor<LogicalNode> nodeCaptor;

    @Captor
    private ArgumentCaptor<LogicalNode> nodeCaptor2;

    @Captor
    private ArgumentCaptor<LogicalTopologySnapshot> topologyCaptor;

    @Captor
    private ArgumentCaptor<LogicalTopologySnapshot> topologyCaptor2;

    @BeforeEach
    void setUp() {
        assertThat(storage.startAsync(new ComponentContext()), willCompleteSuccessfully());

        topology = new LogicalTopologyImpl(storage, new NoOpFailureManager());

        topology.addEventListener(listener);
    }

    @AfterEach
    void tearDown() {
        assertThat(storage.stopAsync(new ComponentContext()), willCompleteSuccessfully());
    }

    /**
     * Tests methods for working with the logical topology.
     */
    @Test
    void testLogicalTopology() {
        assertThat(topology.getLogicalTopology().nodes(), is(empty()));

        var node1 = new LogicalNode(randomUUID(), "bar", new NetworkAddress("localhost", 123));

        topology.putNode(node1);

        assertThat(topology.getLogicalTopology().nodes(), contains(node1));

        var node2 = new LogicalNode(randomUUID(), "quux", new NetworkAddress("localhost", 123));

        topology.putNode(node2);

        assertThat(topology.getLogicalTopology().nodes(), containsInAnyOrder(node1, node2));

        var node3 = new LogicalNode(randomUUID(), "boop", new NetworkAddress("localhost", 123));

        topology.putNode(node3);

        assertThat(topology.getLogicalTopology().nodes(), containsInAnyOrder(node1, node2, node3));

        topology.removeNodes(Set.of(node1, node2));

        assertThat(topology.getLogicalTopology().nodes(), contains(node3));

        topology.removeNodes(Set.of(node3));

        assertThat(topology.getLogicalTopology().nodes(), is(empty()));
    }

    /**
     * Tests that all methods for working with the logical topology are idempotent.
     */
    @Test
    void testLogicalTopologyIdempotence() {
        var node = new LogicalNode(randomUUID(), "bar", new NetworkAddress("localhost", 123));

        topology.putNode(node);
        topology.putNode(node);

        assertThat(topology.getLogicalTopology().nodes(), contains(node));

        topology.removeNodes(Set.of(node));
        topology.removeNodes(Set.of(node));

        assertThat(topology.getLogicalTopology().nodes(), is(empty()));
    }

    @Test
    void additionUsesNameAsNodeKey() {
        topology.putNode(new LogicalNode(nodeId(1), "node", new NetworkAddress("host", 1000)));

        topology.putNode(new LogicalNode(nodeId(2), "node", new NetworkAddress("host", 1000)));

        Collection<LogicalNode> topology = this.topology.getLogicalTopology().nodes();

        assertThat(topology, hasSize(1));

        assertThat(topology.iterator().next().id(), is(nodeId(2)));
    }

    private static UUID nodeId(int id) {
        return new UUID(0, id);
    }

    @Test
    void removalUsesIdAsNodeKey() {
        topology.putNode(new LogicalNode(nodeId(1), "node", new NetworkAddress("host", 1000)));

        topology.removeNodes(Set.of(new LogicalNode(nodeId(2), "node", new NetworkAddress("host", 1000))));

        assertThat(topology.getLogicalTopology().nodes(), hasSize(1));
        assertThat((topology.getLogicalTopology().nodes()).iterator().next().id(), is(nodeId(1)));

        topology.removeNodes(Set.of(new LogicalNode(nodeId(1), "another-name", new NetworkAddress("host", 1000))));

        assertThat(topology.getLogicalTopology().nodes(), is(empty()));
    }

    @Test
    void inLogicalTopologyTestUsesIdAsNodeKey() {
        topology.putNode(new LogicalNode(nodeId(1), "node", new NetworkAddress("host", 1000)));

        assertTrue(topology.isNodeInLogicalTopology(new LogicalNode(nodeId(1), "node", new NetworkAddress("host", 1000))));
        assertFalse(topology.isNodeInLogicalTopology(new LogicalNode(nodeId(777), "node", new NetworkAddress("host", 1000))));
    }

    @Test
    void logicalTopologyIsRestoredCorrectlyWithSnapshot() throws Exception {
        Path snapshotDir = workDir.resolve("snapshot");
        Files.createDirectory(snapshotDir);

        topology.putNode(new LogicalNode(randomUUID(), "node", new NetworkAddress("host", 1000)));

        storage.snapshot(snapshotDir).get(10, TimeUnit.SECONDS);

        topology.putNode(new LogicalNode(randomUUID(), "another-node", new NetworkAddress("host", 1001)));

        storage.restoreSnapshot(snapshotDir);

        List<String> namesInTopology = topology.getLogicalTopology().nodes().stream()
                .map(InternalClusterNode::name)
                .collect(toList());
        assertThat(namesInTopology, contains("node"));
    }

    @Test
    void addingNewNodeProducesAppearedEvent() {
        topology.putNode(new LogicalNode(nodeId(1), "node", new NetworkAddress("host", 1000)));

        verify(listener).onNodeJoined(nodeCaptor.capture(), topologyCaptor.capture());

        assertThat(topologyCaptor.getValue().version(), is(1L));

        InternalClusterNode appearedNode = nodeCaptor.getValue();

        assertThat(appearedNode.id(), is(nodeId(1)));
        assertThat(appearedNode.name(), is("node"));
    }

    @Test
    void addingSameExistingNodeProducesNoEvents() {
        topology.putNode(new LogicalNode(nodeId(1), "node", new NetworkAddress("host", 1000)));

        topology.putNode(new LogicalNode(nodeId(1), "node", new NetworkAddress("host", 1000)));

        verify(listener, times(1)).onNodeJoined(any(), any());
    }

    @Test
    void updatingExistingNodeProducesDisappearedAndAppearedEvents() {
        topology.putNode(new LogicalNode(nodeId(1), "node", new NetworkAddress("host", 1000)));

        topology.putNode(new LogicalNode(nodeId(2), "node", new NetworkAddress("host1", 1001)));

        InOrder inOrder = inOrder(listener);

        inOrder.verify(listener).onNodeLeft(nodeCaptor.capture(), topologyCaptor.capture());
        inOrder.verify(listener).onNodeJoined(nodeCaptor2.capture(), topologyCaptor2.capture());

        assertThat(topologyCaptor.getValue().version(), is(2L));
        assertThat(topologyCaptor2.getValue().version(), is(3L));

        InternalClusterNode disappearedNode = nodeCaptor.getValue();

        assertThat(disappearedNode.id(), is(nodeId(1)));
        assertThat(disappearedNode.name(), is("node"));
        assertThat(disappearedNode.address(), is(new NetworkAddress("host", 1000)));

        InternalClusterNode appearedNode = nodeCaptor2.getValue();

        assertThat(appearedNode.id(), is(nodeId(2)));
        assertThat(appearedNode.name(), is("node"));
        assertThat(appearedNode.address(), is(new NetworkAddress("host1", 1001)));
    }

    @Test
    void updatingExistingNodeProducesExactlyOneWriteToDb() {
        topology.putNode(new LogicalNode(randomUUID(), "node", new NetworkAddress("host", 1000)));

        topology.putNode(new LogicalNode(randomUUID(), "node", new NetworkAddress("host1", 1001)));

        // Expecting 2 writes because there are two puts. The test verifies that second put also produces just 1 write.
        verify(storage, times(2)).put(eq("logical".getBytes(UTF_8)), any());
    }

    @Test
    void removingExistingNodeProducesDisappearedEvent() {
        LogicalNode node = new LogicalNode(nodeId(1), "node", new NetworkAddress("host", 1000));
        topology.putNode(node);

        topology.removeNodes(Set.of(node));

        verify(listener).onNodeLeft(nodeCaptor.capture(), topologyCaptor.capture());

        assertThat(topologyCaptor.getValue().version(), is(2L));

        InternalClusterNode disappearedNode = nodeCaptor.getValue();

        assertThat(disappearedNode.id(), is(nodeId(1)));
        assertThat(disappearedNode.name(), is("node"));
    }

    @Test
    void removingNonExistingNodeProducesNoEvents() {
        LogicalNode node = new LogicalNode(randomUUID(), "node", new NetworkAddress("host", 1000));

        topology.removeNodes(Set.of(node));

        verify(listener, never()).onNodeLeft(any(), any());
    }

    @Test
    void multiRemovalProducesDisappearedEventsInOrderOfNodeIds() {
        LogicalNode node1 = new LogicalNode(nodeId(1), "node", new NetworkAddress("host", 1000));
        LogicalNode node2 = new LogicalNode(nodeId(2), "node2", new NetworkAddress("host2", 1000));

        topology.putNode(node1);
        topology.putNode(node2);

        topology.removeNodes(new LinkedHashSet<>(List.of(node2, node1)));

        verify(listener, times(2)).onNodeLeft(nodeCaptor.capture(), topologyCaptor.capture());

        List<LogicalTopologySnapshot> capturedSnapshots = topologyCaptor.getAllValues();

        assertThat(capturedSnapshots.get(0).version(), is(3L));
        assertThat(capturedSnapshots.get(1).version(), is(4L));

        InternalClusterNode disappearedNode1 = nodeCaptor.getAllValues().get(0);
        InternalClusterNode disappearedNode2 = nodeCaptor.getAllValues().get(1);

        assertAll(
                () -> assertThat(disappearedNode1.id(), is(nodeId(1))),
                () -> assertThat(disappearedNode2.id(), is(nodeId(2)))
        );
    }

    @Test
    void multiRemovalProducesExactlyOneWriteToDb() {
        LogicalNode node1 = new LogicalNode(randomUUID(), "node", new NetworkAddress("host", 1000));
        LogicalNode node2 = new LogicalNode(randomUUID(), "node2", new NetworkAddress("host2", 1000));

        topology.putNode(node1);
        topology.putNode(node2);

        topology.removeNodes(Set.of(node1, node2));

        // Expecting 3 writes because there are two puts and one removal. The test verifies that the removal produces just 1 write.
        verify(storage, times(3)).put(eq("logical".getBytes(UTF_8)), any());
    }

    @Test
    void onTopologyLeapIsTriggeredOnSnapshotRestore() {
        topology.putNode(new LogicalNode(nodeId(1), "node", new NetworkAddress("host", 1000)));

        topology.fireTopologyLeap();

        verify(listener).onTopologyLeap(topologyCaptor.capture());

        LogicalTopologySnapshot capturedSnapshot = topologyCaptor.getValue();
        assertThat(capturedSnapshot.version(), is(1L));
        assertThat(capturedSnapshot.nodes(), hasSize(1));
        assertThat(capturedSnapshot.nodes().iterator().next().id(), is(nodeId(1)));
    }

    @Test
    @MuteFailureManagerLogging // Failure is expected.
    void onAppearedListenersExceptionsDoNotBreakNotification() {
        LogicalTopologyEventListener secondListener = mock(LogicalTopologyEventListener.class);

        doThrow(new RuntimeException("Oops")).when(listener).onNodeJoined(any(), any());

        topology.addEventListener(secondListener);

        topology.putNode(new LogicalNode(randomUUID(), "node", new NetworkAddress("host", 1000)));

        verify(listener).onNodeJoined(any(), any());
        verify(secondListener).onNodeJoined(any(), any());
    }

    @Test
    @MuteFailureManagerLogging // Failure is expected.
    void onAppearedListenerErrorIsRethrown() {
        doThrow(new TestError()).when(listener).onNodeJoined(any(), any());

        assertThrows(
                TestError.class,
                () -> topology.putNode(new LogicalNode(randomUUID(), "node", new NetworkAddress("host", 1000)))
        );
    }

    @Test
    @MuteFailureManagerLogging // Failure is expected.
    void onDisappearedListenersExceptionsDoNotBreakNotification() {
        LogicalTopologyEventListener secondListener = mock(LogicalTopologyEventListener.class);

        doThrow(new RuntimeException("Oops")).when(listener).onNodeLeft(any(), any());

        topology.addEventListener(secondListener);

        LogicalNode node = new LogicalNode(randomUUID(), "node", new NetworkAddress("host", 1000));

        topology.putNode(node);
        topology.removeNodes(Set.of(node));

        verify(listener).onNodeLeft(any(), any());
        verify(secondListener).onNodeLeft(any(), any());
    }

    @Test
    @MuteFailureManagerLogging // Failure is expected.
    void onDisappearedListenerErrorIsRethrown() {
        doThrow(new TestError()).when(listener).onNodeLeft(any(), any());

        LogicalNode node = new LogicalNode(randomUUID(), "node", new NetworkAddress("host", 1024));

        topology.putNode(node);

        assertThrows(TestError.class, () -> topology.removeNodes(Set.of(node)));
    }

    @Test
    @MuteFailureManagerLogging // Failure is expected.
    void onTopologyLeapListenersExceptionsDoNotBreakNotification() {
        LogicalTopologyEventListener secondListener = mock(LogicalTopologyEventListener.class);

        doThrow(new RuntimeException("Oops")).when(listener).onTopologyLeap(any());

        topology.addEventListener(secondListener);

        topology.fireTopologyLeap();

        verify(listener).onTopologyLeap(any());
        verify(secondListener).onTopologyLeap(any());
    }

    @Test
    @MuteFailureManagerLogging // Failure is expected.
    void onTopologyLeapListenerErrorIsRethrown() {
        doThrow(new TestError()).when(listener).onTopologyLeap(any());

        assertThrows(TestError.class, () -> topology.fireTopologyLeap());
    }

    @Test
    void eventListenerStopsGettingEventsAfterListenerRemoval() {
        topology.removeEventListener(listener);

        topology.putNode(new LogicalNode(randomUUID(), "node", new NetworkAddress("host", 1000)));

        verify(listener, never()).onNodeJoined(any(), any());
    }

    @SuppressWarnings("serial")
    private static class TestError extends Error {
    }
}
