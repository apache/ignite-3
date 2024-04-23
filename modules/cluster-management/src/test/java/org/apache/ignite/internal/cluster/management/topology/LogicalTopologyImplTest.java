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
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.cluster.management.raft.ClusterStateStorage;
import org.apache.ignite.internal.cluster.management.raft.TestClusterStateStorage;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.network.ClusterNode;
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
class LogicalTopologyImplTest extends BaseIgniteAbstractTest {
    private final ClusterStateStorage storage = spy(new TestClusterStateStorage());

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
        assertThat(storage.startAsync(), willCompleteSuccessfully());

        topology = new LogicalTopologyImpl(storage);

        topology.addEventListener(listener);
    }

    @AfterEach
    void tearDown() {
        assertThat(storage.stopAsync(), willCompleteSuccessfully());
    }

    /**
     * Tests methods for working with the logical topology.
     */
    @Test
    void testLogicalTopology() {
        assertThat(topology.getLogicalTopology().nodes(), is(empty()));

        var node1 = new LogicalNode("foo", "bar", new NetworkAddress("localhost", 123));

        topology.putNode(node1);

        assertThat(topology.getLogicalTopology().nodes(), contains(node1));

        var node2 = new LogicalNode("baz", "quux", new NetworkAddress("localhost", 123));

        topology.putNode(node2);

        assertThat(topology.getLogicalTopology().nodes(), containsInAnyOrder(node1, node2));

        var node3 = new LogicalNode("lol", "boop", new NetworkAddress("localhost", 123));

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
        var node = new LogicalNode("foo", "bar", new NetworkAddress("localhost", 123));

        topology.putNode(node);
        topology.putNode(node);

        assertThat(topology.getLogicalTopology().nodes(), contains(node));

        topology.removeNodes(Set.of(node));
        topology.removeNodes(Set.of(node));

        assertThat(topology.getLogicalTopology().nodes(), is(empty()));
    }

    @Test
    void additionUsesNameAsNodeKey() {
        topology.putNode(new LogicalNode("id1", "node", new NetworkAddress("host", 1000)));

        topology.putNode(new LogicalNode("id2", "node", new NetworkAddress("host", 1000)));

        Collection<LogicalNode> topology = this.topology.getLogicalTopology().nodes();

        assertThat(topology, hasSize(1));

        assertThat(topology.iterator().next().id(), is("id2"));
    }

    @Test
    void removalUsesIdAsNodeKey() {
        topology.putNode(new LogicalNode("id1", "node", new NetworkAddress("host", 1000)));

        topology.removeNodes(Set.of(new LogicalNode("id2", "node", new NetworkAddress("host", 1000))));

        assertThat(topology.getLogicalTopology().nodes(), hasSize(1));
        assertThat((topology.getLogicalTopology().nodes()).iterator().next().id(), is("id1"));

        topology.removeNodes(Set.of(new LogicalNode("id1", "another-name", new NetworkAddress("host", 1000))));

        assertThat(topology.getLogicalTopology().nodes(), is(empty()));
    }

    @Test
    void inLogicalTopologyTestUsesIdAsNodeKey() {
        topology.putNode(new LogicalNode("id1", "node", new NetworkAddress("host", 1000)));

        assertTrue(topology.isNodeInLogicalTopology(new LogicalNode("id1", "node", new NetworkAddress("host", 1000))));
        assertFalse(topology.isNodeInLogicalTopology(new LogicalNode("another-id", "node", new NetworkAddress("host", 1000))));
    }

    @Test
    void logicalTopologyIsRestoredCorrectlyWithSnapshot() throws Exception {
        Path snapshotDir = workDir.resolve("snapshot");
        Files.createDirectory(snapshotDir);

        topology.putNode(new LogicalNode("id1", "node", new NetworkAddress("host", 1000)));

        storage.snapshot(snapshotDir).get(10, TimeUnit.SECONDS);

        topology.putNode(new LogicalNode("id2", "another-node", new NetworkAddress("host", 1001)));

        storage.restoreSnapshot(snapshotDir);

        List<String> namesInTopology = topology.getLogicalTopology().nodes().stream()
                .map(ClusterNode::name)
                .collect(toList());
        assertThat(namesInTopology, contains("node"));
    }

    @Test
    void addingNewNodeProducesAppearedEvent() {
        topology.putNode(new LogicalNode("id1", "node", new NetworkAddress("host", 1000)));

        verify(listener).onNodeJoined(nodeCaptor.capture(), topologyCaptor.capture());

        assertThat(topologyCaptor.getValue().version(), is(1L));

        ClusterNode appearedNode = nodeCaptor.getValue();

        assertThat(appearedNode.id(), is("id1"));
        assertThat(appearedNode.name(), is("node"));
    }

    @Test
    void addingSameExistingNodeProducesNoEvents() {
        topology.putNode(new LogicalNode("id1", "node", new NetworkAddress("host", 1000)));

        topology.putNode(new LogicalNode("id1", "node", new NetworkAddress("host", 1000)));

        verify(listener, times(1)).onNodeJoined(any(), any());
    }

    @Test
    void updatingExistingNodeProducesDisappearedAndAppearedEvents() {
        topology.putNode(new LogicalNode("id1", "node", new NetworkAddress("host", 1000)));

        topology.putNode(new LogicalNode("id2", "node", new NetworkAddress("host1", 1001)));

        InOrder inOrder = inOrder(listener);

        inOrder.verify(listener).onNodeLeft(nodeCaptor.capture(), topologyCaptor.capture());
        inOrder.verify(listener).onNodeJoined(nodeCaptor2.capture(), topologyCaptor2.capture());

        assertThat(topologyCaptor.getValue().version(), is(2L));
        assertThat(topologyCaptor2.getValue().version(), is(3L));

        ClusterNode disappearedNode = nodeCaptor.getValue();

        assertThat(disappearedNode.id(), is("id1"));
        assertThat(disappearedNode.name(), is("node"));
        assertThat(disappearedNode.address(), is(new NetworkAddress("host", 1000)));

        ClusterNode appearedNode = nodeCaptor2.getValue();

        assertThat(appearedNode.id(), is("id2"));
        assertThat(appearedNode.name(), is("node"));
        assertThat(appearedNode.address(), is(new NetworkAddress("host1", 1001)));
    }

    @Test
    void updatingExistingNodeProducesExactlyOneWriteToDb() {
        topology.putNode(new LogicalNode("id1", "node", new NetworkAddress("host", 1000)));

        topology.putNode(new LogicalNode("id2", "node", new NetworkAddress("host1", 1001)));

        // Expecting 2 writes because there are two puts. The test verifies that second put also produces just 1 write.
        verify(storage, times(2)).put(eq("logical".getBytes(UTF_8)), any());
    }

    @Test
    void removingExistingNodeProducesDisappearedEvent() {
        LogicalNode node = new LogicalNode("id1", "node", new NetworkAddress("host", 1000));
        topology.putNode(node);

        topology.removeNodes(Set.of(node));

        verify(listener).onNodeLeft(nodeCaptor.capture(), topologyCaptor.capture());

        assertThat(topologyCaptor.getValue().version(), is(2L));

        ClusterNode disappearedNode = nodeCaptor.getValue();

        assertThat(disappearedNode.id(), is("id1"));
        assertThat(disappearedNode.name(), is("node"));
    }

    @Test
    void removingNonExistingNodeProducesNoEvents() {
        LogicalNode node = new LogicalNode("id1", "node", new NetworkAddress("host", 1000));

        topology.removeNodes(Set.of(node));

        verify(listener, never()).onNodeLeft(any(), any());
    }

    @Test
    void multiRemovalProducesDisappearedEventsInOrderOfNodeIds() {
        LogicalNode node1 = new LogicalNode("id1", "node", new NetworkAddress("host", 1000));
        LogicalNode node2 = new LogicalNode("id2", "node2", new NetworkAddress("host2", 1000));

        topology.putNode(node1);
        topology.putNode(node2);

        topology.removeNodes(new LinkedHashSet<>(List.of(node2, node1)));

        verify(listener, times(2)).onNodeLeft(nodeCaptor.capture(), topologyCaptor.capture());

        List<LogicalTopologySnapshot> capturedSnapshots = topologyCaptor.getAllValues();

        assertThat(capturedSnapshots.get(0).version(), is(3L));
        assertThat(capturedSnapshots.get(1).version(), is(4L));

        ClusterNode disappearedNode1 = nodeCaptor.getAllValues().get(0);
        ClusterNode disappearedNode2 = nodeCaptor.getAllValues().get(1);

        assertAll(
                () -> assertThat(disappearedNode1.id(), is("id1")),
                () -> assertThat(disappearedNode2.id(), is("id2"))
        );
    }

    @Test
    void multiRemovalProducesExactlyOneWriteToDb() {
        LogicalNode node1 = new LogicalNode("id1", "node", new NetworkAddress("host", 1000));
        LogicalNode node2 = new LogicalNode("id2", "node2", new NetworkAddress("host2", 1000));

        topology.putNode(node1);
        topology.putNode(node2);

        topology.removeNodes(Set.of(node1, node2));

        // Expecting 3 writes because there are two puts and one removal. The test verifies that the removal produces just 1 write.
        verify(storage, times(3)).put(eq("logical".getBytes(UTF_8)), any());
    }

    @Test
    void onTopologyLeapIsTriggeredOnSnapshotRestore() {
        topology.putNode(new LogicalNode("id1", "node", new NetworkAddress("host", 1000)));

        topology.fireTopologyLeap();

        verify(listener).onTopologyLeap(topologyCaptor.capture());

        LogicalTopologySnapshot capturedSnapshot = topologyCaptor.getValue();
        assertThat(capturedSnapshot.version(), is(1L));
        assertThat(capturedSnapshot.nodes(), hasSize(1));
        assertThat(capturedSnapshot.nodes().iterator().next().id(), is("id1"));
    }

    @Test
    void onAppearedListenersExceptionsDoNotBreakNotification() {
        LogicalTopologyEventListener secondListener = mock(LogicalTopologyEventListener.class);

        doThrow(new RuntimeException("Oops")).when(listener).onNodeJoined(any(), any());

        topology.addEventListener(secondListener);

        topology.putNode(new LogicalNode("id1", "node", new NetworkAddress("host", 1000)));

        verify(listener).onNodeJoined(any(), any());
        verify(secondListener).onNodeJoined(any(), any());
    }

    @Test
    void onAppearedListenerErrorIsRethrown() {
        doThrow(new TestError()).when(listener).onNodeJoined(any(), any());

        assertThrows(
                TestError.class,
                () -> topology.putNode(new LogicalNode("id1", "node", new NetworkAddress("host", 1000)))
        );
    }

    @Test
    void onDisappearedListenersExceptionsDoNotBreakNotification() {
        LogicalTopologyEventListener secondListener = mock(LogicalTopologyEventListener.class);

        doThrow(new RuntimeException("Oops")).when(listener).onNodeLeft(any(), any());

        topology.addEventListener(secondListener);

        LogicalNode node = new LogicalNode("id1", "node", new NetworkAddress("host", 1000));

        topology.putNode(node);
        topology.removeNodes(Set.of(node));

        verify(listener).onNodeLeft(any(), any());
        verify(secondListener).onNodeLeft(any(), any());
    }

    @Test
    void onDisappearedListenerErrorIsRethrown() {
        doThrow(new TestError()).when(listener).onNodeLeft(any(), any());

        LogicalNode node = new LogicalNode("id1", "node", new NetworkAddress("host", 1000));

        topology.putNode(node);

        assertThrows(TestError.class, () -> topology.removeNodes(Set.of(node)));
    }

    @Test
    void onTopologyLeapListenersExceptionsDoNotBreakNotification() {
        LogicalTopologyEventListener secondListener = mock(LogicalTopologyEventListener.class);

        doThrow(new RuntimeException("Oops")).when(listener).onTopologyLeap(any());

        topology.addEventListener(secondListener);

        topology.fireTopologyLeap();

        verify(listener).onTopologyLeap(any());
        verify(secondListener).onTopologyLeap(any());
    }

    @Test
    void onTopologyLeapListenerErrorIsRethrown() {
        doThrow(new TestError()).when(listener).onTopologyLeap(any());

        assertThrows(TestError.class, () -> topology.fireTopologyLeap());
    }

    @Test
    void eventListenerStopsGettingEventsAfterListenerRemoval() {
        topology.removeEventListener(listener);

        topology.putNode(new LogicalNode("id1", "node", new NetworkAddress("host", 1000)));

        verify(listener, never()).onNodeJoined(any(), any());
    }

    @SuppressWarnings("serial")
    private static class TestError extends Error {
    }
}
