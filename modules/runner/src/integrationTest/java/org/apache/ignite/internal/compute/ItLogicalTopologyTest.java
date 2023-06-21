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

package org.apache.ignite.internal.compute;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.network.message.ScaleCubeMessage;
import org.apache.ignite.internal.tostring.S;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Integration tests for functionality of logical topology events subscription.
 */
@SuppressWarnings("resource")
class ItLogicalTopologyTest extends ClusterPerTestIntegrationTest {
    private final BlockingQueue<Event> events = new LinkedBlockingQueue<>();

    private static final String NODE_ATTRIBUTES = "{region:{attribute:\"US\"},storage:{attribute:\"SSD\"}}";

    private static final Map<String, String> NODE_ATTRIBUTES_MAP = Map.of("region", "US", "storage", "SSD");

    @Language("JSON")
    private static final String NODE_BOOTSTRAP_CFG_TEMPLATE_WITH_NODE_ATTRIBUTES = "{\n"
            + "  network: {\n"
            + "    port: {},\n"
            + "    nodeFinder: {\n"
            + "      netClusterNodes: [ {} ]\n"
            + "    }\n"
            + "  },"
            + "  nodeAttributes: {\n"
            + "    nodeAttributes: " + NODE_ATTRIBUTES
            + "  },\n"
            + "  clientConnector: { port:{} }\n"
            + "}";

    private final LogicalTopologyEventListener listener = new LogicalTopologyEventListener() {
        @Override
        public void onNodeValidated(LogicalNode validatedNode) {
            events.add(new Event(EventType.VALIDATED, validatedNode, -1));
        }

        @Override
        public void onNodeInvalidated(LogicalNode invalidatedNode) {
            events.add(new Event(EventType.INVALIDATED, invalidatedNode, -1));
        }

        @Override
        public void onNodeJoined(LogicalNode joinedNode, LogicalTopologySnapshot newTopology) {
            events.add(new Event(EventType.JOINED, joinedNode, newTopology.version()));
        }

        @Override
        public void onNodeLeft(LogicalNode leftNode, LogicalTopologySnapshot newTopology) {
            events.add(new Event(EventType.LEFT, leftNode, newTopology.version()));
        }
    };

    @Override
    protected int initialNodes() {
        // We don't need any nodes to be started automatically.
        return 0;
    }

    @Override
    protected String getNodeBootstrapConfigTemplate() {
        return FAST_FAILURE_DETECTION_NODE_BOOTSTRAP_CFG_TEMPLATE;
    }

    @Test
    void receivesLogicalTopologyEvents() throws Exception {
        cluster.startAndInit(1);

        IgniteImpl entryNode = node(0);

        entryNode.logicalTopologyService().addEventListener(listener);

        // Checking that onAppeared() is received.
        Ignite secondIgnite = startNode(1);

        Event event = events.poll(10, TimeUnit.SECONDS);

        assertThat(event, is(notNullValue()));
        assertThat(event.eventType, is(EventType.VALIDATED));
        assertThat(event.node.name(), is(secondIgnite.name()));

        event = events.poll(10, TimeUnit.SECONDS);

        assertThat(event, is(notNullValue()));
        assertThat(event.eventType, is(EventType.JOINED));
        assertThat(event.node.name(), is(secondIgnite.name()));
        assertThat(event.topologyVersion, is(2L));

        assertThat(events, is(empty()));

        // Checking that onDisappeared() is received.
        stopNode(1);

        event = events.poll(10, TimeUnit.SECONDS);

        assertThat(event, is(notNullValue()));
        assertThat(event.eventType, is(EventType.LEFT));
        assertThat(event.node.name(), is(secondIgnite.name()));
        assertThat(event.topologyVersion, is(3L));

        assertThat(events, is(empty()));
    }

    @Test
    void receivesLogicalTopologyEventsWithNodeAttributes() throws Exception {
        cluster.startAndInit(1);

        IgniteImpl entryNode = node(0);

        entryNode.logicalTopologyService().addEventListener(listener);

        // Checking that onAppeared() is received.
        Ignite secondIgnite = startNode(1, NODE_BOOTSTRAP_CFG_TEMPLATE_WITH_NODE_ATTRIBUTES);

        Event event = events.poll(10, TimeUnit.SECONDS);

        assertThat(event, is(notNullValue()));
        assertThat(event.eventType, is(EventType.VALIDATED));
        assertThat(event.node.name(), is(secondIgnite.name()));
        assertThat(event.node.nodeAttributes(), is(NODE_ATTRIBUTES_MAP));

        event = events.poll(10, TimeUnit.SECONDS);

        assertThat(event, is(notNullValue()));
        assertThat(event.eventType, is(EventType.JOINED));
        assertThat(event.node.name(), is(secondIgnite.name()));
        assertThat(event.topologyVersion, is(2L));
        assertThat(event.node.nodeAttributes(), is(NODE_ATTRIBUTES_MAP));

        assertThat(events, is(empty()));

        // Checking that onDisappeared() is received.
        stopNode(1);

        event = events.poll(10, TimeUnit.SECONDS);

        assertThat(event, is(notNullValue()));
        assertThat(event.eventType, is(EventType.LEFT));
        assertThat(event.node.name(), is(secondIgnite.name()));
        assertThat(event.topologyVersion, is(3L));
        assertThat(event.node.nodeAttributes(), is(Collections.emptyMap()));

        assertThat(events, is(empty()));
    }

    @Test
    void receiveLogicalTopologyFromLeaderWithAttributes() throws Exception {
        cluster.startAndInit(1);

        IgniteImpl entryNode = node(0);

        IgniteImpl secondIgnite = startNode(1, NODE_BOOTSTRAP_CFG_TEMPLATE_WITH_NODE_ATTRIBUTES);

        List<LogicalNode> logicalTopologyFromLeader = new ArrayList<>(
                entryNode.logicalTopologyService().logicalTopologyOnLeader().get(5, TimeUnit.SECONDS).nodes()
        );

        assertEquals(2, logicalTopologyFromLeader.size());

        Optional<LogicalNode> secondNode = logicalTopologyFromLeader.stream().filter(n -> n.name().equals(secondIgnite.name())).findFirst();

        assertTrue(secondNode.isPresent());

        assertThat(secondNode.get().nodeAttributes(), is(NODE_ATTRIBUTES_MAP));
    }

    @Test
    void receivesLogicalTopologyEventsCausedByNodeRestart() throws Exception {
        cluster.startAndInit(1);

        IgniteImpl entryNode = node(0);

        Ignite secondIgnite = startNode(1);

        entryNode.logicalTopologyService().addEventListener(listener);

        restartNode(1);

        Event event = events.poll(10, TimeUnit.SECONDS);

        assertThat(event, is(notNullValue()));
        assertThat(event.eventType, is(EventType.LEFT));
        assertThat(event.node.name(), is(secondIgnite.name()));
        assertThat(event.topologyVersion, is(3L));

        event = events.poll(10, TimeUnit.SECONDS);

        assertThat(event, is(notNullValue()));
        assertThat(event.eventType, is(EventType.VALIDATED));
        assertThat(event.node.name(), is(secondIgnite.name()));

        event = events.poll(10, TimeUnit.SECONDS);

        assertThat(event, is(notNullValue()));
        assertThat(event.eventType, is(EventType.JOINED));
        assertThat(event.node.name(), is(secondIgnite.name()));
        assertThat(event.topologyVersion, is(4L));

        assertThat(events, is(empty()));
    }

    @Test
    void nodeReturnedToPhysicalTopologyDoesNotReturnToLogicalTopology() throws Exception {
        cluster.startAndInit(1);

        IgniteImpl entryNode = node(0);

        IgniteImpl secondIgnite = startNode(1);

        makeSecondNodeDisappearForFirstNode(entryNode, secondIgnite);

        CountDownLatch secondIgniteAppeared = new CountDownLatch(1);

        entryNode.logicalTopologyService().addEventListener(new LogicalTopologyEventListener() {
            @Override
            public void onNodeJoined(LogicalNode joinedNode, LogicalTopologySnapshot newTopology) {
                if (joinedNode.name().equals(secondIgnite.name())) {
                    secondIgniteAppeared.countDown();
                }
            }
        });

        entryNode.stopDroppingMessages();

        assertFalse(secondIgniteAppeared.await(3, TimeUnit.SECONDS), "Second node returned to logical topology");
    }

    private static void makeSecondNodeDisappearForFirstNode(IgniteImpl firstIgnite, IgniteImpl secondIgnite) throws InterruptedException {
        CountDownLatch secondIgniteDisappeared = new CountDownLatch(1);

        firstIgnite.logicalTopologyService().addEventListener(new LogicalTopologyEventListener() {
            @Override
            public void onNodeLeft(LogicalNode leftNode, LogicalTopologySnapshot newTopology) {
                if (leftNode.name().equals(secondIgnite.name())) {
                    secondIgniteDisappeared.countDown();
                }
            }
        });

        firstIgnite.dropMessages((recipientConsistentId, message) ->
                secondIgnite.node().name().equals(recipientConsistentId) && message instanceof ScaleCubeMessage);

        assertTrue(secondIgniteDisappeared.await(10, TimeUnit.SECONDS), "Did not see second node leaving in time");
    }

    @Test
    void nodeLeavesLogicalTopologyImmediatelyAfterBeingLostBySwim() throws Exception {
        cluster.startAndInit(1);

        IgniteImpl entryNode = node(0);

        IgniteImpl secondNode = startNode(1);

        entryNode.logicalTopologyService().addEventListener(listener);

        // Knock the node out without allowing it to say good bye.
        cluster.simulateNetworkPartitionOf(1);

        Event leaveEvent = events.poll(10, TimeUnit.SECONDS);

        assertThat(leaveEvent, is(notNullValue()));
        assertThat(leaveEvent.node.name(), is(secondNode.name()));
        assertThat(leaveEvent.eventType, is(EventType.LEFT));
    }

    @Test
    void nodeThatCouldNotJoinShouldBeInvalidated(TestInfo testInfo) throws Exception {
        cluster.startAndInit(1);

        IgniteImpl entryNode = node(0);

        entryNode.logicalTopologyService().addEventListener(listener);

        // Disable messaging to the second node as soon as it is validated. This will emulate a situation when a node leaves after
        // validation, but before joining the cluster.
        entryNode.logicalTopologyService().addEventListener(new LogicalTopologyEventListener() {
            @Override
            public void onNodeValidated(LogicalNode validatedNode) {
                entryNode.dropMessages((recipientConsistentId, message) -> validatedNode.name().equals(recipientConsistentId));
            }
        });

        cluster.startNodeAsync(1);

        try {
            Event event = events.poll(10, TimeUnit.SECONDS);

            assertThat(event, is(notNullValue()));
            assertThat(event.eventType, is(EventType.VALIDATED));
            assertThat(event.node.name(), is(not(entryNode.name())));

            event = events.poll(10, TimeUnit.SECONDS);

            assertThat(event, is(notNullValue()));
            assertThat(event.eventType, is(EventType.INVALIDATED));
            assertThat(event.node.name(), is(not(entryNode.name())));
        } finally {
            // Stop the second node manually, because it couldn't start successfully.
            IgnitionManager.stop(testNodeName(testInfo, 1));
        }
    }

    @Test
    void nodeLeavesLogicalTopologyImmediatelyOnGracefulStop() throws Exception {
        cluster.startAndInit(1, DISABLED_FAILURE_DETECTION_NODE_BOOTSTRAP_CFG_TEMPLATE, ignored -> {});

        IgniteImpl entryNode = node(0);

        IgniteImpl secondIgnite = startNode(1);

        entryNode.logicalTopologyService().addEventListener(listener);

        stopNode(1);

        Event leaveEvent = events.poll(10, TimeUnit.SECONDS);

        assertThat(events, is(empty()));

        assertThat("Leave event not received in time", leaveEvent, is(notNullValue()));

        assertThat(leaveEvent.eventType, is(EventType.LEFT));
        assertThat(leaveEvent.node.name(), is(secondIgnite.name()));
    }

    private static class Event {
        private final EventType eventType;
        private final LogicalNode node;
        private final long topologyVersion;

        private Event(EventType eventType, LogicalNode node, long topologyVersion) {
            this.eventType = eventType;
            this.node = node;
            this.topologyVersion = topologyVersion;
        }

        @Override
        public String toString() {
            return S.toString(this);
        }
    }

    private enum EventType {
        JOINED, LEFT, VALIDATED, INVALIDATED
    }
}
