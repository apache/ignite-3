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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.AbstractClusterIntegrationTest;
import org.apache.ignite.internal.Cluster.NodeKnockout;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.cluster.management.configuration.ClusterManagementConfiguration;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.network.message.ScaleCubeMessage;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.network.ClusterNode;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for functionality of logical topology events subscription.
 */
@SuppressWarnings("resource")
class ItLogicalTopologyTest extends AbstractClusterIntegrationTest {
    private final List<Event> events = new CopyOnWriteArrayList<>();

    private final LogicalTopologyEventListener listener = new LogicalTopologyEventListener() {
        @Override
        public void onAppeared(ClusterNode appearedNode, LogicalTopologySnapshot newTopology) {
            events.add(new Event(true, appearedNode, newTopology.version()));
        }

        @Override
        public void onDisappeared(ClusterNode disappearedNode, LogicalTopologySnapshot newTopology) {
            events.add(new Event(false, disappearedNode, newTopology.version()));
        }
    };

    @Override
    protected int initialNodes() {
        return 1;
    }

    @Override
    protected String getNodeBootstrapConfigTemplate() {
        return FAST_FAILURE_DETECTION_NODE_BOOTSTRAP_CFG_TEMPLATE;
    }

    @Test
    void receivesLogicalTopologyEvents() throws Exception {
        IgniteImpl entryNode = node(0);

        entryNode.logicalTopologyService().addEventListener(listener);

        // Checking that onAppeared() is received.
        Ignite secondIgnite = startNode(1);

        assertTrue(waitForCondition(() -> !events.isEmpty(), 10_000));

        assertThat(events, hasSize(1));

        Event firstEvent = events.get(0);

        assertTrue(firstEvent.appeared);
        assertThat(firstEvent.node.name(), is(secondIgnite.name()));
        assertThat(firstEvent.topologyVersion, is(2L));

        // Checking that onDisappeared() is received.
        stopNode(1);

        assertTrue(waitForCondition(() -> events.size() > 1, 10_000));

        assertThat(events, hasSize(2));

        Event secondEvent = events.get(1);

        assertFalse(secondEvent.appeared);
        assertThat(secondEvent.node.name(), is(secondIgnite.name()));
        assertThat(secondEvent.topologyVersion, is(3L));
    }

    @Test
    void receivesLogicalTopologyEventsCausedByNodeRestart() throws Exception {
        IgniteImpl entryNode = node(0);

        Ignite secondIgnite = startNode(1);

        entryNode.logicalTopologyService().addEventListener(listener);

        restartNode(1);

        waitForCondition(() -> events.size() >= 2, 10_000);

        assertThat(events, hasSize(2));

        Event leaveEvent = events.get(0);

        assertFalse(leaveEvent.appeared);
        assertThat(leaveEvent.node.name(), is(secondIgnite.name()));
        assertThat(leaveEvent.topologyVersion, is(3L));

        Event joinEvent = events.get(1);

        assertTrue(joinEvent.appeared);
        assertThat(joinEvent.node.name(), is(secondIgnite.name()));
        assertThat(joinEvent.topologyVersion, is(4L));
    }

    @Test
    void nodeReturnedToPhysicalTopologyReturnsToLogicalTopology() throws Exception {
        IgniteImpl entryNode = node(0);

        IgniteImpl secondIgnite = startNode(1);

        makeSecondNodeDisappearForFirstNode(entryNode, secondIgnite);

        CountDownLatch secondIgniteAppeared = new CountDownLatch(1);

        entryNode.logicalTopologyService().addEventListener(new LogicalTopologyEventListener() {
            @Override
            public void onAppeared(ClusterNode appearedNode, LogicalTopologySnapshot newTopology) {
                if (appearedNode.name().equals(secondIgnite.name())) {
                    secondIgniteAppeared.countDown();
                }
            }
        });

        entryNode.stopDroppingMessages();

        assertTrue(secondIgniteAppeared.await(10, TimeUnit.SECONDS), "Did not see second node coming back in time");
    }

    private static void makeSecondNodeDisappearForFirstNode(IgniteImpl firstIgnite, IgniteImpl secondIgnite) throws InterruptedException {
        CountDownLatch secondIgniteDisappeared = new CountDownLatch(1);

        firstIgnite.logicalTopologyService().addEventListener(new LogicalTopologyEventListener() {
            @Override
            public void onDisappeared(ClusterNode disappearedNode, LogicalTopologySnapshot newTopology) {
                if (disappearedNode.name().equals(secondIgnite.name())) {
                    secondIgniteDisappeared.countDown();
                }
            }
        });

        firstIgnite.dropMessages((recipientConsistentId, message) ->
                secondIgnite.node().name().equals(recipientConsistentId) && message instanceof ScaleCubeMessage);

        assertTrue(secondIgniteDisappeared.await(10, TimeUnit.SECONDS), "Did not see second node leaving in time");
    }

    @Test
    void nodeDoesNotLeaveLogicalTopologyImmediatelyAfterBeingLostBySwim() throws Exception {
        IgniteImpl entryNode = node(0);

        setInfiniteClusterFailoverTimeout(entryNode);

        startNode(1);

        entryNode.logicalTopologyService().addEventListener(listener);

        // Knock the node out without allowing it to say good bye.
        cluster.knockOutNode(1, NodeKnockout.PARTITION_NETWORK);

        // 1 second is usually insufficient on my machine to get an event, even if it's produced. On the CI we
        // should probably account for spurious delays due to other processes, hence 2 seconds.
        waitForCondition(() -> !events.isEmpty(), 2_000);

        assertThat(events, is(empty()));
    }

    private static void setInfiniteClusterFailoverTimeout(IgniteImpl node)
            throws InterruptedException, ExecutionException, TimeoutException {
        node.nodeConfiguration().getConfiguration(ClusterManagementConfiguration.KEY)
                .failoverTimeout()
                .update(Long.MAX_VALUE)
                .get(10, TimeUnit.SECONDS);
    }

    private static class Event {
        private final boolean appeared;
        private final ClusterNode node;
        private final long topologyVersion;

        private Event(boolean appeared, ClusterNode node, long topologyVersion) {
            this.appeared = appeared;
            this.node = node;
            this.topologyVersion = topologyVersion;
        }

        @Override
        public String toString() {
            return S.toString(this);
        }
    }
}
