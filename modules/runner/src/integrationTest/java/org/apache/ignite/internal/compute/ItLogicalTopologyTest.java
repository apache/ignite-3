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
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.AbstractClusterIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.network.ClusterNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

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

    @Test
    void receivesLogicalTopologyEvents(TestInfo testInfo) throws Exception {
        IgniteImpl entryNode = node(0);

        entryNode.logicalTopologyService().addEventListener(listener);

        // Checking that onAppeared() is received.
        Ignite secondIgnite = startNode(1, testInfo);

        assertTrue(waitForCondition(() -> !events.isEmpty(), 10_000));

        assertThat(events, hasSize(1));

        Event firstEvent = events.get(0);

        assertTrue(firstEvent.appeared);
        assertThat(firstEvent.node.name(), is(secondIgnite.name()));
        assertThat(firstEvent.topologyVersion, is(2L));

        // Checking that onDisappeared() is received.
        stopNode(1, testInfo);

        assertTrue(waitForCondition(() -> events.size() > 1, 10_000));

        assertThat(events, hasSize(2));

        Event secondEvent = events.get(1);

        assertFalse(secondEvent.appeared);
        assertThat(secondEvent.node.name(), is(secondIgnite.name()));
        assertThat(secondEvent.topologyVersion, is(3L));
    }

    @Test
    void receivesLogicalTopologyEventsFromRestart(TestInfo testInfo) throws Exception {
        IgniteImpl entryNode = node(0);

        Ignite secondIgnite = startNode(1, testInfo);

        entryNode.logicalTopologyService().addEventListener(listener);

        restartNode(1, testInfo);

        assertTrue(waitForCondition(() -> events.size() >= 4, 10_000));

        assertThat(events, hasSize(4));

        Event leaveEvent = events.get(2);

        assertFalse(leaveEvent.appeared);
        assertThat(leaveEvent.node.name(), is(secondIgnite.name()));
        assertThat(leaveEvent.topologyVersion, is(3L));

        Event joinEvent = events.get(3);

        assertTrue(joinEvent.appeared);
        assertThat(joinEvent.node.name(), is(secondIgnite.name()));
        assertThat(joinEvent.topologyVersion, is(4L));
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
    }
}
