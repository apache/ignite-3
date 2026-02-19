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

package org.apache.ignite.internal.distributionzones;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.distributionzones.DataNodesTestUtil.createZoneWithInfiniteTimers;
import static org.apache.ignite.internal.distributionzones.DataNodesTestUtil.recalculateZoneDataNodesManuallyAndWaitForDataNodes;
import static org.apache.ignite.internal.distributionzones.DataNodesTestUtil.waitForDataNodes;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

class ItDataNodesManagerTest extends ClusterPerTestIntegrationTest {
    private static final String ZONE_NAME = "test_zone";

    @Override
    protected int initialNodes() {
        return 1;
    }

    @Test
    public void manualDataNodeRecalculationIdempotencyTest() {
        IgniteImpl node = unwrapIgniteImpl(node(0));

        createZoneWithInfiniteTimers(node, ZONE_NAME);

        waitForDataNodes(node, ZONE_NAME, Set.of(node.name()));

        recalculateZoneDataNodesManuallyAndWaitForDataNodes(node, ZONE_NAME, Set.of(node.name()));
    }

    @Test
    public void manualDataNodeRecalculationAfterNewNodeAddedTest() {
        IgniteImpl node = unwrapIgniteImpl(node(0));

        createZoneWithInfiniteTimers(node, ZONE_NAME);

        waitForDataNodes(node, ZONE_NAME, Set.of(node.name()));

        startNode(1);

        LogicalTopologyService logicalTopologyService = node.logicalTopologyService();

        Awaitility.waitAtMost(2, TimeUnit.SECONDS)
                .untilAsserted(() -> assertEquals(2, logicalTopologyService.localLogicalTopology().size()));

        waitForDataNodes(node, ZONE_NAME, Set.of(node.name()));

        recalculateZoneDataNodesManuallyAndWaitForDataNodes(node, ZONE_NAME, Set.of(node.name(), node(1).name()));
    }
}
