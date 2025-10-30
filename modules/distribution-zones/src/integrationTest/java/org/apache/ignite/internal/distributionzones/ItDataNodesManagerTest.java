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
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.hlc.ClockService;
import org.junit.jupiter.api.Test;

class ItDataNodesManagerTest extends ClusterPerTestIntegrationTest {
    private static final String ZONE_NAME = "test_zone";

    @Override
    protected int initialNodes() {
        return 1;
    }

    @Test
    public void manualDataNodeRecalculationIdempotencyTest() throws InterruptedException {
        IgniteImpl node = unwrapIgniteImpl(node(0));

        createZoneWithInfiniteTimers(node);

        waitForDataNodes(node, ZONE_NAME, Set.of(node.name()));

        recalculateZoneDataNodesManuallyAndWaitForDataNodes(node, ZONE_NAME, Set.of(node.name()));
    }

    @Test
    public void manualDataNodeRecalculationAfterNewNodeAddedTest() throws InterruptedException {
        IgniteImpl node = unwrapIgniteImpl(node(0));

        createZoneWithInfiniteTimers(node);

        waitForDataNodes(node, ZONE_NAME, Set.of(node.name()));

        startNode(1);

        assertTrue(waitForCondition(() -> node.logicalTopologyService().localLogicalTopology().nodes().size() == 2, 1000));

        waitForDataNodes(node, ZONE_NAME, Set.of(node.name()));

        recalculateZoneDataNodesManuallyAndWaitForDataNodes(node, ZONE_NAME, Set.of(node.name(), node(1).name()));
    }

    private static void createZoneWithInfiniteTimers(IgniteImpl node) {
        DistributionZonesTestUtil.createZone(node.catalogManager(), ZONE_NAME, INFINITE_TIMER_VALUE, INFINITE_TIMER_VALUE, null);

        CatalogManager catalogManager = node.catalogManager();

        CatalogZoneDescriptor zoneDesc = catalogManager.catalog(catalogManager.latestCatalogVersion()).zone(ZONE_NAME);

        assertNotNull(zoneDesc);
    }

    private static void waitForDataNodes(
            IgniteImpl node,
            String zoneName,
            Set<String> expectedNodes
    ) throws InterruptedException {
        CatalogManager catalogManager = node.catalogManager();

        ClockService clock = node.clockService();

        CatalogZoneDescriptor zone = catalogManager.activeCatalog(clock.now().longValue()).zone(zoneName);
        int zoneId = zone.id();

        DataNodesManager dataNodesManager = node.distributionZoneManager().dataNodesManager();

        boolean success = waitForCondition(() -> {
            CompletableFuture<Set<String>> dataNodesFuture = dataNodesManager.dataNodes(zoneId, clock.now());
            assertThat(dataNodesFuture, willSucceedFast());
            return dataNodesFuture.join().equals(expectedNodes);
        }, 10_000);

        assertTrue(
                success,
                format(
                        "Expected {}, but actual {}.",
                        expectedNodes,
                        dataNodesManager.dataNodes(zoneId, clock.now()).join()
                )
        );
    }

    private static void recalculateZoneDataNodesManuallyAndWaitForDataNodes(
            IgniteImpl node,
            String zoneName,
            Set<String> expectedDataNodes
    ) throws InterruptedException {
        CompletableFuture<Set<String>> futureRecalculationResult = node.distributionZoneManager()
                .dataNodesManager()
                .recalculateDataNodes(zoneName);

        assertThat(futureRecalculationResult, willCompleteSuccessfully());

        assertEquals(expectedDataNodes, futureRecalculationResult.join());

        waitForDataNodes(node, zoneName, expectedDataNodes);
    }
}
