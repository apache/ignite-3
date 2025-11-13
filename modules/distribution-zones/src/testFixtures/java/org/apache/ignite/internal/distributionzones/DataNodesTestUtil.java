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
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.hlc.ClockService;

/**
 * Class that contains useful methods for data nodes testing purposes.
 */
public final class DataNodesTestUtil {
    /**
     * Creates a zone with given name and both scale up and scale down timers as infinite.
     *
     * @param node Ignite node.
     * @param zoneName New zone name.
     */
    public static void createZoneWithInfiniteTimers(IgniteImpl node, String  zoneName) {
        DistributionZonesTestUtil.createZone(
                node.catalogManager(),
                zoneName,
                (Integer) INFINITE_TIMER_VALUE,
                (Integer) INFINITE_TIMER_VALUE,
                null
        );

        CatalogManager catalogManager = node.catalogManager();

        CatalogZoneDescriptor zoneDesc = catalogManager.catalog(catalogManager.latestCatalogVersion()).zone(zoneName);

        assertNotNull(zoneDesc);
    }

    /**
     * Waits for data nodes are recalculated as expected for given zone.
     *
     * @param node Ignite node.
     * @param zoneName Zone name to check data nodes for.
     * @param expectedDataNodes Expected data node names to wait for.
     */
    public static void waitForDataNodes(
            IgniteImpl node,
            String zoneName,
            Set<String> expectedDataNodes
    ) throws InterruptedException {
        CatalogManager catalogManager = node.catalogManager();

        ClockService clock = node.clockService();

        CatalogZoneDescriptor zone = catalogManager.activeCatalog(clock.now().longValue()).zone(zoneName);
        int zoneId = zone.id();

        DataNodesManager dataNodesManager = node.distributionZoneManager().dataNodesManager();

        boolean success = waitForCondition(() -> {
            CompletableFuture<Set<String>> dataNodesFuture = dataNodesManager.dataNodes(zoneId, clock.now());
            assertThat(dataNodesFuture, willSucceedFast());
            return dataNodesFuture.join().equals(expectedDataNodes);
        }, 10_000);

        assertTrue(
                success,
                format(
                        "Expected {}, but actual {}.",
                        expectedDataNodes,
                        dataNodesManager.dataNodes(zoneId, clock.now()).join()
                )
        );
    }

    /**
     * Performs manual data nodes recalculation and waits until recalculated data nodes will be as expected for given zone.
     *
     * @param node Ignite node.
     * @param zoneName Zone name to check data nodes for.
     * @param expectedDataNodes Expected data node names to wait for.
     */
    public static void recalculateZoneDataNodesManuallyAndWaitForDataNodes(
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
