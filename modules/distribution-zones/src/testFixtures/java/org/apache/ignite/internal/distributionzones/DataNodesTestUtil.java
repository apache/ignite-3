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
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.zoneId;
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
import java.util.function.Function;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.distributionzones.DataNodesManager.ZoneTimerSchedule;
import org.apache.ignite.internal.distributionzones.DataNodesManager.ZoneTimers;
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

    /**
     * Checks that scale up timer for the given zone was set up.
     *
     * @param node Ignite node.
     * @param zoneName Zone name.
     * @throws InterruptedException In case of waiting timeout.
     */
    public static void assertScaleUpScheduledOrDone(IgniteImpl node, String zoneName) throws InterruptedException {
        CatalogManager catalogManager = node.catalogManager();
        DataNodesManager dataNodesManager = node.distributionZoneManager().dataNodesManager();

        assertScaleUpScheduledOrDone(catalogManager, dataNodesManager, zoneName);
    }

    /**
     * Checks that scale up timer for the given zone was set up.
     *
     * @param catalogManager Catalog manager.
     * @param dataNodesManager Data nodes manager.
     * @param zoneName Zone name.
     * @throws InterruptedException In case of waiting timeout.
     */
    public static void assertScaleUpScheduledOrDone(
            CatalogManager catalogManager,
            DataNodesManager dataNodesManager,
            String zoneName
    ) throws InterruptedException {
        assertDistributionZoneScaleTimerScheduledOrDone(
                catalogManager,
                dataNodesManager,
                zoneName,
                timers -> timers.scaleUp
        );
    }

    /**
     * Checks that scale down timer for the given zone was set up.
     *
     * @param node Ignite node.
     * @param zoneName Zone name.
     * @throws InterruptedException In case of waiting timeout.
     */
    public static void assertScaleDownScheduledOrDone(IgniteImpl node, String zoneName) throws InterruptedException {
        CatalogManager catalogManager = node.catalogManager();
        DataNodesManager dataNodesManager = node.distributionZoneManager().dataNodesManager();

        assertScaleDownScheduledOrDone(catalogManager, dataNodesManager, zoneName);
    }

    /**
     * Checks that scale down timer for the given zone was set up.
     *
     * @param catalogManager Catalog manager.
     * @param dataNodesManager Data nodes manager.
     * @param zoneName Zone name.
     * @throws InterruptedException In case of waiting timeout.
     */
    public static void assertScaleDownScheduledOrDone(
            CatalogManager catalogManager,
            DataNodesManager dataNodesManager,
            String zoneName
    ) throws InterruptedException {
        assertDistributionZoneScaleTimerScheduledOrDone(
                catalogManager,
                dataNodesManager,
                zoneName,
                timers -> timers.scaleDown
        );
    }

    private static void assertDistributionZoneScaleTimerScheduledOrDone(
            CatalogManager catalogManager,
            DataNodesManager dataNodesManager,
            String zoneName,
            Function<ZoneTimers, ZoneTimerSchedule> getScaleTimer
    ) throws InterruptedException {
        boolean success = waitForCondition(() -> {
            ZoneTimerSchedule schedule = getScaleTimer.apply(dataNodesManager.zoneTimers(zoneId(catalogManager, zoneName)));
            return schedule.taskIsScheduled() || schedule.taskIsDone();
        }, 2000);

        ZoneTimerSchedule schedule = getScaleTimer.apply(dataNodesManager.zoneTimers(zoneId(catalogManager, zoneName)));
        assertTrue(success, format("Unsuccessful schedule [taskIsScheduled={}, taskIsCancelled={}, taskIsDone={}].",
                schedule.taskIsScheduled(), schedule.taskIsCancelled(), schedule.taskIsDone()));
    }

    /**
     * Checks that there no scheduled scale up/down timers for given distribution zone.
     *
     * @param node Ignite node.
     * @param zoneName Zone name.
     * @throws InterruptedException In case of waiting timeout.
     */
    public static void assertDistributionZoneScaleTimersAreNotScheduled(
            IgniteImpl node,
            String zoneName
    ) throws InterruptedException {
        CatalogManager catalogManager = node.catalogManager();
        DataNodesManager dataNodesManager = node.distributionZoneManager().dataNodesManager();

        assertTrue(waitForCondition(() ->
                        !scaleUpScheduled(catalogManager, dataNodesManager, zoneName)
                                && !scaleDownScheduled(catalogManager, dataNodesManager, zoneName),
                2000
        ));
    }

    /**
     * Checks that there no scheduled scale up/down timers for given distribution zone.
     *
     * @param catalogManager Catalog manager.
     * @param dataNodesManager Data nodes manager.
     * @param zoneName Zone name.
     * @throws InterruptedException In case of waiting timeout.
     */
    public static void assertDistributionZoneScaleTimersAreNotScheduled(
            CatalogManager catalogManager,
            DataNodesManager dataNodesManager,
            String zoneName
    ) throws InterruptedException {
        assertTrue(waitForCondition(() ->
                !scaleUpScheduled(catalogManager, dataNodesManager, zoneName)
                        && !scaleDownScheduled(catalogManager, dataNodesManager, zoneName),
                2000
        ));
    }

    private static boolean scaleUpScheduled(CatalogManager catalogManager, DataNodesManager dataNodesManager, String zoneName) {
        return dataNodesManager.zoneTimers(zoneId(catalogManager, zoneName)).scaleUp.taskIsScheduled();
    }

    private static boolean scaleDownScheduled(CatalogManager catalogManager, DataNodesManager dataNodesManager, String zoneName) {
        return dataNodesManager.zoneTimers(zoneId(catalogManager, zoneName)).scaleDown.taskIsScheduled();
    }
}
