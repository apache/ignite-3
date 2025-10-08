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

import static java.util.UUID.randomUUID;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertDataNodesFromManager;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.createDefaultZone;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.catalog.descriptors.ConsistencyMode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Test scenarios when filter of a zone is altered and immediate scale up is triggered.
 */
public class DistributionZoneManagerAlterFilterTest extends BaseDistributionZoneManagerTest {
    private static final String FILTER = "$[?(@.storage == 'SSD' || @.region == 'US')]";

    private static final LogicalNode A = new LogicalNode(
            new ClusterNodeImpl(randomUUID(), "A", new NetworkAddress("localhost", 123)),
            Map.of("region", "US", "storage", "SSD", "dataRegionSize", "10"),
            Map.of(),
            List.of(DEFAULT_STORAGE_PROFILE)
    );

    private static final LogicalNode B = new LogicalNode(
            new ClusterNodeImpl(randomUUID(), "B", new NetworkAddress("localhost", 123)),
            Map.of("region", "EU", "storage", "HHD", "dataRegionSize", "30"),
            Map.of(),
            List.of(DEFAULT_STORAGE_PROFILE)
    );

    private static final LogicalNode C = new LogicalNode(
            new ClusterNodeImpl(randomUUID(), "C", new NetworkAddress("localhost", 123)),
            Map.of("region", "CN", "storage", "SSD", "dataRegionSize", "20"),
            Map.of(),
            List.of(DEFAULT_STORAGE_PROFILE)
    );

    private static final LogicalNode D = new LogicalNode(
            new ClusterNodeImpl(randomUUID(), "D", new NetworkAddress("localhost", 123)),
            Map.of("region", "CN", "storage", "HDD", "dataRegionSize", "20"),
            Map.of(),
            List.of(DEFAULT_STORAGE_PROFILE)
    );

    /**
     * Tests that node that was added before altering filter is taken into account after altering of a filter and corresponding
     * immediate scale up.
     *
     * @throws Exception If failed.
     */
    @ParameterizedTest(name = "defaultZone={0},consistencyMode={1}")
    @CsvSource({
            "true,",
            "false, HIGH_AVAILABILITY",
            "false, STRONG_CONSISTENCY",
    })
    void testAlterFilter(boolean defaultZone, ConsistencyMode consistencyMode) throws Exception {
        String zoneName = preparePrerequisites(defaultZone, consistencyMode);

        // Change timers to infinite, add new node, alter filter and check that data nodes was changed.
        alterZone(zoneName, INFINITE_TIMER_VALUE, INFINITE_TIMER_VALUE, FILTER);

        topology.putNode(D);

        // Nodes C and D match the filter.
        String newFilter = "$[?(@.region == 'CN')]";

        alterZone(zoneName, null, null, newFilter);

        assertDataNodesFromManager(distributionZoneManager, metaStorageManager::appliedRevision, catalogManager::latestCatalogVersion,
                getZoneId(zoneName), Set.of(C, D), ZONE_MODIFICATION_AWAIT_TIMEOUT);
    }

    /**
     * Tests that empty data nodes are propagated after altering of a filter and corresponding immediate scale up.
     *
     * @throws Exception If failed.
     */
    @ParameterizedTest(name = "defaultZone={0},consistencyMode={1}")
    @CsvSource({
            "true,",
            "false, HIGH_AVAILABILITY",
            "false, STRONG_CONSISTENCY",
    })
    void testAlterFilterToEmtpyNodes(boolean defaultZone, ConsistencyMode consistencyMode) throws Exception {
        String zoneName = preparePrerequisites(defaultZone, consistencyMode);

        // Change timers to infinite, add new node, alter filter and check that data nodes was changed.
        alterZone(zoneName, INFINITE_TIMER_VALUE, INFINITE_TIMER_VALUE, FILTER);

        topology.putNode(D);

        // No nodes are matching the filter
        String newFilter = "$[?(@.region == 'JP')]";

        alterZone(zoneName, null, null, newFilter);

        assertDataNodesFromManager(distributionZoneManager, metaStorageManager::appliedRevision, catalogManager::latestCatalogVersion,
                getZoneId(zoneName), Set.of(), ZONE_MODIFICATION_AWAIT_TIMEOUT);
    }

    /**
     * Tests that altering of a filter affects only scale up timers and only added nodes.
     *
     * @throws Exception If failed.
     */
    @ParameterizedTest(name = "defaultZone={0},consistencyMode={1}")
    @CsvSource({
            "true,",
            "false, HIGH_AVAILABILITY",
            "false, STRONG_CONSISTENCY",
    })
    void testAlterFilterDoNotAffectScaleDown(boolean defaultZone, ConsistencyMode consistencyMode) throws Exception {
        String zoneName = preparePrerequisites(
                IMMEDIATE_TIMER_VALUE,
                COMMON_UP_DOWN_AUTOADJUST_TIMER_SECONDS,
                defaultZone,
                consistencyMode
        );

        topology.putNode(D);

        int zoneId = getZoneId(zoneName);

        topology.removeNodes(Set.of(C));

        // Check that scale down task was scheduled.
        assertTrue(
                waitForCondition(
                        () -> distributionZoneManager.dataNodesManager().zoneTimers(zoneId) != null
                                && distributionZoneManager.dataNodesManager().zoneTimers(zoneId).scaleDown.taskIsScheduled(),
                        ZONE_MODIFICATION_AWAIT_TIMEOUT
                )
        );

        // Nodes C and D match the filter.
        String newFilter = "$[?(@.region == 'CN')]";

        alterZone(zoneName, null, null, newFilter);

        // Node C is removed from data nodes instantly.
        assertDataNodesFromManager(distributionZoneManager, metaStorageManager::appliedRevision, catalogManager::latestCatalogVersion,
                zoneId, Set.of(D), ZONE_MODIFICATION_AWAIT_TIMEOUT);

        // Check that scale down task is not scheduled.
        assertTrue(distributionZoneManager.dataNodesManager().zoneTimers(zoneId).scaleDown.taskIsCancelled());
    }

    /**
     * Tests that node, that was added after altering of a filter (meaning, that immediate scale up was triggered), is added to data nodes
     * only after altering and corresponding scale up.
     *
     * @throws Exception If failed.
     */
    @ParameterizedTest(name = "defaultZone={0},consistencyMode={1}")
    @CsvSource({
            "true,",
            "false, HIGH_AVAILABILITY",
            "false, STRONG_CONSISTENCY",
    })
    void testNodeAddedWhileAlteringFilter(boolean defaultZone, ConsistencyMode consistencyMode) throws Exception {
        String zoneName = preparePrerequisites(COMMON_UP_DOWN_AUTOADJUST_TIMER_SECONDS, INFINITE_TIMER_VALUE, defaultZone, consistencyMode);

        int zoneId = getZoneId(zoneName);

        topology.putNode(D);

        // Check that scale up task was scheduled.
        assertTrue(
                waitForCondition(
                        () -> distributionZoneManager.dataNodesManager().zoneTimers(zoneId) != null
                            && distributionZoneManager.dataNodesManager().zoneTimers(zoneId).scaleUp.taskIsScheduled(),
                        ZONE_MODIFICATION_AWAIT_TIMEOUT
                )
        );

        // Nodes C and D and E match the filter.
        String newFilter = "$[?(@.region == 'CN')]";

        alterZone(zoneName, null, null, newFilter);

        LogicalNode e = new LogicalNode(
                new ClusterNodeImpl(randomUUID(), "E", new NetworkAddress("localhost", 123)),
                Map.of("region", "CN", "storage", "HDD", "dataRegionSize", "20"),
                Map.of(),
                List.of(DEFAULT_STORAGE_PROFILE)
        );

        topology.putNode(e);

        // Check that node E, that was added while filter's altering, is not instantly propagated to data nodes.
        assertDataNodesFromManager(distributionZoneManager, metaStorageManager::appliedRevision, catalogManager::latestCatalogVersion,
                zoneId, Set.of(C, D), ZONE_MODIFICATION_AWAIT_TIMEOUT);

        // Assert that scheduled timer was created because of node E addition.
        assertTrue(
                waitForCondition(() -> distributionZoneManager.dataNodesManager().zoneTimers(zoneId).scaleUp.taskIsScheduled(), 2000)
        );

        alterZone(zoneName, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE, newFilter);

        // Check that node E, that was added after filter's altering, was added only after altering immediate scale up.
        assertDataNodesFromManager(distributionZoneManager, metaStorageManager::appliedRevision, catalogManager::latestCatalogVersion,
                zoneId, Set.of(C, D, e), ZONE_MODIFICATION_AWAIT_TIMEOUT);
    }

    /**
     * Starts distribution zone manager with a zone and checks that two out of three nodes, which match filter,
     * are presented in the zones data nodes.
     *
     * @throws Exception If failed
     */
    private String preparePrerequisites(boolean defaultZone, ConsistencyMode consistencyMode) throws Exception {
        return preparePrerequisites(IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE, defaultZone, consistencyMode);
    }

    /**
     * Starts distribution zone manager with a zone and checks that two out of three nodes, which match filter,
     * are presented in the zones data nodes.
     *
     * @throws Exception If failed
     */
    private String preparePrerequisites(
            int scaleUpTimer,
            int scaleDownTimer,
            boolean defaultZone,
            ConsistencyMode consistencyMode
    ) throws Exception {
        startDistributionZoneManager();

        topology.putNode(A);
        topology.putNode(B);
        topology.putNode(C);

        String zoneName;

        if (defaultZone) {
            createDefaultZone(catalogManager);
            zoneName = getDefaultZone().name();
            alterZone(zoneName, scaleUpTimer, scaleDownTimer, FILTER);
        } else {
            zoneName = ZONE_NAME;
            createZone(ZONE_NAME, scaleUpTimer, scaleDownTimer, FILTER, consistencyMode, DEFAULT_STORAGE_PROFILE);
        }

        assertDataNodesFromManager(distributionZoneManager, metaStorageManager::appliedRevision, catalogManager::latestCatalogVersion,
                getZoneId(zoneName), Set.of(A, C), ZONE_MODIFICATION_AWAIT_TIMEOUT);

        return zoneName;
    }
}
