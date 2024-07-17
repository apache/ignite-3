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

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertDataNodesFromManager;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleUpChangeTriggerKey;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.metastorage.server.If;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test scenarios when filter of a zone is altered and immediate scale up is triggered.
 */
public class DistributionZoneManagerAlterFilterTest extends BaseDistributionZoneManagerTest {
    private static final String FILTER = "$[?(@.storage == 'SSD' || @.region == 'US')]";

    private static final LogicalNode A = new LogicalNode(
            new ClusterNodeImpl("1", "A", new NetworkAddress("localhost", 123)),
            Map.of("region", "US", "storage", "SSD", "dataRegionSize", "10"),
            Map.of(),
            List.of(DEFAULT_STORAGE_PROFILE)
    );

    private static final LogicalNode B = new LogicalNode(
            new ClusterNodeImpl("2", "B", new NetworkAddress("localhost", 123)),
            Map.of("region", "EU", "storage", "HHD", "dataRegionSize", "30"),
            Map.of(),
            List.of(DEFAULT_STORAGE_PROFILE)
    );

    private static final LogicalNode C = new LogicalNode(
            new ClusterNodeImpl("3", "C", new NetworkAddress("localhost", 123)),
            Map.of("region", "CN", "storage", "SSD", "dataRegionSize", "20"),
            Map.of(),
            List.of(DEFAULT_STORAGE_PROFILE)
    );

    private static final LogicalNode D = new LogicalNode(
            new ClusterNodeImpl("4", "D", new NetworkAddress("localhost", 123)),
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
    @ParameterizedTest(name = "defaultZone={0}")
    @ValueSource(booleans = {true, false})
    void testAlterFilter(boolean defaultZone) throws Exception {
        String zoneName = preparePrerequisites(defaultZone);

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
    @ParameterizedTest(name = "defaultZone={0}")
    @ValueSource(booleans = {true, false})
    void testAlterFilterToEmtpyNodes(boolean defaultZone) throws Exception {
        String zoneName = preparePrerequisites(defaultZone);

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
    @ParameterizedTest(name = "defaultZone={0}")
    @ValueSource(booleans = {true, false})
    void testAlterFilterDoNotAffectScaleDown(boolean defaultZone) throws Exception {
        String zoneName = preparePrerequisites(IMMEDIATE_TIMER_VALUE, COMMON_UP_DOWN_AUTOADJUST_TIMER_SECONDS, defaultZone);

        topology.putNode(D);

        int zoneId = getZoneId(zoneName);

        if (ZONE_NAME.equals(zoneName)) {
            assertNull(distributionZoneManager.zonesState().get(zoneId).scaleDownTask());
        }

        topology.removeNodes(Set.of(C));

        // Check that scale down task was scheduled.
        assertTrue(
                waitForCondition(
                        () -> distributionZoneManager.zonesState().get(zoneId).scaleDownTask() != null,
                        ZONE_MODIFICATION_AWAIT_TIMEOUT
                )
        );

        // Nodes C and D match the filter.
        String newFilter = "$[?(@.region == 'CN')]";

        alterZone(zoneName, null, null, newFilter);

        // Node C is still in data nodes because altering a filter triggers only immediate scale up.
        assertDataNodesFromManager(distributionZoneManager, metaStorageManager::appliedRevision, catalogManager::latestCatalogVersion,
                zoneId, Set.of(C, D), ZONE_MODIFICATION_AWAIT_TIMEOUT);

        // Check that scale down task is still scheduled.
        assertNotNull(distributionZoneManager.zonesState().get(zoneId).scaleUpTask());

        // Alter zone so we could check that node C is removed from data nodes.
        alterZone(zoneName, INFINITE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE, newFilter);

        assertDataNodesFromManager(distributionZoneManager, metaStorageManager::appliedRevision, catalogManager::latestCatalogVersion,
                zoneId, Set.of(D), ZONE_MODIFICATION_AWAIT_TIMEOUT);
    }

    /**
     * Tests that node, that was added after altering of a filter (meaning, that immediate scale up was triggered), is added to data nodes
     * only after altering and corresponding scale up.
     *
     * @throws Exception If failed.
     */
    @ParameterizedTest(name = "defaultZone={0}")
    @ValueSource(booleans = {true, false})
    void testNodeAddedWhileAlteringFilter(boolean defaultZone) throws Exception {
        String zoneName = preparePrerequisites(COMMON_UP_DOWN_AUTOADJUST_TIMER_SECONDS, INFINITE_TIMER_VALUE, defaultZone);

        int zoneId = getZoneId(zoneName);

        if (ZONE_NAME.equals(zoneName)) {
            assertNull(distributionZoneManager.zonesState().get(zoneId).scaleUpTask());
        }

        topology.putNode(D);

        // Check that scale up task was scheduled.
        assertTrue(
                waitForCondition(
                        () -> distributionZoneManager.zonesState().get(zoneId).scaleUpTask() != null,
                        ZONE_MODIFICATION_AWAIT_TIMEOUT
                )
        );

        // Nodes C and D and E match the filter.
        String newFilter = "$[?(@.region == 'CN')]";

        alterZone(zoneName, null, null, newFilter);

        LogicalNode e = new LogicalNode(
                new ClusterNodeImpl("5", "E", new NetworkAddress("localhost", 123)),
                Map.of("region", "CN", "storage", "HDD", "dataRegionSize", "20"),
                Map.of(),
                List.of(DEFAULT_STORAGE_PROFILE)
        );

        doAnswer(invocation -> {
            If iif = invocation.getArgument(0);

            // Emulate a situation when immediate timer was run after filter altering and new node was added, so timer was scheduled.
            byte[] key = zoneScaleUpChangeTriggerKey(zoneId).bytes();

            if (Arrays.stream(iif.cond().keys()).anyMatch(k -> Arrays.equals(key, k))) {
                assertNotNull(distributionZoneManager.zonesState().get(zoneId).scaleUpTask());

                topology.putNode(e);
            }
            return invocation.callRealMethod();
        }).when(keyValueStorage).invoke(any(), any(), any());

        // Check that node E, that was added while filter's altering, is not propagated to data nodes.
        assertDataNodesFromManager(distributionZoneManager, metaStorageManager::appliedRevision, catalogManager::latestCatalogVersion,
                zoneId, Set.of(C, D), ZONE_MODIFICATION_AWAIT_TIMEOUT);

        // Assert that scheduled timer was not canceled because of immediate scale up after filter altering.
        assertNotNull(distributionZoneManager.zonesState().get(zoneId).scaleUpTask());

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
    private String preparePrerequisites(boolean defaultZone) throws Exception {
        return preparePrerequisites(IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE, defaultZone);
    }

    /**
     * Starts distribution zone manager with a zone and checks that two out of three nodes, which match filter,
     * are presented in the zones data nodes.
     *
     * @throws Exception If failed
     */
    private String preparePrerequisites(int scaleUpTimer, int scaleDownTimer, boolean defaultZone) throws Exception {
        startDistributionZoneManager();

        topology.putNode(A);
        topology.putNode(B);
        topology.putNode(C);

        String zoneName;

        if (defaultZone) {
            zoneName = getDefaultZone().name();
            alterZone(zoneName, scaleUpTimer, scaleDownTimer, FILTER);
        } else {
            zoneName = ZONE_NAME;
            createZone(ZONE_NAME, scaleUpTimer, scaleDownTimer, FILTER, DEFAULT_STORAGE_PROFILE);
        }

        assertDataNodesFromManager(distributionZoneManager, metaStorageManager::appliedRevision, catalogManager::latestCatalogVersion,
                getZoneId(zoneName), Set.of(A, C), ZONE_MODIFICATION_AWAIT_TIMEOUT);

        return zoneName;
    }
}
