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

import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_ZONE_ID;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_ZONE_NAME;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertDataNodesFromManager;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleUpChangeTriggerKey;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.metastorage.server.If;
import org.apache.ignite.network.ClusterNodeImpl;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test scenarios when filter of a zone is altered and immediate scale up is triggered.
 */
public class DistributionZoneManagerAlterFilterTest extends BaseDistributionZoneManagerTest {
    private static final String FILTER = "$[?(@.storage == 'SSD' || @.region == 'US')]";

    private static final LogicalNode A = new LogicalNode(
            new ClusterNodeImpl("1", "A", new NetworkAddress("localhost", 123)),
            Map.of("region", "US", "storage", "SSD", "dataRegionSize", "10")
    );

    private static final LogicalNode B = new LogicalNode(
            new ClusterNodeImpl("2", "B", new NetworkAddress("localhost", 123)),
            Map.of("region", "EU", "storage", "HHD", "dataRegionSize", "30")
    );

    private static final LogicalNode C = new LogicalNode(
            new ClusterNodeImpl("3", "C", new NetworkAddress("localhost", 123)),
            Map.of("region", "CN", "storage", "SSD", "dataRegionSize", "20")
    );

    private static final LogicalNode D = new LogicalNode(
            new ClusterNodeImpl("4", "D", new NetworkAddress("localhost", 123)),
            Map.of("region", "CN", "storage", "HDD", "dataRegionSize", "20")
    );

    /**
     * Tests that node that was added before altering filter is taken into account after altering of a filter and corresponding
     * immediate scale up.
     *
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @MethodSource("provideArgumentsForFilterAlteringTests")
    void testAlterFilter(int zoneId, String zoneName) throws Exception {
        preparePrerequisites(zoneId);

        // Change timers to infinite, add new node, alter filter and check that data nodes was changed.
        alterZone(zoneName, INFINITE_TIMER_VALUE, INFINITE_TIMER_VALUE, FILTER);

        topology.putNode(D);

        // Nodes C and D match the filter.
        String newFilter = "$[?(@.region == 'CN')]";

        alterZone(zoneName, null, null, newFilter);

        assertDataNodesFromManager(distributionZoneManager, () -> metaStorageManager.appliedRevision(), zoneId, Set.of(C, D),
                TIMEOUT_MILLIS);
    }

    /**
     * Tests that empty data nodes are propagated after altering of a filter and corresponding immediate scale up.
     *
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @MethodSource("provideArgumentsForFilterAlteringTests")
    void testAlterFilterToEmtpyNodes(int zoneId, String zoneName) throws Exception {
        preparePrerequisites(zoneId);

        // Change timers to infinite, add new node, alter filter and check that data nodes was changed.
        alterZone(zoneName, INFINITE_TIMER_VALUE, INFINITE_TIMER_VALUE, FILTER);

        topology.putNode(D);

        // No nodes are matching the filter
        String newFilter = "$[?(@.region == 'JP')]";

        alterZone(zoneName, null, null, newFilter);

        assertDataNodesFromManager(distributionZoneManager, () -> metaStorageManager.appliedRevision(), zoneId, Set.of(), TIMEOUT_MILLIS);
    }

    /**
     * Tests that altering of a filter affects only scale up timers and only added nodes.
     *
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @MethodSource("provideArgumentsForFilterAlteringTests")
    void testAlterFilterDoNotAffectScaleDown(int zoneId, String zoneName) throws Exception {
        preparePrerequisites(IMMEDIATE_TIMER_VALUE, TIMER_SECONDS, zoneId);

        topology.putNode(D);

        if (zoneId == ZONE_ID) {
            assertNull(distributionZoneManager.zonesState().get(ZONE_ID).scaleDownTask());
        }

        topology.removeNodes(Set.of(C));

        // Check that scale down task was scheduled.
        assertTrue(
                waitForCondition(
                        () -> distributionZoneManager.zonesState().get(zoneId).scaleDownTask() != null,
                        TIMEOUT_MILLIS
                )
        );

        // Nodes C and D match the filter.
        String newFilter = "$[?(@.region == 'CN')]";

        alterZone(zoneName, null, null, newFilter);

        // Node C is still in data nodes because altering a filter triggers only immediate scale up.
        assertDataNodesFromManager(distributionZoneManager, () -> metaStorageManager.appliedRevision(), zoneId, Set.of(C, D),
                TIMEOUT_MILLIS);

        // Check that scale down task is still scheduled.
        assertNotNull(distributionZoneManager.zonesState().get(zoneId).scaleUpTask());

        // Alter zone so we could check that node C is removed from data nodes.
        alterZone(zoneName, INFINITE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE, newFilter);

        assertDataNodesFromManager(distributionZoneManager, () -> metaStorageManager.appliedRevision(), zoneId, Set.of(D), TIMEOUT_MILLIS);
    }

    /**
     * Tests that node, that was added after altering of a filter (meaning, that immediate scale up was triggered), is added to data nodes
     * only after altering and corresponding scale up.
     *
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @MethodSource("provideArgumentsForFilterAlteringTests")
    void testNodeAddedWhileAlteringFilter(int zoneId, String zoneName) throws Exception {
        preparePrerequisites(TIMER_SECONDS, INFINITE_TIMER_VALUE, zoneId);

        if (zoneId == ZONE_ID) {
            assertNull(distributionZoneManager.zonesState().get(ZONE_ID).scaleUpTask());
        }

        topology.putNode(D);

        // Check that scale up task was scheduled.
        assertTrue(
                waitForCondition(
                        () -> distributionZoneManager.zonesState().get(zoneId).scaleUpTask() != null,
                        TIMEOUT_MILLIS
                )
        );

        // Nodes C and D and E match the filter.
        String newFilter = "$[?(@.region == 'CN')]";

        alterZone(zoneName, null, null, newFilter);

        LogicalNode e = new LogicalNode(
                new ClusterNodeImpl("5", "E", new NetworkAddress("localhost", 123)),
                Map.of("region", "CN", "storage", "HDD", "dataRegionSize", "20")
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
        }).when(keyValueStorage).invoke(any(), any());

        // Check that node E, that was added while filter's altering, is not propagated to data nodes.
        assertDataNodesFromManager(distributionZoneManager, () -> metaStorageManager.appliedRevision(), zoneId, Set.of(C, D),
                TIMEOUT_MILLIS);

        // Assert that scheduled timer was not canceled because of immediate scale up after filter altering.
        assertNotNull(distributionZoneManager.zonesState().get(zoneId).scaleUpTask());

        alterZone(zoneName, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE, newFilter);

        // Check that node E, that was added after filter's altering, was added only after altering immediate scale up.
        assertDataNodesFromManager(distributionZoneManager, () -> metaStorageManager.appliedRevision(), zoneId, Set.of(C, D, e),
                TIMEOUT_MILLIS);
    }

    /**
     * Starts distribution zone manager with a zone and checks that two out of three nodes, which match filter,
     * are presented in the zones data nodes.
     *
     * @throws Exception If failed
     */
    private void preparePrerequisites(int zoneId) throws Exception {
        preparePrerequisites(IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE, zoneId);
    }

    /**
     * Starts distribution zone manager with a zone and checks that two out of three nodes, which match filter,
     * are presented in the zones data nodes.
     *
     * @throws Exception If failed
     */
    private void preparePrerequisites(int scaleUpTimer, int scaleDownTimer, int zoneId) throws Exception {
        startDistributionZoneManager();

        topology.putNode(A);
        topology.putNode(B);
        topology.putNode(C);

        if (zoneId == DEFAULT_ZONE_ID) {
            alterZone(DEFAULT_ZONE_NAME, scaleUpTimer, scaleDownTimer, FILTER);
        } else {
            createZone(ZONE_NAME, scaleUpTimer, scaleDownTimer, FILTER);
        }

        assertDataNodesFromManager(distributionZoneManager, () -> metaStorageManager.appliedRevision(), zoneId, Set.of(A, C),
                TIMEOUT_MILLIS);
    }

    private static Stream<Arguments> provideArgumentsForFilterAlteringTests() {
        List<Arguments> args = new ArrayList<>();

        args.add(Arguments.of(DEFAULT_ZONE_ID, DEFAULT_ZONE_NAME));
        args.add(Arguments.of(ZONE_ID, ZONE_NAME));

        return args.stream();
    }
}
