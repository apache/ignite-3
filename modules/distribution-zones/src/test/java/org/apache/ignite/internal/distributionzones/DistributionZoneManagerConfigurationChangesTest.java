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
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertDataNodesFromLogicalNodesInStorage;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertLogicalTopology;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.logicalNodeFromNode;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.PARTITION_DISTRIBUTION_RESET_TIMEOUT;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.REBALANCE_RETRY_DELAY_MS;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleDownTimerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleUpTimerKey;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import org.apache.ignite.internal.catalog.descriptors.ConsistencyMode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.configuration.utils.SystemConfigurationPropertyCompatibilityChecker;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** Tests distribution zones configuration changes and reaction to that changes. */
public class DistributionZoneManagerConfigurationChangesTest extends BaseDistributionZoneManagerTest {
    private static final String ZONE_NAME = "zone1";

    private static final String NEW_ZONE_NAME = "zone2";

    private static final LogicalNode NODE_1 = logicalNodeFromNode(new Node("node1", randomUUID()));

    private static final Set<LogicalNode> nodes = Set.of(NODE_1);

    @BeforeEach
    @Override
    public void setUp() throws Exception {
        super.setUp();

        startDistributionZoneManager();

        topology.putNode(NODE_1);

        assertLogicalTopology(nodes, keyValueStorage);
    }

    @ParameterizedTest
    @EnumSource(ConsistencyMode.class)
    void testDataNodesPropagationAfterZoneCreation(ConsistencyMode consistencyMode) throws Exception {
        createZone(ZONE_NAME, consistencyMode);

        int zoneId = getZoneId(ZONE_NAME);

        assertZonesKeysInMetaStorage(zoneId, nodes);
    }

    @ParameterizedTest
    @EnumSource(ConsistencyMode.class)
    void testSeveralZoneCreationsUpdatesTriggerKey(ConsistencyMode consistencyMode) throws Exception {
        createZone(ZONE_NAME, consistencyMode);

        createZone(NEW_ZONE_NAME, consistencyMode);

        int zoneId = getZoneId(ZONE_NAME);
        int zoneId2 = getZoneId(NEW_ZONE_NAME);

        assertZonesKeysInMetaStorage(zoneId, nodes);
        assertZonesKeysInMetaStorage(zoneId2, nodes);
    }

    @Test
    void testCompatibilityPropertyNamePartitionDistributionResetTimeoutWasNotChanged() {
        SystemConfigurationPropertyCompatibilityChecker.checkSystemConfigurationPropertyNameWasNotChanged(
                "PARTITION_DISTRIBUTION_RESET_TIMEOUT",
                "partitionDistributionResetTimeout",
                PARTITION_DISTRIBUTION_RESET_TIMEOUT
        );
    }

    @Test
    void testCompatibilityPropertyNameRebalanceRetryDelayMsNameWasNotChanged() {
        SystemConfigurationPropertyCompatibilityChecker.checkSystemConfigurationPropertyNameWasNotChanged(
                "REBALANCE_RETRY_DELAY_MS",
                "rebalanceRetryDelay",
                REBALANCE_RETRY_DELAY_MS
        );
    }

    private void assertZonesKeysInMetaStorage(int zoneId, @Nullable Set<LogicalNode> clusterNodes) throws InterruptedException {
        assertZonesKeysInMetaStorage(zoneId, clusterNodes, true);
    }

    private void assertZonesKeysInMetaStorage(int zoneId, @Nullable Set<LogicalNode> clusterNodes, boolean checkNodes)
            throws InterruptedException {
        if (checkNodes) {
            assertDataNodesFromLogicalNodesInStorage(zoneId, clusterNodes, keyValueStorage);
        }

        if (clusterNodes != null) {
            assertTrue(waitForCondition(() -> keyValueStorage.get(zoneScaleUpTimerKey(zoneId).bytes()).value() != null, 5000));
            assertTrue(waitForCondition(() -> keyValueStorage.get(zoneScaleDownTimerKey(zoneId).bytes()).value() != null, 5000));
        } else {
            assertTrue(waitForCondition(() -> keyValueStorage.get(zoneScaleUpTimerKey(zoneId).bytes()).value() == null, 5000));
            assertTrue(waitForCondition(() -> keyValueStorage.get(zoneScaleDownTimerKey(zoneId).bytes()).value() == null, 5000));
        }

        assertArrayEquals(
                keyValueStorage.get(zoneScaleUpTimerKey(zoneId).bytes()).value(),
                keyValueStorage.get(zoneScaleDownTimerKey(zoneId).bytes()).value()
        );
    }

    private void createZone(String zoneName, ConsistencyMode consistencyMode) {
        createZone(zoneName, INFINITE_TIMER_VALUE, INFINITE_TIMER_VALUE, null, consistencyMode, DEFAULT_STORAGE_PROFILE);
    }
}
