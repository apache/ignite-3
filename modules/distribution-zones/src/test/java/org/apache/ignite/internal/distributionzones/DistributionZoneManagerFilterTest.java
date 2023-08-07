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

import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertDataNodesFromManager;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.network.ClusterNodeImpl;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.Test;

/**
 * Tests distribution zone manager interactions with data nodes filtering.
 */
public class DistributionZoneManagerFilterTest extends BaseDistributionZoneManagerTest {
    private static final String ZONE_NAME = "zone1";

    private static final int ZONE_ID = 1;

    private static final long TIMEOUT_MILLIS = 10_000L;

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
            Map.of("region", "CN", "storage", "SSD", "dataRegionSize", "20")
    );

    @Test
    void testFilterOnScaleUp() throws Exception {
        preparePrerequisites();

        topology.putNode(D);

        assertDataNodesFromManager(distributionZoneManager, () -> metaStorageManager.appliedRevision(), ZONE_ID,
                Set.of(A, C, D), TIMEOUT_MILLIS);
    }

    @Test
    void testFilterOnScaleDown() throws Exception {
        preparePrerequisites();

        topology.removeNodes(Set.of(C));

        assertDataNodesFromManager(distributionZoneManager, () -> metaStorageManager.appliedRevision(), ZONE_ID, Set.of(A), TIMEOUT_MILLIS);
    }

    @Test
    void testFilterOnScaleUpWithNewAttributesAfterRestart() throws Exception {
        preparePrerequisites();

        topology.removeNodes(Set.of(B));

        LogicalNode newB = new LogicalNode(
                new ClusterNodeImpl("2", "newB", new NetworkAddress("localhost", 123)),
                Map.of("region", "US", "storage", "HHD", "dataRegionSize", "30")
        );

        topology.putNode(newB);

        assertDataNodesFromManager(distributionZoneManager, () -> metaStorageManager.appliedRevision(), ZONE_ID,
                Set.of(A, newB, C), TIMEOUT_MILLIS);
    }

    /**
     * Starts distribution zone manager with a zone and checks that two out of three nodes, which match filter,
     * are presented in the zones data nodes.
     *
     * @throws Exception If failed
     */
    private void preparePrerequisites() throws Exception {
        String filter = "$[?(@.storage == 'SSD' || @.region == 'US')]";

        startDistributionZoneManager();

        topology.putNode(A);
        topology.putNode(B);
        topology.putNode(C);

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME)
                        .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                        .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                        .filter(filter)
                        .build()
        ).get(10_000, TimeUnit.MILLISECONDS);

        assertDataNodesFromManager(distributionZoneManager, () -> metaStorageManager.appliedRevision(), ZONE_ID,
                Set.of(A, C), TIMEOUT_MILLIS);
    }
}
