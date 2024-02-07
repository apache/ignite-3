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

import static org.apache.ignite.internal.catalog.commands.CatalogUtils.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertDataNodesFromManager;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.Test;

/**
 * Tests distribution zone manager interactions with storage profiles filtering.
 */
public class DistributionZoneManagerStorageProfilesFilterTest extends BaseDistributionZoneManagerTest {
    private static final LogicalNode A = new LogicalNode(
            new ClusterNodeImpl("1", "A", new NetworkAddress("localhost", 123)),
            Map.of(),
            Map.of(),
            List.of("clock_rocks", "segmented_aipersist")
    );

    private static final LogicalNode B = new LogicalNode(
            new ClusterNodeImpl("2", "B", new NetworkAddress("localhost", 123)),
            Map.of(),
            Map.of(),
            List.of("lru_rocks", "clock_rocks", "segmented_aipersist")
    );

    private static final LogicalNode C = new LogicalNode(
            new ClusterNodeImpl("3", "C", new NetworkAddress("localhost", 123)),
            Map.of(),
            Map.of(),
            List.of("lru_rocks")
    );

    private static final LogicalNode D = new LogicalNode(
            new ClusterNodeImpl("4", "D", new NetworkAddress("localhost", 123)),
            Map.of(),
            Map.of(),
            List.of("clock_rocks", "segmented_aipersist")
    );

    @Test
    void testFilterOnScaleUp() throws Exception {
        preparePrerequisites();

        topology.putNode(D);

        assertDataNodesFromManager(distributionZoneManager, metaStorageManager::appliedRevision, catalogManager::latestCatalogVersion,
                getZoneId(ZONE_NAME), Set.of(A, B, D), ZONE_MODIFICATION_AWAIT_TIMEOUT);
    }

    @Test
    void testFilterOnScaleDown() throws Exception {
        preparePrerequisites();

        topology.removeNodes(Set.of(A));

        assertDataNodesFromManager(distributionZoneManager, metaStorageManager::appliedRevision, catalogManager::latestCatalogVersion,
                getZoneId(ZONE_NAME), Set.of(B), ZONE_MODIFICATION_AWAIT_TIMEOUT);
    }

    @Test
    void testFilterOnScaleUpWithNewAttributesAfterRestart() throws Exception {
        preparePrerequisites();

        topology.removeNodes(Set.of(C));

        LogicalNode newC = new LogicalNode(
                new ClusterNodeImpl("3", "newC", new NetworkAddress("localhost", 123)),
                Map.of(),
                Map.of(),
                List.of("clock_rocks", "segmented_aipersist")
        );

        topology.putNode(newC);

        assertDataNodesFromManager(distributionZoneManager, metaStorageManager::appliedRevision, catalogManager::latestCatalogVersion,
                getZoneId(ZONE_NAME), Set.of(A, B, newC), ZONE_MODIFICATION_AWAIT_TIMEOUT);
    }

    /**
     * Starts distribution zone manager with a zone and checks that one out of three nodes, which match filter and storage profile,
     * is presented in the zones data nodes.
     *
     * @throws Exception If failed
     */
    private void preparePrerequisites() throws Exception {
        startDistributionZoneManager();

        topology.putNode(A);
        topology.putNode(B);
        topology.putNode(C);

        createZone(ZONE_NAME, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE, null, "clock_rocks,segmented_aipersist");

        assertDataNodesFromManager(distributionZoneManager, metaStorageManager::appliedRevision, catalogManager::latestCatalogVersion,
                getZoneId(ZONE_NAME), Set.of(A, B), ZONE_MODIFICATION_AWAIT_TIMEOUT);
    }
}
