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
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertDataNodesForZone;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.mockVaultZonesLogicalTopologyKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.setLogicalTopologyInMetaStorage;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleUpChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesChangeTriggerKey;
import static org.apache.ignite.internal.util.ByteUtils.longToBytes;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Tests distribution zones logical topology changes and reaction to that changes.
 */
//TODO: IGNITE-18564 Add tests with not default distribution zones, when distributionZones.change.trigger per zone will be created.
public class DistributionZoneManagerWatchListenerTest extends BaseDistributionZoneManagerTest {
    private static final LogicalNode NODE_1 = new LogicalNode("node1", "node1", new NetworkAddress("localhost", 123));
    private static final LogicalNode NODE_2 = new LogicalNode("node2", "node2", new NetworkAddress("localhost", 123));

    @Test
    @Disabled("IGNITE-18564")
    void testStaleWatchEvent() throws Exception {
        mockVaultZonesLogicalTopologyKey(Set.of(NODE_1), vaultMgr, metaStorageManager.appliedRevision());

        startDistributionZoneManager();

        distributionZoneManager.alterZone(
                DEFAULT_ZONE_NAME,
                new DistributionZoneConfigurationParameters.Builder(DEFAULT_ZONE_NAME)
                        .dataNodesAutoAdjustScaleUp(0)
                        .dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE)
                        .build()
        ).get();

        long revision = 100;

        keyValueStorage.putAll(
                List.of(zoneScaleUpChangeTriggerKey(DEFAULT_ZONE_ID).bytes(), zoneDataNodesKey(DEFAULT_ZONE_ID).bytes()),
                List.of(longToBytes(revision), keyValueStorage.get(zoneDataNodesKey(DEFAULT_ZONE_ID).bytes()).value()),
                HybridTimestamp.MIN_VALUE
        );

        Set<LogicalNode> nodes = Set.of(NODE_1, NODE_2);

        setLogicalTopologyInMetaStorage(nodes, 100, metaStorageManager);

        // TODO: IGNITE-18564 This is incorrect to check that data nodes are the same right after logical topology is changes manually.
        assertDataNodesForZone(DEFAULT_ZONE_ID, Set.of(NODE_1), keyValueStorage);
    }

    @Test
    void testStaleVaultRevisionOnZoneManagerStart() throws Exception {
        long revision = 100;

        keyValueStorage.put(zonesChangeTriggerKey(DEFAULT_ZONE_ID).bytes(), longToBytes(revision), HybridTimestamp.MIN_VALUE);

        Set<LogicalNode> nodes = Set.of(
                new LogicalNode(new ClusterNode("node1", "node1", NetworkAddress.from("127.0.0.1:127")), Collections.emptyMap()),
                new LogicalNode(new ClusterNode("node2", "node2", NetworkAddress.from("127.0.0.1:127")), Collections.emptyMap())
        );

        mockVaultZonesLogicalTopologyKey(nodes, vaultMgr, metaStorageManager.appliedRevision());

        startDistributionZoneManager();

        assertDataNodesForZone(DEFAULT_ZONE_ID, null, keyValueStorage);
    }

    @Test
    void testDataNodesUpdatedOnZoneManagerStart() throws Exception {
        Set<LogicalNode> nodes = Set.of(
                new LogicalNode(new ClusterNode("node1", "node1", NetworkAddress.from("127.0.0.1:127")), Collections.emptyMap()),
                new LogicalNode(new ClusterNode("node2", "node2", NetworkAddress.from("127.0.0.1:127")), Collections.emptyMap())
        );

        mockVaultZonesLogicalTopologyKey(nodes, vaultMgr, metaStorageManager.appliedRevision());

        startDistributionZoneManager();

        assertDataNodesForZone(DEFAULT_ZONE_ID, nodes, keyValueStorage);
    }
}
