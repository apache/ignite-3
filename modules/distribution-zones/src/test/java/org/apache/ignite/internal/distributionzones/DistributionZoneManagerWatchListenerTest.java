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

import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_ZONE_NAME;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.IMMEDIATE_TIMER_VALUE;
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
import org.apache.ignite.network.ClusterNodeImpl;
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

        alterZone(DEFAULT_ZONE_NAME, IMMEDIATE_TIMER_VALUE, INFINITE_TIMER_VALUE, null);

        long revision = 100;

        int defaultZoneId = getZoneId(DEFAULT_ZONE_NAME);

        keyValueStorage.putAll(
                List.of(zoneScaleUpChangeTriggerKey(defaultZoneId).bytes(), zoneDataNodesKey(defaultZoneId).bytes()),
                List.of(longToBytes(revision), keyValueStorage.get(zoneDataNodesKey(defaultZoneId).bytes()).value()),
                HybridTimestamp.MIN_VALUE
        );

        Set<LogicalNode> nodes = Set.of(NODE_1, NODE_2);

        setLogicalTopologyInMetaStorage(nodes, 100, metaStorageManager);

        // TODO: IGNITE-18564 This is incorrect to check that data nodes are the same right after logical topology is changes manually.
        assertDataNodesForZone(defaultZoneId, Set.of(NODE_1), keyValueStorage);
    }

    @Test
    void testStaleVaultRevisionOnZoneManagerStart() throws Exception {
        long revision = 100;

        int defaultZoneId = getZoneId(DEFAULT_ZONE_NAME);

        keyValueStorage.put(zonesChangeTriggerKey(defaultZoneId).bytes(), longToBytes(revision), HybridTimestamp.MIN_VALUE);

        Set<LogicalNode> nodes = Set.of(
                new LogicalNode(new ClusterNodeImpl("node1", "node1", NetworkAddress.from("127.0.0.1:127")), Collections.emptyMap()),
                new LogicalNode(new ClusterNodeImpl("node2", "node2", NetworkAddress.from("127.0.0.1:127")), Collections.emptyMap())
        );

        mockVaultZonesLogicalTopologyKey(nodes, vaultMgr, metaStorageManager.appliedRevision());

        startDistributionZoneManager();

        assertDataNodesForZone(defaultZoneId, null, keyValueStorage);
    }

    @Test
    void testDataNodesUpdatedOnZoneManagerStart() throws Exception {
        Set<LogicalNode> nodes = Set.of(
                new LogicalNode(new ClusterNodeImpl("node1", "node1", NetworkAddress.from("127.0.0.1:127")), Collections.emptyMap()),
                new LogicalNode(new ClusterNodeImpl("node2", "node2", NetworkAddress.from("127.0.0.1:127")), Collections.emptyMap())
        );

        mockVaultZonesLogicalTopologyKey(nodes, vaultMgr, metaStorageManager.appliedRevision());

        startDistributionZoneManager();

        assertDataNodesForZone(getZoneId(DEFAULT_ZONE_NAME), nodes, keyValueStorage);
    }
}
