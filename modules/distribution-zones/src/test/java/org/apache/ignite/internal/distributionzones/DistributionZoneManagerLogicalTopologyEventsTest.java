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

import static org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl.LOGICAL_TOPOLOGY_KEY;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertLogicalTopology;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertLogicalTopologyVersion;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyVersionKey;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Set;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.Test;

/**
 * Tests reactions to topology changes in accordance with distribution zones logic.
 */
public class DistributionZoneManagerLogicalTopologyEventsTest extends BaseDistributionZoneManagerTest {
    private static final LogicalNode NODE_1 = new LogicalNode("1", "name1", new NetworkAddress("localhost", 123));

    private static final LogicalNode NODE_2 = new LogicalNode("2", "name2", new NetworkAddress("localhost", 123));

    @Test
    void testMetaStorageKeysInitializedOnStartWhenTopVerEmpty() throws Exception {
        distributionZoneManager.startAsync();

        topology.putNode(NODE_1);

        assertLogicalTopologyVersion(1L, keyValueStorage);

        assertLogicalTopology(Set.of(NODE_1), keyValueStorage);
    }

    @Test
    void testMetaStorageKeysInitializedOnStartWhenTopVerEqualsToCmgTopVer() throws Exception {
        keyValueStorage.put(zonesLogicalTopologyVersionKey().bytes(), ByteUtils.longToBytes(2L), HybridTimestamp.MIN_VALUE);

        distributionZoneManager.startAsync();

        assertLogicalTopologyVersion(2L, keyValueStorage);

        assertLogicalTopology(null, keyValueStorage);
    }

    @Test
    void testMetaStorageKeysInitializedOnStartWhenTopVerGreaterThanCmgTopVer() throws Exception {
        keyValueStorage.put(zonesLogicalTopologyVersionKey().bytes(), ByteUtils.longToBytes(3L), HybridTimestamp.MIN_VALUE);

        distributionZoneManager.startAsync();

        assertLogicalTopologyVersion(3L, keyValueStorage);

        assertLogicalTopology(null, keyValueStorage);
    }

    @Test
    void testNodeAddingUpdatesLogicalTopologyInMetaStorage() throws Exception {
        distributionZoneManager.startAsync();

        topology.putNode(NODE_1);

        topology.putNode(NODE_2);

        var clusterNodes2 = Set.of(NODE_1, NODE_2);

        assertLogicalTopology(clusterNodes2, keyValueStorage);

        assertLogicalTopologyVersion(2L, keyValueStorage);
    }

    @Test
    void testNodeStaleAddingDoNotUpdatesLogicalTopologyInMetaStorage() throws Exception {
        distributionZoneManager.startAsync();

        topology.putNode(NODE_1);

        Set<LogicalNode> clusterNodes = Set.of(NODE_1);

        // Wait for Zone Manager to initialize Meta Storage on start.
        assertLogicalTopologyVersion(1L, keyValueStorage);

        keyValueStorage.put(zonesLogicalTopologyVersionKey().bytes(), ByteUtils.longToBytes(4L), HybridTimestamp.MIN_VALUE);

        topology.putNode(NODE_2);

        assertEquals(2L, topology.getLogicalTopology().version());

        assertLogicalTopology(clusterNodes, keyValueStorage);

        assertLogicalTopologyVersion(4L, keyValueStorage);
    }

    @Test
    void testNodeRemovingUpdatesLogicalTopologyInMetaStorage() throws Exception {
        distributionZoneManager.startAsync();

        topology.putNode(NODE_1);

        topology.putNode(NODE_2);

        Set<LogicalNode> clusterNodes = Set.of(NODE_1, NODE_2);

        assertLogicalTopology(clusterNodes, keyValueStorage);

        topology.removeNodes(Set.of(NODE_2));

        var clusterNodes2 = Set.of(NODE_1);

        assertLogicalTopology(clusterNodes2, keyValueStorage);

        assertLogicalTopologyVersion(3L, keyValueStorage);
    }

    @Test
    void testNodeStaleRemovingDoNotUpdatesLogicalTopologyInMetaStorage() throws Exception {
        distributionZoneManager.startAsync();

        topology.putNode(NODE_1);

        topology.putNode(NODE_2);

        assertEquals(2L, topology.getLogicalTopology().version());

        Set<LogicalNode> clusterNodes = Set.of(NODE_1, NODE_2);

        // Wait for Zone Manager to initialize Meta Storage on start.
        assertLogicalTopologyVersion(2L, keyValueStorage);

        keyValueStorage.put(zonesLogicalTopologyVersionKey().bytes(), ByteUtils.longToBytes(4L), HybridTimestamp.MIN_VALUE);

        topology.removeNodes(Set.of(NODE_2));

        assertLogicalTopology(clusterNodes, keyValueStorage);

        assertLogicalTopologyVersion(4L, keyValueStorage);
    }

    @Test
    void testTopologyLeapUpdatesLogicalTopologyInMetaStorage() throws Exception {
        distributionZoneManager.startAsync();

        topology.putNode(NODE_1);

        Set<LogicalNode> clusterNodes = Set.of(NODE_1);

        assertLogicalTopology(clusterNodes, keyValueStorage);

        var clusterNodes2 = Set.of(NODE_1, NODE_2);

        clusterStateStorage.put(LOGICAL_TOPOLOGY_KEY, ByteUtils.toBytes(new LogicalTopologySnapshot(10L, clusterNodes2)));

        topology.fireTopologyLeap();

        assertLogicalTopology(clusterNodes2, keyValueStorage);

        assertLogicalTopologyVersion(10L, keyValueStorage);
    }

    @Test
    void testStaleTopologyLeapDoNotUpdatesLogicalTopologyInMetaStorage() throws Exception {
        distributionZoneManager.startAsync();

        topology.putNode(NODE_1);

        Set<LogicalNode> clusterNodes = Set.of(NODE_1);

        assertLogicalTopology(clusterNodes, keyValueStorage);

        var clusterNodes2 = Set.of(NODE_1, NODE_2);

        clusterStateStorage.put(LOGICAL_TOPOLOGY_KEY, ByteUtils.toBytes(new LogicalTopologySnapshot(10L, clusterNodes2)));

        keyValueStorage.put(zonesLogicalTopologyVersionKey().bytes(), ByteUtils.longToBytes(11L), HybridTimestamp.MIN_VALUE);

        topology.fireTopologyLeap();

        assertLogicalTopology(clusterNodes, keyValueStorage);

        assertLogicalTopologyVersion(11L, keyValueStorage);
    }
}
