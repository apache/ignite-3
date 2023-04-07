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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl.LOGICAL_TOPOLOGY_KEY;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyVersionKey;
import static org.apache.ignite.internal.distributionzones.util.DistributionZonesTestUtil.assertLogicalTopology;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.util.ByteUtils.bytesToLong;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Set;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
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
        Set<LogicalNode> clusterNodes = Set.of(NODE_1);

        mockCmgLocalNodes(1L, clusterNodes);

        distributionZoneManager.start();

        verify(keyValueStorage, timeout(1000).times(1)).invoke(any());

        assertLogicalTopVer(1L);

        assertLogicalTopology(clusterNodes, keyValueStorage);
    }

    @Test
    void testMetaStorageKeysInitializedOnStartWhenTopVerIsLessThanCmgTopVer() throws Exception {
        Set<LogicalNode> clusterNodes = Set.of(NODE_1);

        mockCmgLocalNodes(2L, clusterNodes);

        keyValueStorage.put(zonesLogicalTopologyVersionKey().bytes(), ByteUtils.longToBytes(1L));

        distributionZoneManager.start();

        verify(keyValueStorage, timeout(1000).times(1)).invoke(any());

        assertLogicalTopVer(2L);

        assertLogicalTopology(clusterNodes, keyValueStorage);
    }

    @Test
    void testMetaStorageKeysInitializedOnStartWhenTopVerEqualsToCmgTopVer() throws Exception {
        Set<LogicalNode> clusterNodes = Set.of(NODE_1);

        mockCmgLocalNodes(2L, clusterNodes);

        keyValueStorage.put(zonesLogicalTopologyVersionKey().bytes(), ByteUtils.longToBytes(2L));

        distributionZoneManager.start();

        verify(keyValueStorage, after(500).never()).invoke(any());

        assertLogicalTopVer(2L);

        assertLogicalTopology(null, keyValueStorage);
    }

    @Test
    void testMetaStorageKeysInitializedOnStartWhenTopVerGreaterThanCmgTopVer() throws Exception {
        Set<LogicalNode> clusterNodes = Set.of(NODE_1);

        mockCmgLocalNodes(2L, clusterNodes);

        keyValueStorage.put(zonesLogicalTopologyVersionKey().bytes(), ByteUtils.longToBytes(3L));

        distributionZoneManager.start();

        verify(keyValueStorage, after(500).never()).invoke(any());

        assertLogicalTopVer(3L);

        assertLogicalTopology(null, keyValueStorage);
    }

    @Test
    void testNodeAddingUpdatesLogicalTopologyInMetaStorage() throws Exception {
        topology.putNode(NODE_1);

        Set<LogicalNode> clusterNodes = Set.of(NODE_1);

        mockCmgLocalNodes(1L, clusterNodes);

        distributionZoneManager.start();

        topology.putNode(NODE_2);

        var clusterNodes2 = Set.of(NODE_1, NODE_2);

        assertLogicalTopology(clusterNodes2, keyValueStorage);

        assertLogicalTopVer(2L);
    }

    @Test
    void testNodeStaleAddingDoNotUpdatesLogicalTopologyInMetaStorage() throws Exception {
        topology.putNode(NODE_1);

        Set<LogicalNode> clusterNodes = Set.of(NODE_1);

        mockCmgLocalNodes(1L, clusterNodes);

        distributionZoneManager.start();

        // Wait for Zone Manager to initialize Meta Storage on start.
        assertLogicalTopVer(1L);

        keyValueStorage.put(zonesLogicalTopologyVersionKey().bytes(), ByteUtils.longToBytes(4L));

        topology.putNode(NODE_2);

        assertEquals(2L, topology.getLogicalTopology().version());

        assertLogicalTopology(clusterNodes, keyValueStorage);

        assertLogicalTopVer(4L);
    }

    @Test
    void testNodeRemovingUpdatesLogicalTopologyInMetaStorage() throws Exception {
        topology.putNode(NODE_1);

        topology.putNode(NODE_2);

        Set<LogicalNode> clusterNodes = Set.of(NODE_1, NODE_2);

        mockCmgLocalNodes(2L, clusterNodes);

        distributionZoneManager.start();

        assertLogicalTopology(clusterNodes, keyValueStorage);

        topology.removeNodes(Set.of(NODE_2));

        var clusterNodes2 = Set.of(NODE_1);

        assertLogicalTopology(clusterNodes2, keyValueStorage);

        assertLogicalTopVer(3L);
    }

    @Test
    void testNodeStaleRemovingDoNotUpdatesLogicalTopologyInMetaStorage() throws Exception {
        topology.putNode(NODE_1);

        topology.putNode(NODE_2);

        assertEquals(2L, topology.getLogicalTopology().version());

        Set<LogicalNode> clusterNodes = Set.of(NODE_1, NODE_2);

        mockCmgLocalNodes(2L, clusterNodes);

        distributionZoneManager.start();

        // Wait for Zone Manager to initialize Meta Storage on start.
        assertLogicalTopVer(2L);

        keyValueStorage.put(zonesLogicalTopologyVersionKey().bytes(), ByteUtils.longToBytes(4L));

        topology.removeNodes(Set.of(NODE_2));

        assertLogicalTopology(clusterNodes, keyValueStorage);

        assertLogicalTopVer(4L);
    }

    @Test
    void testTopologyLeapUpdatesLogicalTopologyInMetaStorage() throws Exception {
        topology.putNode(NODE_1);

        Set<LogicalNode> clusterNodes = Set.of(NODE_1);

        mockCmgLocalNodes(2L, clusterNodes);

        distributionZoneManager.start();

        assertLogicalTopology(clusterNodes, keyValueStorage);

        var clusterNodes2 = Set.of(NODE_1, NODE_2);

        clusterStateStorage.put(LOGICAL_TOPOLOGY_KEY, ByteUtils.toBytes(new LogicalTopologySnapshot(10L, clusterNodes2)));

        topology.fireTopologyLeap();

        assertLogicalTopology(clusterNodes2, keyValueStorage);

        assertLogicalTopVer(10L);
    }

    @Test
    void testStaleTopologyLeapDoNotUpdatesLogicalTopologyInMetaStorage() throws Exception {
        topology.putNode(NODE_1);

        Set<LogicalNode> clusterNodes = Set.of(NODE_1);

        mockCmgLocalNodes(2L, clusterNodes);

        distributionZoneManager.start();

        assertLogicalTopology(clusterNodes, keyValueStorage);

        var clusterNodes2 = Set.of(NODE_1, NODE_2);

        clusterStateStorage.put(LOGICAL_TOPOLOGY_KEY, ByteUtils.toBytes(new LogicalTopologySnapshot(10L, clusterNodes2)));

        keyValueStorage.put(zonesLogicalTopologyVersionKey().bytes(), ByteUtils.longToBytes(11L));

        topology.fireTopologyLeap();

        assertLogicalTopology(clusterNodes, keyValueStorage);

        assertLogicalTopVer(11L);
    }

    private void mockCmgLocalNodes(long version, Set<LogicalNode> clusterNodes) {
        LogicalTopologySnapshot logicalTopologySnapshot = new LogicalTopologySnapshot(version, clusterNodes);

        when(cmgManager.logicalTopology()).thenReturn(completedFuture(logicalTopologySnapshot));
    }

    private void assertLogicalTopVer(long topVer) throws InterruptedException {
        byte[] key = zonesLogicalTopologyVersionKey().bytes();

        boolean success = waitForCondition(() -> {
            byte[] versionBytes = keyValueStorage.get(key).value();

            return versionBytes != null && bytesToLong(versionBytes) == topVer;
        }, 1000);

        if (!success) {
            byte[] versionBytes = keyValueStorage.get(key).value();

            assertThat(versionBytes == null ? null : bytesToLong(versionBytes), is(topVer));
        }
    }
}
