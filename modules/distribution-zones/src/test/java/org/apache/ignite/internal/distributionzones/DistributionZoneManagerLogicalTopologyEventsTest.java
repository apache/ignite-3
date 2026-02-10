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

import static java.time.Duration.ofSeconds;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl.LOGICAL_TOPOLOGY_KEY;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertLogicalTopology;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertLogicalTopologyVersion;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.parseStorageProfiles;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesHistoryKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyVersionKey;
import static org.apache.ignite.internal.metastorage.server.KeyValueUpdateContext.kvContext;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.ByteUtils.longToBytesKeepingOrder;
import static org.awaitility.Awaitility.waitAtMost;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.commands.CreateZoneCommand;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshotSerializer;
import org.apache.ignite.internal.distributionzones.DataNodesHistory.DataNodesHistorySerializer;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.server.KeyValueUpdateContext;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.Test;

/**
 * Tests reactions to topology changes in accordance with distribution zones logic.
 */
public class DistributionZoneManagerLogicalTopologyEventsTest extends BaseDistributionZoneManagerTest {
    private static final LogicalNode NODE_1 = new LogicalNode(randomUUID(), "name1", new NetworkAddress("localhost", 123));

    private static final LogicalNode NODE_2 = new LogicalNode(randomUUID(), "name2", new NetworkAddress("localhost", 123));

    private static final KeyValueUpdateContext KV_UPDATE_CONTEXT = kvContext(HybridTimestamp.MIN_VALUE);

    private final UUID clusterId = randomUUID();

    @Test
    void testMetaStorageKeysInitializedOnStartWhenTopVerEmpty() throws Exception {
        assertThat(distributionZoneManager.startAsync(new ComponentContext()), willCompleteSuccessfully());

        topology.putNode(NODE_1);

        assertLogicalTopologyVersion(1L, keyValueStorage);

        assertLogicalTopology(Set.of(NODE_1), keyValueStorage);
    }

    @Test
    void testMetaStorageKeysInitializedOnStartWhenTopVerEqualsToCmgTopVer() throws Exception {
        keyValueStorage.put(zonesLogicalTopologyVersionKey().bytes(), longToBytesKeepingOrder(2L), KV_UPDATE_CONTEXT);

        assertThat(distributionZoneManager.startAsync(new ComponentContext()), willCompleteSuccessfully());

        assertLogicalTopologyVersion(2L, keyValueStorage);

        assertLogicalTopology(null, keyValueStorage);
    }

    @Test
    void testMetaStorageKeysInitializedOnStartWhenTopVerGreaterThanCmgTopVer() throws Exception {
        keyValueStorage.put(zonesLogicalTopologyVersionKey().bytes(), longToBytesKeepingOrder(3L), KV_UPDATE_CONTEXT);

        assertThat(distributionZoneManager.startAsync(new ComponentContext()), willCompleteSuccessfully());

        assertLogicalTopologyVersion(3L, keyValueStorage);

        assertLogicalTopology(null, keyValueStorage);
    }

    @Test
    void testNodeAddingUpdatesLogicalTopologyInMetaStorage() throws Exception {
        assertThat(distributionZoneManager.startAsync(new ComponentContext()), willCompleteSuccessfully());

        topology.putNode(NODE_1);

        topology.putNode(NODE_2);

        var clusterNodes2 = Set.of(NODE_1, NODE_2);

        assertLogicalTopology(clusterNodes2, keyValueStorage);

        assertLogicalTopologyVersion(2L, keyValueStorage);
    }

    @Test
    void testNodeStaleAddingDoNotUpdatesLogicalTopologyInMetaStorage() throws Exception {
        assertThat(distributionZoneManager.startAsync(new ComponentContext()), willCompleteSuccessfully());

        topology.putNode(NODE_1);

        Set<LogicalNode> clusterNodes = Set.of(NODE_1);

        // Wait for Zone Manager to initialize Meta Storage on start.
        assertLogicalTopologyVersion(1L, keyValueStorage);

        keyValueStorage.put(zonesLogicalTopologyVersionKey().bytes(), longToBytesKeepingOrder(4L), KV_UPDATE_CONTEXT);

        topology.putNode(NODE_2);

        assertEquals(2L, topology.getLogicalTopology().version());

        assertLogicalTopology(clusterNodes, keyValueStorage);

        assertLogicalTopologyVersion(4L, keyValueStorage);
    }

    @Test
    void testNodeRemovingUpdatesLogicalTopologyInMetaStorage() throws Exception {
        assertThat(distributionZoneManager.startAsync(new ComponentContext()), willCompleteSuccessfully());

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
        assertThat(distributionZoneManager.startAsync(new ComponentContext()), willCompleteSuccessfully());

        topology.putNode(NODE_1);

        topology.putNode(NODE_2);

        assertEquals(2L, topology.getLogicalTopology().version());

        Set<LogicalNode> clusterNodes = Set.of(NODE_1, NODE_2);

        // Wait for Zone Manager to initialize Meta Storage on start.
        assertLogicalTopologyVersion(2L, keyValueStorage);

        keyValueStorage.put(zonesLogicalTopologyVersionKey().bytes(), longToBytesKeepingOrder(4L), KV_UPDATE_CONTEXT);

        topology.removeNodes(Set.of(NODE_2));

        assertLogicalTopology(clusterNodes, keyValueStorage);

        assertLogicalTopologyVersion(4L, keyValueStorage);
    }

    @Test
    void testTopologyLeapUpdatesLogicalTopologyInMetaStorage() throws Exception {
        assertThat(distributionZoneManager.startAsync(new ComponentContext()), willCompleteSuccessfully());

        topology.putNode(NODE_1);

        Set<LogicalNode> clusterNodes = Set.of(NODE_1);

        assertLogicalTopology(clusterNodes, keyValueStorage);

        var clusterNodes2 = Set.of(NODE_1, NODE_2);

        clusterStateStorage.put(
                LOGICAL_TOPOLOGY_KEY,
                VersionedSerialization.toBytes(
                        new LogicalTopologySnapshot(10L, clusterNodes2, clusterId),
                        LogicalTopologySnapshotSerializer.INSTANCE
                )
        );

        topology.fireTopologyLeap();

        assertLogicalTopology(clusterNodes2, keyValueStorage);

        assertLogicalTopologyVersion(10L, keyValueStorage);
    }

    @Test
    void testStaleTopologyLeapDoNotUpdatesLogicalTopologyInMetaStorage() throws Exception {
        assertThat(distributionZoneManager.startAsync(new ComponentContext()), willCompleteSuccessfully());

        topology.putNode(NODE_1);

        Set<LogicalNode> clusterNodes = Set.of(NODE_1);

        assertLogicalTopology(clusterNodes, keyValueStorage);

        var clusterNodes2 = Set.of(NODE_1, NODE_2);

        clusterStateStorage.put(
                LOGICAL_TOPOLOGY_KEY,
                VersionedSerialization.toBytes(
                        new LogicalTopologySnapshot(10L, clusterNodes2, clusterId),
                        LogicalTopologySnapshotSerializer.INSTANCE
                )
        );

        keyValueStorage.put(zonesLogicalTopologyVersionKey().bytes(), longToBytesKeepingOrder(11L), KV_UPDATE_CONTEXT);

        topology.fireTopologyLeap();

        assertLogicalTopology(clusterNodes, keyValueStorage);

        assertLogicalTopologyVersion(11L, keyValueStorage);
    }

    /**
     * Tests that zone data nodes get correctly set if the zone had been created right before the topology was updated.
     */
    @Test
    void testZoneStartAndTopologyUpdateOrder() {
        startDistributionZoneManager();

        Set<NodeWithAttributes> topology = Stream.of(NODE_1, NODE_2)
                .map(n -> new NodeWithAttributes(n.name(), n.id(), n.userAttributes(), List.of(DEFAULT_STORAGE_PROFILE)))
                .collect(toSet());

        CatalogCommand createZoneCommand = CreateZoneCommand.builder()
                .zoneName(ZONE_NAME)
                .storageProfilesParams(parseStorageProfiles(DEFAULT_STORAGE_PROFILE))
                .build();

        // We intentionally don't wait for the returned future, because we want to wait for the zone to appear, but not for the catalog
        // to become active.
        catalogManager.execute(createZoneCommand);

        waitAtMost(ofSeconds(10)).until(
                () -> catalogManager.latestCatalog().zone(ZONE_NAME),
                is(notNullValue())
        );

        CompletableFuture<?> metaStorageUpdateFuture = metaStorageManager.putAll(Map.of(
                zonesLogicalTopologyKey(), LogicalTopologySetSerializer.serialize(topology),
                zonesLogicalTopologyVersionKey(), longToBytesKeepingOrder(123)
        ));

        assertThat(metaStorageUpdateFuture, willCompleteSuccessfully());

        int zoneId = catalogManager.latestCatalog().zone(ZONE_NAME).id();

        waitAtMost(ofSeconds(10)).until(
                () -> {
                    CompletableFuture<Entry> dataNodesHistoryFuture = metaStorageManager.get(zoneDataNodesHistoryKey(zoneId));

                    assertThat(dataNodesHistoryFuture, willCompleteSuccessfully());

                    DataNodesHistory dataNodesHistory = DataNodesHistorySerializer.deserialize(dataNodesHistoryFuture.join().value());

                    return dataNodesHistory.dataNodesForTimestamp(HybridTimestamp.MAX_VALUE).dataNodes();
                },
                is(topology)
        );
    }
}
