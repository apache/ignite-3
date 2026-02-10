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

import static java.util.Collections.emptyMap;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertDataNodesFromLogicalNodesInStorage;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertDataNodesInStorage;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertLogicalTopology;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.createDefaultZone;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.logicalNodeFromNode;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.PARTITION_DISTRIBUTION_RESET_TIMEOUT;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyKey;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.descriptors.ConsistencyMode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.distributionzones.events.HaZoneTopologyUpdateEvent;
import org.apache.ignite.internal.distributionzones.events.HaZoneTopologyUpdateEventParams;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/**
 * Test scenarios for the distribution zone scale up and scale down.
 */
public class DistributionZoneManagerScaleUpScaleDownTest extends BaseDistributionZoneManagerTest {
    private static final IgniteLogger LOG = Loggers.forClass(DistributionZoneManagerScaleUpScaleDownTest.class);

    private static final Node A  = new Node("A", randomUUID());

    private static final Node B  = new Node("B", randomUUID());

    private static final Node C  = new Node("C", randomUUID());

    private static final Node D  = new Node("D", randomUUID());

    private static final Node E  = new Node("E", randomUUID());

    private static final LogicalNode NODE_A = logicalNodeFromNode(A);

    private static final LogicalNode NODE_B = logicalNodeFromNode(B);

    private static final LogicalNode NODE_C = logicalNodeFromNode(C);

    @Test
    void testDataNodesPropagationAfterScaleUpTriggered() throws Exception {
        startDistributionZoneManager();

        topology.putNode(NODE_A);

        Set<LogicalNode> clusterNodes = Set.of(NODE_A);

        createDefaultZone(catalogManager);

        int defaultZoneId = getDefaultZone().id();

        assertDataNodesFromLogicalNodesInStorage(defaultZoneId, clusterNodes, keyValueStorage);

        topology.putNode(NODE_B);

        Set<LogicalNode> clusterNodes2 = Set.of(NODE_A, NODE_B);

        assertLogicalTopology(clusterNodes2, keyValueStorage);

        createZone(ZONE_NAME, IMMEDIATE_TIMER_VALUE, INFINITE_TIMER_VALUE, null);

        int zoneId = getZoneId(ZONE_NAME);

        assertDataNodesFromLogicalNodesInStorage(zoneId, clusterNodes2, keyValueStorage);

        topology.putNode(NODE_C);

        Set<LogicalNode> clusterNodes3 = Set.of(NODE_A, NODE_B, NODE_C);

        assertLogicalTopology(clusterNodes3, keyValueStorage);

        assertDataNodesFromLogicalNodesInStorage(zoneId, clusterNodes3, keyValueStorage);
        assertDataNodesFromLogicalNodesInStorage(defaultZoneId, clusterNodes3, keyValueStorage);
    }

    @Test
    void testDataNodesPropagationAfterScaleUpTriggeredOnNewCluster() throws Exception {
        startDistributionZoneManager();

        createDefaultZone(catalogManager);

        alterZone(getDefaultZone().name(), IMMEDIATE_TIMER_VALUE, INFINITE_TIMER_VALUE, null);

        createZone(ZONE_NAME, IMMEDIATE_TIMER_VALUE, null, null);

        topology.putNode(NODE_A);

        assertDataNodesFromLogicalNodesInStorage(getDefaultZone().id(), Set.of(NODE_A), keyValueStorage);
        assertDataNodesFromLogicalNodesInStorage(getZoneId(ZONE_NAME), Set.of(NODE_A), keyValueStorage);
    }

    @Test
    void testDataNodesPropagationAfterScaleDownTriggered() throws Exception {
        startDistributionZoneManager();

        topology.putNode(NODE_A);

        topology.putNode(NODE_B);

        Set<LogicalNode> clusterNodes = Set.of(NODE_A, NODE_B);

        createDefaultZone(catalogManager);

        int defaultZoneId = getDefaultZone().id();

        assertDataNodesFromLogicalNodesInStorage(defaultZoneId, clusterNodes, keyValueStorage);

        createZone(ZONE_NAME, null, IMMEDIATE_TIMER_VALUE, null);

        int zoneId = getZoneId(ZONE_NAME);

        assertDataNodesFromLogicalNodesInStorage(zoneId, clusterNodes, keyValueStorage);

        topology.removeNodes(Set.of(NODE_B));

        Set<LogicalNode> clusterNodes2 = Set.of(NODE_A);

        assertLogicalTopology(clusterNodes2, keyValueStorage);

        assertDataNodesFromLogicalNodesInStorage(zoneId, clusterNodes2, keyValueStorage);

        // Check that default zone still has both node1 and node2 because dafault zones' scaleDown is INF.
        assertDataNodesFromLogicalNodesInStorage(defaultZoneId, clusterNodes, keyValueStorage);
    }

    @Test
    void testDataNodesPropagationForDefaultZoneAfterScaleUpTriggered() throws Exception {
        startDistributionZoneManager();

        topology.putNode(NODE_A);

        Set<LogicalNode> clusterNodes = Set.of(NODE_A);

        createDefaultZone(catalogManager);

        CatalogZoneDescriptor defaultZone = getDefaultZone();

        assertDataNodesFromLogicalNodesInStorage(defaultZone.id(), clusterNodes, keyValueStorage);

        alterZone(defaultZone.name(), INFINITE_TIMER_VALUE, INFINITE_TIMER_VALUE, null);

        topology.putNode(NODE_B);

        Set<LogicalNode> clusterNodes2 = Set.of(NODE_A, NODE_B);

        assertLogicalTopology(clusterNodes2, keyValueStorage);

        alterZone(defaultZone.name(), IMMEDIATE_TIMER_VALUE, INFINITE_TIMER_VALUE, null);

        assertDataNodesFromLogicalNodesInStorage(defaultZone.id(), clusterNodes2, keyValueStorage);
    }

    @Test
    void testDataNodesPropagationForDefaultZoneAfterScaleDownTriggered() throws Exception {
        startDistributionZoneManager();

        topology.putNode(NODE_A);

        topology.putNode(NODE_B);

        Set<LogicalNode> clusterNodes = Set.of(NODE_A, NODE_B);

        createDefaultZone(catalogManager);

        CatalogZoneDescriptor defaultZone = getDefaultZone();

        assertDataNodesFromLogicalNodesInStorage(defaultZone.id(), clusterNodes, keyValueStorage);

        alterZone(defaultZone.name(), INFINITE_TIMER_VALUE, INFINITE_TIMER_VALUE, null);

        topology.removeNodes(Set.of(NODE_B));

        Set<LogicalNode> clusterNodes2 = Set.of(NODE_A);

        assertLogicalTopology(clusterNodes2, keyValueStorage);

        alterZone(defaultZone.name(), INFINITE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE, null);

        assertDataNodesFromLogicalNodesInStorage(defaultZone.id(), clusterNodes2, keyValueStorage);
    }

    @Test
    void testEmptyDataNodesOnStart() throws Exception {
        startDistributionZoneManager();

        createZone(ZONE_NAME, IMMEDIATE_TIMER_VALUE, null, null);

        int zoneId = getZoneId(ZONE_NAME);

        assertDataNodesFromLogicalNodesInStorage(zoneId, Set.of(), keyValueStorage);

        topology.putNode(NODE_A);

        assertLogicalTopology(Set.of(NODE_A), keyValueStorage);

        assertDataNodesFromLogicalNodesInStorage(zoneId, Set.of(NODE_A), keyValueStorage);
    }

    @Test
    void testUpdateZoneScaleUpTriggersDataNodePropagation() throws Exception {
        startDistributionZoneManager();

        createZone(ZONE_NAME, 100, null, null);

        int zoneId = getZoneId(ZONE_NAME);

        assertDataNodesFromLogicalNodesInStorage(zoneId, Set.of(), keyValueStorage);

        topology.putNode(NODE_A);

        assertLogicalTopology(Set.of(NODE_A), keyValueStorage);

        assertDataNodesFromLogicalNodesInStorage(zoneId, Set.of(), keyValueStorage);

        alterZone(ZONE_NAME, IMMEDIATE_TIMER_VALUE, null, null);

        assertDataNodesFromLogicalNodesInStorage(zoneId, Set.of(NODE_A), keyValueStorage);
    }

    @Test
    void testUpdateZoneScaleDownTriggersDataNodePropagation() throws Exception {
        startDistributionZoneManager();

        topology.putNode(NODE_A);

        assertLogicalTopology(Set.of(NODE_A), keyValueStorage);

        createZone(ZONE_NAME, null, 100, null);

        int zoneId = getZoneId(ZONE_NAME);

        assertDataNodesFromLogicalNodesInStorage(zoneId, Set.of(NODE_A), keyValueStorage);

        topology.removeNodes(Set.of(NODE_A));

        assertDataNodesFromLogicalNodesInStorage(zoneId, Set.of(NODE_A), keyValueStorage);

        alterZone(ZONE_NAME, null, IMMEDIATE_TIMER_VALUE, null);

        assertDataNodesFromLogicalNodesInStorage(zoneId, Set.of(), keyValueStorage);
    }

    @Test
    void testHistory() throws Exception {
        preparePrerequisites();

        HybridTimestamp time = clock.now();

        LOG.info("Time: {}", time);

        int zoneId = getZoneId(ZONE_NAME);

        assertDataNodesInStorage(zoneId, Set.of(A, B, C), keyValueStorage);

        addNodes(Set.of(D));
        addNodes(Set.of(E));

        assertDataNodesInStorage(zoneId, Set.of(A, B, C, D, E), keyValueStorage);

        removeNodes(Set.of(C));

        assertDataNodesInStorage(zoneId, Set.of(A, B, D, E), keyValueStorage);
    }

    @Test
    void testPartitionDistributionResetTaskScheduling() throws Exception {
        startDistributionZoneManager();

        topology.putNode(NODE_A);
        topology.putNode(NODE_B);
        topology.putNode(NODE_C);

        assertLogicalTopology(Set.of(NODE_A, NODE_B, NODE_C), keyValueStorage);

        String haZoneName = "haZone";
        String scZoneName = "scZone";

        createZone(haZoneName, ConsistencyMode.HIGH_AVAILABILITY);
        createZone(scZoneName, ConsistencyMode.STRONG_CONSISTENCY);

        int haZoneId = getZoneId(haZoneName);
        int scZoneId = getZoneId(scZoneName);

        List<HaZoneTopologyUpdateEventParams> eventList = new CopyOnWriteArrayList<>();

        distributionZoneManager.listen(HaZoneTopologyUpdateEvent.TOPOLOGY_REDUCED, event -> {
            eventList.add(event);

            return falseCompletedFuture();
        });

        topology.removeNodes(Set.of(NODE_C));

        assertLogicalTopology(Set.of(NODE_A, NODE_B), keyValueStorage);

        long topologyRevision = metaStorageManager.get(
                zonesLogicalTopologyKey()).get(1, TimeUnit.SECONDS).revision();

        assertTrue(waitForCondition(() -> metaStorageManager.appliedRevision() >= topologyRevision, 1000));

        assertFalse(distributionZoneManager.dataNodesManager().zoneTimers(scZoneId).partitionReset.taskIsScheduled());

        assertTrue(waitForCondition(() -> !eventList.isEmpty(), 2000));
        assertEquals(1, eventList.size());
        assertEquals(haZoneId, eventList.get(0).zoneId());

        // We don't want trigger stop of the timer, so, the target configuration must be smaller than Integer.MAX_VALUE
        CompletableFuture<Void> changeConfigFuture = systemDistributedConfiguration.change(c0 -> c0.changeProperties()
                .update(PARTITION_DISTRIBUTION_RESET_TIMEOUT, c1 -> c1.changePropertyValue(String.valueOf(Integer.MAX_VALUE - 1)))
        );
        assertThat(changeConfigFuture, willCompleteSuccessfully());

        topology.removeNodes(Set.of(NODE_B));

        assertLogicalTopology(Set.of(NODE_A), keyValueStorage);

        assertEquals(1, eventList.size());
        assertFalse(distributionZoneManager.dataNodesManager().zoneTimers(scZoneId).partitionReset.taskIsScheduled());
    }

    @Test
    void testVariousScaleUpScaleDownScenarios1_1() throws Exception {
        preparePrerequisites();

        int zoneId = getZoneId(ZONE_NAME);

        addNodes(Set.of(D));
        removeNodes(Set.of(C));

        assertDataNodesInStorage(zoneId, Set.of(A, B, D), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios1_2() throws Exception {
        preparePrerequisites();

        int zoneId = getZoneId(ZONE_NAME);

        removeNodes(Set.of(C));
        addNodes(Set.of(D));

        assertDataNodesInStorage(zoneId, Set.of(A, B, D), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios2_1() throws Exception {
        preparePrerequisites();

        int zoneId = getZoneId(ZONE_NAME);

        addNodes(Set.of(D));
        removeNodes(Set.of(D));

        assertDataNodesInStorage(zoneId, Set.of(A, B, C), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios2_2() throws Exception {
        preparePrerequisites();

        int zoneId = getZoneId(ZONE_NAME);

        removeNodes(Set.of(C));
        addNodes(Set.of(C));

        assertDataNodesInStorage(zoneId, Set.of(A, B, C), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios3_1() throws Exception {
        preparePrerequisites();

        int zoneId = getZoneId(ZONE_NAME);

        addNodes(Set.of(D));
        removeNodes(Set.of(D));
        addNodes(Set.of(D));

        assertDataNodesInStorage(zoneId, Set.of(A, B, C, D), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios3_2() throws Exception {
        preparePrerequisites();

        int zoneId = getZoneId(ZONE_NAME);

        addNodes(Set.of(D));
        removeNodes(Set.of(D));
        addNodes(Set.of(D));

        assertDataNodesInStorage(zoneId, Set.of(A, B, C, D), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios3_3() throws Exception {
        preparePrerequisites();

        int zoneId = getZoneId(ZONE_NAME);

        addNodes(Set.of(D));
        removeNodes(Set.of(D));

        assertDataNodesInStorage(zoneId, Set.of(A, B, C), keyValueStorage);

        addNodes(Set.of(D));

        assertDataNodesInStorage(zoneId, Set.of(A, B, C, D), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios3_4() throws Exception {
        preparePrerequisites();

        int zoneId = getZoneId(ZONE_NAME);

        removeNodes(Set.of(C));
        addNodes(Set.of(C));

        assertDataNodesInStorage(zoneId, Set.of(A, B, C), keyValueStorage);

        removeNodes(Set.of(C));

        assertDataNodesInStorage(zoneId, Set.of(A, B), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios4_1() throws Exception {
        preparePrerequisites();

        int zoneId = getZoneId(ZONE_NAME);

        addNodes(Set.of(D));
        removeNodes(Set.of(D));
        addNodes(Set.of(E));

        assertDataNodesInStorage(zoneId, Set.of(A, B, C, E), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios4_2() throws Exception {
        preparePrerequisites();

        int zoneId = getZoneId(ZONE_NAME);

        addNodes(Set.of(D));
        removeNodes(Set.of(D));
        addNodes(Set.of(E));

        assertDataNodesInStorage(zoneId, Set.of(A, B, C, E), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios4_3() throws Exception {
        preparePrerequisites();

        int zoneId = getZoneId(ZONE_NAME);

        removeNodes(Set.of(C));
        addNodes(Set.of(C));
        removeNodes(Set.of(B));

        assertDataNodesInStorage(zoneId, Set.of(A, C), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios5_1() throws Exception {
        preparePrerequisites();

        int zoneId = getZoneId(ZONE_NAME);

        addNodes(Set.of(D));
        removeNodes(Set.of(C));
        addNodes(Set.of(C));

        assertDataNodesInStorage(zoneId, Set.of(A, B, C, D), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios5_2() throws Exception {
        preparePrerequisites();

        int zoneId = getZoneId(ZONE_NAME);

        removeNodes(Set.of(C));
        addNodes(Set.of(D));
        removeNodes(Set.of(D));

        assertDataNodesInStorage(zoneId, Set.of(A, B), keyValueStorage);
    }

    /**
     * Creates a zone with the auto adjust scale up scale down trigger equals to 0 and the data nodes equals ["A", B, "C"].
     *
     * @throws Exception when something goes wrong.
     */
    private void preparePrerequisites() throws Exception {
        preparePrerequisites(null);
    }

    private void preparePrerequisites(@Nullable String filter) throws Exception {
        startDistributionZoneManager();

        LogicalNode a = logicalNodeFromNode(A);

        LogicalNode b = logicalNodeFromNode(B);

        LogicalNode c = logicalNodeFromNode(C);

        LogicalNode d = logicalNodeFromNode(D);

        LogicalNode e = logicalNodeFromNode(E);

        topology.putNode(a);
        topology.putNode(b);
        topology.putNode(c);

        // This is done because we need to have node's D and E attributes in the DistributionZoneManager.nodeAttributes, so won't be
        // any NPE when we add or remove these nodes manually from manager's state.
        // (see testVariousScaleUpScaleDownScenarios*)
        topology.putNode(d);
        topology.removeNodes(Set.of(d));
        topology.putNode(e);
        topology.removeNodes(Set.of(e));

        Set<LogicalNode> clusterNodes = Set.of(a, b, c);

        createZone(ZONE_NAME, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE, filter);

        int zoneId = getZoneId(ZONE_NAME);

        assertDataNodesFromLogicalNodesInStorage(zoneId, clusterNodes, keyValueStorage);
    }

    protected void createZone(
            String zoneName,
            ConsistencyMode consistencyMode
    ) {
        DistributionZonesTestUtil.createZone(
                catalogManager,
                zoneName,
                consistencyMode
        );
    }

    private void addNodes(Set<Node> nodes) {
        for (Node n : nodes) {
            topology.putNode(new LogicalNode(
                    new ClusterNodeImpl(n.nodeId(), n.nodeName(), new NetworkAddress("localhost", 123)),
                    emptyMap(),
                    emptyMap(),
                    List.of("default")
            ));
        }
    }

    private void removeNodes(Set<Node> nodes) {
        topology.removeNodes(
                nodes.stream()
                        .map(n -> new LogicalNode(n.nodeId(), n.nodeName(), new NetworkAddress("localhost", 123)))
                        .collect(toSet())
        );
    }
}
