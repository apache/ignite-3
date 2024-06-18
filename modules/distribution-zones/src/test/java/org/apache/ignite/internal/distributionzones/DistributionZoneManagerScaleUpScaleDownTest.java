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
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertDataNodesFromLogicalNodesInStorage;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertLogicalTopology;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertZoneScaleDownChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertZoneScaleUpChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.createZoneManagerExecutor;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleDownChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleUpChangeTriggerKey;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.util.ByteUtils.bytesToLongKeepingOrder;
import static org.apache.ignite.internal.util.ByteUtils.longToBytesKeepingOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager.ZoneState;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.server.If;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.thread.StripedScheduledThreadPoolExecutor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/**
 * Test scenarios for the distribution zone scale up and scale down.
 */
public class DistributionZoneManagerScaleUpScaleDownTest extends BaseDistributionZoneManagerTest {
    private static final IgniteLogger LOG = Loggers.forClass(DistributionZoneManagerScaleUpScaleDownTest.class);

    private static final LogicalNode NODE_1 = new LogicalNode("1", "node1", new NetworkAddress("localhost", 123));

    private static final LogicalNode NODE_2 = new LogicalNode("2", "node2", new NetworkAddress("localhost", 123));

    private static final LogicalNode NODE_3 = new LogicalNode("3", "node3", new NetworkAddress("localhost", 123));

    private static final Node A  = new Node("A", "id_A");

    private static final Node B  = new Node("B", "id_B");

    private static final Node C  = new Node("C", "id_C");

    private static final Node D  = new Node("D", "id_D");

    private static final Node E  = new Node("E", "id_E");

    @Test
    void testDataNodesPropagationAfterScaleUpTriggered() throws Exception {
        startDistributionZoneManager();

        topology.putNode(NODE_1);

        Set<LogicalNode> clusterNodes = Set.of(NODE_1);

        int defaultZoneId = getDefaultZone().id();

        assertDataNodesFromLogicalNodesInStorage(defaultZoneId, clusterNodes, keyValueStorage);

        topology.putNode(NODE_2);

        Set<LogicalNode> clusterNodes2 = Set.of(NODE_1, NODE_2);

        assertLogicalTopology(clusterNodes2, keyValueStorage);

        createZone(ZONE_NAME, IMMEDIATE_TIMER_VALUE, INFINITE_TIMER_VALUE, null);

        int zoneId = getZoneId(ZONE_NAME);

        assertDataNodesFromLogicalNodesInStorage(zoneId, clusterNodes2, keyValueStorage);

        topology.putNode(NODE_3);

        Set<LogicalNode> clusterNodes3 = Set.of(NODE_1, NODE_2, NODE_3);

        assertLogicalTopology(clusterNodes3, keyValueStorage);

        assertDataNodesFromLogicalNodesInStorage(zoneId, clusterNodes3, keyValueStorage);
        assertDataNodesFromLogicalNodesInStorage(defaultZoneId, clusterNodes3, keyValueStorage);
    }

    @Test
    void testDataNodesPropagationAfterScaleUpTriggeredOnNewCluster() throws Exception {
        startDistributionZoneManager();

        alterZone(getDefaultZone().name(), IMMEDIATE_TIMER_VALUE, INFINITE_TIMER_VALUE, null);

        createZone(ZONE_NAME, IMMEDIATE_TIMER_VALUE, null, null);

        topology.putNode(NODE_1);

        assertDataNodesFromLogicalNodesInStorage(getDefaultZone().id(), Set.of(NODE_1), keyValueStorage);
        assertDataNodesFromLogicalNodesInStorage(getZoneId(ZONE_NAME), Set.of(NODE_1), keyValueStorage);
    }

    @Test
    void testDataNodesPropagationAfterScaleDownTriggered() throws Exception {
        startDistributionZoneManager();

        topology.putNode(NODE_1);

        topology.putNode(NODE_2);

        Set<LogicalNode> clusterNodes = Set.of(NODE_1, NODE_2);

        int defaultZoneId = getDefaultZone().id();

        assertDataNodesFromLogicalNodesInStorage(defaultZoneId, clusterNodes, keyValueStorage);

        createZone(ZONE_NAME, null, IMMEDIATE_TIMER_VALUE, null);

        int zoneId = getZoneId(ZONE_NAME);

        assertDataNodesFromLogicalNodesInStorage(zoneId, clusterNodes, keyValueStorage);

        topology.removeNodes(Set.of(NODE_2));

        Set<LogicalNode> clusterNodes2 = Set.of(NODE_1);

        assertLogicalTopology(clusterNodes2, keyValueStorage);

        assertDataNodesFromLogicalNodesInStorage(zoneId, clusterNodes2, keyValueStorage);

        // Check that default zone still has both node1 and node2 because dafault zones' scaleDown is INF.
        assertDataNodesFromLogicalNodesInStorage(defaultZoneId, clusterNodes, keyValueStorage);
    }

    @Test
    void testDataNodesPropagationForDefaultZoneAfterScaleUpTriggered() throws Exception {
        startDistributionZoneManager();

        topology.putNode(NODE_1);

        Set<LogicalNode> clusterNodes = Set.of(NODE_1);

        CatalogZoneDescriptor defaultZone = getDefaultZone();

        assertDataNodesFromLogicalNodesInStorage(defaultZone.id(), clusterNodes, keyValueStorage);

        alterZone(defaultZone.name(), INFINITE_TIMER_VALUE, INFINITE_TIMER_VALUE, null);

        topology.putNode(NODE_2);

        Set<LogicalNode> clusterNodes2 = Set.of(NODE_1, NODE_2);

        assertLogicalTopology(clusterNodes2, keyValueStorage);

        alterZone(defaultZone.name(), IMMEDIATE_TIMER_VALUE, INFINITE_TIMER_VALUE, null);

        assertDataNodesFromLogicalNodesInStorage(defaultZone.id(), clusterNodes2, keyValueStorage);
    }

    @Test
    void testDataNodesPropagationForDefaultZoneAfterScaleDownTriggered() throws Exception {
        startDistributionZoneManager();

        topology.putNode(NODE_1);

        topology.putNode(NODE_2);

        Set<LogicalNode> clusterNodes = Set.of(NODE_1, NODE_2);

        CatalogZoneDescriptor defaultZone = getDefaultZone();

        assertDataNodesFromLogicalNodesInStorage(defaultZone.id(), clusterNodes, keyValueStorage);

        alterZone(defaultZone.name(), INFINITE_TIMER_VALUE, INFINITE_TIMER_VALUE, null);

        topology.removeNodes(Set.of(NODE_2));

        Set<LogicalNode> clusterNodes2 = Set.of(NODE_1);

        assertLogicalTopology(clusterNodes2, keyValueStorage);

        alterZone(defaultZone.name(), INFINITE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE, null);

        assertDataNodesFromLogicalNodesInStorage(defaultZone.id(), clusterNodes2, keyValueStorage);
    }

    @Test
    void testDropZoneDoNotPropagateDataNodesAfterScaleUp() throws Exception {
        startDistributionZoneManager();

        topology.putNode(NODE_1);

        topology.putNode(NODE_2);

        Set<LogicalNode> clusterNodes2 = Set.of(NODE_1, NODE_2);

        assertLogicalTopology(clusterNodes2, keyValueStorage);

        createZone(ZONE_NAME, IMMEDIATE_TIMER_VALUE, null, null);

        int zoneId = getZoneId(ZONE_NAME);

        assertDataNodesFromLogicalNodesInStorage(zoneId, clusterNodes2, keyValueStorage);

        assertNotNull(keyValueStorage.get(zoneScaleUpChangeTriggerKey(zoneId).bytes()).value(),
                "zoneScaleUpChangeTriggerKey must be not null.");
        assertNotNull(keyValueStorage.get(zoneScaleDownChangeTriggerKey(zoneId).bytes()).value(),
                "zoneScaleDownChangeTriggerKey must be not null.");

        dropZone(ZONE_NAME);

        assertDataNodesFromLogicalNodesInStorage(zoneId, null, keyValueStorage);
        assertZoneScaleUpChangeTriggerKey(null, zoneId, keyValueStorage);
        assertZoneScaleDownChangeTriggerKey(null, zoneId, keyValueStorage);
    }

    @Test
    void testDropZoneDoNotPropagateDataNodesAfterScaleDown() throws Exception {
        startDistributionZoneManager();

        topology.putNode(NODE_1);

        topology.putNode(NODE_2);

        topology.removeNodes(Set.of(NODE_2));

        Set<LogicalNode> clusterNodes2 = Set.of(NODE_1);

        assertLogicalTopology(clusterNodes2, keyValueStorage);

        createZone(ZONE_NAME, null, IMMEDIATE_TIMER_VALUE, null);

        int zoneId = getZoneId(ZONE_NAME);

        assertDataNodesFromLogicalNodesInStorage(zoneId, clusterNodes2, keyValueStorage);

        assertNotNull(keyValueStorage.get(zoneScaleUpChangeTriggerKey(zoneId).bytes()).value(),
                "zoneScaleUpChangeTriggerKey must be not null.");
        assertNotNull(keyValueStorage.get(zoneScaleDownChangeTriggerKey(zoneId).bytes()).value(),
                "zoneScaleDownChangeTriggerKey must be not null.");

        dropZone(ZONE_NAME);

        assertDataNodesFromLogicalNodesInStorage(zoneId, null, keyValueStorage);
        assertZoneScaleUpChangeTriggerKey(null, zoneId, keyValueStorage);
        assertZoneScaleDownChangeTriggerKey(null, zoneId, keyValueStorage);
    }

    @Test
    void testEmptyDataNodesOnStart() throws Exception {
        startDistributionZoneManager();

        createZone(ZONE_NAME, IMMEDIATE_TIMER_VALUE, null, null);

        int zoneId = getZoneId(ZONE_NAME);

        assertDataNodesFromLogicalNodesInStorage(zoneId, Set.of(), keyValueStorage);

        topology.putNode(NODE_1);

        assertLogicalTopology(Set.of(NODE_1), keyValueStorage);

        assertDataNodesFromLogicalNodesInStorage(zoneId, Set.of(NODE_1), keyValueStorage);
    }

    @Test
    void testUpdateZoneScaleUpTriggersDataNodePropagation() throws Exception {
        startDistributionZoneManager();

        createZone(ZONE_NAME, 100, null, null);

        int zoneId = getZoneId(ZONE_NAME);

        assertDataNodesFromLogicalNodesInStorage(zoneId, Set.of(), keyValueStorage);

        topology.putNode(NODE_1);

        assertLogicalTopology(Set.of(NODE_1), keyValueStorage);

        assertDataNodesFromLogicalNodesInStorage(zoneId, Set.of(), keyValueStorage);

        alterZone(ZONE_NAME, IMMEDIATE_TIMER_VALUE, null, null);

        assertDataNodesFromLogicalNodesInStorage(zoneId, Set.of(NODE_1), keyValueStorage);
    }

    @Test
    void testUpdateZoneScaleDownTriggersDataNodePropagation() throws Exception {
        startDistributionZoneManager();

        topology.putNode(NODE_1);

        assertLogicalTopology(Set.of(NODE_1), keyValueStorage);

        createZone(ZONE_NAME, null, 100, null);

        int zoneId = getZoneId(ZONE_NAME);

        assertDataNodesFromLogicalNodesInStorage(zoneId, Set.of(NODE_1), keyValueStorage);

        topology.removeNodes(Set.of(NODE_1));

        assertDataNodesFromLogicalNodesInStorage(zoneId, Set.of(NODE_1), keyValueStorage);

        alterZone(ZONE_NAME, null, IMMEDIATE_TIMER_VALUE, null);

        assertDataNodesFromLogicalNodesInStorage(zoneId, Set.of(), keyValueStorage);
    }

    @Test
    void testCleanUpAfterSchedulers() throws Exception {
        preparePrerequisites();

        int zoneId = getZoneId(ZONE_NAME);

        ZoneState zoneState = distributionZoneManager.zonesState().get(zoneId);

        zoneState.nodesToAddToDataNodes(Set.of(D), 1003);
        zoneState.nodesToAddToDataNodes(Set.of(E), 1004);
        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 1007);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(zoneId, 1003);

        assertTrue(zoneState.topologyAugmentationMap().containsKey(1003L));
        assertTrue(zoneState.topologyAugmentationMap().containsKey(1004L));
        assertTrue(zoneState.topologyAugmentationMap().containsKey(1007L));

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(zoneId, 1004);

        assertTrue(zoneState.topologyAugmentationMap().containsKey(1003L));
        assertTrue(zoneState.topologyAugmentationMap().containsKey(1004L));
        assertTrue(zoneState.topologyAugmentationMap().containsKey(1007L));

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(zoneId, 1007);

        assertFalse(zoneState.topologyAugmentationMap().containsKey(1003L));
        assertFalse(zoneState.topologyAugmentationMap().containsKey(1004L));
        assertTrue(zoneState.topologyAugmentationMap().containsKey(1007L));

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(zoneId, 1015);

        assertTrue(zoneState.topologyAugmentationMap().isEmpty());
        DistributionZonesTestUtil.assertDataNodesInStorage(zoneId, Set.of(A, B, D, E), keyValueStorage);
    }

    @Test
    void testScaleUpSetToMaxInt() throws Exception {
        startDistributionZoneManager();

        createZone(ZONE_NAME, 100, null, null);

        int zoneId = getZoneId(ZONE_NAME);

        assertDataNodesFromLogicalNodesInStorage(zoneId, Set.of(), keyValueStorage);

        ZoneState zoneState = distributionZoneManager.zonesState().get(zoneId);

        assertNull(zoneState.scaleUpTask());

        topology.putNode(NODE_1);

        assertLogicalTopology(Set.of(NODE_1), keyValueStorage);

        assertTrue(waitForCondition(() -> zoneState.scaleUpTask() != null, 1000L));

        alterZone(ZONE_NAME, INFINITE_TIMER_VALUE, null, null);

        assertTrue(waitForCondition(() -> zoneState.scaleUpTask().isCancelled(), 1000L));
    }

    @Test
    void testScaleDownSetToMaxInt() throws Exception {
        startDistributionZoneManager();

        topology.putNode(NODE_1);

        assertLogicalTopology(Set.of(NODE_1), keyValueStorage);

        createZone(ZONE_NAME, null, 100, null);

        int zoneId = getZoneId(ZONE_NAME);

        assertDataNodesFromLogicalNodesInStorage(zoneId, Set.of(NODE_1), keyValueStorage);

        ZoneState zoneState = distributionZoneManager.zonesState().get(zoneId);

        assertNull(zoneState.scaleDownTask());

        topology.removeNodes(Set.of(NODE_1));

        assertTrue(waitForCondition(() -> zoneState.scaleDownTask() != null, 1000L));

        alterZone(ZONE_NAME, null, INFINITE_TIMER_VALUE, null);

        assertTrue(waitForCondition(() -> zoneState.scaleDownTask().isCancelled(), 1000L));
    }

    @Test
    void testScaleUpDidNotChangeDataNodesWhenTriggerKeyWasConcurrentlyChanged() throws Exception {
        startDistributionZoneManager();

        createZone(ZONE_NAME, IMMEDIATE_TIMER_VALUE, null, null);

        int zoneId = getZoneId(ZONE_NAME);

        assertDataNodesFromLogicalNodesInStorage(zoneId, Set.of(), keyValueStorage);

        assertTrue(waitForCondition(() -> keyValueStorage.get(zoneScaleUpChangeTriggerKey(zoneId).bytes()).value() != null, 5000));

        long revisionOfScaleUp = bytesToLongKeepingOrder(keyValueStorage.get(zoneScaleUpChangeTriggerKey(zoneId).bytes()).value());

        doAnswer(invocation -> {
            If iif = invocation.getArgument(0);

            // Emulate a situation when one of the scale up keys gets concurrently updated during a Meta Storage invoke. We then expect
            // that the invoke call will be retried.
            byte[] keyScaleUp = zoneScaleUpChangeTriggerKey(zoneId).bytes();
            byte[] keyDataNodes = zoneDataNodesKey(zoneId).bytes();

            if (Arrays.stream(iif.cond().keys()).anyMatch(k -> Arrays.equals(keyScaleUp, k))) {
                byte[] value = keyValueStorage.get(zoneDataNodesKey(zoneId).bytes()).value();

                keyValueStorage.putAll(
                        List.of(keyScaleUp, keyDataNodes),
                        List.of(longToBytesKeepingOrder(revisionOfScaleUp + 100L), value),
                        HybridTimestamp.MIN_VALUE
                );
            }

            return invocation.callRealMethod();
        }).when(keyValueStorage).invoke(any(), any(), any());

        topology.putNode(NODE_1);

        assertLogicalTopology(Set.of(NODE_1), keyValueStorage);

        assertZoneScaleUpChangeTriggerKey(revisionOfScaleUp + 100L, zoneId, keyValueStorage);

        assertDataNodesFromLogicalNodesInStorage(zoneId, Set.of(), keyValueStorage);
    }

    @Test
    void testScaleDownDidNotChangeDataNodesWhenTriggerKeyWasConcurrentlyChanged() throws Exception {
        startDistributionZoneManager();

        topology.putNode(NODE_1);

        assertLogicalTopology(Set.of(NODE_1), keyValueStorage);

        int defaultZoneId = getDefaultZone().id();

        assertDataNodesFromLogicalNodesInStorage(defaultZoneId, Set.of(NODE_1), keyValueStorage);

        createZone(ZONE_NAME, null, IMMEDIATE_TIMER_VALUE, null);

        int zoneId = getZoneId(ZONE_NAME);

        assertDataNodesFromLogicalNodesInStorage(zoneId, Set.of(NODE_1), keyValueStorage);

        assertTrue(waitForCondition(() -> keyValueStorage.get(zoneScaleDownChangeTriggerKey(zoneId).bytes()).value() != null, 5000));

        long revisionOfScaleDown = bytesToLongKeepingOrder(keyValueStorage.get(zoneScaleDownChangeTriggerKey(zoneId).bytes()).value());

        doAnswer(invocation -> {
            If iif = invocation.getArgument(0);

            // Emulate a situation when one of the scale down keys gets concurrently updated during a Meta Storage invoke. We then expect
            // that the invoke call will be retried.
            byte[] keyScaleDown = zoneScaleDownChangeTriggerKey(zoneId).bytes();
            byte[] keyDataNodes = zoneDataNodesKey(zoneId).bytes();

            if (Arrays.stream(iif.cond().keys()).anyMatch(k -> Arrays.equals(keyScaleDown, k))) {
                keyValueStorage.putAll(
                        List.of(keyScaleDown, keyDataNodes),
                        List.of(longToBytesKeepingOrder(revisionOfScaleDown + 100), keyValueStorage.get(zoneDataNodesKey(zoneId).bytes()).value()),
                        HybridTimestamp.MIN_VALUE
                );
            }

            return invocation.callRealMethod();
        }).when(keyValueStorage).invoke(any(), any(), any());

        topology.removeNodes(Set.of(NODE_1));

        assertDataNodesFromLogicalNodesInStorage(zoneId, Set.of(NODE_1), keyValueStorage);

        assertZoneScaleDownChangeTriggerKey(revisionOfScaleDown + 100L, zoneId, keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios1_1() throws Exception {
        preparePrerequisites();

        int zoneId = getZoneId(ZONE_NAME);

        ZoneState zoneState = distributionZoneManager.zonesState().get(zoneId);

        zoneState.nodesToAddToDataNodes(Set.of(D), 1003);
        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 1007);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(zoneId, 1003);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(zoneId, 1007);

        DistributionZonesTestUtil.assertDataNodesInStorage(zoneId, Set.of(A, B, D), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios1_2() throws Exception {
        preparePrerequisites();

        int zoneId = getZoneId(ZONE_NAME);

        ZoneState zoneState = distributionZoneManager.zonesState().get(zoneId);

        zoneState.nodesToAddToDataNodes(Set.of(D), 1003);
        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 1007);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(zoneId, 1007);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(zoneId, 1003);

        DistributionZonesTestUtil.assertDataNodesInStorage(zoneId, Set.of(A, B, D), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios1_3() throws Exception {
        preparePrerequisites();

        int zoneId = getZoneId(ZONE_NAME);

        ZoneState zoneState = distributionZoneManager.zonesState().get(zoneId);

        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 1003);
        zoneState.nodesToAddToDataNodes(Set.of(D), 1007);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(zoneId, 1003);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(zoneId, 1007);

        DistributionZonesTestUtil.assertDataNodesInStorage(zoneId, Set.of(A, B, D), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios1_4() throws Exception {
        preparePrerequisites();

        int zoneId = getZoneId(ZONE_NAME);

        ZoneState zoneState = distributionZoneManager.zonesState().get(zoneId);

        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 1003);
        zoneState.nodesToAddToDataNodes(Set.of(D), 1007);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(zoneId, 1007);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(zoneId, 1003);

        DistributionZonesTestUtil.assertDataNodesInStorage(zoneId, Set.of(A, B, D), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios2_1() throws Exception {
        preparePrerequisites();

        int zoneId = getZoneId(ZONE_NAME);

        ZoneState zoneState = distributionZoneManager.zonesState().get(zoneId);

        zoneState.nodesToAddToDataNodes(Set.of(D), 1003);
        zoneState.nodesToRemoveFromDataNodes(Set.of(D), 1007);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(zoneId, 1003);
        DistributionZonesTestUtil.assertDataNodesInStorage(zoneId, Set.of(A, B, C, D), keyValueStorage);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(zoneId, 1007);

        DistributionZonesTestUtil.assertDataNodesInStorage(zoneId, Set.of(A, B, C), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios2_2() throws Exception {
        preparePrerequisites();

        int zoneId = getZoneId(ZONE_NAME);

        ZoneState zoneState = distributionZoneManager.zonesState().get(zoneId);

        zoneState.nodesToAddToDataNodes(Set.of(D), 1003);
        zoneState.nodesToRemoveFromDataNodes(Set.of(D), 1007);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(zoneId, 1007);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(zoneId, 1003);

        DistributionZonesTestUtil.assertDataNodesInStorage(zoneId, Set.of(A, B, C), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios2_3() throws Exception {
        preparePrerequisites();

        int zoneId = getZoneId(ZONE_NAME);

        ZoneState zoneState = distributionZoneManager.zonesState().get(zoneId);

        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 1003);
        zoneState.nodesToAddToDataNodes(Set.of(C), 1007);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(zoneId, 1003);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(zoneId, 1007);

        DistributionZonesTestUtil.assertDataNodesInStorage(zoneId, Set.of(A, B, C), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios2_4() throws Exception {
        preparePrerequisites();

        int zoneId = getZoneId(ZONE_NAME);

        ZoneState zoneState = distributionZoneManager.zonesState().get(zoneId);

        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 1003);
        zoneState.nodesToAddToDataNodes(Set.of(C), 1007);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(zoneId, 1007);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(zoneId, 1003);

        DistributionZonesTestUtil.assertDataNodesInStorage(zoneId, Set.of(A, B, C), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios3_1() throws Exception {
        preparePrerequisites();

        int zoneId = getZoneId(ZONE_NAME);

        ZoneState zoneState = distributionZoneManager.zonesState().get(zoneId);

        zoneState.nodesToAddToDataNodes(Set.of(D), 1003);
        zoneState.nodesToRemoveFromDataNodes(Set.of(D), 1007);
        zoneState.nodesToAddToDataNodes(Set.of(D), 1009);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(zoneId, 1009);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(zoneId, 1007);

        DistributionZonesTestUtil.assertDataNodesInStorage(zoneId, Set.of(A, B, C, D), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios3_2() throws Exception {
        preparePrerequisites();

        int zoneId = getZoneId(ZONE_NAME);

        ZoneState zoneState = distributionZoneManager.zonesState().get(zoneId);

        zoneState.nodesToAddToDataNodes(Set.of(D), 1003);
        zoneState.nodesToRemoveFromDataNodes(Set.of(D), 1007);
        zoneState.nodesToAddToDataNodes(Set.of(D), 1009);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(zoneId, 1007);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(zoneId, 1009);

        DistributionZonesTestUtil.assertDataNodesInStorage(zoneId, Set.of(A, B, C, D), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios3_3() throws Exception {
        preparePrerequisites();

        int zoneId = getZoneId(ZONE_NAME);

        ZoneState zoneState = distributionZoneManager.zonesState().get(zoneId);

        zoneState.nodesToAddToDataNodes(Set.of(D), 1003);
        zoneState.nodesToRemoveFromDataNodes(Set.of(D), 1007);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(zoneId, 1007);

        zoneState.nodesToAddToDataNodes(Set.of(D), 1009);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(zoneId, 1009);

        DistributionZonesTestUtil.assertDataNodesInStorage(zoneId, Set.of(A, B, C, D), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios3_4() throws Exception {
        preparePrerequisites();

        int zoneId = getZoneId(ZONE_NAME);

        ZoneState zoneState = distributionZoneManager.zonesState().get(zoneId);

        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 1003);
        zoneState.nodesToAddToDataNodes(Set.of(C), 1007);
        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 1009);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(zoneId, 1009);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(zoneId, 1007);

        DistributionZonesTestUtil.assertDataNodesInStorage(zoneId, Set.of(A, B), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios3_5() throws Exception {
        preparePrerequisites();

        int zoneId = getZoneId(ZONE_NAME);

        ZoneState zoneState = distributionZoneManager.zonesState().get(zoneId);

        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 1003);
        zoneState.nodesToAddToDataNodes(Set.of(C), 1007);
        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 1009);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(zoneId, 1007);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(zoneId, 1009);

        DistributionZonesTestUtil.assertDataNodesInStorage(zoneId, Set.of(A, B), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios3_6() throws Exception {
        preparePrerequisites();

        int zoneId = getZoneId(ZONE_NAME);

        ZoneState zoneState = distributionZoneManager.zonesState().get(zoneId);

        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 1003);
        zoneState.nodesToAddToDataNodes(Set.of(C), 1007);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(zoneId, 1007);

        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 1009);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(zoneId, 1009);

        DistributionZonesTestUtil.assertDataNodesInStorage(zoneId, Set.of(A, B), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios4_1() throws Exception {
        preparePrerequisites();

        int zoneId = getZoneId(ZONE_NAME);

        ZoneState zoneState = distributionZoneManager.zonesState().get(zoneId);

        zoneState.nodesToAddToDataNodes(Set.of(D), 1003);
        zoneState.nodesToRemoveFromDataNodes(Set.of(D), 1007);
        zoneState.nodesToAddToDataNodes(Set.of(E), 1009);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(zoneId, 1009);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(zoneId, 1007);

        DistributionZonesTestUtil.assertDataNodesInStorage(zoneId, Set.of(A, B, C, E), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios4_2() throws Exception {
        preparePrerequisites();

        int zoneId = getZoneId(ZONE_NAME);

        ZoneState zoneState = distributionZoneManager.zonesState().get(zoneId);

        zoneState.nodesToAddToDataNodes(Set.of(D), 1003);
        zoneState.nodesToRemoveFromDataNodes(Set.of(D), 1007);
        zoneState.nodesToAddToDataNodes(Set.of(E), 1009);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(zoneId, 1007);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(zoneId, 1009);

        DistributionZonesTestUtil.assertDataNodesInStorage(zoneId, Set.of(A, B, C, E), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios4_3() throws Exception {
        preparePrerequisites();

        int zoneId = getZoneId(ZONE_NAME);

        ZoneState zoneState = distributionZoneManager.zonesState().get(zoneId);

        zoneState.nodesToAddToDataNodes(Set.of(D), 1003);
        zoneState.nodesToRemoveFromDataNodes(Set.of(D), 1007);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(zoneId, 1007);

        zoneState.nodesToAddToDataNodes(Set.of(E), 1009);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(zoneId, 1009);

        DistributionZonesTestUtil.assertDataNodesInStorage(zoneId, Set.of(A, B, C, E), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios4_4() throws Exception {
        preparePrerequisites();

        int zoneId = getZoneId(ZONE_NAME);

        ZoneState zoneState = distributionZoneManager.zonesState().get(zoneId);

        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 1003);
        zoneState.nodesToAddToDataNodes(Set.of(C), 1007);
        zoneState.nodesToRemoveFromDataNodes(Set.of(B), 1009);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(zoneId, 1009);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(zoneId, 1007);

        DistributionZonesTestUtil.assertDataNodesInStorage(zoneId, Set.of(A, C), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios4_5() throws Exception {
        preparePrerequisites();

        int zoneId = getZoneId(ZONE_NAME);

        ZoneState zoneState = distributionZoneManager.zonesState().get(zoneId);

        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 1003);
        zoneState.nodesToAddToDataNodes(Set.of(C), 1007);
        zoneState.nodesToRemoveFromDataNodes(Set.of(B), 1009);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(zoneId, 1007);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(zoneId, 1009);

        DistributionZonesTestUtil.assertDataNodesInStorage(zoneId, Set.of(A, C), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios4_6() throws Exception {
        preparePrerequisites();

        int zoneId = getZoneId(ZONE_NAME);

        ZoneState zoneState = distributionZoneManager.zonesState().get(zoneId);

        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 1003);
        zoneState.nodesToAddToDataNodes(Set.of(C), 1007);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(zoneId, 1007);

        zoneState.nodesToRemoveFromDataNodes(Set.of(B), 1009);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(zoneId, 1009);

        DistributionZonesTestUtil.assertDataNodesInStorage(zoneId, Set.of(A, C), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios5_1() throws Exception {
        preparePrerequisites();

        int zoneId = getZoneId(ZONE_NAME);

        ZoneState zoneState = distributionZoneManager.zonesState().get(zoneId);

        zoneState.nodesToAddToDataNodes(Set.of(D), 1003);
        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 1007);
        zoneState.nodesToAddToDataNodes(Set.of(C), 1009);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(zoneId, 1009);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(zoneId, 1007);

        DistributionZonesTestUtil.assertDataNodesInStorage(zoneId, Set.of(A, B, C, D), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios5_2() throws Exception {
        preparePrerequisites();

        int zoneId = getZoneId(ZONE_NAME);

        ZoneState zoneState = distributionZoneManager.zonesState().get(zoneId);

        zoneState.nodesToAddToDataNodes(Set.of(D), 1003);
        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 1007);
        zoneState.nodesToAddToDataNodes(Set.of(C), 1009);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(zoneId, 1007);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(zoneId, 1009);

        DistributionZonesTestUtil.assertDataNodesInStorage(zoneId, Set.of(A, B, C, D), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios5_3() throws Exception {
        preparePrerequisites();

        int zoneId = getZoneId(ZONE_NAME);

        ZoneState zoneState = distributionZoneManager.zonesState().get(zoneId);

        zoneState.nodesToAddToDataNodes(Set.of(D), 1003);
        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 1007);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(zoneId, 1007);

        zoneState.nodesToAddToDataNodes(Set.of(C), 1009);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(zoneId, 1009);

        DistributionZonesTestUtil.assertDataNodesInStorage(zoneId, Set.of(A, B, C, D), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios5_4() throws Exception {
        preparePrerequisites();

        int zoneId = getZoneId(ZONE_NAME);

        ZoneState zoneState = distributionZoneManager.zonesState().get(zoneId);

        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 1003);
        zoneState.nodesToAddToDataNodes(Set.of(D), 1007);
        zoneState.nodesToRemoveFromDataNodes(Set.of(D), 1009);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(zoneId, 1009);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(zoneId, 1007);

        DistributionZonesTestUtil.assertDataNodesInStorage(zoneId, Set.of(A, B), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios5_5() throws Exception {
        preparePrerequisites();

        int zoneId = getZoneId(ZONE_NAME);

        ZoneState zoneState = distributionZoneManager.zonesState().get(zoneId);

        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 1003);
        zoneState.nodesToAddToDataNodes(Set.of(D), 1007);
        zoneState.nodesToRemoveFromDataNodes(Set.of(D), 1009);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(zoneId, 1007);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(zoneId, 1009);

        DistributionZonesTestUtil.assertDataNodesInStorage(zoneId, Set.of(A, B), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios5_6() throws Exception {
        preparePrerequisites();

        int zoneId = getZoneId(ZONE_NAME);

        ZoneState zoneState = distributionZoneManager.zonesState().get(zoneId);

        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 1003);
        zoneState.nodesToAddToDataNodes(Set.of(D), 1007);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(zoneId, 1007);

        zoneState.nodesToRemoveFromDataNodes(Set.of(D), 1009);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(zoneId, 1009);

        DistributionZonesTestUtil.assertDataNodesInStorage(zoneId, Set.of(A, B), keyValueStorage);
    }

    @Test
    void testZoneStateAddRemoveNodesPreservesDuplicationsOfNodes() {
        StripedScheduledThreadPoolExecutor executor = createZoneManagerExecutor(
                1,
                new NamedThreadFactory("test-dst-zones-scheduler", LOG)
        );

        try {
            ZoneState zoneState = new ZoneState(executor);

            zoneState.nodesToAddToDataNodes(Set.of(A, B), 1);
            zoneState.nodesToAddToDataNodes(Set.of(A, B), 2);

            List<Node> nodes = zoneState.nodesToBeAddedToDataNodes(0, 2);

            nodes.sort(Comparator.comparing(Node::nodeName));

            assertEquals(List.of(A, A, B, B), nodes);

            zoneState.nodesToRemoveFromDataNodes(Set.of(C, D), 3);
            zoneState.nodesToRemoveFromDataNodes(Set.of(C, D), 4);

            nodes = zoneState.nodesToBeRemovedFromDataNodes(2, 4);

            nodes.sort(Comparator.comparing(Node::nodeName));

            assertEquals(List.of(C, C, D, D), nodes);
        } finally {
            IgniteUtils.shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS);
        }
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

        LogicalNode a = new LogicalNode(A.nodeId(), A.nodeName(), new NetworkAddress("localhost", 123));

        LogicalNode b = new LogicalNode(B.nodeId(), B.nodeName(), new NetworkAddress("localhost", 123));

        LogicalNode c = new LogicalNode(C.nodeId(), C.nodeName(), new NetworkAddress("localhost", 123));

        LogicalNode d = new LogicalNode(D.nodeId(), D.nodeName(), new NetworkAddress("localhost", 123));

        LogicalNode e = new LogicalNode(E.nodeId(), E.nodeName(), new NetworkAddress("localhost", 123));

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

        long scaleUpChangeTriggerKey = bytesToLongKeepingOrder(
                keyValueStorage.get(zoneScaleUpChangeTriggerKey(zoneId).bytes()).value()
        );

        long scaleDownChangeTriggerKey = bytesToLongKeepingOrder(
                keyValueStorage.get(zoneScaleDownChangeTriggerKey(zoneId).bytes()).value()
        );

        assertEquals(scaleUpChangeTriggerKey, scaleDownChangeTriggerKey,
                "zoneScaleUpChangeTriggerKey and zoneScaleDownChangeTriggerKey are not equal");
    }
}
