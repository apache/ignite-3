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

import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_FILTER;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_ZONE_ID;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_ZONE_NAME;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertDataNodesForZone;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertDataNodesForZoneWithAttributes;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertLogicalTopology;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertZoneScaleDownChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertZoneScaleUpChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.deployWatchesAndUpdateMetaStorageRevision;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.mockVaultZonesLogicalTopologyKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleDownChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleUpChangeTriggerKey;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.util.ByteUtils.longToBytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.NamedConfigurationTree;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager.NodeWithAttributes;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager.ZoneState;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneChange;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneConfiguration;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneView;
import org.apache.ignite.internal.metastorage.server.If;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Test scenarios for the distribution zone scale up and scale down.
 */
public class DistributionZoneManagerScaleUpTest extends BaseDistributionZoneManagerTest {
    private static final String ZONE_NAME = "zone1";

    private static final LogicalNode NODE_1 = new LogicalNode("1", "node1", new NetworkAddress("localhost", 123));

    private static final LogicalNode NODE_2 = new LogicalNode("2", "node2", new NetworkAddress("localhost", 123));

    private static final NodeWithAttributes A  = new NodeWithAttributes("A", Collections.emptyMap());

    private static final NodeWithAttributes B  = new NodeWithAttributes("B", Collections.emptyMap());

    private static final NodeWithAttributes C  = new NodeWithAttributes("C", Collections.emptyMap());

    private static final NodeWithAttributes D  = new NodeWithAttributes("D", Collections.emptyMap());

    private static final NodeWithAttributes E  = new NodeWithAttributes("E", Collections.emptyMap());

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-19255")
    void testDataNodesPropagationAfterScaleUpTriggered() throws Exception {
        topology.putNode(NODE_1);

        Set<LogicalNode> clusterNodes = Set.of(NODE_1);

        mockVaultZonesLogicalTopologyKey(clusterNodes, vaultMgr);

        startDistributionZoneManager();

        assertDataNodesForZone(DEFAULT_ZONE_ID, clusterNodes, keyValueStorage);

        topology.putNode(NODE_2);

        Set<LogicalNode> clusterNodes2 = Set.of(NODE_1, NODE_2);

        assertLogicalTopology(clusterNodes2, keyValueStorage);

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME)
                        .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                        .dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE)
                        .build()
        ).get();

        assertDataNodesForZone(1, clusterNodes2, keyValueStorage);

        assertZoneScaleUpChangeTriggerKey(1, 1, keyValueStorage);
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-19255")
    @Test
    void testDataNodesPropagationAfterScaleUpTriggeredOnNewCluster() throws Exception {
        topology.putNode(NODE_1);

        startDistributionZoneManager();

        distributionZoneManager.alterZone(DEFAULT_ZONE_NAME,
                new DistributionZoneConfigurationParameters.Builder(DEFAULT_ZONE_NAME)
                        .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                        .dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE)
                        .build()
        ).get();

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE).build()
        ).get();

        int zoneId = distributionZoneManager.getZoneId(ZONE_NAME);

        assertDataNodesForZone(DEFAULT_ZONE_ID, Set.of(NODE_1), keyValueStorage);
        assertDataNodesForZone(zoneId, Set.of(NODE_1), keyValueStorage);
    }

    @Test
    void testDataNodesPropagationAfterScaleDownTriggered() throws Exception {
        topology.putNode(NODE_1);

        topology.putNode(NODE_2);

        Set<LogicalNode> clusterNodes = Set.of(NODE_1, NODE_2);

        mockVaultZonesLogicalTopologyKey(clusterNodes, vaultMgr);

        startDistributionZoneManager();

        assertDataNodesForZone(DEFAULT_ZONE_ID, clusterNodes, keyValueStorage);

        topology.removeNodes(Set.of(NODE_2));

        Set<LogicalNode> clusterNodes2 = Set.of(NODE_1);

        assertLogicalTopology(clusterNodes2, keyValueStorage);

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE).build()
        ).get();

        assertZoneScaleDownChangeTriggerKey(1, 1, keyValueStorage);

        assertDataNodesForZone(1, clusterNodes2, keyValueStorage);
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-19255")
    @Test
    void testDataNodesPropagationForDefaultZoneAfterScaleUpTriggered() throws Exception {
        topology.putNode(NODE_1);

        Set<LogicalNode> clusterNodes = Set.of(NODE_1);

        mockVaultZonesLogicalTopologyKey(clusterNodes, vaultMgr);

        startDistributionZoneManager();

        assertDataNodesForZone(DEFAULT_ZONE_ID, clusterNodes, keyValueStorage);

        topology.putNode(NODE_2);

        Set<LogicalNode> clusterNodes2 = Set.of(NODE_1, NODE_2);

        assertLogicalTopology(clusterNodes2, keyValueStorage);

        distributionZoneManager.alterZone(
                DEFAULT_ZONE_NAME,
                new DistributionZoneConfigurationParameters.Builder(DEFAULT_ZONE_NAME)
                        .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                        .dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE)
                        .build()
        ).get();

        assertDataNodesForZone(DEFAULT_ZONE_ID, clusterNodes2, keyValueStorage);
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-19255")
    @Test
    void testDataNodesPropagationForDefaultZoneAfterScaleDownTriggered() throws Exception {
        topology.putNode(NODE_1);

        topology.putNode(NODE_2);

        Set<LogicalNode> clusterNodes = Set.of(NODE_1, NODE_2);

        mockVaultZonesLogicalTopologyKey(clusterNodes, vaultMgr);

        startDistributionZoneManager();

        assertDataNodesForZone(DEFAULT_ZONE_ID, clusterNodes, keyValueStorage);

        topology.removeNodes(Set.of(NODE_2));

        Set<LogicalNode> clusterNodes2 = Set.of(NODE_1);

        assertLogicalTopology(clusterNodes2, keyValueStorage);

        distributionZoneManager.alterZone(
                DEFAULT_ZONE_NAME,
                new DistributionZoneConfigurationParameters.Builder(DEFAULT_ZONE_NAME)
                        .dataNodesAutoAdjustScaleUp(INFINITE_TIMER_VALUE)
                        .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                        .build()
        ).get();

        assertDataNodesForZone(DEFAULT_ZONE_ID, clusterNodes2, keyValueStorage);
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-19255")
    @Test
    void testDropZoneDoNotPropagateDataNodesAfterScaleUp() throws Exception {
        topology.putNode(NODE_1);

        Set<LogicalNode> clusterNodes = Set.of(NODE_1);

        mockVaultZonesLogicalTopologyKey(clusterNodes, vaultMgr);

        startDistributionZoneManager();

        topology.putNode(NODE_2);

        Set<LogicalNode> clusterNodes2 = Set.of(NODE_1, NODE_2);
        Set<String> clusterNodesNames2 = Set.of(NODE_1.name(), NODE_2.name());

        assertLogicalTopology(clusterNodes2, keyValueStorage);

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE).build()
        ).get();

        assertDataNodesForZone(1, clusterNodes2, keyValueStorage);

        assertZoneScaleUpChangeTriggerKey(1, 1, keyValueStorage);

        distributionZoneManager.dropZone(ZONE_NAME).get();

        assertNotEqualsDataNodesForZone(1, clusterNodesNames2);
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-19255")
    @Test
    void testDropZoneDoNotPropagateDataNodesAfterScaleDown() throws Exception {
        topology.putNode(NODE_1);

        topology.putNode(NODE_2);

        Set<LogicalNode> clusterNodes = Set.of(NODE_1, NODE_2);

        mockVaultZonesLogicalTopologyKey(clusterNodes, vaultMgr);

        startDistributionZoneManager();

        topology.removeNodes(Set.of(NODE_2));

        Set<LogicalNode> clusterNodes2 = Set.of(NODE_1);
        Set<String> clusterNodesNames2 = Set.of(NODE_1.name());

        assertLogicalTopology(clusterNodes2, keyValueStorage);

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE).build()
        ).get();

        assertDataNodesForZone(1, clusterNodes2, keyValueStorage);

        assertZoneScaleDownChangeTriggerKey(1, 1, keyValueStorage);

        distributionZoneManager.dropZone(ZONE_NAME).get();

        assertNotEqualsDataNodesForZone(1, clusterNodesNames2);
    }

    @Test
    void testTwoScaleUpTimersSecondTimerRunFirst() throws Exception {
        preparePrerequisites();

        NamedConfigurationTree<DistributionZoneConfiguration, DistributionZoneView, DistributionZoneChange> zones =
                zonesConfiguration.distributionZones();

        DistributionZoneView zoneView = zones.value().get(0);

        CountDownLatch in1 = new CountDownLatch(1);
        CountDownLatch in2 = new CountDownLatch(1);
        CountDownLatch out1 = new CountDownLatch(1);
        CountDownLatch out2 = new CountDownLatch(1);

        distributionZoneManager.scheduleTimers(
                zoneView,
                Set.of(D),
                Set.of(),
                2,
                (zoneId, revision) -> {
                    try {
                        in1.await();
                    } catch (InterruptedException e) {
                        fail();
                    }

                    return testSaveDataNodesOnScaleUp(zoneId, revision).thenRun(out1::countDown);
                },
                (t1, t2) -> null
        );

        // Assert that first task was run and event about adding node "D" with revision 2 was added
        // to the topologyAugmentationMap of the zone.
        assertTrue(
                waitForCondition(
                        () -> distributionZoneManager.zonesTimers().get(1).topologyAugmentationMap().containsKey(2L),
                        1000
                )
        );

        distributionZoneManager.scheduleTimers(
                zoneView,
                Set.of(E),
                Set.of(),
                3,
                (zoneId, revision) -> {
                    try {
                        in2.await();
                    } catch (InterruptedException e) {
                        fail();
                    }

                    return testSaveDataNodesOnScaleUp(zoneId, revision).thenRun(() -> {
                        try {
                            out2.await();
                        } catch (InterruptedException e) {
                            fail();
                        }
                    });
                },
                (t1, t2) -> null
        );

        // Assert that second task was run and event about adding node "E" with revision 3 was added
        // to the topologyAugmentationMap of the zone.
        assertTrue(
                waitForCondition(
                        () -> distributionZoneManager.zonesTimers().get(1).topologyAugmentationMap().containsKey(3L),
                        1000
                )
        );


        //Second task is propagating data nodes first.
        in2.countDown();

        assertZoneScaleUpChangeTriggerKey(3, 1, keyValueStorage);

        assertDataNodesForZoneWithAttributes(1, Set.of(A, B, C, D, E), keyValueStorage);

        out2.countDown();

        in1.countDown();

        //Waiting for the first scheduler ends it work.
        out1.countDown();

        // Assert that nothing has been changed.
        assertZoneScaleUpChangeTriggerKey(3, 1, keyValueStorage);

        assertDataNodesForZoneWithAttributes(1, Set.of(A, B, C, D, E), keyValueStorage);
    }

    @Test
    void testTwoScaleDownTimersSecondTimerRunFirst() throws Exception {
        preparePrerequisites();

        NamedConfigurationTree<DistributionZoneConfiguration, DistributionZoneView, DistributionZoneChange> zones =
                zonesConfiguration.distributionZones();

        DistributionZoneView zoneView = zones.value().get(0);

        CountDownLatch in1 = new CountDownLatch(1);
        CountDownLatch in2 = new CountDownLatch(1);
        CountDownLatch out1 = new CountDownLatch(1);
        CountDownLatch out2 = new CountDownLatch(1);

        distributionZoneManager.scheduleTimers(
                zoneView,
                Set.of(),
                Set.of(B),
                2,
                (t1, t2) -> null,
                (zoneId, revision) -> {
                    try {
                        in1.await();
                    } catch (InterruptedException e) {
                        fail();
                    }

                    return testSaveDataNodesOnScaleDown(zoneId, revision).thenRun(out1::countDown);
                }
        );

        // Assert that first task was run and event about removing node "B" with revision 2 was added
        // to the topologyAugmentationMap of the zone.
        assertTrue(
                waitForCondition(
                        () -> distributionZoneManager.zonesTimers().get(1).topologyAugmentationMap().containsKey(2L),
                        1000
                )
        );

        distributionZoneManager.scheduleTimers(
                zoneView,
                Set.of(),
                Set.of(C),
                3,
                (t1, t2) -> null,
                (zoneId, revision) -> {
                    try {
                        in2.await();
                    } catch (InterruptedException e) {
                        fail();
                    }

                    return testSaveDataNodesOnScaleDown(zoneId, revision).thenRun(() -> {
                        try {
                            out2.await();
                        } catch (InterruptedException e) {
                            fail();
                        }
                    });
                }
        );

        // Assert that second task was run and event about removing node "C" with revision 3 was added
        // to the topologyAugmentationMap of the zone.
        assertTrue(
                waitForCondition(
                        () -> distributionZoneManager.zonesTimers().get(1).topologyAugmentationMap().containsKey(3L),
                        1000
                )
        );


        //Second task is propagating data nodes first.
        in2.countDown();

        assertZoneScaleDownChangeTriggerKey(3, 1, keyValueStorage);

        assertDataNodesForZoneWithAttributes(1, Set.of(A), keyValueStorage);

        out2.countDown();

        in1.countDown();

        //Waiting for the first scheduler ends it work.
        out1.countDown();

        // Assert that nothing has been changed.
        assertZoneScaleDownChangeTriggerKey(3, 1, keyValueStorage);

        assertDataNodesForZoneWithAttributes(1, Set.of(A), keyValueStorage);
    }

    @Test
    void testTwoScaleUpTimersFirstTimerRunFirst() throws Exception {
        preparePrerequisites();

        NamedConfigurationTree<DistributionZoneConfiguration, DistributionZoneView, DistributionZoneChange> zones =
                zonesConfiguration.distributionZones();

        DistributionZoneView zoneView = zones.value().get(0);

        CountDownLatch in1 = new CountDownLatch(1);
        CountDownLatch in2 = new CountDownLatch(1);
        CountDownLatch out1 = new CountDownLatch(1);

        distributionZoneManager.scheduleTimers(
                zoneView,
                Set.of(D),
                Set.of(),
                2,
                (zoneId, revision) -> {
                    in1.countDown();

                    return testSaveDataNodesOnScaleUp(zoneId, revision).thenRun(() -> {
                        try {
                            out1.await();
                        } catch (InterruptedException e) {
                            fail();
                        }
                    });
                },
                (t1, t2) -> null
        );

        // Waiting for the first task to be run. We have to do that to be sure that watch events,
        // which we try to emulate, are handled sequentially.
        in1.await();

        distributionZoneManager.scheduleTimers(
                zoneView,
                Set.of(E),
                Set.of(),
                3,
                (zoneId, revision) -> {
                    try {
                        in2.await();
                    } catch (InterruptedException e) {
                        fail();
                    }

                    return testSaveDataNodesOnScaleUp(zoneId, revision);
                },
                (t1, t2) -> null
        );

        // Assert that second task was run and event about adding node "E" with revision 3 was added
        // to the topologyAugmentationMap of the zone.
        assertTrue(
                waitForCondition(
                        () -> distributionZoneManager.zonesTimers().get(1).topologyAugmentationMap().containsKey(3L),
                        1000
                )
        );

        assertZoneScaleUpChangeTriggerKey(2, 1, keyValueStorage);

        assertDataNodesForZoneWithAttributes(1, Set.of(A, B, C, D), keyValueStorage);

        // Second task is run and we await that data nodes will be changed from ["A", "B", "C", "D"] to ["A", "B", "C", "D", "E"]
        in2.countDown();

        assertZoneScaleUpChangeTriggerKey(3, 1, keyValueStorage);

        assertDataNodesForZoneWithAttributes(1, Set.of(A, B, C, D, E), keyValueStorage);

        out1.countDown();
    }

    @Test
    void testTwoScaleDownTimersFirstTimerRunFirst() throws Exception {
        preparePrerequisites();

        NamedConfigurationTree<DistributionZoneConfiguration, DistributionZoneView, DistributionZoneChange> zones =
                zonesConfiguration.distributionZones();

        DistributionZoneView zoneView = zones.value().get(0);

        CountDownLatch in1 = new CountDownLatch(1);
        CountDownLatch in2 = new CountDownLatch(1);
        CountDownLatch out1 = new CountDownLatch(1);

        distributionZoneManager.scheduleTimers(
                zoneView,
                Set.of(),
                Set.of(B),
                2,
                (t1, t2) -> null,
                (zoneId, revision) -> {
                    in1.countDown();

                    return testSaveDataNodesOnScaleDown(zoneId, revision).thenRun(() -> {
                        try {
                            out1.await();
                        } catch (InterruptedException e) {
                            fail();
                        }
                    });
                }
        );

        // Waiting for the first task to be run. We have to do that to be sure that watch events,
        // which we try to emulate, are handled sequentially.
        in1.await();

        distributionZoneManager.scheduleTimers(
                zoneView,
                Set.of(),
                Set.of(C),
                3,
                (t1, t2) -> null,
                (zoneId, revision) -> {
                    try {
                        in2.await();
                    } catch (InterruptedException e) {
                        fail();
                    }

                    return testSaveDataNodesOnScaleDown(zoneId, revision);
                }
        );

        // Assert that second task was run and event about removing node "C" with revision 3 was added
        // to the topologyAugmentationMap of the zone.
        assertTrue(
                waitForCondition(
                        () -> distributionZoneManager.zonesTimers().get(1).topologyAugmentationMap().containsKey(3L),
                        1000
                )
        );

        assertZoneScaleDownChangeTriggerKey(2, 1, keyValueStorage);

        assertDataNodesForZoneWithAttributes(1, Set.of(A, C), keyValueStorage);

        // Second task is run and we await that data nodes will be changed from ["A", "C"] to ["A"]
        in2.countDown();

        assertZoneScaleDownChangeTriggerKey(3, 1, keyValueStorage);

        assertDataNodesForZoneWithAttributes(1, Set.of(A), keyValueStorage);

        out1.countDown();
    }

    @Test
    void testEmptyDataNodesOnStart() throws Exception {
        startDistributionZoneManager();

        assertLogicalTopology(Set.of(), keyValueStorage);

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjustScaleUp(0).build()
        ).get();

        assertDataNodesForZone(1, Set.of(), keyValueStorage);

        assertZoneScaleDownChangeTriggerKey(1, 1, keyValueStorage);

        topology.putNode(NODE_1);

        assertLogicalTopology(Set.of(NODE_1), keyValueStorage);

        assertDataNodesForZone(1, Set.of(NODE_1), keyValueStorage);
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-19255")
    @Test
    void testUpdateZoneScaleUpTriggersDataNodePropagation() throws Exception {
        startDistributionZoneManager();

        assertLogicalTopology(Set.of(), keyValueStorage);

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjustScaleUp(100).build()
        ).get();

        assertDataNodesForZone(1, Set.of(), keyValueStorage);

        assertZoneScaleDownChangeTriggerKey(1, 1, keyValueStorage);

        topology.putNode(NODE_1);

        assertLogicalTopology(Set.of(NODE_1), keyValueStorage);

        assertDataNodesForZone(1, Set.of(), keyValueStorage);

        distributionZoneManager.alterZone(
                ZONE_NAME,
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjustScaleUp(0).build()
        ).get();

        assertDataNodesForZone(1, Set.of(NODE_1), keyValueStorage);
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-19255")
    @Test
    void testUpdateZoneScaleDownTriggersDataNodePropagation() throws Exception {
        topology.putNode(NODE_1);

        mockVaultZonesLogicalTopologyKey(Set.of(NODE_1), vaultMgr);

        startDistributionZoneManager();

        assertLogicalTopology(Set.of(NODE_1), keyValueStorage);

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjustScaleDown(100).build()
        ).get();

        assertDataNodesForZone(1, Set.of(NODE_1), keyValueStorage);

        assertZoneScaleDownChangeTriggerKey(1, 1, keyValueStorage);

        topology.removeNodes(Set.of(NODE_1));

        assertDataNodesForZone(1, Set.of(NODE_1), keyValueStorage);

        distributionZoneManager.alterZone(
                ZONE_NAME,
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjustScaleDown(0).build()
        ).get();

        assertDataNodesForZone(1, Set.of(), keyValueStorage);
    }

    @Test
    void testCleanUpAfterSchedulers() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToAddToDataNodes(Set.of(D), 3);
        zoneState.nodesToAddToDataNodes(Set.of(E), 4);
        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 7);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 3);

        assertTrue(zoneState.topologyAugmentationMap().containsKey(3L));
        assertTrue(zoneState.topologyAugmentationMap().containsKey(4L));
        assertTrue(zoneState.topologyAugmentationMap().containsKey(7L));

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 4);

        assertTrue(zoneState.topologyAugmentationMap().containsKey(3L));
        assertTrue(zoneState.topologyAugmentationMap().containsKey(4L));
        assertTrue(zoneState.topologyAugmentationMap().containsKey(7L));

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 7);

        assertFalse(zoneState.topologyAugmentationMap().containsKey(3L));
        assertFalse(zoneState.topologyAugmentationMap().containsKey(4L));
        assertTrue(zoneState.topologyAugmentationMap().containsKey(7L));

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 15);

        assertTrue(zoneState.topologyAugmentationMap().isEmpty());
        assertDataNodesForZoneWithAttributes(1, Set.of(A, B, D, E), keyValueStorage);
    }

    @Test
    void testScaleUpSetToMaxInt() throws Exception {
        startDistributionZoneManager();

        assertLogicalTopology(Set.of(), keyValueStorage);

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjustScaleUp(100).build()
        ).get();

        assertDataNodesForZone(1, Set.of(), keyValueStorage);

        assertZoneScaleDownChangeTriggerKey(1, 1, keyValueStorage);

        topology.putNode(NODE_1);

        assertLogicalTopology(Set.of(NODE_1), keyValueStorage);

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        assertNull(zoneState.scaleUpTask());

        assertTrue(waitForCondition(() -> zoneState.scaleUpTask() != null, 1000L));

        distributionZoneManager.alterZone(
                ZONE_NAME,
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjustScaleUp(INFINITE_TIMER_VALUE).build()
        ).get();

        assertTrue(waitForCondition(() -> zoneState.scaleUpTask().isCancelled(), 1000L));
    }

    @Test
    void testScaleDownSetToMaxInt() throws Exception {
        topology.putNode(NODE_1);

        mockVaultZonesLogicalTopologyKey(Set.of(NODE_1), vaultMgr);

        startDistributionZoneManager();

        assertLogicalTopology(Set.of(NODE_1), keyValueStorage);

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjustScaleDown(100).build()
        ).get();

        assertDataNodesForZone(1, Set.of(NODE_1), keyValueStorage);

        assertZoneScaleDownChangeTriggerKey(1, 1, keyValueStorage);

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        assertNull(zoneState.scaleDownTask());

        topology.removeNodes(Set.of(NODE_1));

        assertTrue(waitForCondition(() -> zoneState.scaleDownTask() != null, 1000L));

        distributionZoneManager.alterZone(
                ZONE_NAME,
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE).build()
        ).get();

        assertTrue(waitForCondition(() -> zoneState.scaleDownTask().isCancelled(), 1000L));
    }

    @Test
    void testScaleUpDidNotChangeDataNodesWhenTriggerKeyWasConcurrentlyChanged() throws Exception {
        startDistributionZoneManager();

        assertLogicalTopology(Set.of(), keyValueStorage);

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE).build()
        ).get();

        assertDataNodesForZone(1, Set.of(), keyValueStorage);

        assertZoneScaleDownChangeTriggerKey(1, 1, keyValueStorage);

        doAnswer(invocation -> {
            If iif = invocation.getArgument(0);

            // Emulate a situation when one of the scale up keys gets concurrently updated during a Meta Storage invoke. We then expect
            // that the invoke call will be retried.
            byte[] key = zoneScaleUpChangeTriggerKey(1).bytes();

            if (Arrays.stream(iif.cond().keys()).anyMatch(k -> Arrays.equals(key, k))) {
                keyValueStorage.put(key, longToBytes(100));
            }

            return invocation.callRealMethod();
        }).when(keyValueStorage).invoke(any());

        topology.putNode(NODE_1);

        assertLogicalTopology(Set.of(NODE_1), keyValueStorage);

        assertZoneScaleUpChangeTriggerKey(100, 1, keyValueStorage);

        assertDataNodesForZone(1, Set.of(), keyValueStorage);
    }

    @Test
    void testScaleDownDidNotChangeDataNodesWhenTriggerKeyWasConcurrentlyChanged() throws Exception {
        topology.putNode(NODE_1);

        mockVaultZonesLogicalTopologyKey(Set.of(NODE_1), vaultMgr);

        startDistributionZoneManager();

        assertLogicalTopology(Set.of(NODE_1), keyValueStorage);

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE).build()
        ).get();

        assertDataNodesForZone(1, Set.of(NODE_1), keyValueStorage);

        assertZoneScaleDownChangeTriggerKey(1, 1, keyValueStorage);

        doAnswer(invocation -> {
            If iif = invocation.getArgument(0);

            // Emulate a situation when one of the scale down keys gets concurrently updated during a Meta Storage invoke. We then expect
            // that the invoke call will be retried.
            byte[] key = zoneScaleDownChangeTriggerKey(1).bytes();

            if (Arrays.stream(iif.cond().keys()).anyMatch(k -> Arrays.equals(key, k))) {
                keyValueStorage.put(key, longToBytes(100));
            }

            return invocation.callRealMethod();
        }).when(keyValueStorage).invoke(any());

        topology.removeNodes(Set.of(NODE_1));

        assertDataNodesForZone(1, Set.of(NODE_1), keyValueStorage);

        assertZoneScaleDownChangeTriggerKey(100, 1, keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios1_1() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToAddToDataNodes(Set.of(D), 3);
        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 7);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 3);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 7);

        assertDataNodesForZoneWithAttributes(1, Set.of(A, B, D), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios1_2() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToAddToDataNodes(Set.of(D), 3);
        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 7);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 7);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 3);

        assertDataNodesForZoneWithAttributes(1, Set.of(A, B, D), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios1_3() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 3);
        zoneState.nodesToAddToDataNodes(Set.of(D), 7);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 3);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 7);

        assertDataNodesForZoneWithAttributes(1, Set.of(A, B, D), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios1_4() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 3);
        zoneState.nodesToAddToDataNodes(Set.of(D), 7);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 7);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 3);

        assertDataNodesForZoneWithAttributes(1, Set.of(A, B, D), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios2_1() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToAddToDataNodes(Set.of(D), 3);
        zoneState.nodesToRemoveFromDataNodes(Set.of(D), 7);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 3);
        assertDataNodesForZoneWithAttributes(1, Set.of(A, B, C, D), keyValueStorage);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 7);

        assertDataNodesForZoneWithAttributes(1, Set.of(A, B, C), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios2_2() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToAddToDataNodes(Set.of(D), 3);
        zoneState.nodesToRemoveFromDataNodes(Set.of(D), 7);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 7);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 3);

        assertDataNodesForZoneWithAttributes(1, Set.of(A, B, C), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios2_3() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 3);
        zoneState.nodesToAddToDataNodes(Set.of(C), 7);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 3);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 7);

        assertDataNodesForZoneWithAttributes(1, Set.of(A, B, C), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios2_4() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 3);
        zoneState.nodesToAddToDataNodes(Set.of(C), 7);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 7);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 3);

        assertDataNodesForZoneWithAttributes(1, Set.of(A, B, C), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios3_1() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToAddToDataNodes(Set.of(D), 3);
        zoneState.nodesToRemoveFromDataNodes(Set.of(D), 7);
        zoneState.nodesToAddToDataNodes(Set.of(D), 9);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 9);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 7);

        assertDataNodesForZoneWithAttributes(1, Set.of(A, B, C, D), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios3_2() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToAddToDataNodes(Set.of(D), 3);
        zoneState.nodesToRemoveFromDataNodes(Set.of(D), 7);
        zoneState.nodesToAddToDataNodes(Set.of(D), 9);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 7);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 9);

        assertDataNodesForZoneWithAttributes(1, Set.of(A, B, C, D), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios3_3() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToAddToDataNodes(Set.of(D), 3);
        zoneState.nodesToRemoveFromDataNodes(Set.of(D), 7);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 7);

        zoneState.nodesToAddToDataNodes(Set.of(D), 9);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 9);

        assertDataNodesForZoneWithAttributes(1, Set.of(A, B, C, D), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios3_4() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 3);
        zoneState.nodesToAddToDataNodes(Set.of(C), 7);
        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 9);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 9);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 7);

        assertDataNodesForZoneWithAttributes(1, Set.of(A, B), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios3_5() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 3);
        zoneState.nodesToAddToDataNodes(Set.of(C), 7);
        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 9);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 7);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 9);

        assertDataNodesForZoneWithAttributes(1, Set.of(A, B), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios3_6() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 3);
        zoneState.nodesToAddToDataNodes(Set.of(C), 7);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 7);

        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 9);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 9);

        assertDataNodesForZoneWithAttributes(1, Set.of(A, B), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios4_1() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToAddToDataNodes(Set.of(D), 3);
        zoneState.nodesToRemoveFromDataNodes(Set.of(D), 7);
        zoneState.nodesToAddToDataNodes(Set.of(E), 9);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 9);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 7);

        assertDataNodesForZoneWithAttributes(1, Set.of(A, B, C, E), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios4_2() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToAddToDataNodes(Set.of(D), 3);
        zoneState.nodesToRemoveFromDataNodes(Set.of(D), 7);
        zoneState.nodesToAddToDataNodes(Set.of(E), 9);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 7);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 9);

        assertDataNodesForZoneWithAttributes(1, Set.of(A, B, C, E), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios4_3() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToAddToDataNodes(Set.of(D), 3);
        zoneState.nodesToRemoveFromDataNodes(Set.of(D), 7);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 7);

        zoneState.nodesToAddToDataNodes(Set.of(E), 9);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 9);

        assertDataNodesForZoneWithAttributes(1, Set.of(A, B, C, E), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios4_4() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 3);
        zoneState.nodesToAddToDataNodes(Set.of(C), 7);
        zoneState.nodesToRemoveFromDataNodes(Set.of(B), 9);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 9);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 7);

        assertDataNodesForZoneWithAttributes(1, Set.of(A, C), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios4_5() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 3);
        zoneState.nodesToAddToDataNodes(Set.of(C), 7);
        zoneState.nodesToRemoveFromDataNodes(Set.of(B), 9);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 7);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 9);

        assertDataNodesForZoneWithAttributes(1, Set.of(A, C), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios4_6() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 3);
        zoneState.nodesToAddToDataNodes(Set.of(C), 7);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 7);

        zoneState.nodesToRemoveFromDataNodes(Set.of(B), 9);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 9);

        assertDataNodesForZoneWithAttributes(1, Set.of(A, C), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios5_1() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToAddToDataNodes(Set.of(D), 3);
        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 7);
        zoneState.nodesToAddToDataNodes(Set.of(C), 9);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 9);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 7);

        assertDataNodesForZoneWithAttributes(1, Set.of(A, B, C, D), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios5_2() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToAddToDataNodes(Set.of(D), 3);
        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 7);
        zoneState.nodesToAddToDataNodes(Set.of(C), 9);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 7);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 9);

        assertDataNodesForZoneWithAttributes(1, Set.of(A, B, C, D), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios5_3() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToAddToDataNodes(Set.of(D), 3);
        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 7);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 7);

        zoneState.nodesToAddToDataNodes(Set.of(C), 9);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 9);

        assertDataNodesForZoneWithAttributes(1, Set.of(A, B, C, D), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios5_4() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 3);
        zoneState.nodesToAddToDataNodes(Set.of(D), 7);
        zoneState.nodesToRemoveFromDataNodes(Set.of(D), 9);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 9);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 7);

        assertDataNodesForZoneWithAttributes(1, Set.of(A, B), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios5_5() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 3);
        zoneState.nodesToAddToDataNodes(Set.of(D), 7);
        zoneState.nodesToRemoveFromDataNodes(Set.of(D), 9);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 7);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 9);

        assertDataNodesForZoneWithAttributes(1, Set.of(A, B), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios5_6() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 3);
        zoneState.nodesToAddToDataNodes(Set.of(D), 7);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 7);

        zoneState.nodesToRemoveFromDataNodes(Set.of(D), 9);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 9);

        assertDataNodesForZoneWithAttributes(1, Set.of(A, B), keyValueStorage);
    }

    @Test
    void testZoneStateAddRemoveNodesPreservesDuplicationsOfNodes() {
        ZoneState zoneState = new ZoneState(new ScheduledThreadPoolExecutor(1));

        zoneState.nodesToAddToDataNodes(Set.of(A, B), 1);
        zoneState.nodesToAddToDataNodes(Set.of(A, B), 2);

        List<NodeWithAttributes> nodes = zoneState.nodesToBeAddedToDataNodes(0, 2);

        nodes.sort((a, b) -> a.nodeName.compareTo(b.nodeName()));

        assertEquals(List.of(A, A, B, B), nodes);

        zoneState.nodesToRemoveFromDataNodes(Set.of(C, D), 3);
        zoneState.nodesToRemoveFromDataNodes(Set.of(C, D), 4);

        nodes = zoneState.nodesToBeRemovedFromDataNodes(2, 4);

        nodes.sort((a, b) -> a.nodeName.compareTo(b.nodeName()));

        assertEquals(List.of(C, C, D, D), nodes);
    }

    @Test
    void testFilterOnScaleUp() throws Exception {
        NodeWithAttributes a1  = new NodeWithAttributes("A1", Map.of("region", "US", "storage", "SSD", "dataRegionSize", "10"));
        NodeWithAttributes a2  = new NodeWithAttributes("A2", Map.of("region", "EU", "storage", "HHD", "dataRegionSize", "30"));
        NodeWithAttributes a3  = new NodeWithAttributes("A3", Map.of("region", "CN", "storage", "SSD", "dataRegionSize", "20"));

        String filter = "$[?(@.storage == 'SSD' || @.region == 'US')]";

        preparePrerequisites(filter);

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToAddToDataNodes(Set.of(a1, a2, a3), 7);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 8);

        assertDataNodesForZoneWithAttributes(1, Set.of(a1, a3), keyValueStorage, filter);
    }

    @Test
    void testFilterOnScaleDown() throws Exception {
        NodeWithAttributes a1  = new NodeWithAttributes("A1", Map.of("region", "US", "storage", "SSD", "dataRegionSize", "10"));
        NodeWithAttributes a2  = new NodeWithAttributes("A2", Map.of("region", "EU", "storage", "HHD", "dataRegionSize", "30"));
        NodeWithAttributes a3  = new NodeWithAttributes("A3", Map.of("region", "CN", "storage", "SSD", "dataRegionSize", "20"));

        String filter = "$[?(@.storage == 'SSD' || @.region == 'US')]";

        preparePrerequisites(filter);

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToAddToDataNodes(Set.of(a1, a2, a3), 7);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 9);

        zoneState.nodesToRemoveFromDataNodes(Set.of(a1, a2), 10);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 11);

        assertDataNodesForZoneWithAttributes(1, Set.of(a3), keyValueStorage, filter);
    }

    @Test
    void testFilterOnScaleUpWithNodeWithNewAttributesAfterRestart() throws Exception {
        NodeWithAttributes a1  = new NodeWithAttributes("A1", Map.of("region", "US", "storage", "SSD", "dataRegionSize", "10"));
        NodeWithAttributes a2  = new NodeWithAttributes("A2", Map.of("region", "EU", "storage", "HHD", "dataRegionSize", "30"));
        NodeWithAttributes a3  = new NodeWithAttributes("A3", Map.of("region", "CN", "storage", "SSD", "dataRegionSize", "20"));

        String filter = "$[?(@.storage == 'SSD' || @.region == 'US')]";

        preparePrerequisites(filter);

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToAddToDataNodes(Set.of(a1, a2, a3), 7);

        zoneState.nodesToRemoveFromDataNodes(Set.of(a3), 8);

        // Now A3 do not fit the filter
        a3 = new NodeWithAttributes("A3", Map.of("region", "CN", "storage", "HDD", "dataRegionSize", "20"));

        zoneState.nodesToAddToDataNodes(Set.of(a3), 9);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 10);

        assertDataNodesForZoneWithAttributes(1, Set.of(a1), keyValueStorage, filter);
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
        LogicalNode a = new LogicalNode("1", "A", new NetworkAddress("localhost", 123));

        LogicalNode b = new LogicalNode("2", "B", new NetworkAddress("localhost", 123));

        LogicalNode c = new LogicalNode("3", "C", new NetworkAddress("localhost", 123));

        topology.putNode(a);
        topology.putNode(b);
        topology.putNode(c);

        Set<LogicalNode> clusterNodes = Set.of(a, b, c);

        mockVaultZonesLogicalTopologyKey(clusterNodes, vaultMgr);

        startDistributionZoneManager();

        if (filter == null) {
            distributionZoneManager.createZone(
                    new DistributionZoneConfigurationParameters.Builder(ZONE_NAME)
                            .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                            .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                            .build()).get();
        } else {
            distributionZoneManager.createZone(
                    new DistributionZoneConfigurationParameters.Builder(ZONE_NAME)
                            .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                            .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                            .filter(filter)
                            .build()).get();
        }

        assertDataNodesForZone(1, clusterNodes, keyValueStorage);

        assertZoneScaleUpChangeTriggerKey(1, 1, keyValueStorage);
        assertZoneScaleDownChangeTriggerKey(1, 1, keyValueStorage);
    }

    private CompletableFuture<Void> testSaveDataNodesOnScaleUp(int zoneId, long revision) {
        return distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(zoneId, revision);
    }

    private CompletableFuture<Void> testSaveDataNodesOnScaleDown(int zoneId, long revision) {
        return distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(zoneId, revision);
    }

    private void assertNotEqualsDataNodesForZone(int zoneId, @Nullable Set<String> clusterNodes) throws InterruptedException {
        assertFalse(waitForCondition(
                () -> {
                    byte[] dataNodes = keyValueStorage.get(zoneDataNodesKey(zoneId).bytes()).value();

                    if (dataNodes == null) {
                        return clusterNodes == null;
                    }

                    Set<String> res = DistributionZonesUtil.dataNodes(ByteUtils.fromBytes(dataNodes), DEFAULT_FILTER).stream()
                            .map(NodeWithAttributes::nodeName).collect(Collectors.toSet());

                    return res.equals(clusterNodes);
                },
                1000
        ));
    }

    private void startDistributionZoneManager() throws Exception {
        deployWatchesAndUpdateMetaStorageRevision(metaStorageManager);

        distributionZoneManager.start();
    }
}
