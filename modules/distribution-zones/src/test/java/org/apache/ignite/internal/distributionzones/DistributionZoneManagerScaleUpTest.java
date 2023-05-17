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
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertDataNodesForZone;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertDataNodesForZoneWithAttributes;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertLogicalTopology;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertZoneScaleDownChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertZoneScaleUpChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.mockVaultZonesLogicalTopologyKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleDownChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleUpChangeTriggerKey;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.util.ByteUtils.bytesToLong;
import static org.apache.ignite.internal.util.ByteUtils.longToBytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.apache.ignite.configuration.NamedConfigurationTree;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.distributionzones.DistributionZoneConfigurationParameters.Builder;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager.Node;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager.ZoneState;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneChange;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneConfiguration;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneView;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.metastorage.server.If;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/**
 * Test scenarios for the distribution zone scale up and scale down.
 */
public class DistributionZoneManagerScaleUpTest extends BaseDistributionZoneManagerTest {
    private static final String ZONE_1_NAME = "zone1";

    private static final int ZONE_1_ID = 1;

    private static final LogicalNode NODE_1 = new LogicalNode("1", "node1", new NetworkAddress("localhost", 123));

    private static final LogicalNode NODE_2 = new LogicalNode("2", "node2", new NetworkAddress("localhost", 123));

    private static final LogicalNode NODE_3 = new LogicalNode("3", "node3", new NetworkAddress("localhost", 123));

    private static final Node A  = new Node("A", "A");

    private static final Node B  = new Node("B", "B");

    private static final Node C  = new Node("C", "C");

    private static final Node D  = new Node("D", "D");

    private static final Node E  = new Node("E", "E");

    private long prerequisiteRevision;

    @Test
    void testDataNodesPropagationAfterScaleUpTriggered() throws Exception {
        topology.putNode(NODE_1);

        Set<LogicalNode> clusterNodes = Set.of(NODE_1);

        startDistributionZoneManager();

        assertDataNodesForZone(DEFAULT_ZONE_ID, clusterNodes, keyValueStorage);

        topology.putNode(NODE_2);

        Set<LogicalNode> clusterNodes2 = Set.of(NODE_1, NODE_2);

        assertLogicalTopology(clusterNodes2, keyValueStorage);

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_1_NAME)
                        .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                        .dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE)
                        .build()
        ).get();

        assertDataNodesForZone(ZONE_1_ID, clusterNodes2, keyValueStorage);

        topology.putNode(NODE_3);

        Set<LogicalNode> clusterNodes3 = Set.of(NODE_1, NODE_2, NODE_3);

        assertLogicalTopology(clusterNodes3, keyValueStorage);

        assertDataNodesForZone(ZONE_1_ID, clusterNodes3, keyValueStorage);
        assertDataNodesForZone(DEFAULT_ZONE_ID, clusterNodes3, keyValueStorage);
    }

    @Test
    void testDataNodesPropagationAfterScaleUpTriggeredOnNewCluster() throws Exception {
        startDistributionZoneManager();

        distributionZoneManager.alterZone(DEFAULT_ZONE_NAME,
                new DistributionZoneConfigurationParameters.Builder(DEFAULT_ZONE_NAME)
                        .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                        .dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE)
                        .build()
        ).get();

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_1_NAME).dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE).build()
        ).get();

        topology.putNode(NODE_1);

        assertDataNodesForZone(DEFAULT_ZONE_ID, Set.of(NODE_1), keyValueStorage);
        assertDataNodesForZone(ZONE_1_ID, Set.of(NODE_1), keyValueStorage);
    }

    @Test
    void testDataNodesPropagationAfterScaleDownTriggered() throws Exception {
        topology.putNode(NODE_1);

        topology.putNode(NODE_2);

        Set<LogicalNode> clusterNodes = Set.of(NODE_1, NODE_2);

        startDistributionZoneManager();

        assertDataNodesForZone(DEFAULT_ZONE_ID, clusterNodes, keyValueStorage);

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_1_NAME).dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE).build()
        ).get();

        assertDataNodesForZone(ZONE_1_ID, clusterNodes, keyValueStorage);

        topology.removeNodes(Set.of(NODE_2));

        Set<LogicalNode> clusterNodes2 = Set.of(NODE_1);

        assertLogicalTopology(clusterNodes2, keyValueStorage);

        assertDataNodesForZone(ZONE_1_ID, clusterNodes2, keyValueStorage);

        // Check that default zone still has both node1 and node2 because dafault zones' scaleDown is INF.
        assertDataNodesForZone(DEFAULT_ZONE_ID, clusterNodes, keyValueStorage);
    }

    @Test
    void testDataNodesPropagationForDefaultZoneAfterScaleUpTriggered() throws Exception {
        topology.putNode(NODE_1);

        Set<LogicalNode> clusterNodes = Set.of(NODE_1);

        startDistributionZoneManager();

        assertDataNodesForZone(DEFAULT_ZONE_ID, clusterNodes, keyValueStorage);

        distributionZoneManager.alterZone(
                DEFAULT_ZONE_NAME,
                new DistributionZoneConfigurationParameters.Builder(DEFAULT_ZONE_NAME)
                        .dataNodesAutoAdjustScaleUp(INFINITE_TIMER_VALUE)
                        .dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE)
                        .build()
        ).get();

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

    @Test
    void testDataNodesPropagationForDefaultZoneAfterScaleDownTriggered() throws Exception {
        topology.putNode(NODE_1);

        topology.putNode(NODE_2);

        Set<LogicalNode> clusterNodes = Set.of(NODE_1, NODE_2);

        startDistributionZoneManager();

        assertDataNodesForZone(DEFAULT_ZONE_ID, clusterNodes, keyValueStorage);

        distributionZoneManager.alterZone(
                DEFAULT_ZONE_NAME,
                new DistributionZoneConfigurationParameters.Builder(DEFAULT_ZONE_NAME)
                        .dataNodesAutoAdjustScaleUp(INFINITE_TIMER_VALUE)
                        .dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE)
                        .build()
        ).get();

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

    @Test
    void testDropZoneDoNotPropagateDataNodesAfterScaleUp() throws Exception {
        topology.putNode(NODE_1);

        startDistributionZoneManager();

        topology.putNode(NODE_2);

        Set<LogicalNode> clusterNodes2 = Set.of(NODE_1, NODE_2);

        assertLogicalTopology(clusterNodes2, keyValueStorage);

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_1_NAME).dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE).build()
        ).get();

        assertDataNodesForZone(ZONE_1_ID, clusterNodes2, keyValueStorage);

        assertNotNull(keyValueStorage.get(zoneScaleUpChangeTriggerKey(ZONE_1_ID).bytes()).value(),
                "zoneScaleUpChangeTriggerKey must be not null.");
        assertNotNull(keyValueStorage.get(zoneScaleDownChangeTriggerKey(ZONE_1_ID).bytes()).value(),
                "zoneScaleDownChangeTriggerKey must be not null.");

        distributionZoneManager.dropZone(ZONE_1_NAME).get();

        assertDataNodesForZone(ZONE_1_ID, null, keyValueStorage);
        assertZoneScaleUpChangeTriggerKey(null, ZONE_1_ID, keyValueStorage);
        assertZoneScaleDownChangeTriggerKey(null, ZONE_1_ID, keyValueStorage);
    }

    @Test
    void testDropZoneDoNotPropagateDataNodesAfterScaleDown() throws Exception {
        topology.putNode(NODE_1);

        topology.putNode(NODE_2);

        startDistributionZoneManager();

        topology.removeNodes(Set.of(NODE_2));

        Set<LogicalNode> clusterNodes2 = Set.of(NODE_1);
        Set<String> clusterNodesNames2 = Set.of(NODE_1.name());

        assertLogicalTopology(clusterNodes2, keyValueStorage);

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_1_NAME).dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE).build()
        ).get();

        assertDataNodesForZone(ZONE_1_ID, clusterNodes2, keyValueStorage);

        assertNotNull(keyValueStorage.get(zoneScaleUpChangeTriggerKey(ZONE_1_ID).bytes()).value(),
                "zoneScaleUpChangeTriggerKey must be not null.");
        assertNotNull(keyValueStorage.get(zoneScaleDownChangeTriggerKey(ZONE_1_ID).bytes()).value(),
                "zoneScaleDownChangeTriggerKey must be not null.");

        distributionZoneManager.dropZone(ZONE_1_NAME).get();

        assertDataNodesForZone(ZONE_1_ID, null, keyValueStorage);
        assertZoneScaleUpChangeTriggerKey(null, ZONE_1_ID, keyValueStorage);
        assertZoneScaleDownChangeTriggerKey(null, ZONE_1_ID, keyValueStorage);
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
                prerequisiteRevision + 1,
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

        // Assert that first task was run and event about adding node "D" with revision {@code prerequisiteRevision + 1} was added
        // to the topologyAugmentationMap of the zone.
        assertThatZonesAugmentationMapContainsRevision(ZONE_1_ID, prerequisiteRevision + 1);

        distributionZoneManager.scheduleTimers(
                zoneView,
                Set.of(E),
                Set.of(),
                prerequisiteRevision + 2,
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

        // Assert that second task was run and event about adding node "E" with revision {@code prerequisiteRevision + 2} was added
        // to the topologyAugmentationMap of the zone.
        assertThatZonesAugmentationMapContainsRevision(ZONE_1_ID, prerequisiteRevision + 2);

        //Second task is propagating data nodes first.
        in2.countDown();

        assertZoneScaleUpChangeTriggerKey(prerequisiteRevision + 2, ZONE_1_ID, keyValueStorage);

        assertDataNodesForZoneWithAttributes(ZONE_1_ID, Set.of(A, B, C, D, E), keyValueStorage);

        out2.countDown();

        in1.countDown();

        //Waiting for the first scheduler ends it work.
        out1.countDown();

        // Assert that nothing has been changed.
        assertZoneScaleUpChangeTriggerKey(prerequisiteRevision + 2, ZONE_1_ID, keyValueStorage);

        assertDataNodesForZoneWithAttributes(ZONE_1_ID, Set.of(A, B, C, D, E), keyValueStorage);
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
                prerequisiteRevision + 1,
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

        // Assert that first task was run and event about removing node "B" with revision {@code prerequisiteRevision + 1} was added
        // to the topologyAugmentationMap of the zone.
        assertThatZonesAugmentationMapContainsRevision(ZONE_1_ID, prerequisiteRevision + 1);

        distributionZoneManager.scheduleTimers(
                zoneView,
                Set.of(),
                Set.of(C),
                prerequisiteRevision + 2,
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

        // Assert that second task was run and event about removing node "C" with revision {@code prerequisiteRevision + 2} was added
        // to the topologyAugmentationMap of the zone.
        assertThatZonesAugmentationMapContainsRevision(ZONE_1_ID, prerequisiteRevision + 2);

        //Second task is propagating data nodes first.
        in2.countDown();

        assertZoneScaleDownChangeTriggerKey(prerequisiteRevision + 2, ZONE_1_ID, keyValueStorage);

        assertDataNodesForZoneWithAttributes(ZONE_1_ID, Set.of(A), keyValueStorage);

        out2.countDown();

        in1.countDown();

        //Waiting for the first scheduler ends it work.
        out1.countDown();

        // Assert that nothing has been changed.
        assertZoneScaleDownChangeTriggerKey(prerequisiteRevision + 2, ZONE_1_ID, keyValueStorage);

        assertDataNodesForZoneWithAttributes(ZONE_1_ID, Set.of(A), keyValueStorage);
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
                prerequisiteRevision + 1,
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
                prerequisiteRevision + 2,
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

        // Assert that second task was run and event about adding node "E" with revision {@code prerequisiteRevision + 2} was added
        // to the topologyAugmentationMap of the zone.
        assertThatZonesAugmentationMapContainsRevision(ZONE_1_ID, prerequisiteRevision + 2);

        assertZoneScaleUpChangeTriggerKey(prerequisiteRevision + 1, ZONE_1_ID, keyValueStorage);

        assertDataNodesForZoneWithAttributes(ZONE_1_ID, Set.of(A, B, C, D), keyValueStorage);

        // Second task is run and we await that data nodes will be changed from ["A", "B", "C", "D"] to ["A", "B", "C", "D", "E"]
        in2.countDown();

        assertZoneScaleUpChangeTriggerKey(prerequisiteRevision + 2, ZONE_1_ID, keyValueStorage);

        assertDataNodesForZoneWithAttributes(ZONE_1_ID, Set.of(A, B, C, D, E), keyValueStorage);

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
                prerequisiteRevision + 1,
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
                prerequisiteRevision + 2,
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

        // Assert that second task was run and event about removing node "C" with revision {@code prerequisiteRevision + 2} was added
        // to the topologyAugmentationMap of the zone.
        assertThatZonesAugmentationMapContainsRevision(ZONE_1_ID, prerequisiteRevision + 2);

        assertZoneScaleDownChangeTriggerKey(prerequisiteRevision + 1, ZONE_1_ID, keyValueStorage);

        assertDataNodesForZoneWithAttributes(ZONE_1_ID, Set.of(A, C), keyValueStorage);

        // Second task is run and we await that data nodes will be changed from ["A", "C"] to ["A"]
        in2.countDown();

        assertZoneScaleDownChangeTriggerKey(prerequisiteRevision + 2, ZONE_1_ID, keyValueStorage);

        assertDataNodesForZoneWithAttributes(ZONE_1_ID, Set.of(A), keyValueStorage);

        out1.countDown();
    }

    @Test
    void testEmptyDataNodesOnStart() throws Exception {
        startDistributionZoneManager();

        assertLogicalTopology(Set.of(), keyValueStorage);

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_1_NAME).dataNodesAutoAdjustScaleUp(0).build()
        ).get();

        assertDataNodesForZone(ZONE_1_ID, Set.of(), keyValueStorage);

        topology.putNode(NODE_1);

        assertLogicalTopology(Set.of(NODE_1), keyValueStorage);

        assertDataNodesForZone(ZONE_1_ID, Set.of(NODE_1), keyValueStorage);
    }

    @Test
    void testUpdateZoneScaleUpTriggersDataNodePropagation() throws Exception {
        startDistributionZoneManager();

        assertLogicalTopology(Set.of(), keyValueStorage);

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_1_NAME).dataNodesAutoAdjustScaleUp(100).build()
        ).get();

        assertDataNodesForZone(ZONE_1_ID, Set.of(), keyValueStorage);

        topology.putNode(NODE_1);

        assertLogicalTopology(Set.of(NODE_1), keyValueStorage);

        assertDataNodesForZone(ZONE_1_ID, Set.of(), keyValueStorage);

        distributionZoneManager.alterZone(
                ZONE_1_NAME,
                new DistributionZoneConfigurationParameters.Builder(ZONE_1_NAME).dataNodesAutoAdjustScaleUp(0).build()
        ).get();

        assertDataNodesForZone(ZONE_1_ID, Set.of(NODE_1), keyValueStorage);
    }

    @Test
    void testUpdateZoneScaleDownTriggersDataNodePropagation() throws Exception {
        topology.putNode(NODE_1);

        startDistributionZoneManager();

        assertLogicalTopology(Set.of(NODE_1), keyValueStorage);

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_1_NAME).dataNodesAutoAdjustScaleDown(100).build()
        ).get();

        assertDataNodesForZone(ZONE_1_ID, Set.of(NODE_1), keyValueStorage);

        topology.removeNodes(Set.of(NODE_1));

        assertDataNodesForZone(ZONE_1_ID, Set.of(NODE_1), keyValueStorage);

        distributionZoneManager.alterZone(
                ZONE_1_NAME,
                new DistributionZoneConfigurationParameters.Builder(ZONE_1_NAME).dataNodesAutoAdjustScaleDown(0).build()
        ).get();

        assertDataNodesForZone(ZONE_1_ID, Set.of(), keyValueStorage);
    }

    @Test
    void testCleanUpAfterSchedulers() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(ZONE_1_ID);

        zoneState.nodesToAddToDataNodes(Set.of(D), 1003);
        zoneState.nodesToAddToDataNodes(Set.of(E), 1004);
        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 1007);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(ZONE_1_ID, 1003);

        assertTrue(zoneState.topologyAugmentationMap().containsKey(1003L));
        assertTrue(zoneState.topologyAugmentationMap().containsKey(1004L));
        assertTrue(zoneState.topologyAugmentationMap().containsKey(1007L));

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(ZONE_1_ID, 1004);

        assertTrue(zoneState.topologyAugmentationMap().containsKey(1003L));
        assertTrue(zoneState.topologyAugmentationMap().containsKey(1004L));
        assertTrue(zoneState.topologyAugmentationMap().containsKey(1007L));

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(ZONE_1_ID, 1007);

        assertFalse(zoneState.topologyAugmentationMap().containsKey(1003L));
        assertFalse(zoneState.topologyAugmentationMap().containsKey(1004L));
        assertTrue(zoneState.topologyAugmentationMap().containsKey(1007L));

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(ZONE_1_ID, 1015);

        assertTrue(zoneState.topologyAugmentationMap().isEmpty());
        assertDataNodesForZoneWithAttributes(ZONE_1_ID, Set.of(A, B, D, E), keyValueStorage);
    }

    @Test
    void testScaleUpSetToMaxInt() throws Exception {
        startDistributionZoneManager();

        assertLogicalTopology(Set.of(), keyValueStorage);

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_1_NAME).dataNodesAutoAdjustScaleUp(100).build()
        ).get();

        assertDataNodesForZone(ZONE_1_ID, Set.of(), keyValueStorage);

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(ZONE_1_ID);

        assertNull(zoneState.scaleUpTask());

        topology.putNode(NODE_1);

        assertLogicalTopology(Set.of(NODE_1), keyValueStorage);

        assertTrue(waitForCondition(() -> zoneState.scaleUpTask() != null, 1000L));

        distributionZoneManager.alterZone(
                ZONE_1_NAME,
                new DistributionZoneConfigurationParameters.Builder(ZONE_1_NAME).dataNodesAutoAdjustScaleUp(INFINITE_TIMER_VALUE).build()
        ).get();

        assertTrue(waitForCondition(() -> zoneState.scaleUpTask().isCancelled(), 1000L));
    }

    @Test
    void testScaleDownSetToMaxInt() throws Exception {
        topology.putNode(NODE_1);

        startDistributionZoneManager();

        assertLogicalTopology(Set.of(NODE_1), keyValueStorage);

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_1_NAME).dataNodesAutoAdjustScaleDown(100).build()
        ).get();

        assertDataNodesForZone(ZONE_1_ID, Set.of(NODE_1), keyValueStorage);

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(ZONE_1_ID);

        assertNull(zoneState.scaleDownTask());

        topology.removeNodes(Set.of(NODE_1));

        assertTrue(waitForCondition(() -> zoneState.scaleDownTask() != null, 1000L));

        distributionZoneManager.alterZone(
                ZONE_1_NAME,
                new DistributionZoneConfigurationParameters.Builder(ZONE_1_NAME).dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE).build()
        ).get();

        assertTrue(waitForCondition(() -> zoneState.scaleDownTask().isCancelled(), 1000L));
    }

    @Test
    void testScaleUpDidNotChangeDataNodesWhenTriggerKeyWasConcurrentlyChanged() throws Exception {
        startDistributionZoneManager();

        assertLogicalTopology(Set.of(), keyValueStorage);

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_1_NAME).dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE).build()
        ).get();

        assertDataNodesForZone(ZONE_1_ID, Set.of(), keyValueStorage);

        assertZoneScaleDownChangeTriggerKey(3L, ZONE_1_ID, keyValueStorage);

        doAnswer(invocation -> {
            If iif = invocation.getArgument(0);

            // Emulate a situation when one of the scale up keys gets concurrently updated during a Meta Storage invoke. We then expect
            // that the invoke call will be retried.
            byte[] key = zoneScaleUpChangeTriggerKey(ZONE_1_ID).bytes();

            if (Arrays.stream(iif.cond().keys()).anyMatch(k -> Arrays.equals(key, k))) {
                keyValueStorage.put(key, longToBytes(100), HybridTimestamp.MIN_VALUE);
            }

            return invocation.callRealMethod();
        }).when(keyValueStorage).invoke(any(), any());

        topology.putNode(NODE_1);

        assertLogicalTopology(Set.of(NODE_1), keyValueStorage);

        assertZoneScaleUpChangeTriggerKey(100L, ZONE_1_ID, keyValueStorage);

        assertDataNodesForZone(ZONE_1_ID, Set.of(), keyValueStorage);
    }

    @Test
    void testScaleDownDidNotChangeDataNodesWhenTriggerKeyWasConcurrentlyChanged() throws Exception {
        topology.putNode(NODE_1);

        startDistributionZoneManager();

        assertLogicalTopology(Set.of(NODE_1), keyValueStorage);

        assertDataNodesForZone(DEFAULT_ZONE_ID, Set.of(NODE_1.name()), keyValueStorage);

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_1_NAME).dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE).build()
        ).get();

        assertDataNodesForZone(ZONE_1_ID, Set.of(NODE_1), keyValueStorage);

        assertZoneScaleDownChangeTriggerKey(4L, ZONE_1_ID, keyValueStorage);

        doAnswer(invocation -> {
            If iif = invocation.getArgument(0);

            // Emulate a situation when one of the scale down keys gets concurrently updated during a Meta Storage invoke. We then expect
            // that the invoke call will be retried.
            byte[] key = zoneScaleDownChangeTriggerKey(ZONE_1_ID).bytes();

            if (Arrays.stream(iif.cond().keys()).anyMatch(k -> Arrays.equals(key, k))) {
                keyValueStorage.put(key, longToBytes(100), HybridTimestamp.MIN_VALUE);
            }

            return invocation.callRealMethod();
        }).when(keyValueStorage).invoke(any(), any());

        topology.removeNodes(Set.of(NODE_1));

        assertDataNodesForZone(ZONE_1_ID, Set.of(NODE_1), keyValueStorage);

        assertZoneScaleDownChangeTriggerKey(100L, ZONE_1_ID, keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios1_1() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(ZONE_1_ID);

        zoneState.nodesToAddToDataNodes(Set.of(D), 1003);
        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 1007);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(ZONE_1_ID, 1003);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(ZONE_1_ID, 1007);

        assertDataNodesForZoneWithAttributes(ZONE_1_ID, Set.of(A, B, D), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios1_2() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(ZONE_1_ID);

        zoneState.nodesToAddToDataNodes(Set.of(D), 1003);
        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 1007);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(ZONE_1_ID, 1007);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(ZONE_1_ID, 1003);

        assertDataNodesForZoneWithAttributes(ZONE_1_ID, Set.of(A, B, D), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios1_3() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(ZONE_1_ID);

        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 1003);
        zoneState.nodesToAddToDataNodes(Set.of(D), 1007);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(ZONE_1_ID, 1003);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(ZONE_1_ID, 1007);

        assertDataNodesForZoneWithAttributes(ZONE_1_ID, Set.of(A, B, D), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios1_4() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(ZONE_1_ID);

        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 1003);
        zoneState.nodesToAddToDataNodes(Set.of(D), 1007);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(ZONE_1_ID, 1007);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(ZONE_1_ID, 1003);

        assertDataNodesForZoneWithAttributes(ZONE_1_ID, Set.of(A, B, D), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios2_1() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(ZONE_1_ID);

        zoneState.nodesToAddToDataNodes(Set.of(D), 1003);
        zoneState.nodesToRemoveFromDataNodes(Set.of(D), 1007);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(ZONE_1_ID, 1003);
        assertDataNodesForZoneWithAttributes(ZONE_1_ID, Set.of(A, B, C, D), keyValueStorage);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(ZONE_1_ID, 1007);

        assertDataNodesForZoneWithAttributes(ZONE_1_ID, Set.of(A, B, C), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios2_2() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(ZONE_1_ID);

        zoneState.nodesToAddToDataNodes(Set.of(D), 1003);
        zoneState.nodesToRemoveFromDataNodes(Set.of(D), 1007);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(ZONE_1_ID, 1007);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(ZONE_1_ID, 1003);

        assertDataNodesForZoneWithAttributes(ZONE_1_ID, Set.of(A, B, C), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios2_3() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(ZONE_1_ID);

        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 1003);
        zoneState.nodesToAddToDataNodes(Set.of(C), 1007);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(ZONE_1_ID, 1003);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(ZONE_1_ID, 1007);

        assertDataNodesForZoneWithAttributes(ZONE_1_ID, Set.of(A, B, C), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios2_4() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(ZONE_1_ID);

        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 1003);
        zoneState.nodesToAddToDataNodes(Set.of(C), 1007);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(ZONE_1_ID, 1007);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(ZONE_1_ID, 1003);

        assertDataNodesForZoneWithAttributes(ZONE_1_ID, Set.of(A, B, C), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios3_1() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(ZONE_1_ID);

        zoneState.nodesToAddToDataNodes(Set.of(D), 1003);
        zoneState.nodesToRemoveFromDataNodes(Set.of(D), 1007);
        zoneState.nodesToAddToDataNodes(Set.of(D), 1009);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(ZONE_1_ID, 1009);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(ZONE_1_ID, 1007);

        assertDataNodesForZoneWithAttributes(ZONE_1_ID, Set.of(A, B, C, D), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios3_2() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(ZONE_1_ID);

        zoneState.nodesToAddToDataNodes(Set.of(D), 1003);
        zoneState.nodesToRemoveFromDataNodes(Set.of(D), 1007);
        zoneState.nodesToAddToDataNodes(Set.of(D), 1009);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(ZONE_1_ID, 1007);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(ZONE_1_ID, 1009);

        assertDataNodesForZoneWithAttributes(ZONE_1_ID, Set.of(A, B, C, D), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios3_3() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(ZONE_1_ID);

        zoneState.nodesToAddToDataNodes(Set.of(D), 1003);
        zoneState.nodesToRemoveFromDataNodes(Set.of(D), 1007);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(ZONE_1_ID, 1007);

        zoneState.nodesToAddToDataNodes(Set.of(D), 1009);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(ZONE_1_ID, 1009);

        assertDataNodesForZoneWithAttributes(ZONE_1_ID, Set.of(A, B, C, D), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios3_4() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(ZONE_1_ID);

        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 1003);
        zoneState.nodesToAddToDataNodes(Set.of(C), 1007);
        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 1009);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(ZONE_1_ID, 1009);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(ZONE_1_ID, 1007);

        assertDataNodesForZoneWithAttributes(ZONE_1_ID, Set.of(A, B), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios3_5() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(ZONE_1_ID);

        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 1003);
        zoneState.nodesToAddToDataNodes(Set.of(C), 1007);
        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 1009);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(ZONE_1_ID, 1007);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(ZONE_1_ID, 1009);

        assertDataNodesForZoneWithAttributes(ZONE_1_ID, Set.of(A, B), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios3_6() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(ZONE_1_ID);

        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 1003);
        zoneState.nodesToAddToDataNodes(Set.of(C), 1007);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(ZONE_1_ID, 1007);

        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 1009);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(ZONE_1_ID, 1009);

        assertDataNodesForZoneWithAttributes(ZONE_1_ID, Set.of(A, B), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios4_1() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(ZONE_1_ID);

        zoneState.nodesToAddToDataNodes(Set.of(D), 1003);
        zoneState.nodesToRemoveFromDataNodes(Set.of(D), 1007);
        zoneState.nodesToAddToDataNodes(Set.of(E), 1009);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(ZONE_1_ID, 1009);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(ZONE_1_ID, 1007);

        assertDataNodesForZoneWithAttributes(ZONE_1_ID, Set.of(A, B, C, E), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios4_2() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(ZONE_1_ID);

        zoneState.nodesToAddToDataNodes(Set.of(D), 1003);
        zoneState.nodesToRemoveFromDataNodes(Set.of(D), 1007);
        zoneState.nodesToAddToDataNodes(Set.of(E), 1009);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(ZONE_1_ID, 1007);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(ZONE_1_ID, 1009);

        assertDataNodesForZoneWithAttributes(ZONE_1_ID, Set.of(A, B, C, E), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios4_3() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(ZONE_1_ID);

        zoneState.nodesToAddToDataNodes(Set.of(D), 1003);
        zoneState.nodesToRemoveFromDataNodes(Set.of(D), 1007);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(ZONE_1_ID, 1007);

        zoneState.nodesToAddToDataNodes(Set.of(E), 1009);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(ZONE_1_ID, 1009);

        assertDataNodesForZoneWithAttributes(ZONE_1_ID, Set.of(A, B, C, E), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios4_4() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(ZONE_1_ID);

        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 1003);
        zoneState.nodesToAddToDataNodes(Set.of(C), 1007);
        zoneState.nodesToRemoveFromDataNodes(Set.of(B), 1009);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(ZONE_1_ID, 1009);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(ZONE_1_ID, 1007);

        assertDataNodesForZoneWithAttributes(ZONE_1_ID, Set.of(A, C), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios4_5() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(ZONE_1_ID);

        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 1003);
        zoneState.nodesToAddToDataNodes(Set.of(C), 1007);
        zoneState.nodesToRemoveFromDataNodes(Set.of(B), 1009);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(ZONE_1_ID, 1007);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(ZONE_1_ID, 1009);

        assertDataNodesForZoneWithAttributes(ZONE_1_ID, Set.of(A, C), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios4_6() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(ZONE_1_ID);

        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 1003);
        zoneState.nodesToAddToDataNodes(Set.of(C), 1007);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(ZONE_1_ID, 1007);

        zoneState.nodesToRemoveFromDataNodes(Set.of(B), 1009);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(ZONE_1_ID, 1009);

        assertDataNodesForZoneWithAttributes(ZONE_1_ID, Set.of(A, C), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios5_1() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(ZONE_1_ID);

        zoneState.nodesToAddToDataNodes(Set.of(D), 1003);
        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 1007);
        zoneState.nodesToAddToDataNodes(Set.of(C), 1009);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(ZONE_1_ID, 1009);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(ZONE_1_ID, 1007);

        assertDataNodesForZoneWithAttributes(ZONE_1_ID, Set.of(A, B, C, D), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios5_2() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(ZONE_1_ID);

        zoneState.nodesToAddToDataNodes(Set.of(D), 1003);
        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 1007);
        zoneState.nodesToAddToDataNodes(Set.of(C), 1009);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(ZONE_1_ID, 1007);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(ZONE_1_ID, 1009);

        assertDataNodesForZoneWithAttributes(ZONE_1_ID, Set.of(A, B, C, D), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios5_3() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(ZONE_1_ID);

        zoneState.nodesToAddToDataNodes(Set.of(D), 1003);
        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 1007);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(ZONE_1_ID, 1007);

        zoneState.nodesToAddToDataNodes(Set.of(C), 1009);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(ZONE_1_ID, 1009);

        assertDataNodesForZoneWithAttributes(ZONE_1_ID, Set.of(A, B, C, D), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios5_4() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(ZONE_1_ID);

        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 1003);
        zoneState.nodesToAddToDataNodes(Set.of(D), 1007);
        zoneState.nodesToRemoveFromDataNodes(Set.of(D), 1009);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(ZONE_1_ID, 1009);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(ZONE_1_ID, 1007);

        assertDataNodesForZoneWithAttributes(ZONE_1_ID, Set.of(A, B), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios5_5() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(ZONE_1_ID);

        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 1003);
        zoneState.nodesToAddToDataNodes(Set.of(D), 1007);
        zoneState.nodesToRemoveFromDataNodes(Set.of(D), 1009);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(ZONE_1_ID, 1007);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(ZONE_1_ID, 1009);

        assertDataNodesForZoneWithAttributes(ZONE_1_ID, Set.of(A, B), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios5_6() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(ZONE_1_ID);

        zoneState.nodesToRemoveFromDataNodes(Set.of(C), 1003);
        zoneState.nodesToAddToDataNodes(Set.of(D), 1007);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(ZONE_1_ID, 1007);

        zoneState.nodesToRemoveFromDataNodes(Set.of(D), 1009);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(ZONE_1_ID, 1009);

        assertDataNodesForZoneWithAttributes(ZONE_1_ID, Set.of(A, B), keyValueStorage);
    }

    @Test
    void testZoneStateAddRemoveNodesPreservesDuplicationsOfNodes() {
        ZoneState zoneState = new ZoneState(new ScheduledThreadPoolExecutor(1));

        zoneState.nodesToAddToDataNodes(Set.of(A, B), 1);
        zoneState.nodesToAddToDataNodes(Set.of(A, B), 2);

        List<Node> nodes = zoneState.nodesToBeAddedToDataNodes(0, 2);

        nodes.sort((a, b) -> a.nodeName.compareTo(b.nodeName()));

        assertEquals(List.of(A, A, B, B), nodes);

        zoneState.nodesToRemoveFromDataNodes(Set.of(C, D), 3);
        zoneState.nodesToRemoveFromDataNodes(Set.of(C, D), 4);

        nodes = zoneState.nodesToBeRemovedFromDataNodes(2, 4);

        nodes.sort((a, b) -> a.nodeName.compareTo(b.nodeName()));

        assertEquals(List.of(C, C, D, D), nodes);
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

        startDistributionZoneManager();

        if (filter == null) {
            distributionZoneManager.createZone(
                    new Builder(ZONE_1_NAME)
                            .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                            .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                            .build()).get();
        } else {
            distributionZoneManager.createZone(
                    new DistributionZoneConfigurationParameters.Builder(ZONE_1_NAME)
                            .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                            .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                            .filter(filter)
                            .build()).get();
        }

        assertDataNodesForZone(ZONE_1_ID, clusterNodes, keyValueStorage);

        long scaleUpChangeTriggerKey = bytesToLong(
                keyValueStorage.get(zoneScaleUpChangeTriggerKey(ZONE_1_ID).bytes()).value()
        );

        long scaleDownChangeTriggerKey = bytesToLong(
                keyValueStorage.get(zoneScaleDownChangeTriggerKey(ZONE_1_ID).bytes()).value()
        );

        assertEquals(scaleUpChangeTriggerKey, scaleDownChangeTriggerKey,
                "zoneScaleUpChangeTriggerKey and zoneScaleDownChangeTriggerKey are not equal");

        prerequisiteRevision = scaleUpChangeTriggerKey;
    }

    private CompletableFuture<Void> testSaveDataNodesOnScaleUp(int zoneId, long revision) {
        return distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(zoneId, revision);
    }

    private CompletableFuture<Void> testSaveDataNodesOnScaleDown(int zoneId, long revision) {
        return distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(zoneId, revision);
    }

    private void assertThatZonesAugmentationMapContainsRevision(int zoneId, long revisionToAssert) throws InterruptedException {
        assertTrue(
                waitForCondition(
                        () -> distributionZoneManager.zonesTimers().get(zoneId).topologyAugmentationMap().containsKey(revisionToAssert),
                        1000
                )
        );
    }
}
