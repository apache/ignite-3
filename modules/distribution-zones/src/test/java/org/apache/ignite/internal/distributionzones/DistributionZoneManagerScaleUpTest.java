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
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleDownChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleUpChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyKey;
import static org.apache.ignite.internal.distributionzones.util.DistributionZonesTestUtil.assertDataNodesForZone;
import static org.apache.ignite.internal.distributionzones.util.DistributionZonesTestUtil.assertLogicalTopology;
import static org.apache.ignite.internal.distributionzones.util.DistributionZonesTestUtil.assertZoneScaleDownChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.util.DistributionZonesTestUtil.assertZoneScaleUpChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.util.DistributionZonesTestUtil.deployWatchesAndUpdateMetaStorageRevision;
import static org.apache.ignite.internal.metastorage.dsl.Operations.noop;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.ByteUtils.longToBytes;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.hamcrest.MatcherAssert.assertThat;
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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.NamedConfigurationTree;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager.ZoneState;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneChange;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneConfiguration;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneView;
import org.apache.ignite.internal.metastorage.dsl.Conditions;
import org.apache.ignite.internal.metastorage.server.If;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Test scenarios for the distribution zone scale up and scale down.
 */
public class DistributionZoneManagerScaleUpTest extends BaseDistributionZoneManagerTest {
    private static final String ZONE_NAME = "zone1";

    private static final LogicalNode NODE_1 = new LogicalNode("1", "A", new NetworkAddress("localhost", 123));

    private static final LogicalNode NODE_2 = new LogicalNode("2", "B", new NetworkAddress("localhost", 123));

    private static final LogicalNode NODE_3 = new LogicalNode("3", "C", new NetworkAddress("localhost", 123));

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-19255")
    void testDataNodesPropagationAfterScaleUpTriggered() throws Exception {
        topology.putNode(NODE_1);

        Set<LogicalNode> clusterNodes = Set.of(NODE_1);

        mockVaultZonesLogicalTopologyKey(clusterNodes);

        startDistributionZoneManager();

        assertDataNodesForZone(DEFAULT_ZONE_ID, clusterNodes.stream().map(ClusterNode::name).collect(Collectors.toSet()), keyValueStorage);

        topology.putNode(NODE_2);

        Set<LogicalNode> clusterNodes2 = Set.of(NODE_1, NODE_2);

        assertLogicalTopology(clusterNodes2, keyValueStorage);

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME)
                        .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                        .dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE)
                        .build()
        ).get();

        assertDataNodesForZone(1, clusterNodes2.stream().map(ClusterNode::name).collect(Collectors.toSet()), keyValueStorage);

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

        assertDataNodesForZone(DEFAULT_ZONE_ID, Set.of(NODE_1.name()), keyValueStorage);
        assertDataNodesForZone(zoneId, Set.of(NODE_1.name()), keyValueStorage);
    }

    @Test
    void testDataNodesPropagationAfterScaleDownTriggered() throws Exception {
        topology.putNode(NODE_1);

        topology.putNode(NODE_2);

        Set<String> clusterNodesNames = Set.of(NODE_1.name(), NODE_2.name());
        Set<LogicalNode> clusterNodes = Set.of(NODE_1, NODE_2);

        mockVaultZonesLogicalTopologyKey(clusterNodes);

        startDistributionZoneManager();

        assertDataNodesForZone(DEFAULT_ZONE_ID, clusterNodesNames, keyValueStorage);

        topology.removeNodes(Set.of(NODE_2));

        Set<LogicalNode> clusterNodes2 = Set.of(NODE_1);

        assertLogicalTopology(clusterNodes2, keyValueStorage);

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE).build()
        ).get();

        assertZoneScaleDownChangeTriggerKey(1, 1, keyValueStorage);

        assertDataNodesForZone(1, clusterNodes2.stream().map(ClusterNode::name).collect(Collectors.toSet()), keyValueStorage);
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-19255")
    @Test
    void testDataNodesPropagationForDefaultZoneAfterScaleUpTriggered() throws Exception {
        topology.putNode(NODE_1);

        Set<LogicalNode> clusterNodes = Set.of(NODE_1);

        mockVaultZonesLogicalTopologyKey(clusterNodes);

        startDistributionZoneManager();

        assertDataNodesForZone(DEFAULT_ZONE_ID, clusterNodes.stream().map(ClusterNode::name).collect(Collectors.toSet()), keyValueStorage);

        topology.putNode(NODE_2);

        Set<LogicalNode> clusterNodes2 = Set.of(NODE_1, NODE_2);
        Set<String> clusterNodesNames2 = Set.of(NODE_1.name(), NODE_2.name());

        assertLogicalTopology(clusterNodes2, keyValueStorage);

        distributionZoneManager.alterZone(
                DEFAULT_ZONE_NAME,
                new DistributionZoneConfigurationParameters.Builder(DEFAULT_ZONE_NAME)
                        .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                        .dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE)
                        .build()
        ).get();

        assertDataNodesForZone(DEFAULT_ZONE_ID, clusterNodesNames2, keyValueStorage);
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-19255")
    @Test
    void testDataNodesPropagationForDefaultZoneAfterScaleDownTriggered() throws Exception {
        topology.putNode(NODE_1);

        topology.putNode(NODE_2);

        Set<LogicalNode> clusterNodes = Set.of(NODE_1, NODE_2);
        Set<String> clusterNodesNames = Set.of(NODE_1.name(), NODE_2.name());

        mockVaultZonesLogicalTopologyKey(clusterNodes);

        startDistributionZoneManager();

        assertDataNodesForZone(DEFAULT_ZONE_ID, clusterNodesNames, keyValueStorage);

        topology.removeNodes(Set.of(NODE_2));

        Set<LogicalNode> clusterNodes2 = Set.of(NODE_1);
        Set<String> clusterNodesNames2 = Set.of(NODE_1.name());

        assertLogicalTopology(clusterNodes2, keyValueStorage);

        distributionZoneManager.alterZone(
                DEFAULT_ZONE_NAME,
                new DistributionZoneConfigurationParameters.Builder(DEFAULT_ZONE_NAME)
                        .dataNodesAutoAdjustScaleUp(INFINITE_TIMER_VALUE)
                        .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                        .build()
        ).get();

        assertDataNodesForZone(DEFAULT_ZONE_ID, clusterNodesNames2, keyValueStorage);
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-19255")
    @Test
    void testDropZoneDoNotPropagateDataNodesAfterScaleUp() throws Exception {
        topology.putNode(NODE_1);

        Set<LogicalNode> clusterNodes = Set.of(NODE_1);

        mockVaultZonesLogicalTopologyKey(clusterNodes);

        startDistributionZoneManager();

        topology.putNode(NODE_2);

        Set<LogicalNode> clusterNodes2 = Set.of(NODE_1, NODE_2);
        Set<String> clusterNodesNames2 = Set.of(NODE_1.name(), NODE_2.name());

        assertLogicalTopology(clusterNodes2, keyValueStorage);

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE).build()
        ).get();

        assertDataNodesForZone(1, clusterNodesNames2, keyValueStorage);

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

        mockVaultZonesLogicalTopologyKey(clusterNodes);

        startDistributionZoneManager();

        topology.removeNodes(Set.of(NODE_2));

        Set<LogicalNode> clusterNodes2 = Set.of(NODE_1);
        Set<String> clusterNodesNames2 = Set.of(NODE_1.name());

        assertLogicalTopology(clusterNodes2, keyValueStorage);

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE).build()
        ).get();

        assertDataNodesForZone(1, clusterNodesNames2, keyValueStorage);

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
                Set.of("D"),
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
                Set.of("E"),
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

        assertDataNodesForZone(1, Set.of("A", "B", "C", "D", "E"), keyValueStorage);

        out2.countDown();

        in1.countDown();

        //Waiting for the first scheduler ends it work.
        out1.countDown();

        // Assert that nothing has been changed.
        assertZoneScaleUpChangeTriggerKey(3, 1, keyValueStorage);

        assertDataNodesForZone(1, Set.of("A", "B", "C", "D", "E"), keyValueStorage);
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
                Set.of("B"),
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
                Set.of("C"),
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

        assertDataNodesForZone(1, Set.of("A"), keyValueStorage);

        out2.countDown();

        in1.countDown();

        //Waiting for the first scheduler ends it work.
        out1.countDown();

        // Assert that nothing has been changed.
        assertZoneScaleDownChangeTriggerKey(3, 1, keyValueStorage);

        assertDataNodesForZone(1, Set.of("A"), keyValueStorage);
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
                Set.of("D"),
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
                Set.of("E"),
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

        assertDataNodesForZone(1, Set.of("A", "B", "C", "D"), keyValueStorage);

        // Second task is run and we await that data nodes will be changed from ["A", "B", "C", "D"] to ["A", "B", "C", "D", "E"]
        in2.countDown();

        assertZoneScaleUpChangeTriggerKey(3, 1, keyValueStorage);

        assertDataNodesForZone(1, Set.of("A", "B", "C", "D", "E"), keyValueStorage);

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
                Set.of("B"),
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
                Set.of("C"),
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

        assertDataNodesForZone(1, Set.of("A", "C"), keyValueStorage);

        // Second task is run and we await that data nodes will be changed from ["A", "C"] to ["A"]
        in2.countDown();

        assertZoneScaleDownChangeTriggerKey(3, 1, keyValueStorage);

        assertDataNodesForZone(1, Set.of("A"), keyValueStorage);

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

        assertDataNodesForZone(1, Set.of(NODE_1.name()), keyValueStorage);
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

        assertDataNodesForZone(1, Set.of(NODE_1.name()), keyValueStorage);
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-19255")
    @Test
    void testUpdateZoneScaleDownTriggersDataNodePropagation() throws Exception {
        topology.putNode(NODE_1);

        mockVaultZonesLogicalTopologyKey(Set.of(NODE_1));

        startDistributionZoneManager();

        assertLogicalTopology(Set.of(NODE_1), keyValueStorage);

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjustScaleDown(100).build()
        ).get();

        assertDataNodesForZone(1, Set.of(NODE_1.name()), keyValueStorage);

        assertZoneScaleDownChangeTriggerKey(1, 1, keyValueStorage);

        topology.removeNodes(Set.of(NODE_1));

        assertDataNodesForZone(1, Set.of(NODE_1.name()), keyValueStorage);

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

        zoneState.nodesToAddToDataNodes(Set.of("D"), 3);
        zoneState.nodesToAddToDataNodes(Set.of("E"), 4);
        zoneState.nodesToRemoveFromDataNodes(Set.of("C"), 7);

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
        assertDataNodesForZone(1, Set.of("A", "B", "D", "E"), keyValueStorage);
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

        mockVaultZonesLogicalTopologyKey(Set.of(NODE_1));

        startDistributionZoneManager();

        assertLogicalTopology(Set.of(NODE_1), keyValueStorage);

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjustScaleDown(100).build()
        ).get();

        assertDataNodesForZone(1, Set.of(NODE_1.name()), keyValueStorage);

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

        mockVaultZonesLogicalTopologyKey(Set.of(NODE_1));

        startDistributionZoneManager();

        assertLogicalTopology(Set.of(NODE_1), keyValueStorage);

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE).build()
        ).get();

        assertDataNodesForZone(1, Set.of(NODE_1.name()), keyValueStorage);

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

        assertDataNodesForZone(1, Set.of(NODE_1.name()), keyValueStorage);

        assertZoneScaleDownChangeTriggerKey(100, 1, keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios1_1() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToAddToDataNodes(Set.of("D"), 3);
        zoneState.nodesToRemoveFromDataNodes(Set.of("C"), 7);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 3);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 7);

        assertDataNodesForZone(1, Set.of("A", "B", "D"), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios1_2() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToAddToDataNodes(Set.of("D"), 3);
        zoneState.nodesToRemoveFromDataNodes(Set.of("C"), 7);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 7);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 3);

        assertDataNodesForZone(1, Set.of("A", "B", "D"), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios1_3() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToRemoveFromDataNodes(Set.of("C"), 3);
        zoneState.nodesToAddToDataNodes(Set.of("D"), 7);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 3);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 7);

        assertDataNodesForZone(1, Set.of("A", "B", "D"), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios1_4() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToRemoveFromDataNodes(Set.of("C"), 3);
        zoneState.nodesToAddToDataNodes(Set.of("D"), 7);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 7);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 3);

        assertDataNodesForZone(1, Set.of("A", "B", "D"), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios2_1() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToAddToDataNodes(Set.of("D"), 3);
        zoneState.nodesToRemoveFromDataNodes(Set.of("D"), 7);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 3);
        assertDataNodesForZone(1, Set.of("A", "B", "C", "D"), keyValueStorage);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 7);

        assertDataNodesForZone(1, Set.of("A", "B", "C"), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios2_2() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToAddToDataNodes(Set.of("D"), 3);
        zoneState.nodesToRemoveFromDataNodes(Set.of("D"), 7);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 7);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 3);

        assertDataNodesForZone(1, Set.of("A", "B", "C"), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios2_3() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToRemoveFromDataNodes(Set.of("C"), 3);
        zoneState.nodesToAddToDataNodes(Set.of("C"), 7);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 3);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 7);

        assertDataNodesForZone(1, Set.of("A", "B", "C"), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios2_4() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToRemoveFromDataNodes(Set.of("C"), 3);
        zoneState.nodesToAddToDataNodes(Set.of("C"), 7);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 7);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 3);

        assertDataNodesForZone(1, Set.of("A", "B", "C"), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios3_1() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToAddToDataNodes(Set.of("D"), 3);
        zoneState.nodesToRemoveFromDataNodes(Set.of("D"), 7);
        zoneState.nodesToAddToDataNodes(Set.of("D"), 9);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 9);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 7);

        assertDataNodesForZone(1, Set.of("A", "B", "C", "D"), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios3_2() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToAddToDataNodes(Set.of("D"), 3);
        zoneState.nodesToRemoveFromDataNodes(Set.of("D"), 7);
        zoneState.nodesToAddToDataNodes(Set.of("D"), 9);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 7);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 9);

        assertDataNodesForZone(1, Set.of("A", "B", "C", "D"), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios3_3() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToAddToDataNodes(Set.of("D"), 3);
        zoneState.nodesToRemoveFromDataNodes(Set.of("D"), 7);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 7);

        zoneState.nodesToAddToDataNodes(Set.of("D"), 9);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 9);

        assertDataNodesForZone(1, Set.of("A", "B", "C", "D"), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios3_4() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToRemoveFromDataNodes(Set.of("C"), 3);
        zoneState.nodesToAddToDataNodes(Set.of("C"), 7);
        zoneState.nodesToRemoveFromDataNodes(Set.of("C"), 9);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 9);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 7);

        assertDataNodesForZone(1, Set.of("A", "B"), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios3_5() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToRemoveFromDataNodes(Set.of("C"), 3);
        zoneState.nodesToAddToDataNodes(Set.of("C"), 7);
        zoneState.nodesToRemoveFromDataNodes(Set.of("C"), 9);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 7);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 9);

        assertDataNodesForZone(1, Set.of("A", "B"), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios3_6() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToRemoveFromDataNodes(Set.of("C"), 3);
        zoneState.nodesToAddToDataNodes(Set.of("C"), 7);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 7);

        zoneState.nodesToRemoveFromDataNodes(Set.of("C"), 9);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 9);

        assertDataNodesForZone(1, Set.of("A", "B"), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios4_1() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToAddToDataNodes(Set.of("D"), 3);
        zoneState.nodesToRemoveFromDataNodes(Set.of("D"), 7);
        zoneState.nodesToAddToDataNodes(Set.of("E"), 9);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 9);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 7);

        assertDataNodesForZone(1, Set.of("A", "B", "C", "E"), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios4_2() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToAddToDataNodes(Set.of("D"), 3);
        zoneState.nodesToRemoveFromDataNodes(Set.of("D"), 7);
        zoneState.nodesToAddToDataNodes(Set.of("E"), 9);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 7);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 9);

        assertDataNodesForZone(1, Set.of("A", "B", "C", "E"), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios4_3() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToAddToDataNodes(Set.of("D"), 3);
        zoneState.nodesToRemoveFromDataNodes(Set.of("D"), 7);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 7);

        zoneState.nodesToAddToDataNodes(Set.of("E"), 9);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 9);

        assertDataNodesForZone(1, Set.of("A", "B", "C", "E"), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios4_4() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToRemoveFromDataNodes(Set.of("C"), 3);
        zoneState.nodesToAddToDataNodes(Set.of("C"), 7);
        zoneState.nodesToRemoveFromDataNodes(Set.of("B"), 9);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 9);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 7);

        assertDataNodesForZone(1, Set.of("A", "C"), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios4_5() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToRemoveFromDataNodes(Set.of("C"), 3);
        zoneState.nodesToAddToDataNodes(Set.of("C"), 7);
        zoneState.nodesToRemoveFromDataNodes(Set.of("B"), 9);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 7);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 9);

        assertDataNodesForZone(1, Set.of("A", "C"), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios4_6() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToRemoveFromDataNodes(Set.of("C"), 3);
        zoneState.nodesToAddToDataNodes(Set.of("C"), 7);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 7);

        zoneState.nodesToRemoveFromDataNodes(Set.of("B"), 9);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 9);

        assertDataNodesForZone(1, Set.of("A", "C"), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios5_1() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToAddToDataNodes(Set.of("D"), 3);
        zoneState.nodesToRemoveFromDataNodes(Set.of("C"), 7);
        zoneState.nodesToAddToDataNodes(Set.of("C"), 9);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 9);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 7);

        assertDataNodesForZone(1, Set.of("A", "B", "C", "D"), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios5_2() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToAddToDataNodes(Set.of("D"), 3);
        zoneState.nodesToRemoveFromDataNodes(Set.of("C"), 7);
        zoneState.nodesToAddToDataNodes(Set.of("C"), 9);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 7);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 9);

        assertDataNodesForZone(1, Set.of("A", "B", "C", "D"), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios5_3() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToAddToDataNodes(Set.of("D"), 3);
        zoneState.nodesToRemoveFromDataNodes(Set.of("C"), 7);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 7);

        zoneState.nodesToAddToDataNodes(Set.of("C"), 9);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 9);

        assertDataNodesForZone(1, Set.of("A", "B", "C", "D"), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios5_4() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToRemoveFromDataNodes(Set.of("C"), 3);
        zoneState.nodesToAddToDataNodes(Set.of("D"), 7);
        zoneState.nodesToRemoveFromDataNodes(Set.of("D"), 9);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 9);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 7);

        assertDataNodesForZone(1, Set.of("A", "B"), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios5_5() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToRemoveFromDataNodes(Set.of("C"), 3);
        zoneState.nodesToAddToDataNodes(Set.of("D"), 7);
        zoneState.nodesToRemoveFromDataNodes(Set.of("D"), 9);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 7);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 9);

        assertDataNodesForZone(1, Set.of("A", "B"), keyValueStorage);
    }

    @Test
    void testVariousScaleUpScaleDownScenarios5_6() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.nodesToRemoveFromDataNodes(Set.of("C"), 3);
        zoneState.nodesToAddToDataNodes(Set.of("D"), 7);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 7);

        zoneState.nodesToRemoveFromDataNodes(Set.of("D"), 9);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 9);

        assertDataNodesForZone(1, Set.of("A", "B"), keyValueStorage);
    }

    @Test
    void testZoneStateAddRemoveNodesPreservesDuplicationsOfNodes() {
        ZoneState zoneState = new ZoneState(new ScheduledThreadPoolExecutor(1));

        zoneState.nodesToAddToDataNodes(Set.of("A", "B"), 1);
        zoneState.nodesToAddToDataNodes(Set.of("A", "B"), 2);

        List<String> nodes = zoneState.nodesToBeAddedToDataNodes(0, 2);

        Collections.sort(nodes);
        assertEquals(List.of("A", "A", "B", "B"), nodes);

        zoneState.nodesToRemoveFromDataNodes(Set.of("C", "D"), 3);
        zoneState.nodesToRemoveFromDataNodes(Set.of("C", "D"), 4);

        nodes = zoneState.nodesToBeRemovedFromDataNodes(2, 4);

        Collections.sort(nodes);

        assertEquals(List.of("C", "C", "D", "D"), nodes);
    }

    /**
     * Creates a zone with the auto adjust scale up scale down trigger equals to 0 and the data nodes equals ["A", "B", "C"].
     *
     * @throws Exception when something goes wrong.
     */
    private void preparePrerequisites() throws Exception {
        topology.putNode(NODE_1);
        topology.putNode(NODE_2);
        topology.putNode(NODE_3);

        Set<LogicalNode> clusterNodes = Set.of(NODE_1, NODE_2, NODE_3);
        Set<String> clusterNodesNames = Set.of(NODE_1.name(), NODE_2.name(), NODE_3.name());

        mockVaultZonesLogicalTopologyKey(clusterNodes);

        startDistributionZoneManager();

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME)
                        .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                        .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                        .build()
        ).get();

        assertDataNodesForZone(1, clusterNodesNames, keyValueStorage);

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

                    Set<String> res = DistributionZonesUtil.dataNodes(ByteUtils.fromBytes(dataNodes));

                    return res.equals(clusterNodes);
                },
                1000
        ));
    }

    private void startDistributionZoneManager() throws Exception {
        deployWatchesAndUpdateMetaStorageRevision(metaStorageManager);

        distributionZoneManager.start();
    }

    private void setLogicalTopologyInMetaStorage(Set<String> nodes) {
        CompletableFuture<Boolean> invokeFuture = metaStorageManager.invoke(
                Conditions.exists(zonesLogicalTopologyKey()),
                put(zonesLogicalTopologyKey(), toBytes(nodes)),
                noop()
        );

        assertThat(invokeFuture, willBe(true));
    }

    private void mockVaultZonesLogicalTopologyKey(Set<LogicalNode> nodes) {
        Set<String> nodesNames = nodes.stream().map(ClusterNode::name).collect(Collectors.toSet());

        byte[] newLogicalTopology = toBytes(nodesNames);

        assertThat(vaultMgr.put(zonesLogicalTopologyKey(), newLogicalTopology), willCompleteSuccessfully());
    }
}
