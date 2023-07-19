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

package org.apache.ignite.internal.distributionzones.causalitydatanodes;

import static java.util.Collections.emptySet;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl.LOGICAL_TOPOLOGY_KEY;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_ZONE_ID;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_ZONE_NAME;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.alterFilterAndGetRevision;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertDataNodesFromManager;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertValueInStorage;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.createMetastorageTopologyListener;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.createZoneAndGetRevision;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.createZonesConfigurationListener;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.onUpdateFilter;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.onUpdateScaleDown;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.onUpdateScaleUp;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.putNodeInLogicalTopologyAndGetRevision;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.extractZoneId;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesDataNodesPrefix;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyPrefix;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.ByteUtils.fromBytes;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.apache.ignite.internal.util.IgniteUtils.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.distributionzones.BaseDistributionZoneManagerTest;
import org.apache.ignite.internal.distributionzones.DistributionZoneConfigurationParameters.Builder;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.distributionzones.DistributionZonesUtil;
import org.apache.ignite.internal.distributionzones.Node;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneView;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.EntryEvent;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.DistributionZoneNotFoundException;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Tests for causality data nodes updating in {@link DistributionZoneManager}.
 */
public class DistributionZoneCausalityDataNodesTest extends BaseDistributionZoneManagerTest {
    private static final String ZONE_NAME_1 = "zone1";

    private static final String ZONE_NAME_2 = "zone2";

    private static final String ZONE_NAME_3 = "zone3";

    private static final int ZONE_ID_1 = 1;

    private static final int ZONE_ID_2 = 2;

    private static final int ZONE_ID_3 = 3;

    private static final LogicalNode NODE_0 =
            new LogicalNode("node_id_0", "node_name_0", new NetworkAddress("localhost", 123));

    private static final LogicalNode NODE_1 =
            new LogicalNode("node_id_1", "node_name_1", new NetworkAddress("localhost", 123));

    private static final LogicalNode NODE_2 =
            new LogicalNode("node_id_2", "node_name_2", new NetworkAddress("localhost", 123));

    private static final LogicalNode NODE_3 =
            new LogicalNode("node_id_3", "node_name_3", new NetworkAddress("localhost", 123));

    private static final Set<LogicalNode> ONE_NODE = Set.of(NODE_0);
    private static final Set<String> ONE_NODE_NAME = Set.of(NODE_0.name());

    private static final Set<LogicalNode> TWO_NODES = Set.of(NODE_0, NODE_1);
    private static final Set<String> TWO_NODES_NAMES = Set.of(NODE_0.name(), NODE_1.name());

    private static final Set<LogicalNode> THREE_NODES = Set.of(NODE_0, NODE_1, NODE_2);
    private static final Set<String> THREE_NODES_NAMES = Set.of(NODE_0.name(), NODE_1.name(), NODE_2.name());

    private static final Set<LogicalNode> FOUR_NODES = Set.of(NODE_0, NODE_1, NODE_2, NODE_3);
    private static final Set<String> FOUR_NODES_NAMES = Set.of(NODE_0.name(), NODE_1.name(), NODE_2.name(), NODE_3.name());

    /**
     * Contains futures that is completed when the topology watch listener receive the event with expected logical topology.
     * Mapping of node names -> future with event revision.
     */
    private final ConcurrentHashMap<Set<String>, CompletableFuture<Long>> topologyRevisions = new ConcurrentHashMap<>();

    /**
     * Contains futures that is completed when the data nodes watch listener receive the event with expected zone id and data nodes.
     * Mapping of zone id and node names -> future with event revision.
     */
    private final ConcurrentHashMap<IgniteBiTuple<Integer, Set<String>>, CompletableFuture<Long>> zoneDataNodesRevisions =
            new ConcurrentHashMap<>();

    /**
     * Contains futures that is completed when the scale up update listener receive the event with expected zone id.
     * Mapping of zone id -> future with event revision.
     */
    private final ConcurrentHashMap<Integer, CompletableFuture<Long>> zoneScaleUpRevisions = new ConcurrentHashMap<>();

    /**
     * Contains futures that is completed when the scale down update listener receive the event with expected zone id.
     * Mapping of zone id -> future with event revision.
     */
    private final ConcurrentHashMap<Integer, CompletableFuture<Long>> zoneScaleDownRevisions = new ConcurrentHashMap<>();

    /**
     * Contains futures that is completed when the filter update listener receive the event with expected zone id.
     * Mapping of zone id -> future with event revision.
     */
    private final ConcurrentHashMap<Integer, CompletableFuture<Long>> zoneChangeFilterRevisions = new ConcurrentHashMap<>();

    /**
     * Contains futures that is completed when the zone configuration listener receive the zone creation event with expected zone id.
     * Mapping of zone id -> future with event revision.
     */
    private final ConcurrentHashMap<Integer, CompletableFuture<Long>> createZoneRevisions = new ConcurrentHashMap<>();

    /**
     * Contains futures that is completed when the zone configuration listener receive the zone dropping event with expected zone id.
     * Mapping of zone id -> future with event revision.
     */
    private final ConcurrentHashMap<Integer, CompletableFuture<Long>> dropZoneRevisions = new ConcurrentHashMap<>();

    @BeforeEach
    void beforeEach() {
        metaStorageManager.registerPrefixWatch(zonesLogicalTopologyPrefix(), createMetastorageTopologyListener(topologyRevisions));

        metaStorageManager.registerPrefixWatch(zonesDataNodesPrefix(), createMetastorageDataNodesListener());

        ConfigurationNamedListListener<DistributionZoneView> zonesConfigurationListener = createZonesConfigurationListener(
                createZoneRevisions, dropZoneRevisions);

        zonesConfiguration.distributionZones().listenElements(zonesConfigurationListener);
        zonesConfiguration.distributionZones().any().dataNodesAutoAdjustScaleUp().listen(onUpdateScaleUp(zoneScaleUpRevisions));
        zonesConfiguration.distributionZones().any().dataNodesAutoAdjustScaleDown().listen(onUpdateScaleDown(zoneScaleDownRevisions));
        zonesConfiguration.distributionZones().any().filter().listen(onUpdateFilter(zoneChangeFilterRevisions));

        zonesConfiguration.defaultDistributionZone().listen(zonesConfigurationListener);
        zonesConfiguration.defaultDistributionZone().dataNodesAutoAdjustScaleUp().listen(onUpdateScaleUp(zoneScaleUpRevisions));
        zonesConfiguration.defaultDistributionZone().dataNodesAutoAdjustScaleDown().listen(onUpdateScaleDown(zoneScaleDownRevisions));
        zonesConfiguration.defaultDistributionZone().filter().listen(onUpdateFilter(zoneChangeFilterRevisions));

        distributionZoneManager.start();

        metaStorageManager.deployWatches();
    }

    /**
     * Tests data nodes updating on a topology leap.
     *
     * @throws Exception If failed.
     */
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-19288")
    @Test
    void topologyLeapUpdate() throws Exception {
        // Prerequisite.

        // Create the zone with immediate timers.
        distributionZoneManager.createZone(
                        new Builder(ZONE_NAME_1)
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        // Create the zone with not immediate timers.
        distributionZoneManager.createZone(
                        new Builder(ZONE_NAME_2)
                                .dataNodesAutoAdjustScaleUp(1)
                                .dataNodesAutoAdjustScaleDown(1)
                                .build()
                )
                .get(3, SECONDS);

        // Create logical topology with NODE_0 and NODE_1.
        topology.putNode(NODE_0);

        Set<LogicalNode> twoNodes1 = Set.of(NODE_0, NODE_1);
        Set<String> twoNodesNames1 = Set.of(NODE_0.name(), NODE_1.name());

        CompletableFuture<Long> dataNodesUpdateRevision = getZoneDataNodesRevision(ZONE_ID_2, twoNodes1);

        // Check that data nodes value of both zone is NODE_0 and NODE_1.
        long topologyRevision1 = putNodeInLogicalTopologyAndGetRevision(NODE_1, twoNodes1, topology, topologyRevisions);

        Set<String> dataNodes0 = getDataNodesFromListener(topologyRevision1, ZONE_ID_1);
        assertEquals(twoNodesNames1, dataNodes0);

        long dataNodesRevisionZone = dataNodesUpdateRevision.get(3, SECONDS);

        System.out.println("test_log dataNodesRevisionZone=" + dataNodesRevisionZone);

        Set<String> dataNodes1 = getDataNodesFromListener(dataNodesRevisionZone, ZONE_ID_2);
        assertEquals(twoNodesNames1, dataNodes1);

        // Test steps.

        // Change logical topology. NODE_1 is left. NODE_2 is added.
        Set<LogicalNode> twoNodes2 = Set.of(NODE_0, NODE_2);
        Set<String> twoNodesNames2 = Set.of(NODE_0.name(), NODE_2.name());

        dataNodesUpdateRevision = getZoneDataNodesRevision(ZONE_ID_2, twoNodes2);

        long topologyRevision2 = fireTopologyLeapAndGetRevision(twoNodes2);

        System.out.println("test_log topologyRevision2=" + topologyRevision2);

        // Check that data nodes value of the zone with immediate timers with the topology update revision is NODE_0 and NODE_2.
        Set<String> dataNodes3 = getDataNodesFromListener(topologyRevision2, ZONE_ID_1);
        assertEquals(twoNodesNames2, dataNodes3);

        // Check that data nodes value of the zone with not immediate timers with the topology update revision is NODE_0 and NODE_1.
        Set<String> dataNodes4 = getDataNodesFromListener(topologyRevision2, ZONE_ID_2);
        assertEquals(twoNodesNames1, dataNodes4);

        // Check that data nodes value of the zone with not immediate timers with the data nodes update revision is NODE_0 and NODE_2.
        dataNodesRevisionZone = dataNodesUpdateRevision.get(5, SECONDS);
        Set<String> dataNodes5 = getDataNodesFromListener(dataNodesRevisionZone, ZONE_ID_2);
        assertEquals(twoNodesNames2, dataNodes5);
    }

    /**
     * Tests data nodes updating on a topology leap with not immediate scale up and immediate scale down.
     *
     * @throws Exception If failed.
     */
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-19288")
    @Test
    void topologyLeapUpdateScaleUpNotImmediateAndScaleDownImmediate() throws Exception {
        // Prerequisite.

        // Create the zone with immediate timers.
        distributionZoneManager.createZone(
                        new Builder(ZONE_NAME_1)
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        // Alter the zone with immediate timers.
        distributionZoneManager.alterZone(DEFAULT_ZONE_NAME,
                        new Builder(DEFAULT_ZONE_NAME)
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        // Create logical topology with NODE_0 and NODE_1.
        topology.putNode(NODE_0);

        Set<LogicalNode> twoNodes1 = Set.of(NODE_0, NODE_1);
        Set<String> twoNodesNames1 = Set.of(NODE_0.name(), NODE_1.name());

        // Check that data nodes value of both zone is NODE_0 and NODE_1.
        long topologyRevision1 = putNodeInLogicalTopologyAndGetRevision(NODE_1, twoNodes1, topology, topologyRevisions);

        Set<String> dataNodes0 = getDataNodesFromListener(topologyRevision1, DEFAULT_ZONE_ID);
        assertEquals(twoNodesNames1, dataNodes0);

        Set<String> dataNodes1 = getDataNodesFromListener(topologyRevision1, ZONE_ID_1);
        assertEquals(twoNodesNames1, dataNodes1);

        // Alter zones with not immediate scale up timer.
        distributionZoneManager.alterZone(ZONE_NAME_1,
                        new Builder(ZONE_NAME_1)
                                .dataNodesAutoAdjustScaleUp(1)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        distributionZoneManager.alterZone(DEFAULT_ZONE_NAME,
                        new Builder(DEFAULT_ZONE_NAME)
                                .dataNodesAutoAdjustScaleUp(1)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        // Test steps.

        // Change logical topology. NODE_1 is left. NODE_2 is added.
        Set<LogicalNode> twoNodes2 = Set.of(NODE_0, NODE_2);
        Set<String> twoNodesNames2 = Set.of(NODE_0.name(), NODE_2.name());

        CompletableFuture<Long> dataNodesUpdateRevision0 = getZoneDataNodesRevision(DEFAULT_ZONE_ID, twoNodes2);
        CompletableFuture<Long> dataNodesUpdateRevision1 = getZoneDataNodesRevision(ZONE_ID_1, twoNodes2);

        long topologyRevision2 = fireTopologyLeapAndGetRevision(twoNodes2);

        // Check that data nodes value of zones is NODE_0 because scale up timer has not fired yet.
        Set<String> oneNodeNames = Set.of(NODE_0.name());

        System.out.println("test_log topologyRevision2=" + topologyRevision2);

        Set<String> dataNodes2 = getDataNodesFromListener(topologyRevision2, DEFAULT_ZONE_ID);
        assertEquals(oneNodeNames, dataNodes2);

        Set<String> dataNodes3 = getDataNodesFromListener(topologyRevision2, ZONE_ID_1);
        assertEquals(oneNodeNames, dataNodes3);

        // Check that data nodes value of zones is NODE_0 and NODE_2.
        long dataNodesRevisionZone0 = dataNodesUpdateRevision0.get(3, SECONDS);
        Set<String> dataNodes4 = getDataNodesFromListener(dataNodesRevisionZone0, DEFAULT_ZONE_ID);
        assertEquals(twoNodesNames2, dataNodes4);

        long dataNodesRevisionZone1 = dataNodesUpdateRevision1.get(3, SECONDS);
        Set<String> dataNodes5 = getDataNodesFromListener(dataNodesRevisionZone1, ZONE_ID_1);
        assertEquals(twoNodesNames2, dataNodes5);
    }

    /**
     * Tests data nodes updating on a topology leap with immediate scale up and not immediate scale down.
     *
     * @throws Exception If failed.
     */
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-19288")
    @Test
    void topologyLeapUpdateScaleUpImmediateAndScaleDownNotImmediate() throws Exception {
        // Prerequisite.

        // Create the zone with not immediate scale down timer.
        distributionZoneManager.createZone(
                        new Builder(ZONE_NAME_1)
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(1)
                                .build()
                )
                .get(3, SECONDS);

        // Alter the zone with not immediate scale down timer.
        distributionZoneManager.alterZone(DEFAULT_ZONE_NAME,
                        new Builder(DEFAULT_ZONE_NAME)
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(1)
                                .build()
                )
                .get(3, SECONDS);

        // Create logical topology with NODE_0 and NODE_1.
        topology.putNode(NODE_0);

        Set<LogicalNode> twoNodes1 = Set.of(NODE_0, NODE_1);
        Set<String> twoNodesNames1 = Set.of(NODE_0.name(), NODE_1.name());

        // Check that data nodes value of both zone is NODE_0 and NODE_1.
        long topologyRevision1 = putNodeInLogicalTopologyAndGetRevision(NODE_1, twoNodes1, topology, topologyRevisions);

        Set<String> dataNodes0 = getDataNodesFromListener(topologyRevision1, DEFAULT_ZONE_ID);
        assertEquals(twoNodesNames1, dataNodes0);

        Set<String> dataNodes1 = getDataNodesFromListener(topologyRevision1, ZONE_ID_1);
        assertEquals(twoNodesNames1, dataNodes1);

        // Test steps.

        // Change logical topology. NODE_1 is left. NODE_2 is added.
        Set<LogicalNode> twoNodes2 = Set.of(NODE_0, NODE_2);
        Set<String> twoNodesNames2 = Set.of(NODE_0.name(), NODE_2.name());

        CompletableFuture<Long> dataNodesUpdateRevision0 = getZoneDataNodesRevision(DEFAULT_ZONE_ID, twoNodes2);
        CompletableFuture<Long> dataNodesUpdateRevision1 = getZoneDataNodesRevision(ZONE_ID_1, twoNodes2);

        long topologyRevision2 = fireTopologyLeapAndGetRevision(twoNodes2);

        // Check that data nodes value of zones is NODE_0, NODE_1 and NODE_2 because scale down timer has not fired yet.
        Set<LogicalNode> threeNodes = Set.of(NODE_0, NODE_1, NODE_2);
        Set<String> threeNodesNames = Set.of(NODE_0.name(), NODE_1.name(), NODE_2.name());

        Set<String> dataNodes2 = getDataNodesFromListener(topologyRevision2, DEFAULT_ZONE_ID);
        assertEquals(threeNodesNames, dataNodes2);

        Set<String> dataNodes3 = getDataNodesFromListener(topologyRevision2, ZONE_ID_1);
        assertEquals(threeNodesNames, dataNodes3);

        // Check that data nodes value of zones is NODE_0 and NODE_2.
        long dataNodesRevisionZone0 = dataNodesUpdateRevision0.get(3, SECONDS);
        Set<String> dataNodes4 = getDataNodesFromListener(dataNodesRevisionZone0, DEFAULT_ZONE_ID);
        assertEquals(twoNodesNames2, dataNodes4);

        long dataNodesRevisionZone1 = dataNodesUpdateRevision1.get(3, SECONDS);
        Set<String> dataNodes5 = getDataNodesFromListener(dataNodesRevisionZone1, ZONE_ID_1);
        assertEquals(twoNodesNames2, dataNodes5);
    }

    /**
     * Tests data nodes updating on a scale up changing.
     *
     * @throws Exception If failed.
     */
    @Test
    void dataNodesUpdatedAfterScaleUpChanged() throws Exception {
        // Prerequisite.

        // Create the zone with immediate timers.
        distributionZoneManager.createZone(
                        new Builder(ZONE_NAME_1)
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        // Create logical topology with NODE_0.
        Set<LogicalNode> oneNode = Set.of(NODE_0);
        Set<String> oneNodeName = Set.of(NODE_0.name());

        long topologyRevision1 = putNodeInLogicalTopologyAndGetRevision(NODE_0, oneNode, topology, topologyRevisions);

        System.out.println("test_log topologyRevision1=" + topologyRevision1);

        // Check that data nodes value of the the zone is NODE_0.
        Set<String> dataNodes1 = getDataNodesFromListener(topologyRevision1, ZONE_ID_1);
        assertEquals(oneNodeName, dataNodes1);

        // Changes a scale up timer to not immediate.
        distributionZoneManager.alterZone(
                        ZONE_NAME_1,
                        new Builder(ZONE_NAME_1)
                                .dataNodesAutoAdjustScaleUp(10000)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        // Test steps.

        // Change logical topology. NODE_1 is added.
        Set<LogicalNode> twoNodes = Set.of(NODE_0, NODE_1);
        Set<String> twoNodesNames = Set.of(NODE_0.name(), NODE_1.name());

        long topologyRevision2 = putNodeInLogicalTopologyAndGetRevision(NODE_1, twoNodes, topology, topologyRevisions);

        System.out.println("test_log topologyRevision2=" + topologyRevision2);
        // Check that data nodes value of the zone with the topology update revision is NODE_0 because scale up timer has not fired yet.
        Set<String> dataNodes2 = getDataNodesFromListener(topologyRevision2, ZONE_ID_1);
        assertEquals(oneNodeName, dataNodes2);

        // Change scale up value to immediate.
        long scaleUpRevision = alterZoneScaleUpAndGetRevision(ZONE_NAME_1, IMMEDIATE_TIMER_VALUE);

        System.out.println("test_log scaleUpRevision=" + scaleUpRevision);
        // Check that data nodes value of the zone with the scale up update revision is NODE_0 and NODE_1.
        Set<String> dataNodes3 = getDataNodesFromListener(scaleUpRevision, ZONE_ID_1);
        assertEquals(twoNodesNames, dataNodes3);
    }

    /**
     * Tests data nodes updating on a scale down changing.
     *
     * @throws Exception If failed.
     */
    @Test
    void dataNodesUpdatedAfterScaleDownChanged() throws Exception {
        // Prerequisite.

        // Create the zone with immediate scale up timer and not immediate scale down timer.
        distributionZoneManager.createZone(
                        new Builder(ZONE_NAME_1)
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(10000)
                                .build()
                )
                .get(3, SECONDS);

        // Create logical topology with NODE_0 and NODE_1.
        topology.putNode(NODE_0);

        Set<LogicalNode> twoNodes = Set.of(NODE_0, NODE_1);
        Set<String> twoNodesNames = Set.of(NODE_0.name(), NODE_1.name());

        long topologyRevision1 = putNodeInLogicalTopologyAndGetRevision(NODE_1, twoNodes, topology, topologyRevisions);

        System.out.println("test_log topologyRevision1=" + topologyRevision1);

        // Check that data nodes value of the the zone is NODE_0 and NODE_1.
        Set<String> dataNodes1 = getDataNodesFromListener(topologyRevision1, ZONE_ID_1);
        assertEquals(twoNodesNames, dataNodes1);

        // Test steps.

        // Change logical topology. NODE_1 is left.
        Set<LogicalNode> oneNode = Set.of(NODE_0);
        Set<String> oneNodeName = Set.of(NODE_0.name());

        long topologyRevision2 = removeNodeInLogicalTopologyAndGetRevision(Set.of(NODE_1), oneNode);

        System.out.println("test_log topologyRevision2=" + topologyRevision2);

        // Check that data nodes value of the zone with the topology update revision is NODE_0 and NODE_1
        // because scale down timer has not fired yet.
        Set<String> dataNodes2 = getDataNodesFromListener(topologyRevision2, ZONE_ID_1);
        assertEquals(twoNodesNames, dataNodes2);

        // Change scale down value to immediate.
        long scaleDownRevision = alterZoneScaleDownAndGetRevision(ZONE_NAME_1, IMMEDIATE_TIMER_VALUE);

        // Check that data nodes value of the zone with the scale down update revision is NODE_0.
        Set<String> dataNodes3 = getDataNodesFromListener(scaleDownRevision, ZONE_ID_1);
        assertEquals(oneNodeName, dataNodes3);
    }

    /**
     * Tests data nodes dropping when a scale up task is scheduled.
     *
     * @throws Exception If failed.
     */
    @Test
    void scheduleScaleUpTaskThenDropZone() throws Exception {
        // Prerequisite.

        // Create the zone with immediate timers.
        distributionZoneManager.createZone(
                        new Builder(ZONE_NAME_1)
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        Set<LogicalNode> oneNode = Set.of(NODE_0);
        Set<String> oneNodeName = Set.of(NODE_0.name());

        // Create logical topology with NODE_0.
        long topologyRevision1 = putNodeInLogicalTopologyAndGetRevision(NODE_0, Set.of(NODE_0), topology, topologyRevisions);

        // Check that data nodes value of the zone is NODE_0.
        Set<String> dataNodes1 = getDataNodesFromListener(topologyRevision1, ZONE_ID_1);
        assertEquals(oneNodeName, dataNodes1);

        // Alter the zones with not immediate scale up timer.
        distributionZoneManager.alterZone(ZONE_NAME_1,
                        new Builder(ZONE_NAME_1)
                                .dataNodesAutoAdjustScaleUp(10000)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        // Test steps.

        // Change logical topology. NODE_1 is added.
        Set<LogicalNode> twoNodes = Set.of(NODE_0, NODE_1);
        Set<String> twoNodesNames = Set.of(NODE_0.name(), NODE_1.name());

        long topologyRevision2 = putNodeInLogicalTopologyAndGetRevision(NODE_1, twoNodes, topology, topologyRevisions);

        assertValueInStorage(
                metaStorageManager,
                zoneDataNodesKey(ZONE_ID_1),
                (v) -> DistributionZonesUtil.dataNodes(fromBytes(v)).stream().map(Node::nodeName).collect(toSet()),
                oneNodeName,
                3000
        );

        System.out.println("test_log topologyRevision2=" + topologyRevision2);

        long dropRevision1 = dropZoneAndGetRevision(ZONE_NAME_1);

        System.out.println("test_log dropRevision1=" + dropRevision1);

        // Check that data nodes value of the zone with the topology update revision is NODE_0 because scale up timer has not fired.
        Set<String> dataNodes3 = getDataNodesFromListener(topologyRevision2, ZONE_ID_1);
        assertEquals(oneNodeName, dataNodes3);

        // Check that zones is removed and attempt to get data nodes throws an exception.
        assertThrowsWithCause(() -> distributionZoneManager.dataNodes(dropRevision1, ZONE_ID_1), DistributionZoneNotFoundException.class);
    }

    /**
     * Tests data nodes dropping when a scale down task is scheduled.
     *
     * @throws Exception If failed.
     */
    @Test
    void scheduleScaleDownTaskThenDropZone() throws Exception {
        // Prerequisite.

        // Create the zone with immediate scale up timer and not immediate scale down timer.
        distributionZoneManager.createZone(
                        new Builder(ZONE_NAME_1)
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(10000)
                                .build()
                )
                .get(3, SECONDS);

        // Create logical topology with NODE_0 and NODE_1.
        topology.putNode(NODE_0);

        Set<LogicalNode> twoNodes = Set.of(NODE_0, NODE_1);
        Set<String> twoNodesNames = Set.of(NODE_0.name(), NODE_1.name());

        long topologyRevision1 = putNodeInLogicalTopologyAndGetRevision(NODE_1, twoNodes, topology, topologyRevisions);

        // Check that data nodes value of the the zone is NODE_0 and NODE_1.
        Set<String> dataNodes1 = getDataNodesFromListener(topologyRevision1, ZONE_ID_1);
        assertEquals(twoNodesNames, dataNodes1);

        // Test steps.

        // Change logical topology. NODE_1 is removed.
        Set<LogicalNode> oneNode = Set.of(NODE_0);
        Set<String> oneNodeName = Set.of(NODE_0.name());

        long topologyRevision2 = removeNodeInLogicalTopologyAndGetRevision(Set.of(NODE_1), oneNode);

        assertValueInStorage(
                metaStorageManager,
                zoneDataNodesKey(ZONE_ID_1),
                (v) -> DistributionZonesUtil.dataNodes(fromBytes(v)).stream().map(Node::nodeName).collect(toSet()),
                twoNodesNames,
                3000
        );

        long dropRevision1 = dropZoneAndGetRevision(ZONE_NAME_1);

        // Check that data nodes value of the zone with the topology update revision is NODE_0 and NODE_1
        // because scale down timer has not fired.
        Set<String> dataNodes3 = getDataNodesFromListener(topologyRevision2, ZONE_ID_1);
        assertEquals(twoNodesNames, dataNodes3);

        // Check that zones is removed and attempt to get data nodes throws an exception.
        assertThrowsWithCause(() -> distributionZoneManager.dataNodes(dropRevision1, ZONE_ID_1), DistributionZoneNotFoundException.class);
    }

    /**
     * Tests data nodes updating when a filter is changed even when actual data nodes value is not changed.
     *
     * @throws Exception If failed.
     */
    @Test
    void changeFilter() throws Exception {
        // Prerequisite.

        // Create the zone with immediate timers.
        distributionZoneManager.createZone(
                        new Builder(ZONE_NAME_1)
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        // Alter the zone with immediate timers.
        distributionZoneManager.alterZone(DEFAULT_ZONE_NAME,
                        new Builder(DEFAULT_ZONE_NAME)
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        // Create logical topology with NODE_0.
        Set<LogicalNode> oneNode = Set.of(NODE_0);
        Set<String> oneNodeName = Set.of(NODE_0.name());

        // Check that data nodes value of both zone is NODE_0.
        long topologyRevision1 = putNodeInLogicalTopologyAndGetRevision(NODE_0, Set.of(NODE_0), topology, topologyRevisions);

        Set<String> dataNodes0 = getDataNodesFromListener(topologyRevision1, DEFAULT_ZONE_ID);
        assertEquals(oneNodeName, dataNodes0);

        Set<String> dataNodesFut1 = getDataNodesFromListener(topologyRevision1, ZONE_ID_1);
        assertEquals(oneNodeName, dataNodesFut1);

        // Alter the zones with infinite timers.
        distributionZoneManager.alterZone(ZONE_NAME_1,
                        new Builder(ZONE_NAME_1)
                                .dataNodesAutoAdjustScaleUp(INFINITE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        // Alter the zone with infinite timers.
        distributionZoneManager.alterZone(DEFAULT_ZONE_NAME,
                        new Builder(DEFAULT_ZONE_NAME)
                                .dataNodesAutoAdjustScaleUp(INFINITE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        // Test steps.

        String filter = "$[?($..* || @.region == 'US')]";

        // Change filter and get revision of this change.
        long filterRevision0 = alterFilterAndGetRevision(DEFAULT_ZONE_NAME, filter, distributionZoneManager, zoneChangeFilterRevisions);
        long filterRevision1 = alterFilterAndGetRevision(ZONE_NAME_1, filter, distributionZoneManager, zoneChangeFilterRevisions);

        // Check that data nodes value of the the zone is NODE_0.
        // The future didn't hang due to the fact that the actual data nodes value did not change.
        Set<String> dataNodes3 = getDataNodesFromListener(filterRevision0, DEFAULT_ZONE_ID);
        assertEquals(oneNodeName, dataNodes3);

        Set<String> dataNodes4 = getDataNodesFromListener(filterRevision1, ZONE_ID_1);
        assertEquals(oneNodeName, dataNodes4);

    }

    @Test
    void createZoneWithNotImmediateTimers() throws Exception {
        // Prerequisite.

        // Create logical topology with NODE_0.
        topology.putNode(NODE_0);

        Set<LogicalNode> oneNode = Set.of(NODE_0);
        Set<String> oneNodeName = Set.of(NODE_0.name());

        putNodeInLogicalTopologyAndGetRevision(NODE_0, oneNode, topology, topologyRevisions);

        // Test steps.

        // Create a zone.
        long createZoneRevision = createZoneAndGetRevision(
                ZONE_NAME_1,
                ZONE_ID_1,
                INFINITE_TIMER_VALUE,
                INFINITE_TIMER_VALUE,
                distributionZoneManager,
                createZoneRevisions);

        System.out.println("test_log createZoneRevision=" + createZoneRevision);

        // Check that data nodes value of the zone with the create zone revision is NODE_0.
        Set<String> dataNodes2 = getDataNodesFromListener(createZoneRevision, ZONE_ID_1);
        assertEquals(oneNodeName, dataNodes2);
    }

    /**
     * Tests data nodes obtaining with revision before a zone creation and after a zone dropping.
     *
     * @throws Exception If failed.
     */
    @Test
    void createThenDropZone() throws Exception {
        // Prerequisite.

        // Create logical topology with NODE_0 and NODE_1.
        topology.putNode(NODE_0);
        topology.putNode(NODE_1);

        Set<LogicalNode> oneNode = Set.of(NODE_0);
        Set<String> oneNodeName = Set.of(NODE_0.name());

        Set<String> twoNodesNames = Set.of(NODE_0.name(), NODE_1.name());

        // Test steps.

        // Create a zone.
        long createZoneRevision = createZoneAndGetRevision(
                ZONE_NAME_1,
                ZONE_ID_1,
                IMMEDIATE_TIMER_VALUE,
                IMMEDIATE_TIMER_VALUE,
                distributionZoneManager,
                createZoneRevisions);

        // Check that data nodes value of the zone with the revision lower than the create zone revision is absent.
        assertThrowsWithCause(
                () -> distributionZoneManager.dataNodes(createZoneRevision - 1, ZONE_ID_1),
                DistributionZoneNotFoundException.class
        );

        // Check that data nodes value of the zone with the create zone revision is NODE_0 and NODE_1.
        Set<String> dataNodes2 = getDataNodesFromListener(createZoneRevision, ZONE_ID_1);
        assertEquals(twoNodesNames, dataNodes2);

        // Drop the zone.
        long dropZoneRevision = dropZoneAndGetRevision(ZONE_NAME_1);

        // Check that data nodes value of the zone with the drop zone revision is absent.
        assertThrowsWithCause(
                () -> distributionZoneManager.dataNodes(dropZoneRevision, ZONE_ID_1), DistributionZoneNotFoundException.class
        );
    }

    /**
     * Tests data nodes obtaining with wrong parameters throw an exception.
     *
     * @throws Exception If failed.
     */
    @Test
    void validationTest() {
        assertThrowsWithCause(() -> distributionZoneManager.dataNodes(0, DEFAULT_ZONE_ID), IllegalArgumentException.class);

        assertThrowsWithCause(() -> distributionZoneManager.dataNodes(-1, DEFAULT_ZONE_ID), IllegalArgumentException.class);

        assertThrowsWithCause(() -> distributionZoneManager.dataNodes(1, -1), IllegalArgumentException.class);
    }

    /**
     * Tests data nodes changing when topology is changed.
     *
     * @throws Exception If failed.
     */
    @Test
    void simpleTopologyChanges() throws Exception {
        // Prerequisite.

        // Create zones with immediate timers.
        distributionZoneManager.alterZone(DEFAULT_ZONE_NAME,
                        new Builder(DEFAULT_ZONE_NAME)
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        distributionZoneManager.createZone(new Builder(ZONE_NAME_1)
                        .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                        .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                        .build()
                )
                .get(3, SECONDS);

        // Test steps.

        Set<LogicalNode> oneNode = Set.of(NODE_0);
        Set<String> oneNodeName = Set.of(NODE_0.name());

        Set<LogicalNode> twoNodes = Set.of(NODE_0, NODE_1);
        Set<String> twoNodesNames = Set.of(NODE_0.name(), NODE_1.name());

        Set<LogicalNode> threeNodes = Set.of(NODE_0, NODE_1, NODE_2);
        Set<String> threeNodeNames = Set.of(NODE_0.name(), NODE_1.name(), NODE_2.name());

        // Change logical topology. NODE_0 is added.
        long topologyRevision0 = putNodeInLogicalTopologyAndGetRevision(NODE_0, oneNode, topology, topologyRevisions);

        // Change logical topology. NODE_1 is added.
        long topologyRevision1 = putNodeInLogicalTopologyAndGetRevision(NODE_1, twoNodes, topology, topologyRevisions);

        System.out.println("test_log topologyRevision0=" + topologyRevision0);
        System.out.println("test_log topologyRevision1=" + topologyRevision1);

        Set<String> dataNodes1 = getDataNodesFromListener(topologyRevision0, DEFAULT_ZONE_ID);
        assertEquals(oneNodeName, dataNodes1);
        Set<String> dataNodes2 = getDataNodesFromListener(topologyRevision0, ZONE_ID_1);
        assertEquals(oneNodeName, dataNodes2);

        Set<String> dataNodes5 = getDataNodesFromListener(topologyRevision1, DEFAULT_ZONE_ID);
        assertEquals(twoNodesNames, dataNodes5);
        Set<String> dataNodes6 = getDataNodesFromListener(topologyRevision1, ZONE_ID_1);
        assertEquals(twoNodesNames, dataNodes6);

        Set<LogicalNode> twoNodes1 = Set.of(NODE_0, NODE_2);
        Set<String> twoNodesNames1 = Set.of(NODE_0.name(), NODE_2.name());

        // Change logical topology. NODE_2 is added.
        long topologyRevision2 = putNodeInLogicalTopologyAndGetRevision(NODE_2, threeNodes, topology, topologyRevisions);

        // Change logical topology. NODE_1 is left.
        long topologyRevision3 = removeNodeInLogicalTopologyAndGetRevision(Set.of(NODE_1), twoNodes1);

        System.out.println("test_log topologyRevision2=" + topologyRevision2);
        System.out.println("test_log topologyRevision3=" + topologyRevision3);

        Set<String> dataNodes7 = getDataNodesFromListener(topologyRevision2, DEFAULT_ZONE_ID);
        assertEquals(threeNodeNames, dataNodes7);
        Set<String> dataNodes8 = getDataNodesFromListener(topologyRevision2, ZONE_ID_1);
        assertEquals(threeNodeNames, dataNodes8);

        Set<String> dataNodes9 = getDataNodesFromListener(topologyRevision3, DEFAULT_ZONE_ID);
        assertEquals(twoNodesNames1, dataNodes9);
        Set<String> dataNodes10 = getDataNodesFromListener(topologyRevision3, ZONE_ID_1);
        assertEquals(twoNodesNames1, dataNodes10);
    }

    @Test
    void deadlocks() throws Exception {
        prepareZonesWithOneDataNodes();

        prepareZonesTimerValuesToTest();

        CountDownLatch scaleUpLatch = new CountDownLatch(1);

        Runnable dummyScaleUpTask = () -> {
            try {
                scaleUpLatch.await(5, SECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };

        CountDownLatch scaleDownLatch = new CountDownLatch(1);

        Runnable dummyScaleDownTask = () -> {
            try {
                scaleDownLatch.await(5, SECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };

        distributionZoneManager.zonesState().get(DEFAULT_ZONE_ID)
                .rescheduleScaleUp(IMMEDIATE_TIMER_VALUE, dummyScaleUpTask);
        distributionZoneManager.zonesState().get(ZONE_ID_1)
                .rescheduleScaleUp(IMMEDIATE_TIMER_VALUE, dummyScaleUpTask);
        distributionZoneManager.zonesState().get(ZONE_ID_2)
                .rescheduleScaleUp(IMMEDIATE_TIMER_VALUE, dummyScaleUpTask);
        distributionZoneManager.zonesState().get(ZONE_ID_3)
                .rescheduleScaleUp(IMMEDIATE_TIMER_VALUE, dummyScaleUpTask);

        distributionZoneManager.zonesState().get(DEFAULT_ZONE_ID)
                .rescheduleScaleDown(IMMEDIATE_TIMER_VALUE, dummyScaleDownTask);
        distributionZoneManager.zonesState().get(ZONE_ID_1)
                .rescheduleScaleDown(IMMEDIATE_TIMER_VALUE, dummyScaleDownTask);
        distributionZoneManager.zonesState().get(ZONE_ID_2)
                .rescheduleScaleDown(IMMEDIATE_TIMER_VALUE, dummyScaleDownTask);
        distributionZoneManager.zonesState().get(ZONE_ID_3)
                .rescheduleScaleDown(IMMEDIATE_TIMER_VALUE, dummyScaleDownTask);

        // Change logical topology. NODE_1 is added.
        long topologyRevision1 = putNodeInLogicalTopologyAndGetRevision(NODE_1, TWO_NODES, topology, topologyRevisions);
        waitForCondition(() -> metaStorageManager.appliedRevision() >= topologyRevision1, 3000);

        // Change logical topology. NODE_1 is removed.
        long topologyRevision2 = removeNodeInLogicalTopologyAndGetRevision(Set.of(NODE_1), ONE_NODE);
        waitForCondition(() -> metaStorageManager.appliedRevision() >= topologyRevision2, 3000);

        // Change logical topology. NODE_2 is added.
        long topologyRevision3 = putNodeInLogicalTopologyAndGetRevision(NODE_2, Set.of(NODE_0, NODE_2), topology, topologyRevisions);
        waitForCondition(() -> metaStorageManager.appliedRevision() >= topologyRevision3, 3000);

        // Change logical topology. NODE_3 is added.
        long topologyRevision4 = putNodeInLogicalTopologyAndGetRevision(
                NODE_3, Set.of(NODE_0, NODE_2, NODE_3), topology, topologyRevisions
        );
        waitForCondition(() -> metaStorageManager.appliedRevision() >= topologyRevision4, 3000);

        // Change logical topology. NODE_2 is removed.
        long topologyRevision5 = removeNodeInLogicalTopologyAndGetRevision(Set.of(NODE_2), Set.of(NODE_0, NODE_3));
        waitForCondition(() -> metaStorageManager.appliedRevision() >= topologyRevision5, 3000);

        Set<String> dataNodes5 = getDataNodesFromListener(topologyRevision5, DEFAULT_ZONE_ID);
        assertEquals(Set.of(NODE_0.name(), NODE_3.name()), dataNodes5);

        Set<String> dataNodes6 = getDataNodesFromListener(topologyRevision5, ZONE_ID_1);
        assertEquals(Set.of(NODE_0.name(), NODE_1.name(), NODE_2.name(), NODE_3.name()), dataNodes6);

        Set<String> dataNodes7 = getDataNodesFromListener(topologyRevision5, ZONE_ID_2);
        assertEquals(Set.of(NODE_0.name()), dataNodes7);

        Set<String> dataNodes8 = getDataNodesFromListener(topologyRevision5, ZONE_ID_3);
        assertEquals(Set.of(NODE_0.name()), dataNodes8);

        assertDataNodesFromManager(distributionZoneManager, DEFAULT_ZONE_ID, ONE_NODE, 3000);
        assertDataNodesFromManager(distributionZoneManager, ZONE_ID_1, ONE_NODE, 3000);
        assertDataNodesFromManager(distributionZoneManager, ZONE_ID_2, ONE_NODE, 3000);
        assertDataNodesFromManager(distributionZoneManager, ZONE_ID_3, ONE_NODE, 3000);

        assertValueInStorage(
                metaStorageManager,
                zoneDataNodesKey(DEFAULT_ZONE_ID),
                (v) -> DistributionZonesUtil.dataNodes(fromBytes(v)).stream().map(Node::nodeName).collect(toSet()),
                ONE_NODE_NAME,
                3000
        );

        assertValueInStorage(
                metaStorageManager,
                zoneDataNodesKey(ZONE_ID_1),
                (v) -> DistributionZonesUtil.dataNodes(fromBytes(v)).stream().map(Node::nodeName).collect(toSet()),
                ONE_NODE_NAME,
                3000
        );

        assertValueInStorage(
                metaStorageManager,
                zoneDataNodesKey(ZONE_ID_2),
                (v) -> DistributionZonesUtil.dataNodes(fromBytes(v)).stream().map(Node::nodeName).collect(toSet()),
                ONE_NODE_NAME,
                3000
        );

        assertValueInStorage(
                metaStorageManager,
                zoneDataNodesKey(ZONE_ID_3),
                (v) -> DistributionZonesUtil.dataNodes(fromBytes(v)).stream().map(Node::nodeName).collect(toSet()),
                ONE_NODE_NAME,
                3000
        );

        scaleUpLatch.countDown();
        scaleDownLatch.countDown();

        assertDataNodesFromManager(distributionZoneManager, DEFAULT_ZONE_ID, Set.of(NODE_0, NODE_3), 3000);
        assertDataNodesFromManager(distributionZoneManager, ZONE_ID_1, Set.of(NODE_0, NODE_1, NODE_2, NODE_3), 3000);
        assertDataNodesFromManager(distributionZoneManager, ZONE_ID_2, Set.of(NODE_0), 3000);
        assertDataNodesFromManager(distributionZoneManager, ZONE_ID_3, Set.of(NODE_0), 3000);

        assertValueInStorage(
                metaStorageManager,
                zoneDataNodesKey(DEFAULT_ZONE_ID),
                (v) -> DistributionZonesUtil.dataNodes(fromBytes(v)).stream().map(Node::nodeName).collect(toSet()),
                Set.of(NODE_0.name(), NODE_3.name()),
                3000
        );

        assertValueInStorage(
                metaStorageManager,
                zoneDataNodesKey(ZONE_ID_1),
                (v) -> DistributionZonesUtil.dataNodes(fromBytes(v)).stream().map(Node::nodeName).collect(toSet()),
                Set.of(NODE_0.name(), NODE_1.name(), NODE_2.name(), NODE_3.name()),
                3000
        );

        assertValueInStorage(
                metaStorageManager,
                zoneDataNodesKey(ZONE_ID_2),
                (v) -> DistributionZonesUtil.dataNodes(fromBytes(v)).stream().map(Node::nodeName).collect(toSet()),
                Set.of(NODE_0.name()),
                3000
        );

        assertValueInStorage(
                metaStorageManager,
                zoneDataNodesKey(ZONE_ID_3),
                (v) -> DistributionZonesUtil.dataNodes(fromBytes(v)).stream().map(Node::nodeName).collect(toSet()),
                Set.of(NODE_0.name()),
                3000
        );

        Set<String> dataNodes9 = getDataNodesFromListener(topologyRevision5, DEFAULT_ZONE_ID);
        assertEquals(Set.of(NODE_0.name(), NODE_3.name()), dataNodes9);

        Set<String> dataNodes10 = getDataNodesFromListener(topologyRevision5, ZONE_ID_1);
        assertEquals(Set.of(NODE_0.name(), NODE_1.name(), NODE_2.name(), NODE_3.name()), dataNodes10);

        Set<String> dataNodes11 = getDataNodesFromListener(topologyRevision5, ZONE_ID_2);
        assertEquals(Set.of(NODE_0.name()), dataNodes11);

        Set<String> dataNodes12 = getDataNodesFromListener(topologyRevision5, ZONE_ID_3);
        assertEquals(Set.of(NODE_0.name()), dataNodes12);
    }

    private long prepareZonesWithOneDataNodes() throws Exception {
        // Create zones with immediate timers.
        distributionZoneManager.alterZone(DEFAULT_ZONE_NAME,
                        new Builder(DEFAULT_ZONE_NAME)
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        distributionZoneManager.createZone(new Builder(ZONE_NAME_1)
                        .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                        .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                        .build()
                )
                .get(3, SECONDS);

        distributionZoneManager.createZone(new Builder(ZONE_NAME_2)
                        .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                        .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                        .build()
                )
                .get(3, SECONDS);

        distributionZoneManager.createZone(new Builder(ZONE_NAME_3)
                        .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                        .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                        .build()
                )
                .get(3, SECONDS);

        Set<LogicalNode> oneNode = Set.of(NODE_0);
        Set<String> oneNodeName = Set.of(NODE_0.name());

        // Change logical topology. NODE_0 is added.
        long topologyRevision0 = putNodeInLogicalTopologyAndGetRevision(NODE_0, oneNode, topology, topologyRevisions);

        Set<String> dataNodes1 = getDataNodesFromListener(topologyRevision0, DEFAULT_ZONE_ID);
        assertEquals(oneNodeName, dataNodes1);
        Set<String> dataNodes2 = getDataNodesFromListener(topologyRevision0, ZONE_ID_1);
        assertEquals(oneNodeName, dataNodes2);
        Set<String> dataNodes3 = getDataNodesFromListener(topologyRevision0, ZONE_ID_2);
        assertEquals(oneNodeName, dataNodes3);
        Set<String> dataNodes4 = getDataNodesFromListener(topologyRevision0, ZONE_ID_3);
        assertEquals(oneNodeName, dataNodes4);

        return topologyRevision0;
    }

    private void prepareZonesTimerValuesToTest() throws Exception {
        distributionZoneManager.alterZone(ZONE_NAME_1,
                        new Builder(ZONE_NAME_1)
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        distributionZoneManager.alterZone(ZONE_NAME_2,
                        new Builder(ZONE_NAME_2)
                                .dataNodesAutoAdjustScaleUp(INFINITE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        distributionZoneManager.alterZone(ZONE_NAME_3,
                        new Builder(ZONE_NAME_3)
                                .dataNodesAutoAdjustScaleUp(INFINITE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);
    }

    private Set<String> getDataNodesFromListener(long causalityToken, int zoneId) {
        CompletableFuture<Set<String>> dataNodesFut = new CompletableFuture<>();

        WatchListener dataNodesSupplier = createMetastorageDataNodeSupplierListener(causalityToken, zoneId, dataNodesFut);

        var dataNodesSupplierTrigger = new ByteArray("dataNodesSupplierTrigger");

        metaStorageManager.registerExactWatch(dataNodesSupplierTrigger, dataNodesSupplier);

        metaStorageManager.put(dataNodesSupplierTrigger, toBytes("foo"));

        assertThat(dataNodesFut, willCompleteSuccessfully());

        metaStorageManager.unregisterWatch(dataNodesSupplier);

        return dataNodesFut.join();
    }

    /**
     * Removes given nodes from the logical topology and return revision of a topology watch listener event.
     *
     * @param nodes Nodes to remove.
     * @param expectedTopology Expected topology for future completing.
     * @return Revision.
     * @throws Exception If failed.
     */
    private long removeNodeInLogicalTopologyAndGetRevision(
            Set<LogicalNode> nodes,
            Set<LogicalNode> expectedTopology
    ) throws Exception {
        Set<String> nodeNames = expectedTopology.stream().map(ClusterNode::name).collect(toSet());

        CompletableFuture<Long> revisionFut = new CompletableFuture<>();

        topologyRevisions.put(nodeNames, revisionFut);

        topology.removeNodes(nodes);

        return revisionFut.get(3, SECONDS);
    }

    /**
     * Changes data nodes in logical topology and return revision of a topology watch listener event.
     *
     * @param nodes Nodes to remove.
     * @return Revision.
     * @throws Exception If failed.
     */
    private long fireTopologyLeapAndGetRevision(Set<LogicalNode> nodes) throws Exception {
        Set<String> nodeNames = nodes.stream().map(ClusterNode::name).collect(toSet());

        CompletableFuture<Long> revisionFut = new CompletableFuture<>();

        topologyRevisions.put(nodeNames, revisionFut);

        long topVer = topology.getLogicalTopology().version() + 1;

        clusterStateStorage.put(LOGICAL_TOPOLOGY_KEY, ByteUtils.toBytes(new LogicalTopologySnapshot(topVer, nodes)));

        System.out.println("fireTopologyLeap " + nodeNames);
        topology.fireTopologyLeap();

        return revisionFut.get(3, SECONDS);
    }

    /**
     * Changes a scale up timer value of a zone and return the revision of a zone update event.
     *
     * @param zoneName Zone name.
     * @param scaleUp New scale up value.
     * @return Revision.
     * @throws Exception If failed.
     */
    private long alterZoneScaleUpAndGetRevision(String zoneName, int scaleUp) throws Exception {
        CompletableFuture<Long> revisionFut = new CompletableFuture<>();

        int zoneId = distributionZoneManager.getZoneId(zoneName);

        zoneScaleUpRevisions.put(zoneId, revisionFut);

        distributionZoneManager.alterZone(zoneName, new Builder(zoneName)
                        .dataNodesAutoAdjustScaleUp(scaleUp).build())
                .get(3, SECONDS);

        return revisionFut.get(3, SECONDS);
    }

    /**
     * Changes a scale down timer value of a zone and return the revision of a zone update event.
     *
     * @param zoneName Zone name.
     * @param scaleDown New scale down value.
     * @return Revision.
     * @throws Exception If failed.
     */
    private long alterZoneScaleDownAndGetRevision(String zoneName, int scaleDown) throws Exception {
        CompletableFuture<Long> revisionFut = new CompletableFuture<>();

        int zoneId = distributionZoneManager.getZoneId(zoneName);

        zoneScaleDownRevisions.put(zoneId, revisionFut);

        distributionZoneManager.alterZone(zoneName, new Builder(zoneName)
                        .dataNodesAutoAdjustScaleDown(scaleDown).build())
                .get(3, SECONDS);

        return revisionFut.get(3, SECONDS);
    }

    /**
     * Drops a zone and return the revision of a drop zone event.
     *
     * @param zoneName Zone name.
     * @return Revision.
     * @throws Exception If failed.
     */
    private long dropZoneAndGetRevision(String zoneName) throws Exception {
        CompletableFuture<Long> revisionFut = new CompletableFuture<>();

        int zoneId = distributionZoneManager.getZoneId(zoneName);

        dropZoneRevisions.put(zoneId, revisionFut);

        distributionZoneManager.dropZone(zoneName).get(3, SECONDS);

        return revisionFut.get(3, SECONDS);
    }

    /**
     * Returns a future which will be completed when expected data nodes will be saved to the meta storage.
     * In order to complete the future need to invoke one of the methods that change the logical topology.
     *
     * @param zoneId Zone id.
     * @param nodes Expected data nodes.
     * @return Future with revision.
     */
    private CompletableFuture<Long> getZoneDataNodesRevision(int zoneId, Set<LogicalNode> nodes) {
        Set<String> nodeNames = nodes.stream().map(node -> node.name()).collect(toSet());

        CompletableFuture<Long> revisionFut = new CompletableFuture<>();

        zoneDataNodesRevisions.put(new IgniteBiTuple<>(zoneId, nodeNames), revisionFut);

        return revisionFut;
    }

    /**
     * Creates a data nodes watch listener which completes futures from {@code zoneDataNodesRevisions}
     * when receives event with expected data nodes.
     *
     * @return Watch listener.
     */
    private WatchListener createMetastorageDataNodesListener() {
        return new WatchListener() {
            @Override
            public CompletableFuture<Void> onUpdate(WatchEvent evt) {

                int zoneId = 0;

                Set<Node> newDataNodes = null;

                long revision = 0;

                for (EntryEvent event : evt.entryEvents()) {
                    Entry e = event.newEntry();

                    if (startsWith(e.key(), zoneDataNodesKey().bytes())) {
                        revision = e.revision();

                        zoneId = extractZoneId(e.key());

                        byte[] dataNodesBytes = e.value();

                        if (dataNodesBytes != null) {
                            newDataNodes = DistributionZonesUtil.dataNodes(fromBytes(dataNodesBytes));
                        } else {
                            newDataNodes = emptySet();
                        }
                    }
                }

                Set<String> nodeNames = newDataNodes.stream().map(node -> node.nodeName()).collect(toSet());

                IgniteBiTuple<Integer, Set<String>> zoneDataNodesKey = new IgniteBiTuple<>(zoneId, nodeNames);

                if (zoneDataNodesRevisions.containsKey(zoneDataNodesKey)) {
                    zoneDataNodesRevisions.remove(zoneDataNodesKey).complete(revision);
                }

                return completedFuture(null);
            }

            @Override
            public void onError(Throwable e) {
            }
        };
    }

    /**
     * Creates a data nodes watch listener which completes futures from {@code zoneDataNodesRevisions}
     * when receives event with expected data nodes.
     *
     * @return Watch listener.
     */
    private WatchListener createMetastorageDataNodeSupplierListener(
            Long causalityToken,
            int zoneId,
            CompletableFuture<Set<String>> dataNodesFut
    ) {
        return new WatchListener() {
            @Override
            public CompletableFuture<Void> onUpdate(WatchEvent evt) {
                long revision = causalityToken == null ? evt.revision() : causalityToken;

                System.out.println("test log onUpdate " + causalityToken + " " + zoneId);

                try {
                    dataNodesFut.complete(distributionZoneManager.dataNodes(revision, zoneId));
                } catch (Exception e) {
                    dataNodesFut.completeExceptionally(e);
                }

                return completedFuture(null);
            }

            @Override
            public void onError(Throwable e) {
            }
        };
    }
}
