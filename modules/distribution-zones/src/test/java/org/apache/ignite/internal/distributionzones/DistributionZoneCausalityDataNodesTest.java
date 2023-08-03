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

import static java.util.Collections.emptySet;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl.LOGICAL_TOPOLOGY_KEY;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_ZONE_ID;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_ZONE_NAME;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.extractZoneId;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesDataNodesPrefix;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyPrefix;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyVersionKey;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.util.ByteUtils.fromBytes;
import static org.apache.ignite.internal.util.IgniteUtils.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.configuration.notifications.ConfigurationListener;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneView;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.EntryEvent;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.util.ByteUtils;
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
@Disabled("https://issues.apache.org/jira/browse/IGNITE-19506")
public class DistributionZoneCausalityDataNodesTest extends BaseDistributionZoneManagerTest {
    private static final LogicalNode NODE_0 =
            new LogicalNode("node_id_0", "node_name_0", new NetworkAddress("localhost", 123));

    private static final LogicalNode NODE_1 =
            new LogicalNode("node_id_1", "node_name_1", new NetworkAddress("localhost", 123));

    private static final LogicalNode NODE_2 =
            new LogicalNode("node_id_2", "node_name_2", new NetworkAddress("localhost", 123));

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
        metaStorageManager.registerPrefixWatch(zonesLogicalTopologyPrefix(), createMetastorageTopologyListener());

        metaStorageManager.registerPrefixWatch(zonesDataNodesPrefix(), createMetastorageDataNodesListener());

        ZonesConfigurationListener zonesConfigurationListener = new ZonesConfigurationListener();

        zonesConfiguration.distributionZones().listenElements(zonesConfigurationListener);
        zonesConfiguration.distributionZones().any().dataNodesAutoAdjustScaleUp().listen(onUpdateScaleUp());
        zonesConfiguration.distributionZones().any().dataNodesAutoAdjustScaleDown().listen(onUpdateScaleDown());

        zonesConfiguration.defaultDistributionZone().listen(zonesConfigurationListener);
        zonesConfiguration.defaultDistributionZone().dataNodesAutoAdjustScaleUp().listen(onUpdateScaleUp());
        zonesConfiguration.defaultDistributionZone().dataNodesAutoAdjustScaleDown().listen(onUpdateScaleDown());

        distributionZoneManager.start();

        metaStorageManager.deployWatches();
    }

    /**
     * Tests data nodes updating on a topology leap.
     *
     * @throws Exception If failed.
     */
    @Test
    void topologyLeapUpdate() throws Exception {
        // Prerequisite.

        // Create the zone with immediate timers.
        createZone(ZONE_NAME, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE);

        // Create the zone with not immediate timers.
        createZone(ZONE_NAME_2, 1, 1);

        // Create logical topology with NODE_0 and NODE_1.
        topology.putNode(NODE_0);

        Set<LogicalNode> twoNodes1 = Set.of(NODE_0, NODE_1);
        Set<String> twoNodesNames1 = Set.of(NODE_0.name(), NODE_1.name());

        CompletableFuture<Long> dataNodesUpdateRevision = getZoneDataNodesRevision(ZONE_ID_2, twoNodes1);

        // Check that data nodes value of both zone is NODE_0 and NODE_1.
        long topologyRevision1 = putNodeInLogicalTopologyAndGetRevision(NODE_1, twoNodes1);

        CompletableFuture<Set<String>> dataNodesFut0 = distributionZoneManager.dataNodes(topologyRevision1, ZONE_ID);
        assertThat(dataNodesFut0, willBe(twoNodesNames1));

        long dataNodesRevisionZone = dataNodesUpdateRevision.get(3, SECONDS);

        CompletableFuture<Set<String>> dataNodesFut1 = distributionZoneManager.dataNodes(dataNodesRevisionZone, ZONE_ID_2);
        assertThat(dataNodesFut1, willBe(twoNodesNames1));

        // Test steps.

        // Change logical topology. NODE_1 is left. NODE_2 is added.
        Set<LogicalNode> twoNodes2 = Set.of(NODE_0, NODE_2);
        Set<String> twoNodesNames2 = Set.of(NODE_0.name(), NODE_2.name());

        dataNodesUpdateRevision = getZoneDataNodesRevision(ZONE_ID_2, twoNodes2);

        long topologyRevision2 = fireTopologyLeapAndGetRevision(twoNodes2);

        // Check that data nodes value of the zone with immediate timers with the topology update revision is NODE_0 and NODE_2.
        CompletableFuture<Set<String>> dataNodesFut3 = distributionZoneManager.dataNodes(topologyRevision2, ZONE_ID);
        assertThat(dataNodesFut3, willBe(twoNodesNames2));

        // Check that data nodes value of the zone with not immediate timers with the topology update revision is NODE_0 and NODE_1.
        CompletableFuture<Set<String>> dataNodesFut4 = distributionZoneManager.dataNodes(topologyRevision2, ZONE_ID_2);
        assertThat(dataNodesFut4, willBe(twoNodesNames1));

        // Check that data nodes value of the zone with not immediate timers with the data nodes update revision is NODE_0 and NODE_2.
        dataNodesRevisionZone = dataNodesUpdateRevision.get(3, SECONDS);
        CompletableFuture<Set<String>> dataNodesFut5 = distributionZoneManager.dataNodes(dataNodesRevisionZone, ZONE_ID_2);
        assertThat(dataNodesFut5, willBe(twoNodesNames2));
    }

    /**
     * Tests data nodes updating on a topology leap with not immediate scale up and immediate scale down.
     *
     * @throws Exception If failed.
     */
    @Test
    void topologyLeapUpdateScaleUpNotImmediateAndScaleDownImmediate() throws Exception {
        // Prerequisite.

        // Create the zone with immediate timers.
        createZone(ZONE_NAME, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE);

        // Alter the zone with immediate timers.
        alterZone(DEFAULT_ZONE_NAME, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE);

        // Create logical topology with NODE_0 and NODE_1.
        topology.putNode(NODE_0);

        Set<LogicalNode> twoNodes1 = Set.of(NODE_0, NODE_1);
        Set<String> twoNodesNames1 = Set.of(NODE_0.name(), NODE_1.name());

        // Check that data nodes value of both zone is NODE_0 and NODE_1.
        long topologyRevision1 = putNodeInLogicalTopologyAndGetRevision(NODE_1, twoNodes1);

        CompletableFuture<Set<String>> dataNodesFut0 = distributionZoneManager.dataNodes(topologyRevision1, DEFAULT_ZONE_ID);
        assertThat(dataNodesFut0, willBe(twoNodesNames1));

        CompletableFuture<Set<String>> dataNodesFut1 = distributionZoneManager.dataNodes(topologyRevision1, ZONE_ID);
        assertThat(dataNodesFut1, willBe(twoNodesNames1));

        // Alter zones with not immediate scale up timer.
        alterZone(ZONE_NAME, 1, IMMEDIATE_TIMER_VALUE);

        alterZone(DEFAULT_ZONE_NAME, 1, IMMEDIATE_TIMER_VALUE);

        // Test steps.

        // Change logical topology. NODE_1 is left. NODE_2 is added.
        Set<LogicalNode> twoNodes2 = Set.of(NODE_0, NODE_2);
        Set<String> twoNodesNames2 = Set.of(NODE_0.name(), NODE_2.name());

        CompletableFuture<Long> dataNodesUpdateRevision0 = getZoneDataNodesRevision(DEFAULT_ZONE_ID, twoNodes2);
        CompletableFuture<Long> dataNodesUpdateRevision1 = getZoneDataNodesRevision(ZONE_ID, twoNodes2);

        long topologyRevision2 = fireTopologyLeapAndGetRevision(twoNodes2);

        // Check that data nodes value of zones is NODE_0 because scale up timer has not fired yet.
        Set<String> oneNodeNames = Set.of(NODE_0.name());

        CompletableFuture<Set<String>> dataNodesFut2 = distributionZoneManager.dataNodes(topologyRevision2, DEFAULT_ZONE_ID);
        assertThat(dataNodesFut2, willBe(oneNodeNames));

        CompletableFuture<Set<String>> dataNodesFut3 = distributionZoneManager.dataNodes(topologyRevision2, ZONE_ID);
        assertThat(dataNodesFut3, willBe(oneNodeNames));

        // Check that data nodes value of zones is NODE_0 and NODE_2.
        long dataNodesRevisionZone0 = dataNodesUpdateRevision0.get(3, SECONDS);
        CompletableFuture<Set<String>> dataNodesFut4 = distributionZoneManager.dataNodes(dataNodesRevisionZone0, DEFAULT_ZONE_ID);
        assertThat(dataNodesFut4, willBe(twoNodesNames2));

        long dataNodesRevisionZone1 = dataNodesUpdateRevision1.get(3, SECONDS);
        CompletableFuture<Set<String>> dataNodesFut5 = distributionZoneManager.dataNodes(dataNodesRevisionZone1, ZONE_ID);
        assertThat(dataNodesFut5, willBe(twoNodesNames2));
    }

    /**
     * Tests data nodes updating on a topology leap with immediate scale up and not immediate scale down.
     *
     * @throws Exception If failed.
     */
    @Test
    void topologyLeapUpdateScaleUpImmediateAndScaleDownNotImmediate() throws Exception {
        // Prerequisite.

        // Create the zone with not immediate scale down timer.
        createZone(ZONE_NAME, IMMEDIATE_TIMER_VALUE, 1);

        // Alter the zone with not immediate scale down timer.
        alterZone(DEFAULT_ZONE_NAME, IMMEDIATE_TIMER_VALUE, 1);

        // Create logical topology with NODE_0 and NODE_1.
        topology.putNode(NODE_0);

        Set<LogicalNode> twoNodes1 = Set.of(NODE_0, NODE_1);
        Set<String> twoNodesNames1 = Set.of(NODE_0.name(), NODE_1.name());

        // Check that data nodes value of both zone is NODE_0 and NODE_1.
        long topologyRevision1 = putNodeInLogicalTopologyAndGetRevision(NODE_1, twoNodes1);

        CompletableFuture<Set<String>> dataNodesFut0 = distributionZoneManager.dataNodes(topologyRevision1, DEFAULT_ZONE_ID);
        assertThat(dataNodesFut0, willBe(twoNodesNames1));

        CompletableFuture<Set<String>> dataNodesFut1 = distributionZoneManager.dataNodes(topologyRevision1, ZONE_ID);
        assertThat(dataNodesFut1, willBe(twoNodesNames1));

        // Test steps.

        // Change logical topology. NODE_1 is left. NODE_2 is added.
        Set<LogicalNode> twoNodes2 = Set.of(NODE_0, NODE_2);
        Set<String> twoNodesNames2 = Set.of(NODE_0.name(), NODE_2.name());

        CompletableFuture<Long> dataNodesUpdateRevision0 = getZoneDataNodesRevision(DEFAULT_ZONE_ID, twoNodes2);
        CompletableFuture<Long> dataNodesUpdateRevision1 = getZoneDataNodesRevision(ZONE_ID, twoNodes2);

        long topologyRevision2 = fireTopologyLeapAndGetRevision(twoNodes2);

        // Check that data nodes value of zones is NODE_0, NODE_1 and NODE_2 because scale down timer has not fired yet.
        Set<LogicalNode> threeNodes = Set.of(NODE_0, NODE_1, NODE_2);
        Set<String> threeNodesNames = Set.of(NODE_0.name(), NODE_1.name(), NODE_2.name());

        CompletableFuture<Set<String>> dataNodesFut2 = distributionZoneManager.dataNodes(topologyRevision2, DEFAULT_ZONE_ID);
        assertThat(dataNodesFut2, willBe(threeNodesNames));

        CompletableFuture<Set<String>> dataNodesFut3 = distributionZoneManager.dataNodes(topologyRevision2, ZONE_ID);
        assertThat(dataNodesFut3, willBe(threeNodesNames));

        // Check that data nodes value of zones is NODE_0 and NODE_2.
        long dataNodesRevisionZone0 = dataNodesUpdateRevision0.get(3, SECONDS);
        CompletableFuture<Set<String>> dataNodesFut4 = distributionZoneManager.dataNodes(dataNodesRevisionZone0, DEFAULT_ZONE_ID);
        assertThat(dataNodesFut4, willBe(twoNodesNames2));

        long dataNodesRevisionZone1 = dataNodesUpdateRevision1.get(3, SECONDS);
        CompletableFuture<Set<String>> dataNodesFut5 = distributionZoneManager.dataNodes(dataNodesRevisionZone1, ZONE_ID);
        assertThat(dataNodesFut5, willBe(twoNodesNames2));
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
        createZone(ZONE_NAME, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE);

        // Create logical topology with NODE_0.
        Set<LogicalNode> oneNode = Set.of(NODE_0);
        Set<String> oneNodeName = Set.of(NODE_0.name());

        long topologyRevision1 = putNodeInLogicalTopologyAndGetRevision(NODE_0, oneNode);

        // Check that data nodes value of the the zone is NODE_0.
        CompletableFuture<Set<String>> dataNodesFut1 = distributionZoneManager.dataNodes(topologyRevision1, ZONE_ID);
        assertThat(dataNodesFut1, willBe(oneNodeName));

        // Changes a scale up timer to not immediate.
        alterZone(ZONE_NAME, 10000, IMMEDIATE_TIMER_VALUE);

        // Test steps.

        // Change logical topology. NODE_1 is added.
        Set<LogicalNode> twoNodes = Set.of(NODE_0, NODE_1);
        Set<String> twoNodesNames = Set.of(NODE_0.name(), NODE_1.name());

        long topologyRevision2 = putNodeInLogicalTopologyAndGetRevision(NODE_1, twoNodes);

        // Check that data nodes value of the zone with the topology update revision is NODE_0 because scale up timer has not fired yet.
        CompletableFuture<Set<String>> dataNodesFut2 = distributionZoneManager.dataNodes(topologyRevision2, ZONE_ID);
        assertThat(dataNodesFut2, willBe(oneNode));

        // Change scale up value to immediate.
        long scaleUpRevision = alterZoneScaleUpAndGetRevision(ZONE_NAME, IMMEDIATE_TIMER_VALUE);

        // Check that data nodes value of the zone with the scale up update revision is NODE_0 and NODE_1.
        CompletableFuture<Set<String>> dataNodesFut3 = distributionZoneManager.dataNodes(scaleUpRevision, ZONE_ID);
        assertThat(dataNodesFut3, willBe(twoNodesNames));
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
        createZone(ZONE_NAME, IMMEDIATE_TIMER_VALUE, 10000);

        // Create logical topology with NODE_0 and NODE_1.
        topology.putNode(NODE_0);

        Set<LogicalNode> twoNodes = Set.of(NODE_0, NODE_1);
        Set<String> twoNodesNames = Set.of(NODE_0.name(), NODE_1.name());

        long topologyRevision1 = putNodeInLogicalTopologyAndGetRevision(NODE_1, twoNodes);

        // Check that data nodes value of the the zone is NODE_0 and NODE_1.
        CompletableFuture<Set<String>> dataNodesFut1 = distributionZoneManager.dataNodes(topologyRevision1, ZONE_ID);
        assertThat(dataNodesFut1, willBe(twoNodesNames));

        // Test steps.

        // Change logical topology. NODE_1 is left.
        Set<LogicalNode> oneNode = Set.of(NODE_0);
        Set<String> oneNodeName = Set.of(NODE_0.name());

        long topologyRevision2 = removeNodeInLogicalTopologyAndGetRevision(Set.of(NODE_1), oneNode);

        // Check that data nodes value of the zone with the topology update revision is NODE_0 and NODE_1
        // because scale down timer has not fired yet.
        CompletableFuture<Set<String>> dataNodesFut2 = distributionZoneManager.dataNodes(topologyRevision2, ZONE_ID);
        assertThat(dataNodesFut2, willBe(twoNodesNames));

        // Change scale down value to immediate.
        long scaleDownRevision = alterZoneScaleDownAndGetRevision(ZONE_NAME, IMMEDIATE_TIMER_VALUE);

        // Check that data nodes value of the zone with the scale down update revision is NODE_0.
        CompletableFuture<Set<String>> dataNodesFut3 = distributionZoneManager.dataNodes(scaleDownRevision, ZONE_ID);
        assertThat(dataNodesFut3, willBe(oneNodeName));
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
        createZone(ZONE_NAME, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE);

        // Alter the zone with immediate timers.
        alterZone(DEFAULT_ZONE_NAME, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE);

        // Create logical topology with NODE_0.
        topology.putNode(NODE_0);

        Set<LogicalNode> oneNode = Set.of(NODE_0);
        Set<String> oneNodeName = Set.of(NODE_0.name());

        // Check that data nodes value of both zone is NODE_0.
        long topologyRevision1 = putNodeInLogicalTopologyAndGetRevision(NODE_0, Set.of(NODE_0));

        CompletableFuture<Set<String>> dataNodesFut0 = distributionZoneManager.dataNodes(topologyRevision1, DEFAULT_ZONE_ID);
        assertThat(dataNodesFut0, willBe(oneNodeName));

        CompletableFuture<Set<String>> dataNodesFut1 = distributionZoneManager.dataNodes(topologyRevision1, ZONE_ID);
        assertThat(dataNodesFut1, willBe(oneNodeName));

        // Alter the zones with not immediate scale up timer.
        alterZone(ZONE_NAME, 10000, IMMEDIATE_TIMER_VALUE);

        // Alter the zone with not immediate scale up timer.
        alterZone(DEFAULT_ZONE_NAME, 10000, IMMEDIATE_TIMER_VALUE);

        // Test steps.

        // Change logical topology. NODE_1 is added.
        Set<LogicalNode> twoNodes = Set.of(NODE_0, NODE_1);
        Set<String> twoNodesNames = Set.of(NODE_0.name(), NODE_1.name());

        long topologyRevision2 = putNodeInLogicalTopologyAndGetRevision(NODE_1, twoNodes);

        long dropRevision0 = dropZoneAndGetRevision(DEFAULT_ZONE_NAME);
        long dropRevision1 = dropZoneAndGetRevision(ZONE_NAME);

        // Check that data nodes value of the zone with the topology update revision is NODE_0 because scale up timer has not fired.
        CompletableFuture<Set<String>> dataNodesFut2 = distributionZoneManager.dataNodes(topologyRevision2, DEFAULT_ZONE_ID);
        assertThat(dataNodesFut2, willBe(NODE_0));

        CompletableFuture<Set<String>> dataNodesFut3 = distributionZoneManager.dataNodes(topologyRevision2, ZONE_ID);
        assertThat(dataNodesFut3, willBe(NODE_0));

        // Check that zones is removed and attempt to get data nodes throws an exception.
        CompletableFuture<Set<String>> dataNodesFut4 = distributionZoneManager.dataNodes(dropRevision0, DEFAULT_ZONE_ID);
        assertThrows(DistributionZoneNotFoundException.class, () -> dataNodesFut4.get(3, SECONDS));

        CompletableFuture<Set<String>> dataNodesFut5 = distributionZoneManager.dataNodes(dropRevision1, ZONE_ID);
        assertThrows(DistributionZoneNotFoundException.class, () -> dataNodesFut5.get(3, SECONDS));
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
        createZone(ZONE_NAME, IMMEDIATE_TIMER_VALUE, 10000);

        // Create logical topology with NODE_0 and NODE_1.
        topology.putNode(NODE_0);

        Set<LogicalNode> twoNodes = Set.of(NODE_0, NODE_1);
        Set<String> twoNodesNames = Set.of(NODE_0.name(), NODE_1.name());

        long topologyRevision1 = putNodeInLogicalTopologyAndGetRevision(NODE_1, twoNodes);

        // Check that data nodes value of the the zone is NODE_0 and NODE_1.
        CompletableFuture<Set<String>> dataNodesFut1 = distributionZoneManager.dataNodes(topologyRevision1, ZONE_ID);
        assertThat(dataNodesFut1, willBe(twoNodesNames));

        // Test steps.

        // Change logical topology. NODE_1 is removed.
        Set<LogicalNode> oneNode = Set.of(NODE_0);
        Set<String> oneNodeName = Set.of(NODE_0.name());

        long topologyRevision2 = removeNodeInLogicalTopologyAndGetRevision(Set.of(NODE_1), oneNode);

        long dropRevision0 = dropZoneAndGetRevision(DEFAULT_ZONE_NAME);
        long dropRevision1 = dropZoneAndGetRevision(ZONE_NAME);

        // Check that data nodes value of the zone with the topology update revision is NODE_0 and NODE_1
        // because scale down timer has not fired.
        CompletableFuture<Set<String>> dataNodesFut2 = distributionZoneManager.dataNodes(topologyRevision2, DEFAULT_ZONE_ID);
        assertThat(dataNodesFut2, willBe(twoNodesNames));

        CompletableFuture<Set<String>> dataNodesFut3 = distributionZoneManager.dataNodes(topologyRevision2, ZONE_ID);
        assertThat(dataNodesFut3, willBe(twoNodesNames));

        // Check that zones is removed and attempt to get data nodes throws an exception.
        CompletableFuture<Set<String>> dataNodesFut4 = distributionZoneManager.dataNodes(dropRevision0, DEFAULT_ZONE_ID);
        assertThrows(DistributionZoneNotFoundException.class, () -> dataNodesFut4.get(3, SECONDS));

        CompletableFuture<Set<String>> dataNodesFut5 = distributionZoneManager.dataNodes(dropRevision1, ZONE_ID);
        assertThrows(DistributionZoneNotFoundException.class, () -> dataNodesFut5.get(3, SECONDS));
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

        putNodeInLogicalTopologyAndGetRevision(NODE_0, oneNode);

        Set<String> twoNodesNames = Set.of(NODE_0.name(), NODE_1.name());

        // Test steps.

        // Create a zone.
        long createZoneRevision = createZoneAndGetRevision(ZONE_NAME, ZONE_ID, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE);

        // Check that data nodes value of the zone with the revision lower than the create zone revision is absent.
        CompletableFuture<Set<String>> dataNodesFut1 = distributionZoneManager.dataNodes(createZoneRevision - 1, ZONE_ID);
        assertThrows(DistributionZoneNotFoundException.class, () -> dataNodesFut1.get(3, SECONDS));

        // Check that data nodes value of the zone with the create zone revision is NODE_0 and NODE_1.
        CompletableFuture<Set<String>> dataNodesFut2 = distributionZoneManager.dataNodes(createZoneRevision, ZONE_ID);
        assertThat(dataNodesFut2, willBe(twoNodesNames));

        // Drop the zone.
        long dropZoneRevision = dropZoneAndGetRevision(ZONE_NAME);

        // Check that data nodes value of the zone with the drop zone revision is absent.
        CompletableFuture<Set<String>> dataNodesFut3 = distributionZoneManager.dataNodes(dropZoneRevision, ZONE_ID);
        assertThrows(DistributionZoneNotFoundException.class, () -> dataNodesFut3.get(3, SECONDS));
    }

    /**
     * Tests data nodes obtaining with wrong parameters throw an exception.
     *
     * @throws Exception If failed.
     */
    @Test
    void validationTest() {
        CompletableFuture<Set<String>> dataNodesFut1 = distributionZoneManager.dataNodes(0, DEFAULT_ZONE_ID);
        assertThrows(AssertionError.class, () -> dataNodesFut1.get(3, SECONDS));

        CompletableFuture<Set<String>> dataNodesFut2 = distributionZoneManager.dataNodes(-1, DEFAULT_ZONE_ID);
        assertThrows(AssertionError.class, () -> dataNodesFut2.get(3, SECONDS));

        CompletableFuture<Set<String>> dataNodesFut3 = distributionZoneManager.dataNodes(1, -1);
        assertThrows(AssertionError.class, () -> dataNodesFut3.get(3, SECONDS));
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
        alterZone(DEFAULT_ZONE_NAME, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE);

        alterZone(ZONE_NAME, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE);

        // Test steps.

        Set<LogicalNode> oneNode = Set.of(NODE_0);
        Set<String> oneNodeName = Set.of(NODE_0.name());

        Set<LogicalNode> twoNodes = Set.of(NODE_0, NODE_1);
        Set<String> twoNodesNames = Set.of(NODE_0.name(), NODE_1.name());

        Set<LogicalNode> threeNodes = Set.of(NODE_0, NODE_1, NODE_2);
        Set<String> threeNodeNames = Set.of(NODE_0.name(), NODE_1.name(), NODE_2.name());

        // Change logical topology. NODE_0 is added.
        long topologyRevision0 = putNodeInLogicalTopologyAndGetRevision(NODE_0, oneNode);

        // Change logical topology. NODE_1 is added.
        long topologyRevision1 = putNodeInLogicalTopologyAndGetRevision(NODE_1, twoNodes);

        CompletableFuture<Set<String>> dataNodesFut1 = distributionZoneManager.dataNodes(topologyRevision0, DEFAULT_ZONE_ID);
        assertThat(dataNodesFut1, willBe(oneNodeName));
        CompletableFuture<Set<String>> dataNodesFut2 = distributionZoneManager.dataNodes(topologyRevision0, ZONE_ID);
        assertThat(dataNodesFut2, willBe(oneNodeName));

        CompletableFuture<Set<String>> dataNodesFut3 = distributionZoneManager.dataNodes(topologyRevision0 + 1, DEFAULT_ZONE_ID);
        assertThat(dataNodesFut3, willBe(oneNodeName));
        CompletableFuture<Set<String>> dataNodesFut4 = distributionZoneManager.dataNodes(topologyRevision0 + 1, ZONE_ID);
        assertThat(dataNodesFut4, willBe(oneNodeName));

        CompletableFuture<Set<String>> dataNodesFut5 = distributionZoneManager.dataNodes(topologyRevision1, DEFAULT_ZONE_ID);
        assertThat(dataNodesFut5, willBe(twoNodesNames));
        CompletableFuture<Set<String>> dataNodesFut6 = distributionZoneManager.dataNodes(topologyRevision1, ZONE_ID);
        assertThat(dataNodesFut6, willBe(twoNodesNames));

        Set<LogicalNode> twoNodes1 = Set.of(NODE_0, NODE_2);
        Set<String> twoNodesNames1 = Set.of(NODE_0.name(), NODE_2.name());

        // Change logical topology. NODE_2 is added.
        long topologyRevision2 = putNodeInLogicalTopologyAndGetRevision(NODE_2, twoNodes1);

        // Change logical topology. NODE_1 is left.
        long topologyRevision3 = removeNodeInLogicalTopologyAndGetRevision(Set.of(NODE_1), twoNodes1);

        CompletableFuture<Set<String>> dataNodesFut7 = distributionZoneManager.dataNodes(topologyRevision2, DEFAULT_ZONE_ID);
        assertThat(dataNodesFut7, willBe(threeNodeNames));
        CompletableFuture<Set<String>> dataNodesFut8 = distributionZoneManager.dataNodes(topologyRevision2, ZONE_ID);
        assertThat(dataNodesFut8, willBe(threeNodeNames));

        CompletableFuture<Set<String>> dataNodesFut9 = distributionZoneManager.dataNodes(topologyRevision3, DEFAULT_ZONE_ID);
        assertThat(dataNodesFut9, willBe(twoNodesNames1));
        CompletableFuture<Set<String>> dataNodesFut10 = distributionZoneManager.dataNodes(topologyRevision3, ZONE_ID);
        assertThat(dataNodesFut10, willBe(twoNodesNames1));
    }

    /**
     * Puts a given node as a part of the logical topology and return revision of a topology watch listener event.
     *
     * @param node Node to put.
     * @param expectedTopology Expected topology for future completing.
     * @return Revision.
     * @throws Exception If failed.
     */
    private long putNodeInLogicalTopologyAndGetRevision(
            LogicalNode node,
            Set<LogicalNode> expectedTopology
    ) throws Exception {
        Set<String> nodeNames = expectedTopology.stream().map(ClusterNode::name).collect(toSet());

        CompletableFuture<Long> revisionFut = new CompletableFuture<>();

        topologyRevisions.put(nodeNames, revisionFut);

        topology.putNode(node);

        return revisionFut.get(3, SECONDS);
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

        alterZone(zoneName, scaleUp, null);

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

        alterZone(zoneName, null, scaleDown);

        return revisionFut.get(3, SECONDS);
    }

    /**
     * Creates a zone and return the revision of a create zone event.
     *
     * @param zoneName Zone name.
     * @param zoneId Zone id.
     * @param scaleUp Scale up value.
     * @param scaleDown Scale down value.
     * @return Revision.
     * @throws Exception If failed.
     */
    private long createZoneAndGetRevision(String zoneName, int zoneId, int scaleUp, int scaleDown) throws Exception {
        CompletableFuture<Long> revisionFut = new CompletableFuture<>();

        createZoneRevisions.put(zoneId, revisionFut);

        createZone(zoneName, scaleUp, scaleDown);

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

        dropZone(zoneName);

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

        return zoneDataNodesRevisions.put(new IgniteBiTuple<>(zoneId, nodeNames), revisionFut);
    }

    /**
     * Creates a configuration listener which completes futures from {@code zoneScaleUpRevisions}
     * when receives event with expected zone id.
     *
     * @return Configuration listener.
     */
    private ConfigurationListener<Integer> onUpdateScaleUp() {
        return ctx -> {
            int zoneId = ctx.newValue(DistributionZoneView.class).zoneId();

            if (zoneScaleUpRevisions.containsKey(zoneId)) {
                zoneScaleUpRevisions.remove(zoneId).complete(ctx.storageRevision());
            }

            return completedFuture(null);
        };
    }

    /**
     * Creates a configuration listener which completes futures from {@code zoneScaleDownRevisions}
     * when receives event with expected zone id.
     *
     * @return Configuration listener.
     */
    private ConfigurationListener<Integer> onUpdateScaleDown() {
        return ctx -> {
            int zoneId = ctx.newValue(DistributionZoneView.class).zoneId();

            if (zoneScaleDownRevisions.containsKey(zoneId)) {
                zoneScaleDownRevisions.remove(zoneId).complete(ctx.storageRevision());
            }

            return completedFuture(null);
        };
    }

    /**
     * A configuration listener which completes futures from {@code createZoneRevisions} and {@code dropZoneRevisions}
     * when receives event with expected zone id.
     */
    private class ZonesConfigurationListener implements ConfigurationNamedListListener<DistributionZoneView> {
        @Override
        public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<DistributionZoneView> ctx) {
            int zoneId = ctx.newValue().zoneId();

            if (createZoneRevisions.containsKey(zoneId)) {
                createZoneRevisions.remove(zoneId).complete(ctx.storageRevision());
            }

            return completedFuture(null);
        }

        @Override
        public CompletableFuture<?> onDelete(ConfigurationNotificationEvent<DistributionZoneView> ctx) {
            int zoneId = ctx.oldValue().zoneId();

            if (dropZoneRevisions.containsKey(zoneId)) {
                dropZoneRevisions.remove(zoneId).complete(ctx.storageRevision());
            }

            return completedFuture(null);
        }
    }

    /**
     * Creates a topology watch listener which completes futures from {@code topologyRevisions}
     * when receives event with expected logical topology.
     *
     * @return Watch listener.
     */
    private WatchListener createMetastorageTopologyListener() {
        return new WatchListener() {
            @Override
            public CompletableFuture<Void> onUpdate(WatchEvent evt) {

                Set<NodeWithAttributes> newLogicalTopology = null;

                long revision = 0;

                for (EntryEvent event : evt.entryEvents()) {
                    Entry e = event.newEntry();

                    if (Arrays.equals(e.key(), zonesLogicalTopologyVersionKey().bytes())) {
                        revision = e.revision();
                    } else if (Arrays.equals(e.key(), zonesLogicalTopologyKey().bytes())) {
                        newLogicalTopology = fromBytes(e.value());
                    }
                }

                Set<String> nodeNames = newLogicalTopology.stream().map(node -> node.nodeName()).collect(toSet());

                if (topologyRevisions.containsKey(nodeNames)) {
                    topologyRevisions.remove(nodeNames).complete(revision);
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
                    zoneDataNodesRevisions.remove(new IgniteBiTuple<>(zoneId, nodeNames)).complete(revision);
                }

                return completedFuture(null);
            }

            @Override
            public void onError(Throwable e) {
            }
        };
    }
}
