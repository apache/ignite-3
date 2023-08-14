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
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.ZONE_ALTER;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.ZONE_CREATE;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.ZONE_DROP;
import static org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl.LOGICAL_TOPOLOGY_KEY;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_ZONE_NAME;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertValueInStorage;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.extractZoneId;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesDataNodesPrefix;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyPrefix;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyVersionKey;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.util.ByteUtils.fromBytes;
import static org.apache.ignite.internal.util.IgniteUtils.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.internal.catalog.events.AlterZoneEventParameters;
import org.apache.ignite.internal.catalog.events.CreateZoneEventParameters;
import org.apache.ignite.internal.catalog.events.DropZoneEventParameters;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.distributionzones.BaseDistributionZoneManagerTest;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.distributionzones.DistributionZoneNotFoundException;
import org.apache.ignite.internal.distributionzones.DistributionZonesUtil;
import org.apache.ignite.internal.distributionzones.Node;
import org.apache.ignite.internal.distributionzones.NodeWithAttributes;
import org.apache.ignite.internal.distributionzones.utils.CatalogAlterZoneEventListener;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.EntryEvent;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Tests for causality data nodes updating in {@link DistributionZoneManager}.
 */
public class DistributionZoneCausalityDataNodesTest extends BaseDistributionZoneManagerTest {
    private static final String ZONE_NAME_2 = "zone2";

    private static final String ZONE_NAME_3 = "zone3";

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

    private static final long TIMEOUT = 5_000L;

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
     * Contains futures that is completed when the zone catalog listener receive the zone creation event with expected zone.
     * Mapping of zone name -> future with event revision.
     */
    private final ConcurrentHashMap<String, CompletableFuture<Long>> createZoneRevisions = new ConcurrentHashMap<>();

    /**
     * Contains futures that is completed when the zone catalog listener receive the zone dropping event with expected zone.
     * Mapping of zone id -> future with event revision.
     */
    private final ConcurrentHashMap<Integer, CompletableFuture<Long>> dropZoneRevisions = new ConcurrentHashMap<>();

    @BeforeEach
    void beforeEach() {
        metaStorageManager.registerPrefixWatch(zonesLogicalTopologyPrefix(), createMetastorageTopologyListener());
        metaStorageManager.registerPrefixWatch(zonesDataNodesPrefix(), createMetastorageDataNodesListener());

        addCatalogZoneEventListeners();

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
        createZone(ZONE_NAME, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE, null);

        // Create the zone with not immediate timers.
        createZone(ZONE_NAME_2, 1, 1, null);

        // Create logical topology with NODE_0 and NODE_1.
        topology.putNode(NODE_0);

        int zoneId = getZoneId(ZONE_NAME);
        int zoneId2 = getZoneId(ZONE_NAME_2);

        CompletableFuture<Long> dataNodesUpdateRevision = getZoneDataNodesRevision(zoneId2, TWO_NODES);

        // Check that data nodes value of both zone is NODE_0 and NODE_1.
        long topologyRevision1 = putNodeInLogicalTopologyAndGetRevision(NODE_1, TWO_NODES);

        Set<String> dataNodes0 = distributionZoneManager.dataNodes(topologyRevision1, zoneId).get(TIMEOUT, MILLISECONDS);
        assertEquals(TWO_NODES_NAMES, dataNodes0);

        long dataNodesRevisionZone = dataNodesUpdateRevision.get(TIMEOUT, MILLISECONDS);

        Set<String> dataNodes1 = distributionZoneManager.dataNodes(dataNodesRevisionZone, zoneId2).get(TIMEOUT, MILLISECONDS);
        assertEquals(TWO_NODES_NAMES, dataNodes1);

        // Test steps.

        // Change logical topology. NODE_1 is left. NODE_2 is added.
        Set<LogicalNode> twoNodes2 = Set.of(NODE_0, NODE_2);
        Set<String> twoNodesNames2 = Set.of(NODE_0.name(), NODE_2.name());

        dataNodesUpdateRevision = getZoneDataNodesRevision(zoneId2, twoNodes2);

        long topologyRevision2 = fireTopologyLeapAndGetRevision(twoNodes2);

        // Check that data nodes value of the zone with immediate timers with the topology update revision is NODE_0 and NODE_2.
        Set<String> dataNodes3 = distributionZoneManager.dataNodes(topologyRevision2, zoneId).get(TIMEOUT, MILLISECONDS);
        assertEquals(twoNodesNames2, dataNodes3);

        // Check that data nodes value of the zone with not immediate timers with the topology update revision is NODE_0 and NODE_1.
        Set<String> dataNodes4 = distributionZoneManager.dataNodes(topologyRevision2, zoneId2).get(TIMEOUT, MILLISECONDS);
        assertEquals(TWO_NODES_NAMES, dataNodes4);

        // Check that data nodes value of the zone with not immediate timers with the data nodes update revision is NODE_0 and NODE_2.
        dataNodesRevisionZone = dataNodesUpdateRevision.get(TIMEOUT, MILLISECONDS);
        Set<String> dataNodes5 = distributionZoneManager.dataNodes(dataNodesRevisionZone, zoneId2).get(TIMEOUT, MILLISECONDS);
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
        createZone(ZONE_NAME, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE, null);

        // Alter the zone with immediate timers.
        alterZone(DEFAULT_ZONE_NAME, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE, null);

        // Create logical topology with NODE_0 and NODE_1.
        topology.putNode(NODE_0);

        // Check that data nodes value of both zone is NODE_0 and NODE_1.
        long topologyRevision1 = putNodeInLogicalTopologyAndGetRevision(NODE_1, TWO_NODES);

        int defaultZoneId = getZoneId(DEFAULT_ZONE_NAME);
        int zoneId = getZoneId(ZONE_NAME);

        Set<String> dataNodes0 = distributionZoneManager.dataNodes(topologyRevision1, defaultZoneId).get(TIMEOUT, MILLISECONDS);
        assertEquals(TWO_NODES_NAMES, dataNodes0);

        Set<String> dataNodes1 = distributionZoneManager.dataNodes(topologyRevision1, zoneId).get(TIMEOUT, MILLISECONDS);
        assertEquals(TWO_NODES_NAMES, dataNodes1);

        // Alter zones with not immediate scale up timer.
        alterZone(ZONE_NAME, 1, IMMEDIATE_TIMER_VALUE, null);

        alterZone(DEFAULT_ZONE_NAME, 1, IMMEDIATE_TIMER_VALUE, null);

        // Test steps.

        // Change logical topology. NODE_1 is left. NODE_2 is added.
        Set<LogicalNode> twoNodes2 = Set.of(NODE_0, NODE_2);
        Set<String> twoNodesNames2 = Set.of(NODE_0.name(), NODE_2.name());

        CompletableFuture<Long> dataNodesUpdateRevision0 = getZoneDataNodesRevision(defaultZoneId, twoNodes2);
        CompletableFuture<Long> dataNodesUpdateRevision1 = getZoneDataNodesRevision(zoneId, twoNodes2);

        long topologyRevision2 = fireTopologyLeapAndGetRevision(twoNodes2);

        // Check that data nodes value of zones is NODE_0 because scale up timer has not fired yet.
        Set<String> dataNodes2 = distributionZoneManager.dataNodes(topologyRevision2, defaultZoneId).get(TIMEOUT, MILLISECONDS);
        assertEquals(ONE_NODE_NAME, dataNodes2);

        Set<String> dataNodes3 = distributionZoneManager.dataNodes(topologyRevision2, zoneId).get(TIMEOUT, MILLISECONDS);
        assertEquals(ONE_NODE_NAME, dataNodes3);

        // Check that data nodes value of zones is NODE_0 and NODE_2.
        long dataNodesRevisionZone0 = dataNodesUpdateRevision0.get(TIMEOUT, MILLISECONDS);
        Set<String> dataNodes4 = distributionZoneManager.dataNodes(dataNodesRevisionZone0, defaultZoneId).get(TIMEOUT, MILLISECONDS);
        assertEquals(twoNodesNames2, dataNodes4);

        long dataNodesRevisionZone1 = dataNodesUpdateRevision1.get(TIMEOUT, MILLISECONDS);
        Set<String> dataNodes5 = distributionZoneManager.dataNodes(dataNodesRevisionZone1, zoneId).get(TIMEOUT, MILLISECONDS);
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
        createZone(ZONE_NAME, IMMEDIATE_TIMER_VALUE, 1, null);

        // Alter the zone with not immediate scale down timer.
        alterZone(DEFAULT_ZONE_NAME, IMMEDIATE_TIMER_VALUE, 1, null);

        // Create logical topology with NODE_0 and NODE_1.
        topology.putNode(NODE_0);

        // Check that data nodes value of both zone is NODE_0 and NODE_1.
        long topologyRevision1 = putNodeInLogicalTopologyAndGetRevision(NODE_1, TWO_NODES);

        int defaultZoneId = getZoneId(DEFAULT_ZONE_NAME);
        int zoneId = getZoneId(ZONE_NAME);

        Set<String> dataNodes0 = distributionZoneManager.dataNodes(topologyRevision1, defaultZoneId).get(TIMEOUT, MILLISECONDS);
        assertEquals(TWO_NODES_NAMES, dataNodes0);

        Set<String> dataNodes1 = distributionZoneManager.dataNodes(topologyRevision1, zoneId).get(TIMEOUT, MILLISECONDS);
        assertEquals(TWO_NODES_NAMES, dataNodes1);

        // Test steps.

        // Change logical topology. NODE_1 is left. NODE_2 is added.
        Set<LogicalNode> twoNodes2 = Set.of(NODE_0, NODE_2);
        Set<String> twoNodesNames2 = Set.of(NODE_0.name(), NODE_2.name());

        CompletableFuture<Long> dataNodesUpdateRevision0 = getZoneDataNodesRevision(defaultZoneId, twoNodes2);
        CompletableFuture<Long> dataNodesUpdateRevision1 = getZoneDataNodesRevision(zoneId, twoNodes2);

        long topologyRevision2 = fireTopologyLeapAndGetRevision(twoNodes2);

        // Check that data nodes value of zones is NODE_0, NODE_1 and NODE_2 because scale down timer has not fired yet.
        Set<String> dataNodes2 = distributionZoneManager.dataNodes(topologyRevision2, defaultZoneId).get(TIMEOUT, MILLISECONDS);
        assertEquals(THREE_NODES_NAMES, dataNodes2);

        Set<String> dataNodes3 = distributionZoneManager.dataNodes(topologyRevision2, zoneId).get(TIMEOUT, MILLISECONDS);
        assertEquals(THREE_NODES_NAMES, dataNodes3);

        // Check that data nodes value of zones is NODE_0 and NODE_2.
        long dataNodesRevisionZone0 = dataNodesUpdateRevision0.get(TIMEOUT, MILLISECONDS);
        Set<String> dataNodes4 = distributionZoneManager.dataNodes(dataNodesRevisionZone0, defaultZoneId).get(TIMEOUT, MILLISECONDS);
        assertEquals(twoNodesNames2, dataNodes4);

        long dataNodesRevisionZone1 = dataNodesUpdateRevision1.get(TIMEOUT, MILLISECONDS);
        Set<String> dataNodes5 = distributionZoneManager.dataNodes(dataNodesRevisionZone1, zoneId).get(TIMEOUT, MILLISECONDS);
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
        createZone(ZONE_NAME, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE, null);

        // Create logical topology with NODE_0.
        long topologyRevision1 = putNodeInLogicalTopologyAndGetRevision(NODE_0, ONE_NODE);

        int zoneId = getZoneId(ZONE_NAME);

        // Check that data nodes value of the the zone is NODE_0.
        Set<String> dataNodes1 = distributionZoneManager.dataNodes(topologyRevision1, zoneId).get(TIMEOUT, MILLISECONDS);
        assertEquals(ONE_NODE_NAME, dataNodes1);

        // Changes a scale up timer to not immediate.
        alterZone(ZONE_NAME, 10000, IMMEDIATE_TIMER_VALUE, null);

        // Test steps.

        // Change logical topology. NODE_1 is added.
        long topologyRevision2 = putNodeInLogicalTopologyAndGetRevision(NODE_1, TWO_NODES);

        // Check that data nodes value of the zone with the topology update revision is NODE_0 because scale up timer has not fired yet.
        Set<String> dataNodes2 = distributionZoneManager.dataNodes(topologyRevision2, zoneId).get(TIMEOUT, MILLISECONDS);
        assertEquals(ONE_NODE_NAME, dataNodes2);

        // Change scale up value to immediate.
        long scaleUpRevision = alterZoneScaleUpAndGetRevision(ZONE_NAME, IMMEDIATE_TIMER_VALUE);

        // Check that data nodes value of the zone with the scale up update revision is NODE_0 and NODE_1.
        Set<String> dataNodes3 = distributionZoneManager.dataNodes(scaleUpRevision, zoneId).get(TIMEOUT, MILLISECONDS);
        assertEquals(TWO_NODES_NAMES, dataNodes3);
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
        createZone(ZONE_NAME, IMMEDIATE_TIMER_VALUE, 10000, null);

        // Create logical topology with NODE_0 and NODE_1.
        topology.putNode(NODE_0);

        long topologyRevision1 = putNodeInLogicalTopologyAndGetRevision(NODE_1, TWO_NODES);

        int zoneId = getZoneId(ZONE_NAME);

        // Check that data nodes value of the the zone is NODE_0 and NODE_1.
        Set<String> dataNodes1 = distributionZoneManager.dataNodes(topologyRevision1, zoneId).get(TIMEOUT, MILLISECONDS);
        assertEquals(TWO_NODES_NAMES, dataNodes1);

        // Test steps.

        // Change logical topology. NODE_1 is left.
        long topologyRevision2 = removeNodeInLogicalTopologyAndGetRevision(Set.of(NODE_1), ONE_NODE);

        // Check that data nodes value of the zone with the topology update revision is NODE_0 and NODE_1
        // because scale down timer has not fired yet.
        Set<String> dataNodes2 = distributionZoneManager.dataNodes(topologyRevision2, zoneId).get(TIMEOUT, MILLISECONDS);
        assertEquals(TWO_NODES_NAMES, dataNodes2);

        // Change scale down value to immediate.
        long scaleDownRevision = alterZoneScaleDownAndGetRevision(ZONE_NAME, IMMEDIATE_TIMER_VALUE);

        // Check that data nodes value of the zone with the scale down update revision is NODE_0.
        Set<String> dataNodes3 = distributionZoneManager.dataNodes(scaleDownRevision, zoneId).get(TIMEOUT, MILLISECONDS);
        assertEquals(ONE_NODE_NAME, dataNodes3);
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
        createZone(ZONE_NAME, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE, null);

        // Create logical topology with NODE_0.
        long topologyRevision1 = putNodeInLogicalTopologyAndGetRevision(NODE_0, ONE_NODE);

        int zoneId = getZoneId(ZONE_NAME);

        // Check that data nodes value of the zone is NODE_0.
        Set<String> dataNodes1 = distributionZoneManager.dataNodes(topologyRevision1, zoneId).get(TIMEOUT, MILLISECONDS);
        assertEquals(ONE_NODE_NAME, dataNodes1);

        // Alter the zones with not immediate scale up timer.
        alterZone(ZONE_NAME, 10000, IMMEDIATE_TIMER_VALUE, null);

        // Test steps.

        // Change logical topology. NODE_1 is added.
        long topologyRevision2 = putNodeInLogicalTopologyAndGetRevision(NODE_1, TWO_NODES);

        assertValueInStorage(
                metaStorageManager,
                zoneDataNodesKey(zoneId),
                (v) -> DistributionZonesUtil.dataNodes(fromBytes(v)).stream().map(Node::nodeName).collect(toSet()),
                ONE_NODE_NAME,
                TIMEOUT
        );

        long dropRevision1 = dropZoneAndGetRevision(ZONE_NAME);

        // Check that data nodes value of the zone with the topology update revision is NODE_0 because scale up timer has not fired.
        Set<String> dataNodes3 = distributionZoneManager.dataNodes(topologyRevision2, zoneId).get(TIMEOUT, MILLISECONDS);
        assertEquals(ONE_NODE_NAME, dataNodes3);

        // Check that zones is removed and attempt to get data nodes throws an exception.
        assertThrowsWithCause(() -> distributionZoneManager.dataNodes(dropRevision1, zoneId).get(TIMEOUT, MILLISECONDS),
                DistributionZoneNotFoundException.class);
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
        createZone(ZONE_NAME, IMMEDIATE_TIMER_VALUE, 10000, null);

        // Create logical topology with NODE_0 and NODE_1.
        topology.putNode(NODE_0);

        long topologyRevision1 = putNodeInLogicalTopologyAndGetRevision(NODE_1, TWO_NODES);

        int zoneId = getZoneId(ZONE_NAME);

        // Check that data nodes value of the the zone is NODE_0 and NODE_1.
        Set<String> dataNodes1 = distributionZoneManager.dataNodes(topologyRevision1, zoneId).get(TIMEOUT, MILLISECONDS);
        assertEquals(TWO_NODES_NAMES, dataNodes1);

        // Test steps.

        // Change logical topology. NODE_1 is removed.
        long topologyRevision2 = removeNodeInLogicalTopologyAndGetRevision(Set.of(NODE_1), ONE_NODE);

        assertValueInStorage(
                metaStorageManager,
                zoneDataNodesKey(zoneId),
                (v) -> DistributionZonesUtil.dataNodes(fromBytes(v)).stream().map(Node::nodeName).collect(toSet()),
                TWO_NODES_NAMES,
                TIMEOUT
        );

        long dropRevision1 = dropZoneAndGetRevision(ZONE_NAME);

        // Check that data nodes value of the zone with the topology update revision is NODE_0 and NODE_1
        // because scale down timer has not fired.
        Set<String> dataNodes2 = distributionZoneManager.dataNodes(topologyRevision2, zoneId).get(TIMEOUT, MILLISECONDS);
        assertEquals(TWO_NODES_NAMES, dataNodes2);

        // Check that zones is removed and attempt to get data nodes throws an exception.
        assertThrowsWithCause(() -> distributionZoneManager.dataNodes(dropRevision1, zoneId).get(TIMEOUT, MILLISECONDS),
                DistributionZoneNotFoundException.class);
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
        createZone(ZONE_NAME, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE, null);

        // Alter the zone with immediate timers.
        alterZone(DEFAULT_ZONE_NAME, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE, null);

        // Create logical topology with NODE_0.
        // Check that data nodes value of both zone is NODE_0.
        long topologyRevision1 = putNodeInLogicalTopologyAndGetRevision(NODE_0, ONE_NODE);

        int defaultZoneId = getZoneId(DEFAULT_ZONE_NAME);
        int zoneId = getZoneId(ZONE_NAME);

        Set<String> dataNodes0 = distributionZoneManager.dataNodes(topologyRevision1, defaultZoneId).get(TIMEOUT, MILLISECONDS);
        assertEquals(ONE_NODE_NAME, dataNodes0);

        Set<String> dataNodes1 = distributionZoneManager.dataNodes(topologyRevision1, zoneId).get(TIMEOUT, MILLISECONDS);
        assertEquals(ONE_NODE_NAME, dataNodes1);

        // Alter the zones with infinite timers.
        alterZone(ZONE_NAME, INFINITE_TIMER_VALUE, INFINITE_TIMER_VALUE, null);

        // Alter the zone with infinite timers.
        alterZone(DEFAULT_ZONE_NAME, INFINITE_TIMER_VALUE, INFINITE_TIMER_VALUE, null);

        // Test steps.

        String filter = "$[?($..* || @.region == 'US')]";

        // Change filter and get revision of this change.
        long filterRevision0 = alterFilterAndGetRevision(DEFAULT_ZONE_NAME, filter);
        long filterRevision1 = alterFilterAndGetRevision(ZONE_NAME, filter);

        // Check that data nodes value of the the zone is NODE_0.
        // The future didn't hang due to the fact that the actual data nodes value did not change.
        Set<String> dataNodes2 = distributionZoneManager.dataNodes(filterRevision0, defaultZoneId).get(TIMEOUT, MILLISECONDS);
        assertEquals(ONE_NODE_NAME, dataNodes2);

        Set<String> dataNodes3 = distributionZoneManager.dataNodes(filterRevision1, zoneId).get(TIMEOUT, MILLISECONDS);
        assertEquals(ONE_NODE_NAME, dataNodes3);
    }

    @Test
    void createZoneWithNotImmediateTimers() throws Exception {
        // Prerequisite.

        // Create logical topology with NODE_0.
        topology.putNode(NODE_0);

        putNodeInLogicalTopologyAndGetRevision(NODE_0, ONE_NODE);

        // Test steps.

        // Create a zone.
        long createZoneRevision = createZoneAndGetRevision(
                ZONE_NAME,
                INFINITE_TIMER_VALUE,
                INFINITE_TIMER_VALUE
        );

        // Check that data nodes value of the zone with the create zone revision is NODE_0.
        Set<String> dataNodes1 = distributionZoneManager.dataNodes(createZoneRevision, getZoneId(ZONE_NAME)).get(TIMEOUT, MILLISECONDS);
        assertEquals(ONE_NODE_NAME, dataNodes1);
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

        // Test steps.

        // Create a zone.
        long createZoneRevision = createZoneAndGetRevision(
                ZONE_NAME,
                IMMEDIATE_TIMER_VALUE,
                IMMEDIATE_TIMER_VALUE
        );

        int zoneId = getZoneId(ZONE_NAME);

        // Check that data nodes value of the zone with the revision lower than the create zone revision is absent.
        assertThrowsWithCause(
                () -> distributionZoneManager.dataNodes(createZoneRevision - 1, zoneId).get(TIMEOUT, MILLISECONDS),
                DistributionZoneNotFoundException.class
        );

        // Check that data nodes value of the zone with the create zone revision is NODE_0 and NODE_1.
        Set<String> dataNodes = distributionZoneManager.dataNodes(createZoneRevision, zoneId).get(TIMEOUT, MILLISECONDS);
        assertEquals(TWO_NODES_NAMES, dataNodes);

        // Drop the zone.
        long dropZoneRevision = dropZoneAndGetRevision(ZONE_NAME);

        // Check that data nodes value of the zone with the drop zone revision is absent.
        assertThrowsWithCause(
                () -> distributionZoneManager.dataNodes(dropZoneRevision, zoneId).get(TIMEOUT, MILLISECONDS),
                DistributionZoneNotFoundException.class
        );
    }

    /**
     * Tests data nodes obtaining with wrong parameters throw an exception.
     *
     * @throws Exception If failed.
     */
    @Test
    void validationTest() {
        int defaultZoneId = getZoneId(DEFAULT_ZONE_NAME);

        assertThrowsWithCause(() -> distributionZoneManager.dataNodes(0, defaultZoneId).get(TIMEOUT, MILLISECONDS),
                IllegalArgumentException.class);

        assertThrowsWithCause(() -> distributionZoneManager.dataNodes(-1, defaultZoneId).get(TIMEOUT, MILLISECONDS),
                IllegalArgumentException.class);

        assertThrowsWithCause(() -> distributionZoneManager.dataNodes(1, -1).get(TIMEOUT, MILLISECONDS),
                IllegalArgumentException.class);
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
        alterZone(DEFAULT_ZONE_NAME, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE, null);

        createZone(ZONE_NAME, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE, null);

        // Test steps.
        // Change logical topology. NODE_0 is added.
        long topologyRevision0 = putNodeInLogicalTopologyAndGetRevision(NODE_0, ONE_NODE);

        // Change logical topology. NODE_1 is added.
        long topologyRevision1 = putNodeInLogicalTopologyAndGetRevision(NODE_1, TWO_NODES);

        int defaultZoneId = getZoneId(DEFAULT_ZONE_NAME);
        int zoneId = getZoneId(ZONE_NAME);

        Set<String> dataNodes1 = distributionZoneManager.dataNodes(topologyRevision0, defaultZoneId).get(TIMEOUT, MILLISECONDS);
        assertEquals(ONE_NODE_NAME, dataNodes1);
        Set<String> dataNodes2 = distributionZoneManager.dataNodes(topologyRevision0, zoneId).get(TIMEOUT, MILLISECONDS);
        assertEquals(ONE_NODE_NAME, dataNodes2);

        Set<String> dataNodes3 = distributionZoneManager.dataNodes(topologyRevision1, defaultZoneId).get(TIMEOUT, MILLISECONDS);
        assertEquals(TWO_NODES_NAMES, dataNodes3);
        Set<String> dataNodes4 = distributionZoneManager.dataNodes(topologyRevision1, zoneId).get(TIMEOUT, MILLISECONDS);
        assertEquals(TWO_NODES_NAMES, dataNodes4);

        Set<LogicalNode> twoNodes1 = Set.of(NODE_0, NODE_2);
        Set<String> twoNodesNames1 = Set.of(NODE_0.name(), NODE_2.name());

        // Change logical topology. NODE_2 is added.
        long topologyRevision2 = putNodeInLogicalTopologyAndGetRevision(NODE_2, THREE_NODES);

        // Change logical topology. NODE_1 is left.
        long topologyRevision3 = removeNodeInLogicalTopologyAndGetRevision(Set.of(NODE_1), twoNodes1);

        Set<String> dataNodes5 = distributionZoneManager.dataNodes(topologyRevision2, defaultZoneId).get(TIMEOUT, MILLISECONDS);
        assertEquals(THREE_NODES_NAMES, dataNodes5);
        Set<String> dataNodes6 = distributionZoneManager.dataNodes(topologyRevision2, zoneId).get(TIMEOUT, MILLISECONDS);
        assertEquals(THREE_NODES_NAMES, dataNodes6);

        Set<String> dataNodes7 = distributionZoneManager.dataNodes(topologyRevision3, defaultZoneId).get(TIMEOUT, MILLISECONDS);
        assertEquals(twoNodesNames1, dataNodes7);
        Set<String> dataNodes8 = distributionZoneManager.dataNodes(topologyRevision3, zoneId).get(TIMEOUT, MILLISECONDS);
        assertEquals(twoNodesNames1, dataNodes8);
    }

    /**
     * Check that data nodes calculation is idempotented.
     * The current data nodes value which was calculated by data nodes from the meta storage manager and topology augmentation maps equals
     * to the data nodes value which were calculated by data nodes from the meta storage manager after topology augmentation maps
     * were cleared.
     *
     * @throws Exception If failed.
     */
    @Test
    void checkDataNodesRepeated() throws Exception {
        prepareZonesWithOneDataNodes();

        prepareZonesTimerValuesToTest();

        CountDownLatch scaleUpLatch = blockScaleUpTaskExecution();

        CountDownLatch scaleDownLatch = blockScaleDownTaskExecution();

        Set<LogicalNode> topology1 = Set.of(NODE_0, NODE_1, NODE_2);
        Set<LogicalNode> topology2 = Set.of(NODE_0, NODE_2);

        // Change logical topology. NODE_2 is added.
        long topologyRevision1 = putNodeInLogicalTopologyAndGetRevision(NODE_2, topology1);
        waitForCondition(() -> metaStorageManager.appliedRevision() >= topologyRevision1, TIMEOUT);

        // Change logical topology. NODE_1 is removed.
        long topologyRevision2 = removeNodeInLogicalTopologyAndGetRevision(Set.of(NODE_1), topology2);
        waitForCondition(() -> metaStorageManager.appliedRevision() >= topologyRevision2, TIMEOUT);

        Set<String> expectedDefaultZoneDataNodes = Set.of(NODE_0.name(), NODE_2.name());
        Set<String> expectedZone1DataNodes = Set.of(NODE_0.name(), NODE_1.name(), NODE_2.name());
        Set<String> expectedZone2DataNodes = Set.of(NODE_0.name());
        Set<String> expectedZone3DataNodes = Set.of(NODE_0.name(), NODE_1.name());

        // Check that data nodes values are changed according to scale up and down timers.
        // Data nodes from the meta storage manager and topology augmentation maps are used to calculate current data nodes.
        checkDataNodes(
                topologyRevision2,
                expectedDefaultZoneDataNodes,
                expectedZone1DataNodes,
                expectedZone2DataNodes,
                expectedZone3DataNodes
        );

        // Check that data nodes values are not changed in the meta storage.
        checkThatDataNodesIsNotChangedInMetastorage();

        // Unblock scale up and down tasks.
        scaleUpLatch.countDown();
        scaleDownLatch.countDown();

        // Check that data nodes values are changed in the meta storage after scale up and scale down tasks updated the meta storage.
        checkThatDataNodesIsChangedInMetastorage(
                expectedDefaultZoneDataNodes,
                expectedZone1DataNodes,
                expectedZone2DataNodes,
                expectedZone3DataNodes
        );

        // Check that data nodes values are idempotented.
        // Data nodes from the meta storage manager only used to calculate current data nodes because the meta storage are updated and
        // topology augmentation maps were cleared.
        checkDataNodes(
                topologyRevision2,
                expectedDefaultZoneDataNodes,
                expectedZone1DataNodes,
                expectedZone2DataNodes,
                expectedZone3DataNodes
        );
    }

    /**
     * Added two nodes in topology and assert that data nodes of zones are contains all topology nodes.
     */
    private void prepareZonesWithOneDataNodes() throws Exception {
        alterZone(DEFAULT_ZONE_NAME, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE, null);

        createZone(ZONE_NAME, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE, null);

        createZone(ZONE_NAME_2, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE, null);

        createZone(ZONE_NAME_3, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE, null);

        // Change logical topology. NODE_0 is added.
        topology.putNode(NODE_0);

        // Change logical topology. NODE_1 is added.
        long topologyRevision0 = putNodeInLogicalTopologyAndGetRevision(NODE_1, TWO_NODES);

        int defaultZoneId = getZoneId(DEFAULT_ZONE_NAME);
        int zoneId = getZoneId(ZONE_NAME);
        int zoneId2 = getZoneId(ZONE_NAME_2);
        int zoneId3 = getZoneId(ZONE_NAME_3);

        Set<String> dataNodes1 = distributionZoneManager.dataNodes(topologyRevision0, defaultZoneId).get(TIMEOUT, MILLISECONDS);
        assertEquals(TWO_NODES_NAMES, dataNodes1);
        Set<String> dataNodes2 = distributionZoneManager.dataNodes(topologyRevision0, zoneId).get(TIMEOUT, MILLISECONDS);
        assertEquals(TWO_NODES_NAMES, dataNodes2);
        Set<String> dataNodes3 = distributionZoneManager.dataNodes(topologyRevision0, zoneId2).get(TIMEOUT, MILLISECONDS);
        assertEquals(TWO_NODES_NAMES, dataNodes3);
        Set<String> dataNodes4 = distributionZoneManager.dataNodes(topologyRevision0, zoneId3).get(TIMEOUT, MILLISECONDS);
        assertEquals(TWO_NODES_NAMES, dataNodes4);
    }

    /**
     * Schedule a scale up task which block execution of another scale up tasks.
     *
     * @return Latch to unblock execution of scale up tasks.
     */
    private CountDownLatch blockScaleUpTaskExecution() {
        CountDownLatch scaleUpLatch = new CountDownLatch(1);

        Runnable dummyScaleUpTask = () -> {
            try {
                scaleUpLatch.await(TIMEOUT, MILLISECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };

        int defaultZoneId = getZoneId(DEFAULT_ZONE_NAME);
        int zoneId = getZoneId(ZONE_NAME);
        int zoneId2 = getZoneId(ZONE_NAME_2);
        int zoneId3 = getZoneId(ZONE_NAME_3);

        distributionZoneManager.zonesState().get(defaultZoneId)
                .rescheduleScaleUp(IMMEDIATE_TIMER_VALUE, dummyScaleUpTask, defaultZoneId);
        distributionZoneManager.zonesState().get(zoneId)
                .rescheduleScaleUp(IMMEDIATE_TIMER_VALUE, dummyScaleUpTask, zoneId);
        distributionZoneManager.zonesState().get(zoneId2)
                .rescheduleScaleUp(IMMEDIATE_TIMER_VALUE, dummyScaleUpTask, zoneId2);
        distributionZoneManager.zonesState().get(zoneId3)
                .rescheduleScaleUp(IMMEDIATE_TIMER_VALUE, dummyScaleUpTask, zoneId3);

        return scaleUpLatch;
    }

    /**
     * Schedule a down up task which block execution of another scale down tasks.
     *
     * @return Latch to unblock execution of scale down tasks.
     */
    private CountDownLatch blockScaleDownTaskExecution() {
        CountDownLatch scaleDownLatch = new CountDownLatch(1);

        Runnable dummyScaleDownTask = () -> {
            try {
                scaleDownLatch.await(TIMEOUT, MILLISECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };

        int defaultZoneId = getZoneId(DEFAULT_ZONE_NAME);
        int zoneId = getZoneId(ZONE_NAME);
        int zoneId2 = getZoneId(ZONE_NAME_2);
        int zoneId3 = getZoneId(ZONE_NAME_3);

        distributionZoneManager.zonesState().get(defaultZoneId)
                .rescheduleScaleDown(IMMEDIATE_TIMER_VALUE, dummyScaleDownTask, defaultZoneId);
        distributionZoneManager.zonesState().get(zoneId)
                .rescheduleScaleDown(IMMEDIATE_TIMER_VALUE, dummyScaleDownTask, zoneId);
        distributionZoneManager.zonesState().get(zoneId2)
                .rescheduleScaleDown(IMMEDIATE_TIMER_VALUE, dummyScaleDownTask, zoneId2);
        distributionZoneManager.zonesState().get(zoneId3)
                .rescheduleScaleDown(IMMEDIATE_TIMER_VALUE, dummyScaleDownTask, zoneId3);

        return scaleDownLatch;
    }

    private void checkThatDataNodesIsNotChangedInMetastorage() throws Exception {
        assertValueInStorage(
                metaStorageManager,
                zoneDataNodesKey(getZoneId(DEFAULT_ZONE_NAME)),
                (v) -> DistributionZonesUtil.dataNodes(fromBytes(v)).stream().map(Node::nodeName).collect(toSet()),
                TWO_NODES_NAMES,
                TIMEOUT
        );

        assertValueInStorage(
                metaStorageManager,
                zoneDataNodesKey(getZoneId(ZONE_NAME)),
                (v) -> DistributionZonesUtil.dataNodes(fromBytes(v)).stream().map(Node::nodeName).collect(toSet()),
                TWO_NODES_NAMES,
                TIMEOUT
        );

        assertValueInStorage(
                metaStorageManager,
                zoneDataNodesKey(getZoneId(ZONE_NAME_2)),
                (v) -> DistributionZonesUtil.dataNodes(fromBytes(v)).stream().map(Node::nodeName).collect(toSet()),
                TWO_NODES_NAMES,
                TIMEOUT
        );

        assertValueInStorage(
                metaStorageManager,
                zoneDataNodesKey(getZoneId(ZONE_NAME_3)),
                (v) -> DistributionZonesUtil.dataNodes(fromBytes(v)).stream().map(Node::nodeName).collect(toSet()),
                TWO_NODES_NAMES,
                TIMEOUT
        );
    }

    /**
     * Check current data nodes.
     */
    private void checkDataNodes(
            long revision,
            Set<String> expectedDefaultZoneDataNodes,
            Set<String> expectedZone1DataNodes,
            Set<String> expectedZone2DataNodes,
            Set<String> expectedZone3DataNodes
    ) throws ExecutionException, InterruptedException, TimeoutException {
        assertEquals(
                expectedDefaultZoneDataNodes,
                distributionZoneManager.dataNodes(revision, getZoneId(DEFAULT_ZONE_NAME)).get(TIMEOUT, MILLISECONDS)
        );

        assertEquals(
                expectedZone1DataNodes,
                distributionZoneManager.dataNodes(revision, getZoneId(ZONE_NAME)).get(TIMEOUT, MILLISECONDS)
        );

        assertEquals(
                expectedZone2DataNodes,
                distributionZoneManager.dataNodes(revision, getZoneId(ZONE_NAME_2)).get(TIMEOUT, MILLISECONDS)
        );

        assertEquals(
                expectedZone3DataNodes,
                distributionZoneManager.dataNodes(revision, getZoneId(ZONE_NAME_3)).get(TIMEOUT, MILLISECONDS)
        );
    }

    private void checkThatDataNodesIsChangedInMetastorage(
            Set<String> dataNodes1,
            Set<String> dataNodes2,
            Set<String> dataNodes3,
            Set<String> dataNodes4) throws Exception {
        assertValueInStorage(
                metaStorageManager,
                zoneDataNodesKey(getZoneId(DEFAULT_ZONE_NAME)),
                (v) -> DistributionZonesUtil.dataNodes(fromBytes(v)).stream().map(Node::nodeName).collect(toSet()),
                dataNodes1,
                TIMEOUT
        );

        assertValueInStorage(
                metaStorageManager,
                zoneDataNodesKey(getZoneId(ZONE_NAME)),
                (v) -> DistributionZonesUtil.dataNodes(fromBytes(v)).stream().map(Node::nodeName).collect(toSet()),
                dataNodes2,
                TIMEOUT
        );

        assertValueInStorage(
                metaStorageManager,
                zoneDataNodesKey(getZoneId(ZONE_NAME_2)),
                (v) -> DistributionZonesUtil.dataNodes(fromBytes(v)).stream().map(Node::nodeName).collect(toSet()),
                dataNodes3,
                TIMEOUT
        );

        assertValueInStorage(
                metaStorageManager,
                zoneDataNodesKey(getZoneId(ZONE_NAME_3)),
                (v) -> DistributionZonesUtil.dataNodes(fromBytes(v)).stream().map(Node::nodeName).collect(toSet()),
                dataNodes4,
                TIMEOUT
        );
    }

    private void prepareZonesTimerValuesToTest() {
        // The default zone already has immediate scale up and immediate scale down timers.

        // The zone with immediate scale up and infinity scale down timers.
        alterZone(ZONE_NAME, IMMEDIATE_TIMER_VALUE, INFINITE_TIMER_VALUE, null);

        // The zone with infinity scale up and immediate scale down timers.
        alterZone(ZONE_NAME_2, INFINITE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE, null);

        // The zone with infinity scale up and infinity scale down timers.
        alterZone(ZONE_NAME_3, INFINITE_TIMER_VALUE, INFINITE_TIMER_VALUE, null);
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

        return revisionFut.get(TIMEOUT, MILLISECONDS);
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

        return revisionFut.get(TIMEOUT, MILLISECONDS);
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

        return revisionFut.get(TIMEOUT, MILLISECONDS);
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

        alterZone(zoneName, scaleUp, null, null);

        return revisionFut.get(TIMEOUT, MILLISECONDS);
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

        alterZone(zoneName, null, scaleDown, null);

        return revisionFut.get(TIMEOUT, MILLISECONDS);
    }

    /**
     * Changes a filter value of a zone and return the revision of a zone update event.
     *
     * @param zoneName Zone name.
     * @param filter New filter value.
     * @return Revision.
     * @throws Exception If failed.
     */
    private long alterFilterAndGetRevision(String zoneName, String filter) throws Exception {
        CompletableFuture<Long> revisionFut = new CompletableFuture<>();

        int zoneId = distributionZoneManager.getZoneId(zoneName);

        zoneChangeFilterRevisions.put(zoneId, revisionFut);

        alterZone(zoneName, null, null, filter);

        return revisionFut.get(TIMEOUT, MILLISECONDS);
    }

    /**
     * Creates a zone and return the revision of a create zone event.
     *
     * @param zoneName Zone name.
     * @param scaleUp Scale up value.
     * @param scaleDown Scale down value.
     * @return Revision.
     * @throws Exception If failed.
     */
    private long createZoneAndGetRevision(String zoneName, int scaleUp, int scaleDown) throws Exception {
        CompletableFuture<Long> revisionFut = new CompletableFuture<>();

        createZoneRevisions.put(zoneName, revisionFut);

        createZone(zoneName, scaleUp, scaleDown, null);

        return revisionFut.get(TIMEOUT, MILLISECONDS);
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

        int zoneId = getZoneId(ZONE_NAME);

        dropZoneRevisions.put(zoneId, revisionFut);

        dropZone(zoneName);

        return revisionFut.get(TIMEOUT, MILLISECONDS);
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
        Set<String> nodeNames = nodes.stream().map(ClusterNode::name).collect(toSet());

        CompletableFuture<Long> revisionFut = new CompletableFuture<>();

        zoneDataNodesRevisions.put(new IgniteBiTuple<>(zoneId, nodeNames), revisionFut);

        return revisionFut;
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

                Set<String> nodeNames = newLogicalTopology.stream().map(NodeWithAttributes::nodeName).collect(toSet());

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

                Set<String> nodeNames = newDataNodes.stream().map(Node::nodeName).collect(toSet());

                IgniteBiTuple<Integer, Set<String>> zoneDataNodesKey = new IgniteBiTuple<>(zoneId, nodeNames);

                completeRevisionFuture(zoneDataNodesRevisions.remove(zoneDataNodesKey), revision);

                return completedFuture(null);
            }

            @Override
            public void onError(Throwable e) {
            }
        };
    }

    private void addCatalogZoneEventListeners() {
        catalogManager.listen(ZONE_CREATE, (parameters, exception) -> {
            String zoneName = ((CreateZoneEventParameters) parameters).zoneDescriptor().name();

            completeRevisionFuture(createZoneRevisions.remove(zoneName), parameters.causalityToken());

            return completedFuture(false);
        });

        catalogManager.listen(ZONE_DROP, (parameters, exception) -> {
            completeRevisionFuture(dropZoneRevisions.remove(((DropZoneEventParameters) parameters).zoneId()), parameters.causalityToken());

            return completedFuture(false);
        });

        catalogManager.listen(ZONE_ALTER, new CatalogAlterZoneEventListener(catalogManager) {
            @Override
            protected CompletableFuture<Void> onAutoAdjustScaleUpUpdate(AlterZoneEventParameters parameters, int oldAutoAdjustScaleUp) {
                completeRevisionFuture(zoneScaleUpRevisions.remove(parameters.zoneDescriptor().id()), parameters.causalityToken());

                return completedFuture(null);
            }

            @Override
            protected CompletableFuture<Void> onAutoAdjustScaleDownUpdate(AlterZoneEventParameters parameters, int oldAutoAdjustScaleDown) {
                completeRevisionFuture(zoneScaleUpRevisions.remove(parameters.zoneDescriptor().id()), parameters.causalityToken());

                return completedFuture(null);
            }

            @Override
            protected CompletableFuture<Void> onFilterUpdate(AlterZoneEventParameters parameters, String oldFilter) {
                completeRevisionFuture(zoneChangeFilterRevisions.remove(parameters.zoneDescriptor().id()), parameters.causalityToken());

                return completedFuture(null);
            }
        });
    }

    private static void completeRevisionFuture(@Nullable CompletableFuture<Long> revisionFuture, long revision) {
        if (revisionFuture != null) {
            revisionFuture.complete(revision);
        }
    }
}
