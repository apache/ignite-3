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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptySet;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.ZONE_ALTER;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.ZONE_CREATE;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.ZONE_DROP;
import static org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl.LOGICAL_TOPOLOGY_KEY;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertDataNodesFromManager;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertValueInStorage;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.createDefaultZone;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.DISTRIBUTION_ZONE_DATA_NODES_HISTORY_PREFIX;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.DISTRIBUTION_ZONE_DATA_NODES_HISTORY_PREFIX_BYTES;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.deserializeLogicalTopologySet;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesHistoryKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyPrefix;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyVersionKey;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.extractZoneId;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.ByteUtils.toByteArray;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;
import static org.apache.ignite.internal.util.IgniteUtils.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.events.AlterZoneEventParameters;
import org.apache.ignite.internal.catalog.events.CreateZoneEventParameters;
import org.apache.ignite.internal.catalog.events.DropZoneEventParameters;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshotSerializer;
import org.apache.ignite.internal.distributionzones.BaseDistributionZoneManagerTest;
import org.apache.ignite.internal.distributionzones.DataNodesHistory;
import org.apache.ignite.internal.distributionzones.DataNodesHistory.DataNodesHistorySerializer;
import org.apache.ignite.internal.distributionzones.DataNodesHistoryEntry;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.distributionzones.DistributionZonesUtil;
import org.apache.ignite.internal.distributionzones.Node;
import org.apache.ignite.internal.distributionzones.NodeWithAttributes;
import org.apache.ignite.internal.distributionzones.exception.DistributionZoneNotFoundException;
import org.apache.ignite.internal.distributionzones.utils.CatalogAlterZoneEventListener;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.EntryEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.server.If;
import org.apache.ignite.internal.metastorage.server.raft.MetaStorageWriteHandler;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

/**
 * Tests for causality data nodes updating in {@link DistributionZoneManager}.
 */
public class DistributionZoneCausalityDataNodesTest extends BaseDistributionZoneManagerTest {
    private static final String ZONE_NAME_2 = "zone2";

    private static final String ZONE_NAME_3 = "zone3";

    private static final String ZONE_NAME_4 = "zone4";

    private static final LogicalNode NODE_0 = new LogicalNode(
            new ClusterNodeImpl(randomUUID(), "node_name_0", new NetworkAddress("localhost", 123)),
            Map.of(),
            Map.of(),
            List.of(DEFAULT_STORAGE_PROFILE)
    );

    private static final LogicalNode NODE_1 = new LogicalNode(
            new ClusterNodeImpl(randomUUID(), "node_name_1", new NetworkAddress("localhost", 123)),
            Map.of(),
            Map.of(),
            List.of(DEFAULT_STORAGE_PROFILE)
    );

    private static final LogicalNode NODE_2 = new LogicalNode(
            new ClusterNodeImpl(randomUUID(), "node_name_2", new NetworkAddress("localhost", 123)),
            Map.of(),
            Map.of(),
            List.of(DEFAULT_STORAGE_PROFILE)
    );

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
    private final ConcurrentHashMap<Set<String>, CompletableFuture<RevWithTimestamp>> topologyRevisions = new ConcurrentHashMap<>();

    /**
     * Contains futures that is completed when the data nodes watch listener receive the event with expected zone id and data nodes.
     * Mapping of zone id and node names -> future with event timestamp.
     */
    private final ConcurrentHashMap<IgniteBiTuple<Integer, Set<String>>, CompletableFuture<HybridTimestamp>> zoneDataNodesTimestamps =
            new ConcurrentHashMap<>();

    /**
     * Contains futures that is completed when the scale up update listener receive the event with expected zone id.
     * Mapping of zone id -> future with event timestamp.
     */
    private final ConcurrentHashMap<Integer, CompletableFuture<HybridTimestamp>> zoneScaleUpTimestamps = new ConcurrentHashMap<>();

    /**
     * Contains futures that is completed when the scale down update listener receive the event with expected zone id.
     * Mapping of zone id -> future with event timestamp.
     */
    private final ConcurrentHashMap<Integer, CompletableFuture<HybridTimestamp>> zoneScaleDownTimestamps = new ConcurrentHashMap<>();

    /**
     * Contains futures that is completed when the filter update listener receive the event with expected zone id.
     * Mapping of zone id -> future with event timestamp.
     */
    private final ConcurrentHashMap<Integer, CompletableFuture<HybridTimestamp>> zoneChangeFilterTimestamps = new ConcurrentHashMap<>();

    /**
     * Contains futures that is completed when the zone catalog listener receive the zone creation event with expected zone.
     * Mapping of zone name -> future with event timestamp.
     */
    private final ConcurrentHashMap<String, CompletableFuture<HybridTimestamp>> createZoneTimestamps = new ConcurrentHashMap<>();

    /**
     * Contains futures that is completed when the zone catalog listener receive the zone dropping event with expected zone.
     * Mapping of zone id -> future with event timestamp.
     */
    private final ConcurrentHashMap<Integer, CompletableFuture<HybridTimestamp>> dropZoneTimestamps = new ConcurrentHashMap<>();

    private ExecutorService tempPool;

    @BeforeEach
    void beforeEach() {
        metaStorageManager.registerPrefixWatch(zonesLogicalTopologyPrefix(), createMetastorageTopologyListener());
        metaStorageManager.registerPrefixWatch(
                new ByteArray(DISTRIBUTION_ZONE_DATA_NODES_HISTORY_PREFIX.getBytes(UTF_8)),
                createMetastorageDataNodesListener()
        );

        addCatalogZoneEventListeners();

        assertThat(distributionZoneManager.startAsync(new ComponentContext()), willCompleteSuccessfully());

        assertThat(metaStorageManager.deployWatches(), willCompleteSuccessfully());

        createDefaultZone(catalogManager);

        assertNotNull(getDefaultZone());
    }

    @AfterEach
    void afterEach() throws InterruptedException {
        if (tempPool != null) {
            shutdownAndAwaitTermination(tempPool, 1, SECONDS);
        }
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

        CompletableFuture<HybridTimestamp> dataNodesUpdateRevision = getZoneDataNodesTimestamp(zoneId2, TWO_NODES);

        // Check that data nodes value of both zone is NODE_0 and NODE_1.
        RevWithTimestamp topologyRevision1 = putNodeInLogicalTopologyAndGetTimestamp(NODE_1, TWO_NODES);

        Set<String> dataNodes0 = distributionZoneManager
                .dataNodes(topologyRevision1.timestamp, catalogManager.latestCatalogVersion(), zoneId)
                .get(TIMEOUT, MILLISECONDS);
        assertEquals(TWO_NODES_NAMES, dataNodes0);

        HybridTimestamp dataNodesTsZone = dataNodesUpdateRevision.get(TIMEOUT, MILLISECONDS);

        Set<String> dataNodes1 = distributionZoneManager.dataNodes(dataNodesTsZone, catalogManager.latestCatalogVersion(), zoneId2)
                .get(TIMEOUT, MILLISECONDS);
        assertEquals(TWO_NODES_NAMES, dataNodes1);

        // Test steps.

        // Change logical topology. NODE_1 is left. NODE_2 is added.
        Set<LogicalNode> twoNodes2 = Set.of(NODE_0, NODE_2);
        Set<String> twoNodesNames2 = Set.of(NODE_0.name(), NODE_2.name());

        dataNodesUpdateRevision = getZoneDataNodesTimestamp(zoneId2, twoNodes2);

        RevWithTimestamp topologyRevision2 = fireTopologyLeapAndGetTimestamp(twoNodes2);

        // Check that data nodes value of the zone with immediate timers with the topology update revision is NODE_0 and NODE_2.
        Set<String> dataNodes3 = distributionZoneManager
                .dataNodes(topologyRevision2.timestamp, catalogManager.latestCatalogVersion(), zoneId)
                .get(TIMEOUT, MILLISECONDS);
        assertEquals(twoNodesNames2, dataNodes3);

        // Check that data nodes value of the zone with not immediate timers with the topology update revision is NODE_0 and NODE_1.
        Set<String> dataNodes4 = distributionZoneManager
                .dataNodes(topologyRevision2.timestamp, catalogManager.latestCatalogVersion(), zoneId2)
                .get(TIMEOUT, MILLISECONDS);
        assertEquals(TWO_NODES_NAMES, dataNodes4);

        // Check that data nodes value of the zone with not immediate timers with the data nodes update revision is NODE_0 and NODE_2.
        dataNodesTsZone = dataNodesUpdateRevision.get(TIMEOUT, MILLISECONDS);
        Set<String> dataNodes5 = distributionZoneManager.dataNodes(dataNodesTsZone, catalogManager.latestCatalogVersion(), zoneId2)
                .get(TIMEOUT, MILLISECONDS);
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

        CatalogZoneDescriptor defaultZone = getDefaultZone();
        int defaultZoneId = defaultZone.id();

        // Alter the zone with immediate timers.
        alterZone(defaultZone.name(), IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE, null);

        // Create logical topology with NODE_0 and NODE_1.
        topology.putNode(NODE_0);

        // Check that data nodes value of both zone is NODE_0 and NODE_1.
        RevWithTimestamp topologyRevision1 = putNodeInLogicalTopologyAndGetTimestamp(NODE_1, TWO_NODES);

        int zoneId = getZoneId(ZONE_NAME);

        Set<String> dataNodes0 = distributionZoneManager
                .dataNodes(topologyRevision1.timestamp, catalogManager.latestCatalogVersion(), defaultZoneId)
                .get(TIMEOUT, MILLISECONDS);
        assertEquals(TWO_NODES_NAMES, dataNodes0);

        Set<String> dataNodes1 = distributionZoneManager
                .dataNodes(topologyRevision1.timestamp, catalogManager.latestCatalogVersion(), zoneId)
                .get(TIMEOUT, MILLISECONDS);
        assertEquals(TWO_NODES_NAMES, dataNodes1);

        // Alter zones with not immediate scale up timer.
        alterZone(ZONE_NAME, 1, IMMEDIATE_TIMER_VALUE, null);

        alterZone(getDefaultZone().name(), 1, IMMEDIATE_TIMER_VALUE, null);

        // Test steps.

        // Change logical topology. NODE_1 is left. NODE_2 is added.
        Set<LogicalNode> twoNodes2 = Set.of(NODE_0, NODE_2);
        Set<String> twoNodesNames2 = Set.of(NODE_0.name(), NODE_2.name());

        CompletableFuture<HybridTimestamp> dataNodesUpdateTs0 = getZoneDataNodesTimestamp(defaultZoneId, twoNodes2);
        CompletableFuture<HybridTimestamp> dataNodesUpdateTs1 = getZoneDataNodesTimestamp(zoneId, twoNodes2);

        RevWithTimestamp topologyRevision2 = fireTopologyLeapAndGetTimestamp(twoNodes2);

        // Check that data nodes value of zones is NODE_0 because scale up timer has not fired yet.
        Set<String> dataNodes2 = distributionZoneManager
                .dataNodes(topologyRevision2.timestamp, catalogManager.latestCatalogVersion(), defaultZoneId)
                .get(TIMEOUT, MILLISECONDS);
        assertEquals(ONE_NODE_NAME, dataNodes2);

        Set<String> dataNodes3 = distributionZoneManager
                .dataNodes(topologyRevision2.timestamp, catalogManager.latestCatalogVersion(), zoneId)
                .get(TIMEOUT, MILLISECONDS);
        assertEquals(ONE_NODE_NAME, dataNodes3);

        // Check that data nodes value of zones is NODE_0 and NODE_2.
        HybridTimestamp dataNodesTsZone0 = dataNodesUpdateTs0.get(TIMEOUT, MILLISECONDS);
        Set<String> dataNodes4 = distributionZoneManager.dataNodes(
                dataNodesTsZone0,
                catalogManager.latestCatalogVersion(),
                defaultZoneId
        ).get(TIMEOUT, MILLISECONDS);
        assertEquals(twoNodesNames2, dataNodes4);

        HybridTimestamp dataNodesTsZone1 = dataNodesUpdateTs1.get(TIMEOUT, MILLISECONDS);
        Set<String> dataNodes5 = distributionZoneManager.dataNodes(dataNodesTsZone1, catalogManager.latestCatalogVersion(), zoneId)
                .get(TIMEOUT, MILLISECONDS);
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

        CatalogZoneDescriptor defaultZone = getDefaultZone();

        // Alter the zone with not immediate scale down timer.
        alterZone(defaultZone.name(), IMMEDIATE_TIMER_VALUE, 1, null);

        // Create logical topology with NODE_0 and NODE_1.
        topology.putNode(NODE_0);

        // Check that data nodes value of both zone is NODE_0 and NODE_1.
        RevWithTimestamp topologyRevision1 = putNodeInLogicalTopologyAndGetTimestamp(NODE_1, TWO_NODES);

        int defaultZoneId = defaultZone.id();
        int zoneId = getZoneId(ZONE_NAME);

        Set<String> dataNodes0 = distributionZoneManager
                .dataNodes(topologyRevision1.timestamp, catalogManager.latestCatalogVersion(), defaultZoneId)
                .get(TIMEOUT, MILLISECONDS);
        assertEquals(TWO_NODES_NAMES, dataNodes0);

        Set<String> dataNodes1 = distributionZoneManager
                .dataNodes(topologyRevision1.timestamp, catalogManager.latestCatalogVersion(), zoneId)
                .get(TIMEOUT, MILLISECONDS);
        assertEquals(TWO_NODES_NAMES, dataNodes1);

        // Test steps.

        // Change logical topology. NODE_1 is left. NODE_2 is added.
        Set<LogicalNode> twoNodes2 = Set.of(NODE_0, NODE_2);
        Set<String> twoNodesNames2 = Set.of(NODE_0.name(), NODE_2.name());

        CompletableFuture<HybridTimestamp> dataNodesUpdateTs0 = getZoneDataNodesTimestamp(defaultZoneId, twoNodes2);
        CompletableFuture<HybridTimestamp> dataNodesUpdateTs1 = getZoneDataNodesTimestamp(zoneId, twoNodes2);

        RevWithTimestamp topologyRevision2 = fireTopologyLeapAndGetTimestamp(twoNodes2);

        // Check that data nodes value of zones is NODE_0, NODE_1 and NODE_2 because scale down timer has not fired yet.
        Set<String> dataNodes2 = distributionZoneManager
                .dataNodes(topologyRevision2.timestamp, catalogManager.latestCatalogVersion(), defaultZoneId)
                .get(TIMEOUT, MILLISECONDS);
        assertEquals(THREE_NODES_NAMES, dataNodes2);

        Set<String> dataNodes3 = distributionZoneManager
                .dataNodes(topologyRevision2.timestamp, catalogManager.latestCatalogVersion(), zoneId)
                .get(TIMEOUT, MILLISECONDS);
        assertEquals(THREE_NODES_NAMES, dataNodes3);

        // Check that data nodes value of zones is NODE_0 and NODE_2.
        HybridTimestamp dataNodesRevisionZone0 = dataNodesUpdateTs0.get(TIMEOUT, MILLISECONDS);
        Set<String> dataNodes4 = distributionZoneManager.dataNodes(
                dataNodesRevisionZone0,
                catalogManager.latestCatalogVersion(),
                defaultZoneId
        ).get(TIMEOUT, MILLISECONDS);
        assertEquals(twoNodesNames2, dataNodes4);

        HybridTimestamp dataNodesRevisionZone1 = dataNodesUpdateTs1.get(TIMEOUT, MILLISECONDS);
        Set<String> dataNodes5 = distributionZoneManager.dataNodes(dataNodesRevisionZone1, catalogManager.latestCatalogVersion(), zoneId)
                .get(TIMEOUT, MILLISECONDS);
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
        RevWithTimestamp topologyRevision1 = putNodeInLogicalTopologyAndGetTimestamp(NODE_0, ONE_NODE);

        int zoneId = getZoneId(ZONE_NAME);

        // Check that data nodes value of the zone is NODE_0.
        Set<String> dataNodes1 = distributionZoneManager
                .dataNodes(topologyRevision1.timestamp, catalogManager.latestCatalogVersion(), zoneId)
                .get(TIMEOUT, MILLISECONDS);
        assertEquals(ONE_NODE_NAME, dataNodes1);

        // Changes a scale up timer to not immediate.
        alterZone(ZONE_NAME, 10000, IMMEDIATE_TIMER_VALUE, null);

        // Test steps.

        // Change logical topology. NODE_1 is added.
        RevWithTimestamp topologyRevision2 = putNodeInLogicalTopologyAndGetTimestamp(NODE_1, TWO_NODES);

        // Check that data nodes value of the zone with the topology update revision is NODE_0 because scale up timer has not fired yet.
        Set<String> dataNodes2 = distributionZoneManager
                .dataNodes(topologyRevision2.timestamp, catalogManager.latestCatalogVersion(), zoneId)
                .get(TIMEOUT, MILLISECONDS);
        assertEquals(ONE_NODE_NAME, dataNodes2);

        // Change scale up value to immediate.
        HybridTimestamp scaleUpTs = alterZoneScaleUpAndGetTimestamp(ZONE_NAME, IMMEDIATE_TIMER_VALUE);

        // Check that data nodes value of the zone with the scale up update revision is NODE_0 and NODE_1.
        Set<String> dataNodes3 = distributionZoneManager.dataNodes(scaleUpTs, catalogManager.latestCatalogVersion(), zoneId)
                .get(TIMEOUT, MILLISECONDS);
        assertEquals(TWO_NODES_NAMES, dataNodes3);
    }

    /**
     * Tests that data nodes for a zone with non-immediate scale up/down on a zone creation will eventually return
     * expected data nodes.
     *
     * @throws Exception If failed.
     */
    @Test
    void testZoneWithNonImmediateTimersOnCreation() throws Exception {
        createZone(ZONE_NAME, 1, 1, null);

        topology.putNode(NODE_0);
        topology.putNode(NODE_1);
        topology.removeNodes(Set.of(NODE_1));

        int zoneId = getZoneId(ZONE_NAME);

        assertDataNodesFromManager(
                distributionZoneManager,
                metaStorageManager::appliedRevision,
                catalogManager::latestCatalogVersion,
                zoneId,
                ONE_NODE,
                TIMEOUT
        );
    }

    /**
     * Tests that data nodes for a zone with non-immediate down on a zone creation will eventually return
     * expected data nodes.
     *
     * @throws Exception If failed.
     */
    @Test
    void testZoneWithNonImmediateScaleDownTimerOnCreation() throws Exception {
        createZone(ZONE_NAME, IMMEDIATE_TIMER_VALUE, 1, null);

        topology.putNode(NODE_0);
        RevWithTimestamp topologyRevision = putNodeInLogicalTopologyAndGetTimestamp(NODE_1, TWO_NODES);
        topology.removeNodes(Set.of(NODE_1));

        int zoneId = getZoneId(ZONE_NAME);

        Set<String> dataNodes1 = distributionZoneManager
                .dataNodes(topologyRevision.timestamp, catalogManager.latestCatalogVersion(), zoneId)
                .get(TIMEOUT, MILLISECONDS);
        assertEquals(TWO_NODES_NAMES, dataNodes1);

        assertDataNodesFromManager(
                distributionZoneManager,
                metaStorageManager::appliedRevision,
                catalogManager::latestCatalogVersion,
                zoneId,
                ONE_NODE,
                TIMEOUT
        );
    }

    /**
     * Tests that data nodes for a zone with non-immediate scale up on a zone creation will eventually return
     * expected data nodes.
     *
     * @throws Exception If failed.
     */
    @Test
    void testZoneWithNonImmediateScaleUpTimerOnCreation() throws Exception {
        createZone(ZONE_NAME, 1, IMMEDIATE_TIMER_VALUE, null);

        topology.putNode(NODE_0);
        topology.putNode(NODE_1);
        RevWithTimestamp topologyRevision = removeNodeInLogicalTopologyAndGetTimestamp(Set.of(NODE_1), ONE_NODE);

        int zoneId = getZoneId(ZONE_NAME);

        Set<String> dataNodes1 = distributionZoneManager
                .dataNodes(topologyRevision.timestamp, catalogManager.latestCatalogVersion(), zoneId)
                .get(TIMEOUT, MILLISECONDS);
        assertEquals(Set.of(), dataNodes1);

        assertDataNodesFromManager(
                distributionZoneManager,
                metaStorageManager::appliedRevision,
                catalogManager::latestCatalogVersion,
                zoneId,
                ONE_NODE,
                TIMEOUT
        );
    }

    /**
     * Tests that data nodes for zones with different scale up/down configs are empty when creation of zones were before any
     * topology event. In this test scenario we assume that initialisation of a zone was before the calling of the data nodes method.
     *
     * @throws Exception If failed.
     */
    @Test
    void testEmptyDataNodesOnZoneCreationBeforeTopologyEvent() throws Exception {
        createZone(ZONE_NAME, 1, 1, null);
        createZone(ZONE_NAME_2, 1, IMMEDIATE_TIMER_VALUE, null);
        createZone(ZONE_NAME_3, IMMEDIATE_TIMER_VALUE, 1, null);
        createZone(ZONE_NAME_4, IMMEDIATE_TIMER_VALUE, 1, null);

        int zoneId1 = getZoneId(ZONE_NAME);
        int zoneId2 = getZoneId(ZONE_NAME_2);
        int zoneId3 = getZoneId(ZONE_NAME_3);
        int zoneId4 = getZoneId(ZONE_NAME_4);
        int defaultZoneId = getDefaultZone().id();

        HybridTimestamp timestamp = metaStorageManager.timestampByRevisionLocally(metaStorageManager.appliedRevision());

        Set<String> dataNodes = distributionZoneManager.dataNodes(
                timestamp,
                catalogManager.latestCatalogVersion(),
                zoneId1
        ).get(TIMEOUT, MILLISECONDS);

        assertEquals(emptySet(), dataNodes);

        dataNodes = distributionZoneManager.dataNodes(
                timestamp,
                catalogManager.latestCatalogVersion(),
                zoneId2
        ).get(TIMEOUT, MILLISECONDS);

        assertEquals(emptySet(), dataNodes);

        dataNodes = distributionZoneManager.dataNodes(
                timestamp,
                catalogManager.latestCatalogVersion(),
                zoneId3
        ).get(TIMEOUT, MILLISECONDS);

        assertEquals(emptySet(), dataNodes);

        dataNodes = distributionZoneManager.dataNodes(
                timestamp,
                catalogManager.latestCatalogVersion(),
                zoneId4
        ).get(TIMEOUT, MILLISECONDS);

        assertEquals(emptySet(), dataNodes);

        dataNodes = distributionZoneManager.dataNodes(
                timestamp,
                catalogManager.latestCatalogVersion(),
                defaultZoneId
        ).get(TIMEOUT, MILLISECONDS);

        assertEquals(emptySet(), dataNodes);
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

        RevWithTimestamp topologyRevision1 = putNodeInLogicalTopologyAndGetTimestamp(NODE_1, TWO_NODES);

        int zoneId = getZoneId(ZONE_NAME);

        // Check that data nodes value of the the zone is NODE_0 and NODE_1.
        Set<String> dataNodes1 = distributionZoneManager
                .dataNodes(topologyRevision1.timestamp, catalogManager.latestCatalogVersion(), zoneId)
                .get(TIMEOUT, MILLISECONDS);
        assertEquals(TWO_NODES_NAMES, dataNodes1);

        // Test steps.

        // Change logical topology. NODE_1 is left.
        RevWithTimestamp topologyRevision2 = removeNodeInLogicalTopologyAndGetTimestamp(Set.of(NODE_1), ONE_NODE);

        // Check that data nodes value of the zone with the topology update revision is NODE_0 and NODE_1
        // because scale down timer has not fired yet.
        Set<String> dataNodes2 = distributionZoneManager
                .dataNodes(topologyRevision2.timestamp, catalogManager.latestCatalogVersion(), zoneId)
                .get(TIMEOUT, MILLISECONDS);
        assertEquals(TWO_NODES_NAMES, dataNodes2);

        // Change scale down value to immediate.
        HybridTimestamp scaleDownTs = alterZoneScaleDownAndGetTimestamp(ZONE_NAME, IMMEDIATE_TIMER_VALUE);

        // Check that data nodes value of the zone with the scale down update revision is NODE_0.
        Set<String> dataNodes3 = distributionZoneManager.dataNodes(scaleDownTs, catalogManager.latestCatalogVersion(), zoneId)
                .get(TIMEOUT, MILLISECONDS);
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
        RevWithTimestamp topologyRevision1 = putNodeInLogicalTopologyAndGetTimestamp(NODE_0, ONE_NODE);

        int zoneId = getZoneId(ZONE_NAME);

        // Check that data nodes value of the zone is NODE_0.
        Set<String> dataNodes1 = distributionZoneManager
                .dataNodes(topologyRevision1.timestamp, catalogManager.latestCatalogVersion(), zoneId)
                .get(TIMEOUT, MILLISECONDS);
        assertEquals(ONE_NODE_NAME, dataNodes1);

        // Alter the zones with not immediate scale up timer.
        alterZone(ZONE_NAME, 10000, IMMEDIATE_TIMER_VALUE, null);

        // Test steps.

        // Change logical topology. NODE_1 is added.
        RevWithTimestamp topologyRevision2 = putNodeInLogicalTopologyAndGetTimestamp(NODE_1, TWO_NODES);

        assertValueInStorage(
                metaStorageManager,
                zoneDataNodesHistoryKey(zoneId),
                DistributionZoneCausalityDataNodesTest::dataNodesFromValue,
                ONE_NODE_NAME,
                TIMEOUT
        );

        int catalogVersionBeforeDrop = catalogManager.latestCatalogVersion();

        HybridTimestamp dropRevision1 = dropZoneAndGetTimestamp(ZONE_NAME);

        // Check that data nodes value of the zone with the topology update revision is NODE_0 because scale up timer has not fired.
        Set<String> dataNodes3 = distributionZoneManager.dataNodes(topologyRevision2.timestamp, catalogVersionBeforeDrop, zoneId)
                .get(TIMEOUT, MILLISECONDS);
        assertEquals(ONE_NODE_NAME, dataNodes3);

        // Check that zones is removed and attempt to get data nodes throws an exception.
        assertThrowsWithCause(
                () -> distributionZoneManager.dataNodes(dropRevision1, catalogManager.latestCatalogVersion(), zoneId)
                        .get(TIMEOUT, MILLISECONDS),
                DistributionZoneNotFoundException.class
        );
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

        RevWithTimestamp topologyRevision1 = putNodeInLogicalTopologyAndGetTimestamp(NODE_1, TWO_NODES);

        int zoneId = getZoneId(ZONE_NAME);

        // Check that data nodes value of the the zone is NODE_0 and NODE_1.
        Set<String> dataNodes1 = distributionZoneManager
                .dataNodes(topologyRevision1.timestamp, catalogManager.latestCatalogVersion(), zoneId)
                .get(TIMEOUT, MILLISECONDS);
        assertEquals(TWO_NODES_NAMES, dataNodes1);

        // Test steps.

        // Change logical topology. NODE_1 is removed.
        RevWithTimestamp topologyRevision2 = removeNodeInLogicalTopologyAndGetTimestamp(Set.of(NODE_1), ONE_NODE);

        assertValueInStorage(
                metaStorageManager,
                zoneDataNodesHistoryKey(zoneId),
                DistributionZoneCausalityDataNodesTest::dataNodesFromValue,
                TWO_NODES_NAMES,
                TIMEOUT
        );

        int catalogVersionBeforeDrop = catalogManager.latestCatalogVersion();

        HybridTimestamp dropRevision1 = dropZoneAndGetTimestamp(ZONE_NAME);

        // Check that data nodes value of the zone with the topology update revision is NODE_0 and NODE_1
        // because scale down timer has not fired.
        Set<String> dataNodes2 = distributionZoneManager.dataNodes(topologyRevision2.timestamp, catalogVersionBeforeDrop, zoneId)
                .get(TIMEOUT, MILLISECONDS);
        assertEquals(TWO_NODES_NAMES, dataNodes2);

        // Check that zones is removed and attempt to get data nodes throws an exception.
        assertThrowsWithCause(
                () -> distributionZoneManager.dataNodes(dropRevision1, catalogManager.latestCatalogVersion(), zoneId)
                        .get(TIMEOUT, MILLISECONDS),
                DistributionZoneNotFoundException.class
        );
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

        CatalogZoneDescriptor defaultZone = getDefaultZone();
        int defaultZoneId = defaultZone.id();

        // Alter the zone with immediate timers.
        alterZone(defaultZone.name(), IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE, null);

        // Create logical topology with NODE_0.
        // Check that data nodes value of both zone is NODE_0.
        RevWithTimestamp topologyRevision1 = putNodeInLogicalTopologyAndGetTimestamp(NODE_0, ONE_NODE);

        int zoneId = getZoneId(ZONE_NAME);

        Set<String> dataNodes0 = distributionZoneManager
                .dataNodes(topologyRevision1.timestamp, catalogManager.latestCatalogVersion(), defaultZoneId)
                .get(TIMEOUT, MILLISECONDS);
        assertEquals(ONE_NODE_NAME, dataNodes0);

        Set<String> dataNodes1 = distributionZoneManager
                .dataNodes(topologyRevision1.timestamp, catalogManager.latestCatalogVersion(), zoneId)
                .get(TIMEOUT, MILLISECONDS);
        assertEquals(ONE_NODE_NAME, dataNodes1);

        // Alter the zones with infinite timers.
        alterZone(ZONE_NAME, INFINITE_TIMER_VALUE, INFINITE_TIMER_VALUE, null);

        // Alter the zone with infinite timers.
        alterZone(defaultZone.name(), INFINITE_TIMER_VALUE, INFINITE_TIMER_VALUE, null);

        // Test steps.

        String filter = "$[?($..* || @.region == 'US')]";

        // Change filter and get revision of this change.
        HybridTimestamp filterRevision0 = alterFilterAndGetTimestamp(defaultZone.name(), filter);
        HybridTimestamp filterRevision1 = alterFilterAndGetTimestamp(ZONE_NAME, filter);

        // Check that data nodes value of the the zone is NODE_0.
        // The future didn't hang due to the fact that the actual data nodes value did not change.
        Set<String> dataNodes2 = distributionZoneManager
                .dataNodes(filterRevision0, catalogManager.latestCatalogVersion(), defaultZoneId)
                .get(TIMEOUT, MILLISECONDS);
        assertEquals(ONE_NODE_NAME, dataNodes2);

        Set<String> dataNodes3 = distributionZoneManager
                .dataNodes(filterRevision1, catalogManager.latestCatalogVersion(), zoneId)
                .get(TIMEOUT, MILLISECONDS);
        assertEquals(ONE_NODE_NAME, dataNodes3);
    }

    @Test
    void createZoneWithNotImmediateTimers() throws Exception {
        // Prerequisite.

        // Create logical topology with NODE_0.
        topology.putNode(NODE_0);

        // Test steps.

        // Create a zone.
        HybridTimestamp createZoneTs = createZoneAndGetTimestamp(
                ZONE_NAME,
                INFINITE_TIMER_VALUE,
                INFINITE_TIMER_VALUE
        );

        // Check that data nodes value of the zone with the create zone revision is NODE_0.
        Set<String> dataNodes1 = distributionZoneManager.dataNodes(
                createZoneTs,
                catalogManager.latestCatalogVersion(),
                getZoneId(ZONE_NAME)
        ).get(TIMEOUT, MILLISECONDS);

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
        HybridTimestamp createZoneRevision = createZoneAndGetTimestamp(
                ZONE_NAME,
                IMMEDIATE_TIMER_VALUE,
                IMMEDIATE_TIMER_VALUE
        );

        int zoneId = getZoneId(ZONE_NAME);

        // Check that data nodes value of the zone with the revision lower than the create zone revision is absent.
        assertThrowsWithCause(
                () -> distributionZoneManager
                        .dataNodes(createZoneRevision.subtractPhysicalTime(1), catalogManager.latestCatalogVersion() - 1, zoneId)
                        .get(TIMEOUT, MILLISECONDS),
                DistributionZoneNotFoundException.class
        );

        // Check that data nodes value of the zone with the create zone revision is NODE_0 and NODE_1.
        Set<String> dataNodes = distributionZoneManager.dataNodes(
                createZoneRevision,
                catalogManager.latestCatalogVersion(),
                zoneId
        ).get(TIMEOUT, MILLISECONDS);

        assertEquals(TWO_NODES_NAMES, dataNodes);

        // Drop the zone.
        HybridTimestamp dropZoneTs = dropZoneAndGetTimestamp(ZONE_NAME);

        // Check that data nodes value of the zone with the drop zone revision is absent.
        assertThrowsWithCause(
                () -> distributionZoneManager.dataNodes(dropZoneTs, catalogManager.latestCatalogVersion(), zoneId)
                        .get(TIMEOUT, MILLISECONDS),
                DistributionZoneNotFoundException.class
        );
    }

    /** Tests data nodes obtaining with wrong parameters throw an exception. */
    @Test
    void validationTest() {
        int defaultZoneId = getDefaultZone().id();

        assertThrowsWithCause(
                () -> distributionZoneManager
                        .dataNodes(hybridTimestamp(1), catalogManager.latestCatalogVersion(), -1).get(TIMEOUT, MILLISECONDS),
                IllegalArgumentException.class
        );

        assertThrowsWithCause(
                () -> distributionZoneManager.dataNodes(hybridTimestamp(1), -1, defaultZoneId).get(TIMEOUT, MILLISECONDS),
                IllegalArgumentException.class
        );
    }

    /**
     * Tests data nodes changing when topology is changed.
     *
     * @throws Exception If failed.
     */
    @Test
    void simpleTopologyChanges() throws Exception {
        CatalogZoneDescriptor defaultZone = getDefaultZone();
        int defaultZoneId = defaultZone.id();

        // Prerequisite.

        // Create zones with immediate timers.
        alterZone(defaultZone.name(), IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE, null);

        createZone(ZONE_NAME, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE, null);

        // Test steps.
        // Change logical topology. NODE_0 is added.
        RevWithTimestamp topologyRevision0 = putNodeInLogicalTopologyAndGetTimestamp(NODE_0, ONE_NODE);

        // Change logical topology. NODE_1 is added.
        RevWithTimestamp topologyRevision1 = putNodeInLogicalTopologyAndGetTimestamp(NODE_1, TWO_NODES);

        int zoneId = getZoneId(ZONE_NAME);

        Set<String> dataNodes1 = distributionZoneManager
                .dataNodes(topologyRevision0.timestamp, catalogManager.latestCatalogVersion(), defaultZoneId)
                .get(TIMEOUT, MILLISECONDS);
        assertEquals(ONE_NODE_NAME, dataNodes1);
        Set<String> dataNodes2 = distributionZoneManager
                .dataNodes(topologyRevision0.timestamp, catalogManager.latestCatalogVersion(), zoneId)
                .get(TIMEOUT, MILLISECONDS);
        assertEquals(ONE_NODE_NAME, dataNodes2);

        Set<String> dataNodes3 = distributionZoneManager
                .dataNodes(topologyRevision1.timestamp, catalogManager.latestCatalogVersion(), defaultZoneId)
                .get(TIMEOUT, MILLISECONDS);
        assertEquals(TWO_NODES_NAMES, dataNodes3);
        Set<String> dataNodes4 = distributionZoneManager
                .dataNodes(topologyRevision1.timestamp, catalogManager.latestCatalogVersion(), zoneId)
                .get(TIMEOUT, MILLISECONDS);
        assertEquals(TWO_NODES_NAMES, dataNodes4);

        Set<LogicalNode> twoNodes1 = Set.of(NODE_0, NODE_2);
        Set<String> twoNodesNames1 = Set.of(NODE_0.name(), NODE_2.name());

        // Change logical topology. NODE_2 is added.
        RevWithTimestamp topologyRevision2 = putNodeInLogicalTopologyAndGetTimestamp(NODE_2, THREE_NODES);

        // Change logical topology. NODE_1 is left.
        RevWithTimestamp topologyRevision3 = removeNodeInLogicalTopologyAndGetTimestamp(Set.of(NODE_1), twoNodes1);

        assertTrue(waitForCondition(() -> metaStorageManager.appliedRevision() >= topologyRevision3.revision, 1000));

        Set<String> dataNodes5 = distributionZoneManager
                .dataNodes(topologyRevision2.timestamp, catalogManager.latestCatalogVersion(), defaultZoneId)
                .get(TIMEOUT, MILLISECONDS);
        assertEquals(THREE_NODES_NAMES, dataNodes5);
        Set<String> dataNodes6 = distributionZoneManager
                .dataNodes(topologyRevision2.timestamp, catalogManager.latestCatalogVersion(), zoneId)
                .get(TIMEOUT, MILLISECONDS);
        assertEquals(THREE_NODES_NAMES, dataNodes6);

        Set<String> dataNodes7 = distributionZoneManager
                .dataNodes(topologyRevision3.timestamp, catalogManager.latestCatalogVersion(), defaultZoneId)
                .get(TIMEOUT, MILLISECONDS);
        assertEquals(twoNodesNames1, dataNodes7);
        Set<String> dataNodes8 = distributionZoneManager
                .dataNodes(topologyRevision3.timestamp, catalogManager.latestCatalogVersion(), zoneId)
                .get(TIMEOUT, MILLISECONDS);
        assertEquals(twoNodesNames1, dataNodes8);
    }

    /**
     * Check that data nodes calculation is idempotented.
     * The current data nodes value which was calculated by data nodes from the meta storage manager equals
     * to the data nodes value from the data nodes manager and from data nodes history by the corresponding timestamp.
     *
     * @throws Exception If failed.
     */
    @Test
    void checkDataNodesRepeatedOnNodeAdded() throws Exception {
        prepareZonesWithTwoDataNodes();

        Map<Integer, Set<String>> expectedDataNodes = new HashMap<>();
        expectedDataNodes.put(getZoneId(ZONE_NAME), Set.of(NODE_0.name(), NODE_1.name(), NODE_2.name()));
        expectedDataNodes.put(getZoneId(ZONE_NAME_2), Set.of(NODE_0.name(), NODE_1.name()));
        expectedDataNodes.put(getZoneId(ZONE_NAME_3), Set.of(NODE_0.name(), NODE_1.name()));

        Map<Integer, Set<String>> expectedDataNodesOnTopologyUpdateEvent = new HashMap<>(expectedDataNodes);
        expectedDataNodesOnTopologyUpdateEvent.put(getZoneId(ZONE_NAME_4), Set.of(NODE_0.name(), NODE_1.name()));

        Map<Integer, Set<String>> expectedDataNodesAfterTimersAreExpired = new HashMap<>(expectedDataNodes);
        expectedDataNodesAfterTimersAreExpired.put(getZoneId(ZONE_NAME_4), Set.of(NODE_0.name(), NODE_1.name(), NODE_2.name()));

        checkDataNodesRepeated(expectedDataNodesOnTopologyUpdateEvent, expectedDataNodesAfterTimersAreExpired, true);
    }

    /**
     * Check that data nodes calculation is idempotented.
     * The current data nodes value which was calculated by data nodes from the meta storage manager equals
     * to the data nodes value from the data nodes manager and from data nodes history by the corresponding timestamp.
     *
     * @throws Exception If failed.
     */
    @Test
    void checkDataNodesRepeatedOnNodeRemoved() throws Exception {
        prepareZonesWithTwoDataNodes();

        Map<Integer, Set<String>> expectedDataNodes = new HashMap<>();
        expectedDataNodes.put(getDefaultZone().id(), Set.of(NODE_0.name(), NODE_1.name()));
        expectedDataNodes.put(getZoneId(ZONE_NAME), Set.of(NODE_0.name(), NODE_1.name()));
        expectedDataNodes.put(getZoneId(ZONE_NAME_2), Set.of(NODE_0.name()));
        expectedDataNodes.put(getZoneId(ZONE_NAME_3), Set.of(NODE_0.name(), NODE_1.name()));

        Map<Integer, Set<String>> expectedDataNodesOnTopologyUpdateEvent = new HashMap<>(expectedDataNodes);
        expectedDataNodesOnTopologyUpdateEvent.put(getZoneId(ZONE_NAME_4), Set.of(NODE_0.name(), NODE_1.name()));

        Map<Integer, Set<String>> expectedDataNodesAfterTimersAreExpired = new HashMap<>(expectedDataNodes);
        expectedDataNodesAfterTimersAreExpired.put(getZoneId(ZONE_NAME_4), Set.of(NODE_0.name()));

        checkDataNodesRepeated(expectedDataNodesOnTopologyUpdateEvent, expectedDataNodesAfterTimersAreExpired, false);
    }

    /**
     * Tests that Distribution Zone Manager stores logical topology update history.
     */
    @Test
    void testLogicalTopologyUpdateHistoryIsStored() throws Exception {
        createZone(ZONE_NAME, 1, 1, null);

        Set<LogicalNode> newTopology = new HashSet<>();

        newTopology.add(NODE_0);
        RevWithTimestamp topologyRevisionOneAdded = putNodeInLogicalTopologyAndGetTimestamp(NODE_0, newTopology);
        assertTrue(waitForCondition(() -> metaStorageManager.appliedRevision() >= topologyRevisionOneAdded.revision, TIMEOUT));

        newTopology.add(NODE_1);
        RevWithTimestamp topologyRevisionTwoAdded = putNodeInLogicalTopologyAndGetTimestamp(NODE_1, newTopology);
        assertTrue(waitForCondition(() -> metaStorageManager.appliedRevision() >= topologyRevisionTwoAdded.revision, TIMEOUT));

        newTopology.remove(NODE_1);
        RevWithTimestamp topologyRevisionOneRemoved = removeNodeInLogicalTopologyAndGetTimestamp(Set.of(NODE_1), newTopology);
        assertTrue(waitForCondition(() -> metaStorageManager.appliedRevision() >= topologyRevisionOneRemoved.revision, TIMEOUT));

        int zoneId = getZoneId(ZONE_NAME);

        assertDataNodesFromManager(
                distributionZoneManager,
                metaStorageManager::appliedRevision,
                catalogManager::latestCatalogVersion,
                zoneId,
                ONE_NODE,
                TIMEOUT
        );

        assertEquals(ONE_NODE_NAME, nodeNames(distributionZoneManager.logicalTopology(topologyRevisionOneAdded.revision)));
        assertEquals(TWO_NODES_NAMES, nodeNames(distributionZoneManager.logicalTopology(topologyRevisionTwoAdded.revision)));
        assertEquals(ONE_NODE_NAME, nodeNames(distributionZoneManager.logicalTopology(topologyRevisionOneRemoved.revision)));
    }

    /**
     * Tests logical topology param handling.
     */
    @Test
    void testLogicalTopologyParams() throws Exception {
        createZone(ZONE_NAME, 1, 1, null);

        Set<LogicalNode> newTopology = new HashSet<>();

        newTopology.add(NODE_0);
        RevWithTimestamp topologyRevision = putNodeInLogicalTopologyAndGetTimestamp(NODE_0, newTopology);
        assertTrue(waitForCondition(() -> metaStorageManager.appliedRevision() >= topologyRevision.revision, TIMEOUT));

        // topologyRevision is at least 2 as there have been other revision updates.
        assertTrue(topologyRevision.revision > 2);
        // We need this check to make sure that the topology is empty if requested with a revision = (0, topologyRevision-1];

        Set<NodeWithAttributes> topologyBeforeAddingNodes = distributionZoneManager.logicalTopology(topologyRevision.revision - 1);
        assertThat(topologyBeforeAddingNodes, is(empty()));

        // Same for revision = 0.
        Set<NodeWithAttributes> topology0 = distributionZoneManager.logicalTopology(0);
        assertThat(topology0, is(empty()));

        // Exactly one node should be present in the topology at revision = topologyRevision.
        assertEquals(ONE_NODE_NAME, nodeNames(distributionZoneManager.logicalTopology(topologyRevision.revision)));

        // Same topology for the revision > topologyRevision.
        assertEquals(ONE_NODE_NAME, nodeNames(distributionZoneManager.logicalTopology(topologyRevision.revision + 1)));

        // Fails if revision is negative (invalid).
        assertThrows(AssertionError.class, () -> distributionZoneManager.logicalTopology(-1));
    }

    private void checkDataNodesRepeated(
            Map<Integer, Set<String>> expectedDataNodesOnTopologyUpdateEvent,
            Map<Integer, Set<String>> expectedDataNodesAfterTimersAreExpired,
            boolean addNode
    ) throws Exception {
        prepareZonesTimerValuesToTest();

        CountDownLatch latch = new CountDownLatch(1);

        AtomicBoolean reached = new AtomicBoolean();

        AtomicReference<RevWithTimestamp> topologyUpdateRevision = new AtomicReference<>();

        WatchListener testListener = createLogicalTopologyWatchListenerToCheckDataNodes(
                topologyUpdateRevision,
                latch,
                reached,
                expectedDataNodesOnTopologyUpdateEvent
        );

        metaStorageManager.registerPrefixWatch(zonesLogicalTopologyPrefix(), testListener);

        blockDataNodesUpdatesInMetaStorage(latch);

        Set<LogicalNode> newTopology = new HashSet<>();
        newTopology.add(NODE_0);
        newTopology.add(NODE_1);

        RevWithTimestamp topologyRevision;

        log.info("Test: changing topology.");

        if (addNode) {
            newTopology.add(NODE_2);

            // Change logical topology. NODE_2 is added.
            topologyRevision = putNodeInLogicalTopologyAndGetTimestamp(NODE_2, newTopology);
        } else {
            newTopology.remove(NODE_1);

            // Change logical topology. NODE_1 is removed.
            topologyRevision = removeNodeInLogicalTopologyAndGetTimestamp(Set.of(NODE_1), newTopology);
        }

        assertEquals(topologyRevision.timestamp, topologyUpdateRevision.get().timestamp);

        latch.await();

        assertTrue(reached.get());

        // Check that data nodes values are changed in the meta storage after ms invokes updated the meta storage.
        checkThatDataNodesIsChangedInMetastorage(expectedDataNodesAfterTimersAreExpired);

        // Check that data nodes values are idempotent.
        checkDataNodes(topologyUpdateRevision.get().timestamp, expectedDataNodesOnTopologyUpdateEvent);

        // Above, we have waited for data nodes in meta storage to become the same as in expectedDataNodesAfterTimersAreExpired map,
        // so we use the storage revision and check the value of data nodes in data nodes manager.
        HybridTimestamp timestamp = metaStorageManager.timestampByRevisionLocally(keyValueStorage.revision());
        checkDataNodes(timestamp, expectedDataNodesAfterTimersAreExpired);
    }

    /**
     * Added two nodes in topology and assert that data nodes of zones are contains all topology nodes.
     */
    private void prepareZonesWithTwoDataNodes() throws Exception {
        createZone(ZONE_NAME, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE, null);

        createZone(ZONE_NAME_2, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE, null);

        createZone(ZONE_NAME_3, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE, null);

        createZone(ZONE_NAME_4, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE, null);

        // Change logical topology. NODE_0 is added.
        topology.putNode(NODE_0);

        // Change logical topology. NODE_1 is added.
        RevWithTimestamp topologyRevision = putNodeInLogicalTopologyAndGetTimestamp(NODE_1, TWO_NODES);

        Set<Integer> zoneIds = Set.of(
                getZoneId(ZONE_NAME),
                getZoneId(ZONE_NAME_2),
                getZoneId(ZONE_NAME_3),
                getZoneId(ZONE_NAME_4)
        );

        for (Integer zoneId : zoneIds) {
            Set<String> dataNodes = distributionZoneManager
                    .dataNodes(topologyRevision.timestamp, catalogManager.latestCatalogVersion(), zoneId)
                    .get(TIMEOUT, MILLISECONDS);
            assertEquals(TWO_NODES_NAMES, dataNodes, "zoneId=" + zoneId);
        }
    }

    private WatchListener createLogicalTopologyWatchListenerToCheckDataNodes(
            AtomicReference<RevWithTimestamp> topologyUpdateRevision,
            CountDownLatch latch,
            AtomicBoolean reached,
            Map<Integer, Set<String>> expectedDataNodes
    ) {
        return evt -> {
            for (EntryEvent event : evt.entryEvents()) {
                Entry e = event.newEntry();

                if (Arrays.equals(e.key(), zonesLogicalTopologyVersionKey().bytes())) {
                    topologyUpdateRevision.set(new RevWithTimestamp(e.revision(), e.timestamp()));
                }
            }

            assertTrue(topologyUpdateRevision.get().revision > 0);

            log.info("Test: logical topology watch listener triggered, rev={}", evt.revision());

            tempPool = Executors.newFixedThreadPool(1);

            return CompletableFuture.runAsync(() -> {
                try {
                    // Check that data nodes values are changed according to scale up and down timers.
                    // Data nodes from the data nodes manager are used to calculate current data nodes.
                    checkDataNodes(topologyUpdateRevision.get().timestamp, expectedDataNodes);

                    // Check that data nodes values are not changed in the meta storage.
                    checkThatDataNodesIsNotChangedInMetastorage(expectedDataNodes.keySet());

                    log.info("Test: topology checked.");

                    reached.set(true);
                } catch (Exception e) {
                    fail();
                }
            }, tempPool)
            .thenRunAsync(() -> {
                latch.countDown();

                log.info("Test: latch counted down.");
            }, tempPool);
        };
    }

    private void checkThatDataNodesIsNotChangedInMetastorage(Set<Integer> zoneIds) throws Exception {
        for (Integer zoneId : zoneIds) {
            assertValueInStorage(
                    metaStorageManager,
                    zoneDataNodesHistoryKey(zoneId),
                    DistributionZoneCausalityDataNodesTest::dataNodesFromValue,
                    TWO_NODES_NAMES,
                    TIMEOUT
            );
        }
    }

    private static Set<String> dataNodesFromValue(byte[] value) {
        DataNodesHistory history = DataNodesHistorySerializer.deserialize(value);
        DataNodesHistoryEntry latest = history.dataNodesForTimestamp(HybridTimestamp.MAX_VALUE);
        return DistributionZonesUtil.dataNodes(latest.dataNodes()).stream().map(Node::nodeName).collect(toSet());
    }

    /**
     * Check current data nodes.
     */
    private void checkDataNodes(
            HybridTimestamp timestamp,
            Map<Integer, Set<String>> expectedDataNodes
    ) throws Exception {
        for (Map.Entry<Integer, Set<String>> entry : expectedDataNodes.entrySet()) {
            int zoneId = entry.getKey();
            int catalogVersion = catalogManager.latestCatalogVersion();

            assertEquals(
                    entry.getValue(),
                    distributionZoneManager.dataNodes(timestamp, catalogVersion, zoneId)
                            .get(TIMEOUT, MILLISECONDS),
                    "zoneId=" + zoneId + ", ts=" + timestamp + ", catalogVersion=" + catalogVersion
            );
        }
    }

    private void checkThatDataNodesIsChangedInMetastorage(
            Map<Integer, Set<String>> expectedDataNodes
    ) throws Exception {
        for (Map.Entry<Integer, Set<String>> entry : expectedDataNodes.entrySet()) {
            log.info("Test: checking the data zone's data nodes in the meta storage, zoneId={}", entry.getKey());

            assertValueInStorage(
                    metaStorageManager,
                    zoneDataNodesHistoryKey(entry.getKey()),
                    DistributionZoneCausalityDataNodesTest::dataNodesFromValue,
                    entry.getValue(),
                    TIMEOUT
            );
        }
    }

    private void prepareZonesTimerValuesToTest() {
        // The default zone already has immediate scale up and immediate scale down timers.

        // The zone with immediate scale up and infinity scale down timers.
        alterZone(ZONE_NAME, IMMEDIATE_TIMER_VALUE, INFINITE_TIMER_VALUE, null);

        // The zone with infinity scale up and immediate scale down timers.
        alterZone(ZONE_NAME_2, INFINITE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE, null);

        // The zone with infinity scale up and infinity scale down timers.
        alterZone(ZONE_NAME_3, INFINITE_TIMER_VALUE, INFINITE_TIMER_VALUE, null);

        // The zone with 1 second scale up and 1 second scale down timers.
        alterZone(ZONE_NAME_4, 1, 1, null);
    }

    /**
     * Puts a given node as a part of the logical topology and return revision of a topology watch listener event.
     *
     * @param node Node to put.
     * @param expectedTopology Expected topology for future completing.
     * @return Revision.
     * @throws Exception If failed.
     */
    private RevWithTimestamp putNodeInLogicalTopologyAndGetTimestamp(
            LogicalNode node,
            Set<LogicalNode> expectedTopology
    ) throws Exception {
        Set<String> nodeNames = expectedTopology.stream().map(InternalClusterNode::name).collect(toSet());

        CompletableFuture<RevWithTimestamp> revisionFut = new CompletableFuture<>();

        topologyRevisions.put(nodeNames, revisionFut);

        topology.putNode(node);

        RevWithTimestamp rwt = revisionFut.get(TIMEOUT, MILLISECONDS);

        try {
            assertTrue(waitForCondition(() -> metaStorageManager.appliedRevision() >= rwt.revision, TIMEOUT));
        } catch (AssertionError e) {
            log.info("Failed to wait for revision: appliedRevision={}, rwt={}", metaStorageManager.appliedRevision(), rwt.revision);

            throw e;
        }

        return rwt;
    }

    /**
     * Removes given nodes from the logical topology and return revision of a topology watch listener event.
     *
     * @param nodes Nodes to remove.
     * @param expectedTopology Expected topology for future completing.
     * @return Revision.
     * @throws Exception If failed.
     */
    private RevWithTimestamp removeNodeInLogicalTopologyAndGetTimestamp(
            Set<LogicalNode> nodes,
            Set<LogicalNode> expectedTopology
    ) throws Exception {
        Set<String> nodeNames = expectedTopology.stream().map(InternalClusterNode::name).collect(toSet());

        CompletableFuture<RevWithTimestamp> revisionFut = new CompletableFuture<>();

        topologyRevisions.put(nodeNames, revisionFut);

        topology.removeNodes(nodes);

        RevWithTimestamp rwt = revisionFut.get(TIMEOUT, MILLISECONDS);

        assertTrue(waitForCondition(() -> metaStorageManager.appliedRevision() >= rwt.revision, TIMEOUT));

        return rwt;
    }

    /**
     * Changes data nodes in logical topology and return revision of a topology watch listener event.
     *
     * @param nodes Nodes to remove.
     * @return Revision.
     * @throws Exception If failed.
     */
    private RevWithTimestamp fireTopologyLeapAndGetTimestamp(Set<LogicalNode> nodes) throws Exception {
        Set<String> nodeNames = nodes.stream().map(InternalClusterNode::name).collect(toSet());

        CompletableFuture<RevWithTimestamp> revisionFut = new CompletableFuture<>();

        topologyRevisions.put(nodeNames, revisionFut);

        long topVer = topology.getLogicalTopology().version() + 1;

        clusterStateStorage.put(
                LOGICAL_TOPOLOGY_KEY,
                VersionedSerialization.toBytes(
                        new LogicalTopologySnapshot(topVer, nodes),
                        LogicalTopologySnapshotSerializer.INSTANCE
                )
        );

        topology.fireTopologyLeap();

        RevWithTimestamp rwt = revisionFut.get(TIMEOUT, MILLISECONDS);

        assertTrue(waitForCondition(() -> metaStorageManager.appliedRevision() >= rwt.revision, TIMEOUT));

        return rwt;

    }

    /**
     * Changes a scale up timer value of a zone and return the timestamp of a zone update event.
     *
     * @param zoneName Zone name.
     * @param scaleUp New scale up value.
     * @return Timestamp.
     * @throws Exception If failed.
     */
    private HybridTimestamp alterZoneScaleUpAndGetTimestamp(String zoneName, int scaleUp) throws Exception {
        CompletableFuture<HybridTimestamp> tsFut = new CompletableFuture<>();

        int zoneId = getZoneId(zoneName);

        zoneScaleUpTimestamps.put(zoneId, tsFut);

        alterZone(zoneName, scaleUp, null, null);

        return tsFut.get(TIMEOUT, MILLISECONDS);
    }

    /**
     * Changes a scale down timer value of a zone and return the timestamp of a zone update event.
     *
     * @param zoneName Zone name.
     * @param scaleDown New scale down value.
     * @return Timestamp.
     * @throws Exception If failed.
     */
    private HybridTimestamp alterZoneScaleDownAndGetTimestamp(String zoneName, int scaleDown) throws Exception {
        CompletableFuture<HybridTimestamp> tsFut = new CompletableFuture<>();

        int zoneId = getZoneId(zoneName);

        zoneScaleDownTimestamps.put(zoneId, tsFut);

        alterZone(zoneName, null, scaleDown, null);

        return tsFut.get(TIMEOUT, MILLISECONDS);
    }

    /**
     * Changes a filter value of a zone and return the timestamp of a zone update event.
     *
     * @param zoneName Zone name.
     * @param filter New filter value.
     * @return Timestamp.
     * @throws Exception If failed.
     */
    private HybridTimestamp alterFilterAndGetTimestamp(String zoneName, String filter) throws Exception {
        CompletableFuture<HybridTimestamp> tsFut = new CompletableFuture<>();

        int zoneId = getZoneId(zoneName);

        zoneChangeFilterTimestamps.put(zoneId, tsFut);

        alterZone(zoneName, null, null, filter);

        return tsFut.get(TIMEOUT, MILLISECONDS);
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
    private HybridTimestamp createZoneAndGetTimestamp(String zoneName, int scaleUp, int scaleDown) throws Exception {
        CompletableFuture<HybridTimestamp> tsFut = new CompletableFuture<>();

        createZoneTimestamps.put(zoneName, tsFut);

        createZone(zoneName, scaleUp, scaleDown, null);

        return tsFut.get(TIMEOUT, MILLISECONDS);
    }

    /**
     * Drops a zone and return the timestamp of a drop zone event.
     *
     * @param zoneName Zone name.
     * @return Timestamp.
     * @throws Exception If failed.
     */
    private HybridTimestamp dropZoneAndGetTimestamp(String zoneName) throws Exception {
        CompletableFuture<HybridTimestamp> tsFut = new CompletableFuture<>();

        int zoneId = getZoneId(ZONE_NAME);

        dropZoneTimestamps.put(zoneId, tsFut);

        dropZone(zoneName);

        return tsFut.get(TIMEOUT, MILLISECONDS);
    }

    /**
     * Returns a future which will be completed when expected data nodes will be saved to the meta storage.
     * In order to complete the future need to invoke one of the methods that change the logical topology.
     *
     * @param zoneId Zone id.
     * @param nodes Expected data nodes.
     * @return Future with timestamp.
     */
    private CompletableFuture<HybridTimestamp> getZoneDataNodesTimestamp(int zoneId, Set<LogicalNode> nodes) {
        Set<String> nodeNames = nodes.stream().map(InternalClusterNode::name).collect(toSet());

        CompletableFuture<HybridTimestamp> revisionFut = new CompletableFuture<>();

        zoneDataNodesTimestamps.put(new IgniteBiTuple<>(zoneId, nodeNames), revisionFut);

        return revisionFut;
    }

    /**
     * Creates a topology watch listener which completes futures from {@code topologyRevisions}
     * when receives event with expected logical topology.
     *
     * @return Watch listener.
     */
    private WatchListener createMetastorageTopologyListener() {
        return evt -> {
            Set<NodeWithAttributes> newLogicalTopology = null;

            long revision = 0;
            HybridTimestamp timestamp = HybridTimestamp.MIN_VALUE;

            for (EntryEvent event : evt.entryEvents()) {
                Entry e = event.newEntry();

                if (Arrays.equals(e.key(), zonesLogicalTopologyVersionKey().bytes())) {
                    revision = e.revision();
                    timestamp = e.timestamp();
                } else if (Arrays.equals(e.key(), zonesLogicalTopologyKey().bytes())) {
                    newLogicalTopology = deserializeLogicalTopologySet(e.value());
                }
            }

            Set<String> nodeNames = nodeNames(newLogicalTopology);

            if (topologyRevisions.containsKey(nodeNames)) {
                topologyRevisions.remove(nodeNames).complete(new RevWithTimestamp(revision, timestamp));
                log.info("Test: Topology update event, rev=" + revision);
            }

            return nullCompletedFuture();
        };
    }

    private static Set<String> nodeNames(Set<NodeWithAttributes> newLogicalTopology) {
        return newLogicalTopology.stream().map(NodeWithAttributes::nodeName).collect(toSet());
    }

    /**
     * Creates a data nodes watch listener which completes futures from {@code zoneDataNodesRevisions}
     * when receives event with expected data nodes.
     *
     * @return Watch listener.
     */
    private WatchListener createMetastorageDataNodesListener() {
        return evt -> {

            int zoneId = 0;

            Set<Node> newDataNodes = null;

            HybridTimestamp timestamp = HybridTimestamp.MIN_VALUE;

            for (EntryEvent event : evt.entryEvents()) {
                Entry e = event.newEntry();

                if (startsWith(e.key(), DISTRIBUTION_ZONE_DATA_NODES_HISTORY_PREFIX_BYTES)) {
                    timestamp = e.timestamp();

                    zoneId = extractZoneId(e.key(), DISTRIBUTION_ZONE_DATA_NODES_HISTORY_PREFIX_BYTES);

                    byte[] dataNodesBytes = e.value();

                    if (dataNodesBytes != null) {
                        DataNodesHistory history = DataNodesHistorySerializer.deserialize(dataNodesBytes);
                        Set<NodeWithAttributes> nwa = history.dataNodesForTimestamp(HybridTimestamp.MAX_VALUE).dataNodes();
                        newDataNodes = nwa.stream().map(NodeWithAttributes::node).collect(toSet());
                    } else {
                        newDataNodes = emptySet();
                    }
                }
            }

            Set<String> nodeNames = newDataNodes.stream().map(Node::nodeName).collect(toSet());

            IgniteBiTuple<Integer, Set<String>> zoneDataNodesKey = new IgniteBiTuple<>(zoneId, nodeNames);

            completeTimestampFuture(zoneDataNodesTimestamps.remove(zoneDataNodesKey), timestamp);

            return nullCompletedFuture();
        };
    }

    private void addCatalogZoneEventListeners() {
        catalogManager.listen(ZONE_CREATE, parameters -> {
            String zoneName = ((CreateZoneEventParameters) parameters).zoneDescriptor().name();

            HybridTimestamp timestamp = metaStorageManager.timestampByRevisionLocally(parameters.causalityToken());

            completeTimestampFuture(createZoneTimestamps.remove(zoneName), timestamp);

            return falseCompletedFuture();
        });

        catalogManager.listen(ZONE_DROP, parameters -> {
            HybridTimestamp timestamp = metaStorageManager.timestampByRevisionLocally(parameters.causalityToken());

            completeTimestampFuture(dropZoneTimestamps.remove(((DropZoneEventParameters) parameters).zoneId()), timestamp);

            return falseCompletedFuture();
        });

        catalogManager.listen(ZONE_ALTER, new CatalogAlterZoneEventListener(catalogManager) {
            @Override
            protected CompletableFuture<Void> onAutoAdjustScaleUpUpdate(AlterZoneEventParameters parameters, int oldAutoAdjustScaleUp) {
                HybridTimestamp timestamp = metaStorageManager.timestampByRevisionLocally(parameters.causalityToken());
                completeTimestampFuture(zoneScaleUpTimestamps.remove(parameters.zoneDescriptor().id()), timestamp);

                return nullCompletedFuture();
            }

            @Override
            protected CompletableFuture<Void> onAutoAdjustScaleDownUpdate(AlterZoneEventParameters parameters, int oldAutoAdjustScaleDown) {
                HybridTimestamp timestamp = metaStorageManager.timestampByRevisionLocally(parameters.causalityToken());
                completeTimestampFuture(zoneScaleDownTimestamps.remove(parameters.zoneDescriptor().id()), timestamp);

                return nullCompletedFuture();
            }

            @Override
            protected CompletableFuture<Void> onFilterUpdate(AlterZoneEventParameters parameters, String oldFilter) {
                HybridTimestamp timestamp = metaStorageManager.timestampByRevisionLocally(parameters.causalityToken());
                completeTimestampFuture(zoneChangeFilterTimestamps.remove(parameters.zoneDescriptor().id()), timestamp);

                return nullCompletedFuture();
            }
        });
    }

    private static void completeTimestampFuture(@Nullable CompletableFuture<HybridTimestamp> revisionFuture, HybridTimestamp timestamp) {
        if (revisionFuture != null) {
            revisionFuture.complete(timestamp);
        }
    }

    private void blockDataNodesUpdatesInMetaStorage(CountDownLatch latch) {
        doAnswer((Answer<CompletableFuture<Void>>) invocation -> CompletableFuture.runAsync(() -> {
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).thenCompose(v -> {
            try {
                return (CompletableFuture<Void>) invocation.callRealMethod();
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        })).when(metaStorageManager).invoke(argThat(iif -> {
            If iif1 = MetaStorageWriteHandler.toIf(iif);

            byte[] zoneDataNodes = DISTRIBUTION_ZONE_DATA_NODES_HISTORY_PREFIX.getBytes(UTF_8);

            return iif1.andThen().update().operations().stream().anyMatch(op -> startsWith(toByteArray(op.key()), zoneDataNodes));
        }));
    }

    private static class RevWithTimestamp {
        final long revision;
        final HybridTimestamp timestamp;

        public RevWithTimestamp(long revision, HybridTimestamp timestamp) {
            this.revision = revision;
            this.timestamp = timestamp;
        }
    }
}
