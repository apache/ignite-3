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
import static java.util.Collections.emptySet;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_ZONE_ID;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_ZONE_NAME;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.deployWatchesAndUpdateMetaStorageRevision;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.setLogicalTopologyInMetaStorage;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleDownChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleUpChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyVersionKey;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedIn;
import static org.apache.ignite.internal.util.ByteUtils.longToBytes;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.distributionzones.DistributionZoneConfigurationParameters.Builder;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneConfigurationSchema;
import org.apache.ignite.internal.distributionzones.exception.DistributionZoneWasRemovedException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.server.If;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.DistributionZoneNotFoundException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests awaiting data nodes algorithm in distribution zone manager in case when
 * {@link DistributionZoneConfigurationSchema#dataNodesAutoAdjustScaleUp} or
 * {@link DistributionZoneConfigurationSchema#dataNodesAutoAdjustScaleDown} are immediate.
 */
@ExtendWith(ConfigurationExtension.class)
public class DistributionZoneAwaitDataNodesTest extends BaseDistributionZoneManagerTest {
    private static final IgniteLogger LOG = Loggers.forClass(DistributionZoneAwaitDataNodesTest.class);

    private static final String ZONE_NAME_0 = "zone0";

    private static final String ZONE_NAME_1 = "zone1";

    private static final String ZONE_NAME_2 = "zone2";

    private static final LogicalNode NODE_0 = new LogicalNode("node0", "node0", new NetworkAddress("localhost", 123));

    private static final LogicalNode NODE_1 = new LogicalNode("node1", "node1", new NetworkAddress("localhost", 123));

    private static final LogicalNode NODE_2 = new LogicalNode("node2", "node2", new NetworkAddress("localhost", 123));

    /**
     * This test invokes {@link DistributionZoneManager#topologyVersionedDataNodes(int, long)} with default and non-default zone id and
     * different logical topology versions. Simulates new logical topology with new nodes and with removed nodes. Check that data nodes
     * futures are completed in right order.
     */
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-19288")
    @Test
    void testSeveralScaleUpAndSeveralScaleDownThenScaleUpAndScaleDown() throws Exception {
        startZoneManager();

        int zoneId0 = distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder(ZONE_NAME_0)
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);
        int zoneId1 = distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder(ZONE_NAME_1)
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        LOG.info("Topology with added nodes.");

        CompletableFuture<Set<String>> dataNodesUpFut0 = distributionZoneManager.topologyVersionedDataNodes(DEFAULT_ZONE_ID, 1);
        CompletableFuture<Set<String>> dataNodesUpFut1 = distributionZoneManager.topologyVersionedDataNodes(DEFAULT_ZONE_ID, 1);
        CompletableFuture<Set<String>> dataNodesUpFut2 = distributionZoneManager.topologyVersionedDataNodes(DEFAULT_ZONE_ID, 2);
        CompletableFuture<Set<String>> dataNodesUpFut3 = distributionZoneManager.topologyVersionedDataNodes(DEFAULT_ZONE_ID, 11);
        CompletableFuture<Set<String>> dataNodesUpFut4 = distributionZoneManager.topologyVersionedDataNodes(zoneId0, 1);
        CompletableFuture<Set<String>> dataNodesUpFut5 = distributionZoneManager.topologyVersionedDataNodes(zoneId0, 2);
        CompletableFuture<Set<String>> dataNodesUpFut6 = distributionZoneManager.topologyVersionedDataNodes(zoneId1, 1);
        CompletableFuture<Set<String>> dataNodesUpFut7 = distributionZoneManager.topologyVersionedDataNodes(zoneId1, 2);

        int topVer0 = 2;

        Set<LogicalNode> threeNodes = Set.of(NODE_0, NODE_1, NODE_2);
        Set<String> threeNodesNames = Set.of(NODE_0.name(), NODE_1.name(), NODE_2.name());

        setLogicalTopologyInMetaStorage(threeNodes, topVer0, metaStorageManager);

        assertEquals(threeNodesNames, dataNodesUpFut0.get(5, SECONDS));
        assertEquals(threeNodesNames, dataNodesUpFut1.get(3, SECONDS));
        assertEquals(threeNodesNames, dataNodesUpFut2.get(3, SECONDS));

        assertEquals(threeNodesNames, dataNodesUpFut4.get(3, SECONDS));
        assertEquals(threeNodesNames, dataNodesUpFut5.get(3, SECONDS));
        assertEquals(threeNodesNames, dataNodesUpFut6.get(3, SECONDS));
        assertEquals(threeNodesNames, dataNodesUpFut7.get(3, SECONDS));
        assertFalse(dataNodesUpFut3.isDone());

        LOG.info("Topology with removed nodes.");

        CompletableFuture<Set<String>> dataNodesDownFut0 = distributionZoneManager.topologyVersionedDataNodes(DEFAULT_ZONE_ID, 4);
        CompletableFuture<Set<String>> dataNodesDownFut1 = distributionZoneManager.topologyVersionedDataNodes(DEFAULT_ZONE_ID, 4);
        CompletableFuture<Set<String>> dataNodesDownFut2 = distributionZoneManager.topologyVersionedDataNodes(DEFAULT_ZONE_ID, 5);
        CompletableFuture<Set<String>> dataNodesDownFut3 = distributionZoneManager.topologyVersionedDataNodes(DEFAULT_ZONE_ID, 6);
        CompletableFuture<Set<String>> dataNodesDownFut4 = distributionZoneManager.topologyVersionedDataNodes(zoneId0, 4);
        CompletableFuture<Set<String>> dataNodesDownFut5 = distributionZoneManager.topologyVersionedDataNodes(zoneId0, 5);
        CompletableFuture<Set<String>> dataNodesDownFut6 = distributionZoneManager.topologyVersionedDataNodes(zoneId1, 4);
        CompletableFuture<Set<String>> dataNodesDownFut7 = distributionZoneManager.topologyVersionedDataNodes(zoneId1, 5);

        int topVer1 = 5;

        Set<LogicalNode> twoNodes = Set.of(NODE_0, NODE_1);
        Set<String> twoNodesNames = Set.of(NODE_0.name(), NODE_1.name());

        setLogicalTopologyInMetaStorage(twoNodes, topVer1, metaStorageManager);

        assertEquals(twoNodesNames, dataNodesDownFut0.get(3, SECONDS));
        assertEquals(twoNodesNames, dataNodesDownFut1.get(3, SECONDS));
        assertEquals(twoNodesNames, dataNodesDownFut2.get(3, SECONDS));
        assertEquals(twoNodesNames, dataNodesDownFut4.get(3, SECONDS));
        assertEquals(twoNodesNames, dataNodesDownFut5.get(3, SECONDS));
        assertEquals(twoNodesNames, dataNodesDownFut6.get(3, SECONDS));
        assertEquals(twoNodesNames, dataNodesDownFut7.get(3, SECONDS));
        assertFalse(dataNodesDownFut3.isDone());

        int topVer2 = 20;

        LOG.info("Topology with added and removed nodes.");

        Set<LogicalNode> dataNodes = Set.of(NODE_0, NODE_1);
        Set<String> dataNodesNames = Set.of(NODE_0.name(), NODE_1.name());

        setLogicalTopologyInMetaStorage(dataNodes, topVer2, metaStorageManager);

        assertEquals(dataNodesNames, dataNodesUpFut3.get(3, SECONDS));
        assertEquals(dataNodesNames, dataNodesDownFut3.get(3, SECONDS));
    }

    /**
     * Test checks that data nodes futures are completed on topology with added nodes.
     */
    @Test
    void testScaleUpAndThenScaleDown() throws Exception {
        startZoneManager();

        CompletableFuture<Set<String>> dataNodesFut = distributionZoneManager.topologyVersionedDataNodes(DEFAULT_ZONE_ID, 5);

        assertFalse(dataNodesFut.isDone());

        long topVer = 100;

        Set<LogicalNode> dataNodes = Set.of(NODE_0, NODE_1);
        Set<String> dataNodesNames = Set.of(NODE_0.name(), NODE_1.name());

        setLogicalTopologyInMetaStorage(dataNodes, topVer, metaStorageManager);

        assertFalse(dataNodesFut.isDone());

        assertEquals(dataNodesNames, dataNodesFut.get(3, SECONDS));

        dataNodesFut = distributionZoneManager.topologyVersionedDataNodes(DEFAULT_ZONE_ID, 106);

        Set<LogicalNode> dataNodes1 = Set.of(NODE_0);
        Set<String> dataNodesNames1 = Set.of(NODE_0.name());

        setLogicalTopologyInMetaStorage(dataNodes1, topVer + 100, metaStorageManager);

        assertFalse(dataNodesFut.isDone());

        assertEquals(dataNodesNames1, dataNodesFut.get(3, SECONDS));
    }

    /**
     * Test checks that data nodes futures are completed on topology with added and removed nodes for the zone with
     * dataNodesAutoAdjustScaleUp is immediate and dataNodesAutoAdjustScaleDown is non-zero.
     */
    @Test
    void testAwaitingScaleUpOnly() throws Exception {
        startZoneManager();

        distributionZoneManager.alterZone(DEFAULT_ZONE_NAME, new DistributionZoneConfigurationParameters.Builder(DEFAULT_ZONE_NAME)
                        .dataNodesAutoAdjustScaleUp(INFINITE_TIMER_VALUE).dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE).build())
                .get(3, SECONDS);

        int zoneId = distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder(ZONE_NAME_1)
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        CompletableFuture<Set<String>> dataNodesFut = distributionZoneManager.topologyVersionedDataNodes(zoneId, 1);

        Set<LogicalNode> nodes = Set.of(NODE_0, NODE_1);
        Set<String> nodesNames = Set.of(NODE_0.name(), NODE_1.name());

        setLogicalTopologyInMetaStorage(nodes, 1, metaStorageManager);

        assertEquals(nodesNames, dataNodesFut.get(3, SECONDS));

        dataNodesFut = distributionZoneManager.topologyVersionedDataNodes(zoneId, 2);

        assertFalse(dataNodesFut.isDone());

        setLogicalTopologyInMetaStorage(Set.of(NODE_0), 2, metaStorageManager);

        assertEquals(nodesNames, dataNodesFut.get(3, SECONDS));
    }

    /**
     * Test checks that data nodes futures are completed on topology with added and removed nodes for the zone with
     * dataNodesAutoAdjustScaleUp is non-zero and dataNodesAutoAdjustScaleDown is immediate. And checks that other zones non-zero timers
     * doesn't affect.
     */
    @Test
    void testAwaitingScaleDownOnly() throws Exception {
        startZoneManager();

        distributionZoneManager.alterZone(DEFAULT_ZONE_NAME, new DistributionZoneConfigurationParameters.Builder(DEFAULT_ZONE_NAME)
                        .dataNodesAutoAdjustScaleUp(INFINITE_TIMER_VALUE).dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE).build())
                .get(3, SECONDS);

        int zoneId0 = distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder(ZONE_NAME_0)
                                .dataNodesAutoAdjustScaleUp(INFINITE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        int zoneId1 = distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder(ZONE_NAME_1)
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        int zoneId2 = distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder(ZONE_NAME_2)
                                .dataNodesAutoAdjustScaleUp(INFINITE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        CompletableFuture<Set<String>> dataNodesFut = distributionZoneManager.topologyVersionedDataNodes(zoneId1, 1);

        Set<LogicalNode> nodes0 = Set.of(NODE_0, NODE_1);

        setLogicalTopologyInMetaStorage(nodes0, 1, metaStorageManager);

        dataNodesFut.get(3, SECONDS);

        CompletableFuture<Set<String>> dataNodesFut1Zone0 = distributionZoneManager.topologyVersionedDataNodes(zoneId0, 2);
        CompletableFuture<Set<String>> dataNodesFut1 = distributionZoneManager.topologyVersionedDataNodes(zoneId1, 2);
        CompletableFuture<Set<String>> dataNodesFut1Zone2 = distributionZoneManager.topologyVersionedDataNodes(zoneId2, 2);

        assertFalse(dataNodesFut1Zone0.isDone());
        assertFalse(dataNodesFut1.isDone());
        assertFalse(dataNodesFut1Zone2.isDone());

        Set<LogicalNode> nodes1 = Set.of(NODE_0);
        Set<String> nodesNames1 = Set.of(NODE_0.name());

        distributionZoneManager.alterZone(ZONE_NAME_1, new DistributionZoneConfigurationParameters.Builder(ZONE_NAME_1)
                        .dataNodesAutoAdjustScaleUp(INFINITE_TIMER_VALUE).dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE).build())
                .get(3, SECONDS);

        setLogicalTopologyInMetaStorage(nodes1, 2, metaStorageManager);

        assertEquals(nodesNames1, dataNodesFut1.get(3, SECONDS));

        CompletableFuture<Set<String>> dataNodesFut2 = distributionZoneManager.topologyVersionedDataNodes(zoneId1, 3);

        Set<LogicalNode> nodes2 = Set.of(NODE_0, NODE_1);

        assertFalse(dataNodesFut2.isDone());

        setLogicalTopologyInMetaStorage(nodes2, 3, metaStorageManager);

        assertEquals(nodesNames1, dataNodesFut2.get(3, SECONDS));
    }

    /**
     * Test checks that data nodes futures are completed immediately for the zone with dataNodesAutoAdjustScaleUp is non-zero and
     * dataNodesAutoAdjustScaleDown is non-zero.
     */
    @Test
    void testWithOutAwaiting() throws Exception {
        startZoneManager();

        distributionZoneManager.alterZone(DEFAULT_ZONE_NAME, new DistributionZoneConfigurationParameters.Builder(DEFAULT_ZONE_NAME)
                        .dataNodesAutoAdjustScaleUp(INFINITE_TIMER_VALUE).dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE).build())
                .get(3, SECONDS);

        int zoneId = distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder(ZONE_NAME_1)
                                .dataNodesAutoAdjustScaleUp(INFINITE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        CompletableFuture<Set<String>> dataNodesFut = distributionZoneManager.topologyVersionedDataNodes(zoneId, 1);

        assertFalse(dataNodesFut.isDone());

        Set<LogicalNode> nodes0 = Set.of(NODE_0, NODE_1);

        setLogicalTopologyInMetaStorage(nodes0, 1, metaStorageManager);

        assertEquals(emptySet(), dataNodesFut.get(3, SECONDS));
    }

    /**
     * Test checks that data nodes futures are completed exceptionally if the zone was removed while data nodes awaiting.
     */
    @Test
    void testRemoveZoneWhileAwaitingDataNodes() throws Exception {
        startZoneManager();

        int zoneId = distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder(ZONE_NAME_0)
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        CompletableFuture<Set<String>> dataNodesFut0 = distributionZoneManager.topologyVersionedDataNodes(zoneId, 5);

        doAnswer(invocation -> {
            If iif = invocation.getArgument(0);

            byte[] key = zoneScaleUpChangeTriggerKey(zoneId).bytes();

            if (Arrays.stream(iif.cond().keys()).anyMatch(k -> Arrays.equals(key, k))) {
                //Drop the zone while dataNodesFut1 is awaiting a data nodes update.
                assertThat(distributionZoneManager.dropZone(ZONE_NAME_0), willSucceedIn(3, SECONDS));
            }

            return invocation.callRealMethod();
        }).when(keyValueStorage).invoke(any(), any());

        setLogicalTopologyInMetaStorage(Set.of(NODE_0), 200, metaStorageManager);

        assertFalse(dataNodesFut0.isDone());

        assertThrowsWithCause(() -> dataNodesFut0.get(3, SECONDS), DistributionZoneWasRemovedException.class);
    }

    /**
     * Test checks that data nodes futures are completed exceptionally if the zone was removed while data nodes awaiting.
     */
    @Test
    void testRemoveZoneWhileAwaitingTopologyVersion() throws Exception {
        startZoneManager();

        int zoneId = distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder(ZONE_NAME_0)
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        CompletableFuture<Set<String>> dataNodesFut0 = distributionZoneManager.topologyVersionedDataNodes(zoneId, 5);

        assertThat(distributionZoneManager.dropZone(ZONE_NAME_0), willSucceedIn(3, SECONDS));

        setLogicalTopologyInMetaStorage(Set.of(NODE_0), 200, metaStorageManager);

        assertFalse(dataNodesFut0.isDone());

        assertThrowsWithCause(() -> dataNodesFut0.get(3, SECONDS), DistributionZoneNotFoundException.class);
    }

    /**
     * Test checks that data nodes futures are completed with old data nodes if dataNodesAutoAdjustScaleUp and dataNodesAutoAdjustScaleDown
     * timer increased to non-zero value.
     */
    @Test
    void testScaleUpScaleDownAreChangedWhileAwaitingDataNodes() throws Exception {
        startZoneManager();

        Set<LogicalNode> nodes0 = Set.of(NODE_0, NODE_1);
        Set<String> nodesNames0 = Set.of(NODE_0.name(), NODE_1.name());

        setLogicalTopologyInMetaStorage(nodes0, 1, metaStorageManager);

        CompletableFuture<Set<String>> dataNodesFut0 = distributionZoneManager.topologyVersionedDataNodes(DEFAULT_ZONE_ID, 1);

        assertEquals(nodesNames0, dataNodesFut0.get(3, SECONDS));

        Set<LogicalNode> nodes1 = Set.of(NODE_0, NODE_2);

        CompletableFuture<Set<String>> dataNodesFut1 = distributionZoneManager.topologyVersionedDataNodes(DEFAULT_ZONE_ID, 2);

        AtomicInteger scaleUpCount = new AtomicInteger();
        AtomicInteger scaleDownCount = new AtomicInteger();

        CountDownLatch scaleUpLatch = new CountDownLatch(1);
        CountDownLatch scaleDownLatch = new CountDownLatch(1);

        doAnswer(invocation -> {
            If iif = invocation.getArgument(0);

            byte[] scaleUpKey = zoneScaleUpChangeTriggerKey(DEFAULT_ZONE_ID).bytes();
            byte[] scaleDownKey = zoneScaleDownChangeTriggerKey(DEFAULT_ZONE_ID).bytes();

            if (iif.andThen().update().operations().stream().anyMatch(op -> Arrays.equals(scaleUpKey, op.key()))
                    && scaleUpCount.getAndIncrement() == 0) {
                distributionZoneManager.alterZone(DEFAULT_ZONE_NAME, new Builder(DEFAULT_ZONE_NAME)
                                .dataNodesAutoAdjustScaleUp(1000).build())
                        .get(3, SECONDS);

                scaleUpLatch.await(5, SECONDS);
            }

            if (iif.andThen().update().operations().stream().anyMatch(op -> Arrays.equals(scaleDownKey, op.key()))
                    && scaleDownCount.getAndIncrement() == 0) {
                distributionZoneManager.alterZone(DEFAULT_ZONE_NAME, new Builder(DEFAULT_ZONE_NAME)
                                .dataNodesAutoAdjustScaleDown(1000).build())
                        .get(3, SECONDS);

                scaleDownLatch.await(5, SECONDS);
            }

            return invocation.callRealMethod();
        }).when(keyValueStorage).invoke(any(), any());

        setLogicalTopologyInMetaStorage(nodes1, 2, metaStorageManager);

        assertEquals(nodes0.stream().map(ClusterNode::name).collect(Collectors.toSet()), dataNodesFut1.get(5, SECONDS));

        scaleUpLatch.countDown();
        scaleDownLatch.countDown();
    }

    /**
     * Test checks that data nodes are initialized on zone manager start.
     */
    @Test
    void testInitializedDataNodesOnZoneManagerStart() throws Exception {
        Set<String> dataNodes0 = Set.of("node0", "node1");

        Set<NodeWithAttributes> dataNodes = Set.of(new NodeWithAttributes("node0", "id_node0", emptyMap()),
                new NodeWithAttributes("node1", "id_node1", emptyMap()));

        Map<ByteArray, byte[]> valEntries = new HashMap<>();

        valEntries.put(zonesLogicalTopologyKey(), toBytes(dataNodes));
        valEntries.put(zonesLogicalTopologyVersionKey(), longToBytes(3));

        assertThat(vaultMgr.putAll(valEntries), willCompleteSuccessfully());

        topology.putNode(new LogicalNode(new ClusterNode("id_node0", "node0", new NetworkAddress("local", 1))));
        topology.putNode(new LogicalNode(new ClusterNode("id_node1", "node1", new NetworkAddress("local", 1))));

        startZoneManager();

        assertEquals(dataNodes0, distributionZoneManager.topologyVersionedDataNodes(DEFAULT_ZONE_ID, 2)
                .get(3, SECONDS));
    }

    private void startZoneManager() throws Exception {
        deployWatchesAndUpdateMetaStorageRevision(metaStorageManager);

        distributionZoneManager.start();

        distributionZoneManager.alterZone(
                        DEFAULT_ZONE_NAME, new DistributionZoneConfigurationParameters.Builder(DEFAULT_ZONE_NAME)
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE).build())
                .get(3, SECONDS);
    }
}
