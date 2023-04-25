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
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_ZONE_ID;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_ZONE_NAME;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyVersionKey;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.ByteUtils.longToBytes;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneConfigurationSchema;
import org.apache.ignite.internal.distributionzones.exception.DistributionZoneWasRemovedException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.dsl.Conditions;
import org.apache.ignite.internal.metastorage.dsl.Operations;
import org.apache.ignite.lang.ByteArray;
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

    /**
     * This test invokes {@link DistributionZoneManager#topologyVersionedDataNodes(int, long)} with default and non-default zone id and
     * different logical topology versions. Simulates new logical topology with new nodes and with removed nodes. Check that data nodes
     * futures are completed in right order.
     */
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-19288")
    @Test
    void testSeveralScaleUpAndSeveralScaleDownThenScaleUpAndScaleDown() throws Exception {
        startZoneManager();

        distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder("zone0")
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);
        distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder("zone1")
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        int zoneId0 = distributionZoneManager.getZoneId("zone0");
        int zoneId1 = distributionZoneManager.getZoneId("zone1");

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

        Set<String> threeNodes = Set.of("node0", "node1", "node2");

        setLogicalTopologyInMetaStorage(threeNodes, topVer0);

        assertEquals(threeNodes, dataNodesUpFut0.get(3, SECONDS));
        assertEquals(threeNodes, dataNodesUpFut1.get(3, SECONDS));
        assertEquals(threeNodes, dataNodesUpFut2.get(3, SECONDS));

        assertEquals(threeNodes, dataNodesUpFut4.get(3, SECONDS));
        assertEquals(threeNodes, dataNodesUpFut5.get(3, SECONDS));
        assertEquals(threeNodes, dataNodesUpFut6.get(3, SECONDS));
        assertEquals(threeNodes, dataNodesUpFut7.get(3, SECONDS));
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

        Set<String> twoNodes = Set.of("node0", "node1");

        setLogicalTopologyInMetaStorage(twoNodes, topVer1);

        assertEquals(twoNodes, dataNodesDownFut0.get(3, SECONDS));
        assertEquals(twoNodes, dataNodesDownFut1.get(3, SECONDS));
        assertEquals(twoNodes, dataNodesDownFut2.get(3, SECONDS));
        assertEquals(twoNodes, dataNodesDownFut4.get(3, SECONDS));
        assertEquals(twoNodes, dataNodesDownFut5.get(3, SECONDS));
        assertEquals(twoNodes, dataNodesDownFut6.get(3, SECONDS));
        assertEquals(twoNodes, dataNodesDownFut7.get(3, SECONDS));
        assertFalse(dataNodesDownFut3.isDone());

        int topVer2 = 20;

        LOG.info("Topology with added and removed nodes.");

        Set<String> dataNodes = Set.of("node0", "node2");

        setLogicalTopologyInMetaStorage(dataNodes, topVer2);

        assertEquals(dataNodes, dataNodesUpFut3.get(3, SECONDS));
        assertEquals(dataNodes, dataNodesDownFut3.get(3, SECONDS));
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

        Set<String> dataNodes0 = Set.of("node0", "node1");

        setLogicalTopologyInMetaStorage(dataNodes0, topVer);

        assertFalse(dataNodesFut.isDone());

        assertEquals(dataNodes0, dataNodesFut.get(3, SECONDS));

        dataNodesFut = distributionZoneManager.topologyVersionedDataNodes(DEFAULT_ZONE_ID, 106);

        Set<String> dataNodes1 = Set.of("node0");

        setLogicalTopologyInMetaStorage(dataNodes1, topVer + 100);

        assertFalse(dataNodesFut.isDone());

        assertEquals(dataNodes1, dataNodesFut.get(3, SECONDS));
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

        distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder("zone1")
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        int zoneId = distributionZoneManager.getZoneId("zone1");

        CompletableFuture<Set<String>> dataNodesFut = distributionZoneManager.topologyVersionedDataNodes(zoneId, 1);

        Set<String> nodes0 = Set.of("node0", "node1");

        setLogicalTopologyInMetaStorage(nodes0, 1);

        assertEquals(nodes0, dataNodesFut.get(3, SECONDS));

        dataNodesFut = distributionZoneManager.topologyVersionedDataNodes(zoneId, 2);

        assertFalse(dataNodesFut.isDone());

        setLogicalTopologyInMetaStorage(Set.of("node0"), 2);

        assertEquals(nodes0, dataNodesFut.get(3, SECONDS));
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

        distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder("zone0")
                                .dataNodesAutoAdjustScaleUp(INFINITE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder("zone1")
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder("zone2")
                                .dataNodesAutoAdjustScaleUp(INFINITE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        int zoneId0 = distributionZoneManager.getZoneId("zone0");
        int zoneId1 = distributionZoneManager.getZoneId("zone1");
        int zoneId2 = distributionZoneManager.getZoneId("zone2");

        CompletableFuture<Set<String>> dataNodesFut = distributionZoneManager.topologyVersionedDataNodes(zoneId1, 1);

        Set<String> nodes0 = Set.of("node0", "node1");

        setLogicalTopologyInMetaStorage(nodes0, 1);

        dataNodesFut.get(3, SECONDS);

        CompletableFuture<Set<String>> dataNodesFut1Zone0 = distributionZoneManager.topologyVersionedDataNodes(zoneId0, 2);
        CompletableFuture<Set<String>> dataNodesFut1 = distributionZoneManager.topologyVersionedDataNodes(zoneId1, 2);
        CompletableFuture<Set<String>> dataNodesFut1Zone2 = distributionZoneManager.topologyVersionedDataNodes(zoneId2, 2);

        assertFalse(dataNodesFut1Zone0.isDone());
        assertFalse(dataNodesFut1.isDone());
        assertFalse(dataNodesFut1Zone2.isDone());

        Set<String> nodes1 = Set.of("node0");

        distributionZoneManager.alterZone("zone1", new DistributionZoneConfigurationParameters.Builder("zone1")
                        .dataNodesAutoAdjustScaleUp(INFINITE_TIMER_VALUE).dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE).build())
                .get(3, SECONDS);

        setLogicalTopologyInMetaStorage(nodes1, 2);

        assertEquals(nodes1, dataNodesFut1.get(3, SECONDS));

        CompletableFuture<Set<String>> dataNodesFut2 = distributionZoneManager.topologyVersionedDataNodes(zoneId1, 3);

        Set<String> nodes2 = Set.of("node0", "node1");

        assertFalse(dataNodesFut2.isDone());

        setLogicalTopologyInMetaStorage(nodes2, 3);

        assertEquals(nodes1, dataNodesFut2.get(3, SECONDS));
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

        distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder("zone1")
                                .dataNodesAutoAdjustScaleUp(INFINITE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        int zoneId = distributionZoneManager.getZoneId("zone1");

        CompletableFuture<Set<String>> dataNodesFut = distributionZoneManager.topologyVersionedDataNodes(zoneId, 1);

        assertFalse(dataNodesFut.isDone());

        Set<String> nodes0 = Set.of("node0", "node1");

        setLogicalTopologyInMetaStorage(nodes0, 1);

        assertEquals(emptySet(), dataNodesFut.get(3, SECONDS));
    }

    /**
     * Test checks that data nodes futures are completed exceptionally if the zone was removed while data nodes awaiting.
     */
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-19255")
    @Test
    void testRemoveZoneWhileAwaitingDataNodes() throws Exception {
        startZoneManager();

        distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder("zone0")
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        int zoneId = distributionZoneManager.getZoneId("zone0");

        CompletableFuture<Set<String>> dataNodesFut0 = distributionZoneManager.topologyVersionedDataNodes(zoneId, 5);

        setLogicalTopologyInMetaStorage(Set.of("node0", "node1"), 100);

        assertFalse(dataNodesFut0.isDone());

        assertEquals(Set.of("node0", "node1"), dataNodesFut0.get(3, SECONDS));

        CompletableFuture<Set<String>> dataNodesFut1 = distributionZoneManager.topologyVersionedDataNodes(zoneId, 106);

        setLogicalTopologyInMetaStorage(Set.of("node0", "node2"), 200);

        assertFalse(dataNodesFut1.isDone());

        distributionZoneManager.dropZone("zone0").get();

        assertThrowsWithCause(() -> dataNodesFut1.get(3, SECONDS), DistributionZoneWasRemovedException.class);
    }

    /**
     * Test checks that data nodes futures are completed with old data nodes if dataNodesAutoAdjustScaleUp and dataNodesAutoAdjustScaleDown
     * timer increased to non-zero value.
     */
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-19255")
    @Test
    void testScaleUpScaleDownAreChangedWhileAwaitingDataNodes() throws Exception {
        startZoneManager();

        Set<String> nodes0 = Set.of("node0", "node1");

        setLogicalTopologyInMetaStorage(nodes0, 1);

        CompletableFuture<Set<String>> dataNodesFut = distributionZoneManager.topologyVersionedDataNodes(DEFAULT_ZONE_ID, 1);

        assertEquals(nodes0, dataNodesFut.get(3, SECONDS));

        Set<String> nodes1 = Set.of("node0", "node2");

        dataNodesFut = distributionZoneManager.topologyVersionedDataNodes(DEFAULT_ZONE_ID, 2);

        setLogicalTopologyInMetaStorage(nodes1, 2);

        assertFalse(dataNodesFut.isDone());

        //need to create new zone to fix assert invariant which is broken in this test environment.
        distributionZoneManager.createZone(new DistributionZoneConfigurationParameters.Builder("zone0")
                        .dataNodesAutoAdjustScaleUp(1000).dataNodesAutoAdjustScaleDown(1000).build())
                .get(3, SECONDS);

        assertFalse(dataNodesFut.isDone());

        distributionZoneManager.alterZone(DEFAULT_ZONE_NAME, new DistributionZoneConfigurationParameters.Builder(DEFAULT_ZONE_NAME)
                        .dataNodesAutoAdjustScaleUp(1000).dataNodesAutoAdjustScaleDown(1000).build())
                .get(3, SECONDS);

        assertEquals(nodes0, dataNodesFut.get(3, SECONDS));
    }

    /**
     * Test checks that data nodes are initialized on zone manager start.
     */
    @Test
    void testInitializedDataNodesOnZoneManagerStart() throws Exception {
        Set<String> dataNodes = Set.of("node0", "node1");

        Map<ByteArray, byte[]> valEntries = new HashMap<>();

        valEntries.put(zonesLogicalTopologyKey(), toBytes(dataNodes));
        valEntries.put(zonesLogicalTopologyVersionKey(), longToBytes(3));

        assertThat(vaultMgr.putAll(valEntries), willCompleteSuccessfully());

        topology.putNode(new LogicalNode(new ClusterNode("node0", "node0", new NetworkAddress("local", 1))));
        topology.putNode(new LogicalNode(new ClusterNode("node1", "node1", new NetworkAddress("local", 1))));

        startZoneManager();

        assertEquals(dataNodes, distributionZoneManager.topologyVersionedDataNodes(DEFAULT_ZONE_ID, 2)
                .get(3, SECONDS));
    }

    private void startZoneManager() throws Exception {
        deployWatchesAndUpdateMetaStorageRevision();

        distributionZoneManager.start();

        distributionZoneManager.alterZone(
                        DEFAULT_ZONE_NAME, new DistributionZoneConfigurationParameters.Builder(DEFAULT_ZONE_NAME)
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE).build())
                .get(3, SECONDS);
    }

    private void setLogicalTopologyInMetaStorage(Set<String> nodes, long topVer) {
        CompletableFuture<Boolean> invokeFuture = metaStorageManager.invoke(
                Conditions.exists(zonesLogicalTopologyKey()),
                List.of(
                        Operations.put(zonesLogicalTopologyKey(), toBytes(nodes)),
                        Operations.put(zonesLogicalTopologyVersionKey(), longToBytes(topVer))
                ),
                List.of(Operations.noop())
        );

        assertThat(invokeFuture, willBe(true));
    }
}
