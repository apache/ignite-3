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
import static org.apache.ignite.configuration.annotation.ConfigurationType.DISTRIBUTED;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_ZONE_ID;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_ZONE_NAME;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.toDataNodesMap;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneLogicalTopologyPrefix;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleDownChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleUpChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesDataNodesPrefix;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyVersionKey;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.util.ByteUtils.longToBytes;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.raft.ClusterStateStorage;
import org.apache.ignite.internal.cluster.management.raft.TestClusterStateStorage;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopology;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyServiceImpl;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneConfigurationSchema;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
import org.apache.ignite.internal.distributionzones.exception.DistributionZoneWasRemovedException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.EntryEvent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.dsl.StatementResultImpl;
import org.apache.ignite.internal.metastorage.impl.EntryImpl;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests awaiting data nodes algorithm in distribution zone manager in case when
 * {@link DistributionZoneConfigurationSchema#dataNodesAutoAdjustScaleUp}
 * or {@link DistributionZoneConfigurationSchema#dataNodesAutoAdjustScaleDown} are immediate.
 */
@ExtendWith(ConfigurationExtension.class)
public class DistributionZoneAwaitDataNodesTest extends IgniteAbstractTest {
    private static final IgniteLogger LOG = Loggers.forClass(DistributionZoneAwaitDataNodesTest.class);

    private MetaStorageManager metaStorageManager;

    private DistributionZoneManager distributionZoneManager;

    private LogicalTopology logicalTopology;

    private ClusterStateStorage clusterStateStorage;

    private DistributionZonesConfiguration zonesConfiguration;

    private ConfigurationManager clusterCfgMgr;

    private ClusterManagementGroupManager cmgManager;

    private VaultManager vaultManager;

    @InjectConfiguration
    private TablesConfiguration tablesConfiguration;

    private WatchListener topologyWatchListener;

    private WatchListener dataNodesWatchListener;

    @BeforeEach
    void setUp() {
        vaultManager = mock(VaultManager.class);

        when(vaultManager.get(zonesLogicalTopologyKey())).thenReturn(completedFuture(new VaultEntry(zonesLogicalTopologyKey(), null)));

        when(vaultManager.get(zonesLogicalTopologyVersionKey()))
                .thenReturn(completedFuture(new VaultEntry(zonesLogicalTopologyVersionKey(), longToBytes(0))));

        cmgManager = mock(ClusterManagementGroupManager.class);

        metaStorageManager = mock(MetaStorageManager.class);

        doAnswer(invocation -> {
            ByteArray key = invocation.getArgument(0);

            WatchListener watchListener = invocation.getArgument(1);

            if (Arrays.equals(key.bytes(), zoneLogicalTopologyPrefix().bytes())) {
                topologyWatchListener = watchListener;
            } else if (Arrays.equals(key.bytes(), zonesDataNodesPrefix().bytes())) {
                dataNodesWatchListener = watchListener;
            }

            return null;
        }).when(metaStorageManager).registerPrefixWatch(any(), any());

        when(metaStorageManager.invoke(any()))
                .thenReturn(completedFuture(StatementResultImpl.builder().result(new byte[] {0}).build()));

        clusterStateStorage = new TestClusterStateStorage();

        logicalTopology = new LogicalTopologyImpl(clusterStateStorage);

        LogicalTopologyServiceImpl logicalTopologyService = new LogicalTopologyServiceImpl(logicalTopology, cmgManager);

        clusterCfgMgr = new ConfigurationManager(
                List.of(DistributionZonesConfiguration.KEY),
                Set.of(),
                new TestConfigurationStorage(DISTRIBUTED),
                List.of(),
                List.of()
        );

        clusterCfgMgr.start();

        zonesConfiguration = clusterCfgMgr.configurationRegistry()
                .getConfiguration(DistributionZonesConfiguration.KEY);

        distributionZoneManager = new DistributionZoneManager(
                zonesConfiguration,
                tablesConfiguration,
                metaStorageManager,
                logicalTopologyService,
                vaultManager,
                "node"
        );

        mockCmgLocalNodes();
    }

    @AfterEach
    void tearDown() throws Exception {
        distributionZoneManager.stop();
        clusterCfgMgr.stop();
        clusterStateStorage.stop();
    }

    /**
     * This test invokes {@link DistributionZoneManager#topologyVersionedDataNodes(int, long)} with default and non-default zone id
     * and different logical topology versions.
     * Simulates new logical topology with new nodes and with removed nodes. Check that data nodes futures are completed in right order.
     */
    @Test
    void testSeveralScaleUpAndSeveralScaleDownThenScaleUpAndScaleDown() throws Exception {
        startZoneManager(0);

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

        int dataNodesRevision0 = 2;

        Set<String> threeNodes = Set.of("node0", "node1", "node2");

        topologyWatchListenerOnUpdate(threeNodes, topVer0, dataNodesRevision0);

        assertFalse(dataNodesUpFut0::isDone);
        assertFalse(dataNodesUpFut1::isDone);
        assertFalse(dataNodesUpFut2::isDone);
        assertFalse(dataNodesUpFut4::isDone);
        assertFalse(dataNodesUpFut5::isDone);
        assertFalse(dataNodesUpFut6::isDone);
        assertFalse(dataNodesUpFut7::isDone);

        dataNodesWatchListenerOnUpdate(DEFAULT_ZONE_ID, threeNodes, true, dataNodesRevision0, dataNodesRevision0 + 1);

        assertEquals(threeNodes, dataNodesUpFut0.get(3, SECONDS));
        assertEquals(threeNodes, dataNodesUpFut1.get(3, SECONDS));
        assertEquals(threeNodes, dataNodesUpFut2.get(3, SECONDS));
        assertFalse(dataNodesUpFut4::isDone);
        assertFalse(dataNodesUpFut5::isDone);
        assertFalse(dataNodesUpFut6::isDone);
        assertFalse(dataNodesUpFut7::isDone);

        dataNodesWatchListenerOnUpdate(zoneId0, threeNodes, true, dataNodesRevision0, dataNodesRevision0 + 1);

        assertEquals(threeNodes, dataNodesUpFut4.get(3, SECONDS));
        assertEquals(threeNodes, dataNodesUpFut5.get(3, SECONDS));
        assertFalse(dataNodesUpFut6::isDone);
        assertFalse(dataNodesUpFut7::isDone);

        dataNodesWatchListenerOnUpdate(zoneId1, threeNodes, true, dataNodesRevision0, dataNodesRevision0 + 1);

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

        int dataNodesRevision1 = dataNodesRevision0 + 2;

        Set<String> twoNodes = Set.of("node0", "node1");

        topologyWatchListenerOnUpdate(twoNodes, topVer1, dataNodesRevision1);

        assertFalse(dataNodesDownFut0::isDone);
        assertFalse(dataNodesDownFut1::isDone);
        assertFalse(dataNodesDownFut2::isDone);
        assertFalse(dataNodesDownFut4::isDone);
        assertFalse(dataNodesDownFut5::isDone);
        assertFalse(dataNodesDownFut6::isDone);
        assertFalse(dataNodesDownFut7::isDone);

        dataNodesWatchListenerOnUpdate(DEFAULT_ZONE_ID, twoNodes, false, dataNodesRevision1, dataNodesRevision1 + 1);

        assertEquals(twoNodes, dataNodesDownFut0.get(3, SECONDS));
        assertEquals(twoNodes, dataNodesDownFut1.get(3, SECONDS));
        assertEquals(twoNodes, dataNodesDownFut2.get(3, SECONDS));
        assertFalse(dataNodesDownFut4::isDone);
        assertFalse(dataNodesDownFut5::isDone);
        assertFalse(dataNodesDownFut6::isDone);
        assertFalse(dataNodesDownFut7::isDone);

        dataNodesWatchListenerOnUpdate(zoneId0, twoNodes, false, dataNodesRevision1, dataNodesRevision1 + 1);

        assertEquals(twoNodes, dataNodesDownFut4.get(3, SECONDS));
        assertEquals(twoNodes, dataNodesDownFut5.get(3, SECONDS));
        assertFalse(dataNodesDownFut6::isDone);
        assertFalse(dataNodesDownFut7::isDone);

        dataNodesWatchListenerOnUpdate(zoneId1, twoNodes, false, dataNodesRevision1, dataNodesRevision1 + 1);

        assertEquals(twoNodes, dataNodesDownFut6.get(3, SECONDS));
        assertEquals(twoNodes, dataNodesDownFut7.get(3, SECONDS));
        assertFalse(dataNodesDownFut3.isDone());

        int topVer2 = 20;

        int dataNodesRevision2 = dataNodesRevision1 + 2;

        LOG.info("Topology with added and removed nodes.");

        Set<String> dataNodes = Set.of("node0", "node2");

        topologyWatchListenerOnUpdate(dataNodes, topVer2, dataNodesRevision2);

        dataNodesWatchListenerOnUpdate(DEFAULT_ZONE_ID, dataNodes, true, dataNodesRevision2, dataNodesRevision2 + 1);

        assertFalse(dataNodesUpFut3.isDone());
        assertFalse(dataNodesDownFut3.isDone());

        dataNodesWatchListenerOnUpdate(DEFAULT_ZONE_ID, dataNodes, false, dataNodesRevision2, dataNodesRevision2 + 2);

        assertEquals(dataNodes, dataNodesUpFut3.get(3, SECONDS));
        assertEquals(dataNodes, dataNodesDownFut3.get(3, SECONDS));
    }

    /**
     * Test checks that data nodes futures are completed on topology with added nodes.
     */
    @Test
    void testScaleUpAndThenScaleDown() throws Exception {
        startZoneManager(0);

        CompletableFuture<Set<String>> dataNodesFut = distributionZoneManager.topologyVersionedDataNodes(DEFAULT_ZONE_ID, 5);

        assertFalse(dataNodesFut.isDone());

        long topVer = 100;

        Set<String> dataNodes0 = Set.of("node0", "node1");

        topologyWatchListenerOnUpdate(dataNodes0, topVer, 10);

        assertFalse(dataNodesFut.isDone());

        dataNodesWatchListenerOnUpdate(DEFAULT_ZONE_ID, dataNodes0, true, 10, 11);

        assertEquals(dataNodes0, dataNodesFut.get(3, SECONDS));

        dataNodesFut = distributionZoneManager.topologyVersionedDataNodes(DEFAULT_ZONE_ID, 106);

        Set<String> dataNodes1 = Set.of("node0");

        topologyWatchListenerOnUpdate(dataNodes1, topVer + 100, 12);

        assertFalse(dataNodesFut.isDone());

        dataNodesWatchListenerOnUpdate(DEFAULT_ZONE_ID, dataNodes1, false, 12, 13);

        assertEquals(dataNodes1, dataNodesFut.get(3, SECONDS));
    }

    /**
     * Test checks that data nodes futures are completed on topology with added and removed nodes for the zone with
     * dataNodesAutoAdjustScaleUp is immediate and dataNodesAutoAdjustScaleDown is non-zero.
     */
    @Test
    void testAwaitingScaleUpOnly() throws Exception {
        startZoneManager(0);

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

        topologyWatchListenerOnUpdate(nodes0, 1, 1);

        assertFalse(dataNodesFut.isDone());

        dataNodesWatchListenerOnUpdate(zoneId, nodes0, true, 1, 2);

        assertEquals(nodes0, dataNodesFut.get(3, SECONDS));

        dataNodesFut = distributionZoneManager.topologyVersionedDataNodes(zoneId, 2);

        assertFalse(dataNodesFut.isDone());

        topologyWatchListenerOnUpdate(Set.of("node0"), 2, 2);

        assertEquals(nodes0, dataNodesFut.get(3, SECONDS));
    }

    /**
     * Test checks that data nodes futures are completed on topology with added and removed nodes for the zone with
     * dataNodesAutoAdjustScaleUp is non-zero and dataNodesAutoAdjustScaleDown is immediate. And checks that other zones
     * non-zero timers doesn't affect.
     */
    @Test
    void testAwaitingScaleDownOnly() throws Exception {
        startZoneManager(0);

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
                                .dataNodesAutoAdjustScaleUp(INFINITE_TIMER_VALUE)
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

        topologyWatchListenerOnUpdate(nodes0, 1, 1);

        dataNodesFut.get(3, SECONDS);

        dataNodesWatchListenerOnUpdate(zoneId1, nodes0, true, 1, 2);

        CompletableFuture<Set<String>> dataNodesFut1Zone0 = distributionZoneManager.topologyVersionedDataNodes(zoneId0, 2);
        CompletableFuture<Set<String>> dataNodesFut1 = distributionZoneManager.topologyVersionedDataNodes(zoneId1, 2);
        CompletableFuture<Set<String>> dataNodesFut1Zone2 = distributionZoneManager.topologyVersionedDataNodes(zoneId2, 2);

        assertFalse(dataNodesFut1Zone0.isDone());
        assertFalse(dataNodesFut1.isDone());
        assertFalse(dataNodesFut1Zone2.isDone());

        Set<String> nodes1 = Set.of("node0");

        topologyWatchListenerOnUpdate(nodes1, 2, 3);

        assertTrue(waitForCondition(dataNodesFut1Zone0::isDone, 3000));
        assertTrue(waitForCondition(dataNodesFut1Zone2::isDone, 3000));
        assertFalse(dataNodesFut1.isDone());

        dataNodesWatchListenerOnUpdate(zoneId1, nodes1, false, 3, 4);

        assertEquals(nodes1, dataNodesFut1.get(3, SECONDS));

        CompletableFuture<Set<String>> dataNodesFut2 = distributionZoneManager.topologyVersionedDataNodes(zoneId1, 3);

        Set<String> nodes2 = Set.of("node0", "node1");

        assertFalse(dataNodesFut2.isDone());

        topologyWatchListenerOnUpdate(nodes2, 3, 5);

        assertEquals(nodes1, dataNodesFut2.get(3, SECONDS));
    }

    /**
     * Test checks that data nodes futures are completed immediately for the zone with
     * dataNodesAutoAdjustScaleUp is non-zero and dataNodesAutoAdjustScaleDown is non-zero.
     */
    @Test
    void testWithOutAwaiting() throws Exception {
        startZoneManager(0);

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

        topologyWatchListenerOnUpdate(nodes0, 1, 1);

        assertEquals(emptySet(), dataNodesFut.get(3, SECONDS));

        CompletableFuture<Set<String>> dataNodesFut1 = distributionZoneManager.topologyVersionedDataNodes(zoneId, 1);

        assertEquals(emptySet(), dataNodesFut1.get(3, SECONDS));

        dataNodesWatchListenerOnUpdate(zoneId, nodes0, true, 1, 2);

        CompletableFuture<Set<String>> dataNodesFut2 = distributionZoneManager.topologyVersionedDataNodes(zoneId, 1);

        assertEquals(nodes0, dataNodesFut2.get(3, SECONDS));
    }

    /**
     * Test checks that data nodes futures are completed exceptionally if the zone was removed while
     * data nodes awaiting.
     */
    @Test
    void testRemoveZoneWhileAwaitingDataNodes() throws Exception {
        startZoneManager(0);

        distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder("zone0")
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        int zoneId = distributionZoneManager.getZoneId("zone0");

        CompletableFuture<Set<String>> dataNodesFut0 = distributionZoneManager.topologyVersionedDataNodes(zoneId, 5);

        topologyWatchListenerOnUpdate(Set.of("node0", "node1"), 100, 10);

        assertFalse(dataNodesFut0.isDone());

        dataNodesWatchListenerOnUpdate(zoneId, Set.of("node0", "node1"), true, 10, 11);

        assertEquals(Set.of("node0", "node1"), dataNodesFut0.get(3, SECONDS));

        CompletableFuture<Set<String>> dataNodesFut1 = distributionZoneManager.topologyVersionedDataNodes(zoneId, 106);

        topologyWatchListenerOnUpdate(Set.of("node0", "node2"), 200, 12);

        assertFalse(dataNodesFut1.isDone());

        distributionZoneManager.dropZone("zone0").get();

        assertThrowsWithCause(() -> dataNodesFut1.get(3, SECONDS), DistributionZoneWasRemovedException.class);
    }

    /**
     * Test checks that data nodes futures are completed with old data nodes if dataNodesAutoAdjustScaleUp
     * and dataNodesAutoAdjustScaleDown timer increased to non-zero value.
     */
    @Test
    void testScaleUpScaleDownAreChangedWhileAwaitingDataNodes() throws Exception {
        startZoneManager(0);

        Set<String> nodes0 = Set.of("node0", "node1");

        topologyWatchListenerOnUpdate(nodes0, 1, 1);

        dataNodesWatchListenerOnUpdate(DEFAULT_ZONE_ID, nodes0, true, 1, 2);

        CompletableFuture<Set<String>> dataNodesFut = distributionZoneManager.topologyVersionedDataNodes(DEFAULT_ZONE_ID, 1);

        assertEquals(nodes0, dataNodesFut.get(3, SECONDS));

        Set<String> nodes1 = Set.of("node0", "node2");

        dataNodesFut = distributionZoneManager.topologyVersionedDataNodes(DEFAULT_ZONE_ID, 2);

        topologyWatchListenerOnUpdate(nodes1, 2, 3);

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

        VaultEntry vaultEntry = new VaultEntry(zonesLogicalTopologyKey(), toBytes(dataNodes));

        when(vaultManager.get(zonesLogicalTopologyKey())).thenReturn(completedFuture(vaultEntry));

        when(vaultManager.get(zonesLogicalTopologyVersionKey()))
                .thenReturn(completedFuture(new VaultEntry(zonesLogicalTopologyVersionKey(), longToBytes(3))));

        startZoneManager(5);

        distributionZoneManager.alterZone(
                        DEFAULT_ZONE_NAME, new DistributionZoneConfigurationParameters.Builder(DEFAULT_ZONE_NAME)
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE).build())
                .get(3, SECONDS);

        assertEquals(dataNodes, distributionZoneManager.topologyVersionedDataNodes(DEFAULT_ZONE_ID, 2)
                .get(3, SECONDS));
    }

    private void startZoneManager(long revision) throws Exception {
        when(metaStorageManager.appliedRevision()).thenReturn(revision);

        distributionZoneManager.start();

        distributionZoneManager.alterZone(
                        DEFAULT_ZONE_NAME, new DistributionZoneConfigurationParameters.Builder(DEFAULT_ZONE_NAME)
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE).build())
                .get(3, SECONDS);
    }

    private void topologyWatchListenerOnUpdate(Set<String> nodes, long topVer, long rev) {
        byte[] newLogicalTopology = toBytes(nodes);
        byte[] newTopVer = longToBytes(topVer);

        Entry newEntry0 = new EntryImpl(zonesLogicalTopologyKey().bytes(), newLogicalTopology, rev, 1);
        Entry newEntry1 = new EntryImpl(zonesLogicalTopologyVersionKey().bytes(), newTopVer, rev, 1);

        EntryEvent entryEvent0 = new EntryEvent(null, newEntry0);
        EntryEvent entryEvent1 = new EntryEvent(null, newEntry1);

        WatchEvent evt = new WatchEvent(List.of(entryEvent0, entryEvent1), rev);

        topologyWatchListener.onUpdate(evt);
    }

    private void dataNodesWatchListenerOnUpdate(int zoneId, Set<String> nodes, boolean isScaleUp, long scaleRevision, long rev) {
        byte[] newDataNodes = toBytes(toDataNodesMap(nodes));
        byte[] newScaleRevision = longToBytes(scaleRevision);

        Entry newEntry0 = new EntryImpl(zoneDataNodesKey(zoneId).bytes(), newDataNodes, rev, 1);
        Entry newEntry1 = new EntryImpl(
                isScaleUp ? zoneScaleUpChangeTriggerKey(zoneId).bytes() : zoneScaleDownChangeTriggerKey(zoneId).bytes(),
                newScaleRevision,
                rev,
                1);

        EntryEvent entryEvent0 = new EntryEvent(null, newEntry0);
        EntryEvent entryEvent1 = new EntryEvent(null, newEntry1);

        WatchEvent evt = new WatchEvent(List.of(entryEvent0, entryEvent1), rev);

        dataNodesWatchListener.onUpdate(evt);
    }

    private void mockCmgLocalNodes() {
        when(cmgManager.logicalTopology()).thenReturn(completedFuture(logicalTopology.getLogicalTopology()));
    }
}
