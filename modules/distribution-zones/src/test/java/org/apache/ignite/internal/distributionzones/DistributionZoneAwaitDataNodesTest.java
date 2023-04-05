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
import java.util.concurrent.TimeUnit;
import org.apache.ignite.configuration.NamedConfigurationTree;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.raft.ClusterStateStorage;
import org.apache.ignite.internal.cluster.management.raft.TestClusterStateStorage;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopology;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyServiceImpl;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager.ZoneState;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneConfigurationSchema;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.EntryEvent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.dsl.StatementResultImpl;
import org.apache.ignite.internal.metastorage.impl.EntryImpl;
import org.apache.ignite.internal.schema.configuration.TableChange;
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.apache.ignite.internal.schema.configuration.TableView;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests awaiting data nodes algorithm in distribution zone manager in case when
 * {@link DistributionZoneConfigurationSchema#dataNodesAutoAdjustScaleUp}
 * or {@link DistributionZoneConfigurationSchema#dataNodesAutoAdjustScaleDown} equals to 0.
 */
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

    private WatchListener topologyWatchListener;

    private WatchListener dataNodesWatchListener;

    @BeforeEach
    void setUp() throws Exception {
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

        TablesConfiguration tablesConfiguration = mock(TablesConfiguration.class);

        NamedConfigurationTree<TableConfiguration, TableView, TableChange> tables = mock(NamedConfigurationTree.class);

        when(tablesConfiguration.tables()).thenReturn(tables);

        NamedListView<TableView> tablesValue = mock(NamedListView.class);

        when(tables.value()).thenReturn(tablesValue);

        when(tablesValue.size()).thenReturn(0);

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
     * Test checks that data nodes futures are completed on topology with added and removed nodes.
     */
    @Test
    void testSeveralScaleUpAndSeveralScaleDownThenScaleUpAndScaleDown() throws Exception {
        startZoneManager(0);

        TestSeveralScaleUpAndSeveralScaleDownDataObject testData = testSeveralScaleUpAndSeveralScaleDownGeneral();

        LOG.info("Topology with added and removed nodes.");

        Set<String> dataNodes = Set.of("node0", "node2");

        topologyWatchListenerOnUpdate(dataNodes, testData.topVer2, testData.dataNodesRevision2);

        assertTrue(testData.topVerUpFut2.isDone());
        assertTrue(testData.topVerDownFut2.isDone());

        CompletableFuture<Void> revisionUpFut;

        revisionUpFut = distributionZoneManager.zonesState().get(DEFAULT_ZONE_ID)
                .scaleUpRevisionTracker().waitFor((long) testData.dataNodesRevision2);

        assertFalse(revisionUpFut.isDone());

        dataNodesWatchListenerOnUpdate(DEFAULT_ZONE_ID, dataNodes, true, testData.dataNodesRevision2,
                testData.dataNodesRevision2 + 1);

        assertTrue(waitForCondition(revisionUpFut::isDone, 3000));

        CompletableFuture<Void> revisionDownFut;

        revisionDownFut = distributionZoneManager.zonesState().get(DEFAULT_ZONE_ID).scaleDownRevisionTracker()
                    .waitFor((long) testData.dataNodesRevision2);

        assertFalse(revisionDownFut.isDone());

        assertFalse(testData.dataNodesUpFut3.isDone());
        assertFalse(testData.dataNodesDownFut3.isDone());

        dataNodesWatchListenerOnUpdate(DEFAULT_ZONE_ID, dataNodes, false, testData.dataNodesRevision2,
                testData.dataNodesRevision2 + 2);

        assertTrue(waitForCondition(revisionDownFut::isDone, 3000));

        assertEquals(dataNodes, testData.dataNodesUpFut3.get(3, TimeUnit.SECONDS));
        assertEquals(dataNodes, testData.dataNodesDownFut3.get(3, TimeUnit.SECONDS));
    }

    /**
     * Test checks that data nodes futures are completed on topology with added nodes.
     */
    @Test
    void testSeveralScaleUpAndSeveralScaleDownThenScaleUp() throws Exception {
        startZoneManager(0);

        TestSeveralScaleUpAndSeveralScaleDownDataObject testData = testSeveralScaleUpAndSeveralScaleDownGeneral();

        LOG.info("Topology with added nodes.");

        Set<String> dataNodes = Set.of("node0", "node1", "node2");

        topologyWatchListenerOnUpdate(dataNodes, testData.topVer2, testData.dataNodesRevision2);

        assertTrue(testData.topVerUpFut2.isDone());
        assertTrue(testData.topVerDownFut2.isDone());

        CompletableFuture<Void> revisionUpFut = distributionZoneManager.zonesState().get(DEFAULT_ZONE_ID)
                    .scaleUpRevisionTracker().waitFor((long) testData.dataNodesRevision2);

        assertFalse(revisionUpFut.isDone());

        dataNodesWatchListenerOnUpdate(DEFAULT_ZONE_ID, dataNodes, true, testData.dataNodesRevision2,
                testData.dataNodesRevision2 + 1);

        assertTrue(waitForCondition(revisionUpFut::isDone, 3000));
        assertEquals(dataNodes, testData.dataNodesUpFut3.get(3, TimeUnit.SECONDS));
        assertEquals(dataNodes, testData.dataNodesDownFut3.get(3, TimeUnit.SECONDS));
    }

    /**
     * Test checks that data nodes futures are completed on topology with removed nodes.
     */
    @Test
    void testSeveralScaleUpAndSeveralScaleDownThenScaleDown() throws Exception {
        startZoneManager(0);

        TestSeveralScaleUpAndSeveralScaleDownDataObject testData = testSeveralScaleUpAndSeveralScaleDownGeneral();

        LOG.info("Topology with removed nodes.");

        Set<String> dataNodes = Set.of("node0");

        topologyWatchListenerOnUpdate(dataNodes, testData.topVer2, testData.dataNodesRevision2);

        assertTrue(testData.topVerUpFut2.isDone());
        assertTrue(testData.topVerDownFut2.isDone());

        CompletableFuture<Void> revisionDownFut = distributionZoneManager.zonesState().get(DEFAULT_ZONE_ID)
                .scaleDownRevisionTracker().waitFor((long) testData.dataNodesRevision2);

        assertFalse(revisionDownFut.isDone());

        dataNodesWatchListenerOnUpdate(DEFAULT_ZONE_ID, dataNodes, false, testData.dataNodesRevision2,
                testData.dataNodesRevision2 + 1);

        assertTrue(waitForCondition(revisionDownFut::isDone, 3000));
        assertEquals(dataNodes, testData.dataNodesUpFut3.get(3, TimeUnit.SECONDS));
        assertEquals(dataNodes, testData.dataNodesDownFut3.get(3, TimeUnit.SECONDS));
    }

    private static class TestSeveralScaleUpAndSeveralScaleDownDataObject {
        private final long topVer2;
        private final long dataNodesRevision2;
        private final CompletableFuture<Void> topVerUpFut2;
        private final CompletableFuture<Void> topVerDownFut2;
        private final CompletableFuture<Set<String>> dataNodesUpFut3;
        private final CompletableFuture<Set<String>> dataNodesDownFut3;

        TestSeveralScaleUpAndSeveralScaleDownDataObject(
                long topVer2,
                long dataNodesRevision2,
                CompletableFuture<Void> topVerUpFut2,
                CompletableFuture<Void> topVerDownFut2,
                CompletableFuture<Set<String>> dataNodesUpFut3,
                CompletableFuture<Set<String>> dataNodesDownFut3) {
            this.topVer2 = topVer2;
            this.dataNodesRevision2 = dataNodesRevision2;
            this.topVerUpFut2 = topVerUpFut2;
            this.topVerDownFut2 = topVerDownFut2;
            this.dataNodesUpFut3 = dataNodesUpFut3;
            this.dataNodesDownFut3 = dataNodesDownFut3;
        }
    }

    /**
     * This method invokes {@link DistributionZoneManager#topologyVersionedDataNodes(int, long)} with default and non-default zone id
     * and different logical topology version. Collects data nodes futures.
     * Simulates new logical topology with new nodes. Check that some of data nodes futures are completed.
     * Simulates new logical topology with removed nodes. Check that some of data nodes futures are completed.
     *
     * @return Structure with data for continue testing.
     */
    private TestSeveralScaleUpAndSeveralScaleDownDataObject testSeveralScaleUpAndSeveralScaleDownGeneral()
            throws Exception {
        distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder("zone0")
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                                .build()
                )
                .get(3, TimeUnit.SECONDS);
        distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder("zone1")
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                                .build()
                )
                .get(3, TimeUnit.SECONDS);

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

        CompletableFuture<Void> topVerUpFut0 = distributionZoneManager.topVerTracker().waitFor(1L);
        CompletableFuture<Void> topVerUpFut1 = distributionZoneManager.topVerTracker().waitFor(2L);
        CompletableFuture<Void> topVerUpFut2 = distributionZoneManager.topVerTracker().waitFor(11L);

        assertFalse(topVerUpFut0.isDone());
        assertFalse(topVerUpFut1.isDone());
        assertFalse(topVerUpFut2.isDone());

        Set<String> threeNodes = Set.of("node0", "node1", "node2");

        topologyWatchListenerOnUpdate(threeNodes, topVer0, dataNodesRevision0);

        assertTrue(waitForCondition(topVerUpFut0::isDone, 3_000));
        assertTrue(waitForCondition(topVerUpFut1::isDone, 3_000));
        assertFalse(topVerUpFut2.isDone());

        CompletableFuture<Void> revision2Fut;


        revision2Fut = distributionZoneManager.zonesState().get(DEFAULT_ZONE_ID)
                .scaleUpRevisionTracker().waitFor((long) dataNodesRevision0);


        assertFalse(revision2Fut.isDone());

        dataNodesWatchListenerOnUpdate(DEFAULT_ZONE_ID, threeNodes, true, dataNodesRevision0, dataNodesRevision0 + 1);
        dataNodesWatchListenerOnUpdate(zoneId0, threeNodes, true, dataNodesRevision0, dataNodesRevision0 + 1);
        dataNodesWatchListenerOnUpdate(zoneId1, threeNodes, true, dataNodesRevision0, dataNodesRevision0 + 1);

        assertTrue(waitForCondition(revision2Fut::isDone, 3_000));

        assertEquals(threeNodes, dataNodesUpFut0.get());
        assertEquals(threeNodes, dataNodesUpFut1.get());
        assertEquals(threeNodes, dataNodesUpFut2.get());
        assertEquals(threeNodes, dataNodesUpFut4.get());
        assertEquals(threeNodes, dataNodesUpFut5.get());
        assertEquals(threeNodes, dataNodesUpFut6.get());
        assertEquals(threeNodes, dataNodesUpFut7.get());
        assertFalse(dataNodesUpFut3.isDone());


        LOG.info("Topology with removed nodes.");

        CompletableFuture<Set<String>> dataNodesDownFut0;
        CompletableFuture<Set<String>> dataNodesDownFut1;
        CompletableFuture<Set<String>> dataNodesDownFut2;
        CompletableFuture<Set<String>> dataNodesDownFut3;

        dataNodesDownFut0 = distributionZoneManager.topologyVersionedDataNodes(DEFAULT_ZONE_ID, 4);
        dataNodesDownFut1 = distributionZoneManager.topologyVersionedDataNodes(DEFAULT_ZONE_ID, 4);
        dataNodesDownFut2 = distributionZoneManager.topologyVersionedDataNodes(DEFAULT_ZONE_ID, 5);
        dataNodesDownFut3 = distributionZoneManager.topologyVersionedDataNodes(DEFAULT_ZONE_ID, 6);

        CompletableFuture<Void> topVerDownFut0 = distributionZoneManager.topVerTracker().waitFor(4L);
        CompletableFuture<Void> topVerDownFut1 = distributionZoneManager.topVerTracker().waitFor(5L);
        CompletableFuture<Void> topVerDownFut2 = distributionZoneManager.topVerTracker().waitFor(6L);

        assertFalse(topVerDownFut0.isDone());
        assertFalse(topVerDownFut1.isDone());
        assertFalse(topVerDownFut2.isDone());

        int topVer1 = 5;

        int dataNodesRevision1 = dataNodesRevision0 + 2;

        Set<String> twoNodes = Set.of("node0", "node1");

        topologyWatchListenerOnUpdate(twoNodes, topVer1, dataNodesRevision1);

        assertTrue(waitForCondition(topVerDownFut0::isDone, 3_000));
        assertTrue(waitForCondition(topVerDownFut1::isDone, 3_000));
        assertFalse(waitForCondition(topVerDownFut2::isDone, 3_000));

        ZoneState zoneState = distributionZoneManager.zonesState().get(DEFAULT_ZONE_ID);

        CompletableFuture<Void> revision5Fut = zoneState.scaleDownRevisionTracker().waitFor((long) dataNodesRevision1);

        assertFalse(revision5Fut.isDone());

        dataNodesWatchListenerOnUpdate(DEFAULT_ZONE_ID, twoNodes, false, dataNodesRevision1, dataNodesRevision1 + 1);

        assertTrue(waitForCondition(revision5Fut::isDone, 3_000));

        assertEquals(twoNodes, dataNodesDownFut0.get());
        assertEquals(twoNodes, dataNodesDownFut1.get());
        assertEquals(twoNodes, dataNodesDownFut2.get());
        assertFalse(dataNodesDownFut3.isDone());

        int topVer2 = 20;

        int dataNodesRevision2 = dataNodesRevision1 + 2;

        return new TestSeveralScaleUpAndSeveralScaleDownDataObject(
                topVer2,
                dataNodesRevision2,
                topVerUpFut2,
                topVerDownFut2,
                dataNodesUpFut3,
                dataNodesDownFut3
        );
    }

    /**
     * Test checks that data nodes futures are completed on topology with added nodes.
     */
    @Test
    void testScaleUpAndThenScaleDown() throws Exception {
        startZoneManager(0);

        CompletableFuture<Set<String>> dataNodesFut = distributionZoneManager.topologyVersionedDataNodes(DEFAULT_ZONE_ID, 5);

        CompletableFuture<Void> topVerFut5 = distributionZoneManager.topVerTracker().waitFor(5L);

        assertFalse(topVerFut5.isDone());

        assertFalse(dataNodesFut.isDone());

        topologyWatchListenerOnUpdate(Set.of("node0", "node1"), 100, 10);

        assertTrue(waitForCondition(topVerFut5::isDone, 3_000));

        ZoneState zoneState = distributionZoneManager.zonesState().get(DEFAULT_ZONE_ID);

        CompletableFuture<Void> revisionFut10 = zoneState.scaleUpRevisionTracker().waitFor(10L);

        assertFalse(revisionFut10.isDone());

        assertFalse(dataNodesFut.isDone());

        dataNodesWatchListenerOnUpdate(DEFAULT_ZONE_ID, Set.of("node0", "node1"), true, 10, 11);

        assertTrue(waitForCondition(revisionFut10::isDone, 3_000));

        assertEquals(Set.of("node0", "node1"), dataNodesFut.get());

        dataNodesFut = distributionZoneManager.topologyVersionedDataNodes(DEFAULT_ZONE_ID, 106);

        CompletableFuture<Void> topVerFut106 = distributionZoneManager.topVerTracker().waitFor(106L);

        assertFalse(topVerFut106.isDone());

        topologyWatchListenerOnUpdate(Set.of("node0"), 200, 12);

        assertTrue(waitForCondition(topVerFut106::isDone, 3_000));

        CompletableFuture<Void> revisionFut12 = distributionZoneManager.zonesState().get(DEFAULT_ZONE_ID)
                .scaleDownRevisionTracker().waitFor(12L);

        assertFalse(revisionFut12.isDone());

        assertFalse(dataNodesFut.isDone());

        dataNodesWatchListenerOnUpdate(DEFAULT_ZONE_ID, Set.of("node0"), false, 12, 13);

        assertTrue(waitForCondition(revisionFut12::isDone, 3_000));

        assertEquals(Set.of("node0"), dataNodesFut.get());
    }

    /**
     * Test checks that data nodes futures are completed on topology with added and removed nodes for the zone with
     * dataNodesAutoAdjustScaleUp is zero and dataNodesAutoAdjustScaleDown is non-zero.
     */
    @Test
    void testAwaitingScaleUpOnly() throws Exception {
        startZoneManager(0);

        distributionZoneManager.alterZone(DEFAULT_ZONE_NAME, new DistributionZoneConfigurationParameters.Builder(DEFAULT_ZONE_NAME)
                        .dataNodesAutoAdjustScaleUp(INFINITE_TIMER_VALUE).dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE).build())
                .get(3, TimeUnit.SECONDS);

        distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder("zone1")
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE)
                                .build()
                )
                .get(3, TimeUnit.SECONDS);

        int zoneId = distributionZoneManager.getZoneId("zone1");

        CompletableFuture<Set<String>> dataNodesFut = distributionZoneManager.topologyVersionedDataNodes(zoneId, 1);

        CompletableFuture<Void> topVerUpFut = distributionZoneManager.topVerTracker().waitFor(1L);

        assertFalse(topVerUpFut.isDone());

        Set<String> nodes0 = Set.of("node0", "node1");

        topologyWatchListenerOnUpdate(nodes0, 1, 1);

        dataNodesWatchListenerOnUpdate(zoneId, nodes0, true, 1, 2);

        dataNodesFut.get(3, TimeUnit.SECONDS);

        dataNodesFut = distributionZoneManager.topologyVersionedDataNodes(zoneId, 2);

        Set<String> nodes1 = Set.of("node0");

        topologyWatchListenerOnUpdate(nodes1, 2, 2);

        dataNodesFut.get(3, TimeUnit.SECONDS);
    }

    /**
     * Test checks that data nodes futures are completed on topology with added and removed nodes for the zone with
     * dataNodesAutoAdjustScaleUp is non-zero and dataNodesAutoAdjustScaleDown is zero. And checks that other zones
     * non-zero timers doesn't affect.
     */
    @Test
    void testAwaitingScaleDownOnly() throws Exception {
        startZoneManager(0);

        distributionZoneManager.alterZone(DEFAULT_ZONE_NAME, new DistributionZoneConfigurationParameters.Builder(DEFAULT_ZONE_NAME)
                        .dataNodesAutoAdjustScaleUp(INFINITE_TIMER_VALUE).dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE).build())
                .get(3, TimeUnit.SECONDS);

        distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder("zone0")
                                .dataNodesAutoAdjustScaleUp(INFINITE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE)
                                .build()
                )
                .get(3, TimeUnit.SECONDS);

        distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder("zone1")
                                .dataNodesAutoAdjustScaleUp(INFINITE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                                .build()
                )
                .get(3, TimeUnit.SECONDS);

        distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder("zone2")
                                .dataNodesAutoAdjustScaleUp(INFINITE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE)
                                .build()
                )
                .get(3, TimeUnit.SECONDS);

        int zoneId0 = distributionZoneManager.getZoneId("zone0");
        int zoneId1 = distributionZoneManager.getZoneId("zone1");
        int zoneId2 = distributionZoneManager.getZoneId("zone2");

        CompletableFuture<Set<String>> dataNodesFut = distributionZoneManager.topologyVersionedDataNodes(zoneId1, 1);

        CompletableFuture<Void> topVerUpFut = distributionZoneManager.topVerTracker().waitFor(1L);

        assertFalse(topVerUpFut.isDone());

        Set<String> nodes0 = Set.of("node0", "node1");

        topologyWatchListenerOnUpdate(nodes0, 1, 1);

        dataNodesWatchListenerOnUpdate(zoneId1, nodes0, true, 1, 2);

        dataNodesFut.get(3, TimeUnit.SECONDS);

        CompletableFuture<Set<String>> dataNodesFut1Zone0 = distributionZoneManager.topologyVersionedDataNodes(zoneId0, 2);
        CompletableFuture<Set<String>> dataNodesFut1 = distributionZoneManager.topologyVersionedDataNodes(zoneId1, 2);
        CompletableFuture<Set<String>> dataNodesFut1Zone2 = distributionZoneManager.topologyVersionedDataNodes(zoneId2, 2);


        assertFalse(dataNodesFut1.isDone());

        Set<String> nodes1 = Set.of("node0");

        topologyWatchListenerOnUpdate(nodes1, 2, 3);

        assertTrue(waitForCondition(dataNodesFut1Zone0::isDone, 3000));
        assertTrue(waitForCondition(dataNodesFut1Zone2::isDone, 3000));

        dataNodesWatchListenerOnUpdate(zoneId1, nodes1, false, 3, 4);

        dataNodesFut1.get(3, TimeUnit.SECONDS);

        CompletableFuture<Set<String>> dataNodesFut2 = distributionZoneManager.topologyVersionedDataNodes(zoneId1, 3);

        Set<String> nodes2 = Set.of("node0", "node1");

        topologyWatchListenerOnUpdate(nodes2, 3, 5);

        dataNodesFut2.get(3, TimeUnit.SECONDS);
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
                .get(3, TimeUnit.SECONDS);

        distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder("zone1")
                                .dataNodesAutoAdjustScaleUp(INFINITE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE)
                                .build()
                )
                .get(3, TimeUnit.SECONDS);

        int zoneId = distributionZoneManager.getZoneId("zone1");

        CompletableFuture<Set<String>> dataNodesFut = distributionZoneManager.topologyVersionedDataNodes(zoneId, 1);

        CompletableFuture<Void> topVerUpFut = distributionZoneManager.topVerTracker().waitFor(1L);

        assertFalse(dataNodesFut.isDone());

        Set<String> nodes0 = Set.of("node0", "node1");

        topologyWatchListenerOnUpdate(nodes0, 1, 1);

        assertTrue(waitForCondition(dataNodesFut::isDone, 3000));

        assertEquals(emptySet(), dataNodesFut.get(3, TimeUnit.SECONDS));

        assertTrue(waitForCondition(topVerUpFut::isDone, 3000));

        CompletableFuture<Set<String>> dataNodesFut1 = distributionZoneManager.topologyVersionedDataNodes(zoneId, 1);

        assertEquals(emptySet(), dataNodesFut1.get(3, TimeUnit.SECONDS));

        dataNodesWatchListenerOnUpdate(zoneId, nodes0, true, 1, 2);

        CompletableFuture<Set<String>> dataNodesFut2 = distributionZoneManager.topologyVersionedDataNodes(zoneId, 1);

        assertEquals(nodes0, dataNodesFut2.get(3, TimeUnit.SECONDS));
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
                .get(3, TimeUnit.SECONDS);

        int zoneId = distributionZoneManager.getZoneId("zone0");

        CompletableFuture<Set<String>> dataNodesFut = distributionZoneManager.topologyVersionedDataNodes(zoneId, 5);

        CompletableFuture<Void> topVerFut5 = distributionZoneManager.topVerTracker().waitFor(5L);

        assertFalse(topVerFut5.isDone());

        topologyWatchListenerOnUpdate(Set.of("node0", "node1"), 100, 10);

        assertTrue(waitForCondition(topVerFut5::isDone, 3_000));

        ZoneState zoneState = distributionZoneManager.zonesState().get(zoneId);

        CompletableFuture<Void> revisionDownFut10 = zoneState
                .scaleUpRevisionTracker().waitFor(10L);

        assertFalse(revisionDownFut10.isDone());

        dataNodesWatchListenerOnUpdate(zoneId, Set.of("node0", "node1"), true, 10, 11);

        assertTrue(waitForCondition(revisionDownFut10::isDone, 3_000));

        assertEquals(Set.of("node0", "node1"), dataNodesFut.get());

        dataNodesFut = distributionZoneManager.topologyVersionedDataNodes(zoneId, 106);

        CompletableFuture<Void> topVerFut106 = distributionZoneManager.topVerTracker().waitFor(106L);

        assertFalse(topVerFut106.isDone());

        topologyWatchListenerOnUpdate(Set.of("node0", "node2"), 200, 12);

        assertTrue(waitForCondition(topVerFut106::isDone, 3_000));

        CompletableFuture<Void> revisionUpFut12 = zoneState.scaleUpRevisionTracker().waitFor(12L);

        CompletableFuture<Void> revisionDownFut12 = zoneState.scaleDownRevisionTracker().waitFor(12L);

        assertFalse(revisionDownFut12.isDone());
        assertFalse(revisionUpFut12.isDone());

        distributionZoneManager.dropZone("zone0").get();

        assertTrue(waitForCondition(revisionDownFut12::isDone, 3_000));
        assertTrue(waitForCondition(revisionUpFut12::isDone, 3_000));

        assertTrue(dataNodesFut.isCompletedExceptionally());
    }

    /**
     * Test checks that data nodes futures are completed with old data nodes if dataNodesAutoAdjustScaleUp
     * and dataNodesAutoAdjustScaleDown timer increased to non-zero value.
     */
    @Test
    void testScaleUpScaleDownWhileAwaitingDataNodes() throws Exception {
        startZoneManager(0);

        Set<String> nodes0 = Set.of("node0", "node1");

        topologyWatchListenerOnUpdate(nodes0, 1, 1);

        dataNodesWatchListenerOnUpdate(DEFAULT_ZONE_ID, nodes0, true, 1, 2);

        CompletableFuture<Set<String>> dataNodesFut = distributionZoneManager.topologyVersionedDataNodes(DEFAULT_ZONE_ID, 1);

        assertEquals(nodes0, dataNodesFut.get());

        Set<String> nodes1 = Set.of("node0", "node2");

        dataNodesFut = distributionZoneManager.topologyVersionedDataNodes(DEFAULT_ZONE_ID, 2);

        topologyWatchListenerOnUpdate(nodes1, 2, 3);

        //need to create new zone to fix assert invariant which is broken in this test environment.
        distributionZoneManager.createZone(new DistributionZoneConfigurationParameters.Builder("zone0")
                        .dataNodesAutoAdjustScaleUp(1000).dataNodesAutoAdjustScaleDown(1000).build())
                .get(3, TimeUnit.SECONDS);

        distributionZoneManager.alterZone(DEFAULT_ZONE_NAME, new DistributionZoneConfigurationParameters.Builder(DEFAULT_ZONE_NAME)
                        .dataNodesAutoAdjustScaleUp(1000).dataNodesAutoAdjustScaleDown(1000).build())
                .get(3, TimeUnit.SECONDS);

        assertEquals(nodes0, dataNodesFut.get());
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
                .get(3, TimeUnit.SECONDS);

        assertEquals(dataNodes, distributionZoneManager.topologyVersionedDataNodes(DEFAULT_ZONE_ID, 2)
                .get(3, TimeUnit.SECONDS));
    }

    private void startZoneManager(long revision) throws Exception {
        when(metaStorageManager.appliedRevision()).thenReturn(revision);

        distributionZoneManager.start();

        distributionZoneManager.alterZone(
                        DEFAULT_ZONE_NAME, new DistributionZoneConfigurationParameters.Builder(DEFAULT_ZONE_NAME)
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE).build())
                .get(3, TimeUnit.SECONDS);
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
