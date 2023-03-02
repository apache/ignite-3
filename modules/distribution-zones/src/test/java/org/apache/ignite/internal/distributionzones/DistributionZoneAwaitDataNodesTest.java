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
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesPrefix;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleDownChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleUpChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyVersionKey;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.raft.ClusterStateStorage;
import org.apache.ignite.internal.cluster.management.raft.TestClusterStateStorage;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopology;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyServiceImpl;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
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
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests.
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

    private WatchListener topologyWatchListener;

    private WatchListener dataNodesWatchListener;

    @BeforeEach
    void setUp() {
        VaultManager vaultManager = mock(VaultManager.class);

        when(vaultManager.get(any())).thenReturn(completedFuture(null));

        cmgManager = mock(ClusterManagementGroupManager.class);

        metaStorageManager = mock(MetaStorageManager.class);

        doAnswer(invocation -> {
            ByteArray key = invocation.getArgument(0);

            WatchListener watchListener = invocation.getArgument(1);

            if (Arrays.equals(key.bytes(), zonesLogicalTopologyVersionKey().bytes())) {
                topologyWatchListener = watchListener;
            } else if (Arrays.equals(key.bytes(), zoneDataNodesPrefix().bytes())) {
                dataNodesWatchListener = watchListener;
            }

            return null;
        }).when(metaStorageManager).registerExactWatch(any(), any());

        when(metaStorageManager.invoke(any())).thenReturn(completedFuture(StatementResultImpl.builder().result(new byte[] {0}).build()));

        vaultManager.start();

        clusterStateStorage = new TestClusterStateStorage();

        logicalTopology = new LogicalTopologyImpl(clusterStateStorage);

        LogicalTopologyServiceImpl logicalTopologyService = new LogicalTopologyServiceImpl(logicalTopology, cmgManager);

        TablesConfiguration tablesConfiguration = mock(TablesConfiguration.class);

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

        distributionZoneManager.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        distributionZoneManager.stop();
        clusterCfgMgr.stop();
        clusterStateStorage.stop();
    }

    @Test
    void testSeveralScaleUpAndSeveralScaleDownThenScaleUpAndScaleDown() throws Exception {
        LOG.info("Topology with added and removed nodes.");

        TestSeveralScaleUpAndSeveralScaleDownDataObject testData = testSeveralScaleUpAndSeveralScaleDownGeneral();

        Set<String> dataNodes = Set.of("node0", "node2");

        topologyWatchListenerOnUpdate(dataNodes, testData.topVer2, testData.dataNodesRevision2);

        assertTrue(testData.topVerUpFut2.isDone());
        assertTrue(testData.topVerDownFut2.isDone());

        DistributionZoneManager.DataNodes dataNodesMeta = distributionZoneManager.dataNodes().get(DEFAULT_ZONE_ID);

        assertTrue(waitForCondition(() -> dataNodesMeta.getRevisionScaleUpFutures().size() == 1, 3_000));

        assertTrue(testData.dataNodesMeta.getRevisionScaleDownFutures().isEmpty());

        CompletableFuture<Void> revisionUpFut = testData.dataNodesMeta.getRevisionScaleUpFutures().get((long) testData.dataNodesRevision2);

        assertFalse(revisionUpFut.isDone());

        dataNodesWatchListenerOnUpdate(DEFAULT_ZONE_ID, dataNodes, true, testData.dataNodesRevision2, testData.dataNodesRevision2 + 1);

        assertTrue(waitForCondition(() -> revisionUpFut.isDone(), 3000));

        assertTrue(waitForCondition(() -> dataNodesMeta.getRevisionScaleDownFutures().size() == 1, 3_000));

        CompletableFuture<Void> revisionDownFut = testData.dataNodesMeta.getRevisionScaleDownFutures()
                .get((long) testData.dataNodesRevision2);

        assertFalse(revisionDownFut.isDone());

        assertFalse(testData.dataNodesUpFut3.isDone());
        assertFalse(testData.dataNodesDownFut3.isDone());

        dataNodesWatchListenerOnUpdate(DEFAULT_ZONE_ID, dataNodes, false, testData.dataNodesRevision2, testData.dataNodesRevision2 + 2);

        assertTrue(waitForCondition(() -> revisionDownFut.isDone(), 3000));

        assertEquals(dataNodes, testData.dataNodesUpFut3.get(3, TimeUnit.SECONDS));
        assertEquals(dataNodes, testData.dataNodesDownFut3.get(3, TimeUnit.SECONDS));

        assertTrue(distributionZoneManager.topVerFutures().isEmpty());
        assertTrue(distributionZoneManager.dataNodes().get(DEFAULT_ZONE_ID).getRevisionScaleUpFutures().isEmpty());
        assertTrue(distributionZoneManager.dataNodes().get(DEFAULT_ZONE_ID).getRevisionScaleDownFutures().isEmpty());
    }

    @Test
    void testSeveralScaleUpAndSeveralScaleDownThenScaleUp() throws Exception {
        LOG.info("Topology with added nodes.");

        TestSeveralScaleUpAndSeveralScaleDownDataObject testData = testSeveralScaleUpAndSeveralScaleDownGeneral();

        Set<String> dataNodes = Set.of("node0", "node1", "node2");

        topologyWatchListenerOnUpdate(dataNodes, testData.topVer2, testData.dataNodesRevision2);

        assertTrue(testData.topVerUpFut2.isDone());
        assertTrue(testData.topVerDownFut2.isDone());

        DistributionZoneManager.DataNodes dataNodesMeta = distributionZoneManager.dataNodes().get(DEFAULT_ZONE_ID);

        assertTrue(waitForCondition(() -> dataNodesMeta.getRevisionScaleUpFutures().size() == 1, 3_000));

        assertTrue(testData.dataNodesMeta.getRevisionScaleDownFutures().isEmpty());

        CompletableFuture<Void> revisionUpFut = testData.dataNodesMeta.getRevisionScaleUpFutures().get((long) testData.dataNodesRevision2);

        assertFalse(revisionUpFut.isDone());

        dataNodesWatchListenerOnUpdate(DEFAULT_ZONE_ID, dataNodes, true, testData.dataNodesRevision2, testData.dataNodesRevision2 + 1);

        assertTrue(waitForCondition(() -> revisionUpFut.isDone(), 3000));
        assertEquals(dataNodes, testData.dataNodesUpFut3.get(3, TimeUnit.SECONDS));
        assertEquals(dataNodes, testData.dataNodesDownFut3.get(3, TimeUnit.SECONDS));

        assertTrue(distributionZoneManager.topVerFutures().isEmpty());
        assertTrue(distributionZoneManager.dataNodes().get(DEFAULT_ZONE_ID).getRevisionScaleUpFutures().isEmpty());
        assertTrue(distributionZoneManager.dataNodes().get(DEFAULT_ZONE_ID).getRevisionScaleDownFutures().isEmpty());
    }

    @Test
    void testSeveralScaleUpAndSeveralScaleDownThenScaleDown() throws Exception {
        LOG.info("Topology with removed nodes.");

        TestSeveralScaleUpAndSeveralScaleDownDataObject testData = testSeveralScaleUpAndSeveralScaleDownGeneral();

        Set<String> dataNodes = Set.of("node0");

        topologyWatchListenerOnUpdate(dataNodes, testData.topVer2, testData.dataNodesRevision2);

        assertTrue(testData.topVerUpFut2.isDone());
        assertTrue(testData.topVerDownFut2.isDone());

        assertTrue(waitForCondition(() -> {
            DistributionZoneManager.DataNodes dataNodesMeta0 = distributionZoneManager.dataNodes().get(DEFAULT_ZONE_ID);

            if (dataNodesMeta0 == null) {
                return false;
            }

            return dataNodesMeta0.getRevisionScaleDownFutures().size() == 1;
        },
                3_000));

        assertTrue(testData.dataNodesMeta.getRevisionScaleUpFutures().isEmpty());

        CompletableFuture<Void> revisionDownFut = testData.dataNodesMeta.getRevisionScaleDownFutures()
                .get((long) testData.dataNodesRevision2);

        assertFalse(revisionDownFut.isDone());

        dataNodesWatchListenerOnUpdate(DEFAULT_ZONE_ID, dataNodes, false, testData.dataNodesRevision2, testData.dataNodesRevision2 + 1);

        assertTrue(waitForCondition(() -> revisionDownFut.isDone(), 3000));
        assertEquals(dataNodes, testData.dataNodesUpFut3.get(3, TimeUnit.SECONDS));
        assertEquals(dataNodes, testData.dataNodesDownFut3.get(3, TimeUnit.SECONDS));

        assertTrue(distributionZoneManager.topVerFutures().isEmpty());
        assertTrue(distributionZoneManager.dataNodes().get(DEFAULT_ZONE_ID).getRevisionScaleUpFutures().isEmpty());
        assertTrue(distributionZoneManager.dataNodes().get(DEFAULT_ZONE_ID).getRevisionScaleDownFutures().isEmpty());
    }

    private static class TestSeveralScaleUpAndSeveralScaleDownDataObject {
        private long topVer2;
        private long dataNodesRevision2;
        private CompletableFuture<Void> topVerUpFut2;
        private CompletableFuture<Void> topVerDownFut2;
        private DistributionZoneManager.DataNodes dataNodesMeta;
        private CompletableFuture<Set<String>> dataNodesUpFut3;
        private CompletableFuture<Set<String>> dataNodesDownFut3;

        public TestSeveralScaleUpAndSeveralScaleDownDataObject(
                long topVer2,
                long dataNodesRevision2,
                CompletableFuture<Void> topVerUpFut2,
                CompletableFuture<Void> topVerDownFut2,
                DistributionZoneManager.DataNodes dataNodesMeta,
                CompletableFuture<Set<String>> dataNodesUpFut3,
                CompletableFuture<Set<String>> dataNodesDownFut3) {
            this.topVer2 = topVer2;
            this.dataNodesRevision2 = dataNodesRevision2;
            this.topVerUpFut2 = topVerUpFut2;
            this.topVerDownFut2 = topVerDownFut2;
            this.dataNodesMeta = dataNodesMeta;
            this.dataNodesUpFut3 = dataNodesUpFut3;
            this.dataNodesDownFut3 = dataNodesDownFut3;
        }
    }

    private TestSeveralScaleUpAndSeveralScaleDownDataObject testSeveralScaleUpAndSeveralScaleDownGeneral() throws Exception {
        LOG.info("Topology with added nodes.");

        CompletableFuture<Set<String>> dataNodesUpFut0 = distributionZoneManager.getDataNodes(DEFAULT_ZONE_ID, 1);
        CompletableFuture<Set<String>> dataNodesUpFut1 = distributionZoneManager.getDataNodes(DEFAULT_ZONE_ID, 1);
        CompletableFuture<Set<String>> dataNodesUpFut2 = distributionZoneManager.getDataNodes(DEFAULT_ZONE_ID, 2);
        CompletableFuture<Set<String>> dataNodesUpFut3 = distributionZoneManager.getDataNodes(DEFAULT_ZONE_ID, 11);

        assertTrue(waitForCondition(() -> distributionZoneManager.topVerFutures().size() == 3,
                3_000));

        int topVer0 = 2;

        int dataNodesRevision0 = 2;

        CompletableFuture<Void> topVerUpFut0 = distributionZoneManager.topVerFutures().get(1L);
        CompletableFuture<Void> topVerUpFut1 = distributionZoneManager.topVerFutures().get(2L);
        CompletableFuture<Void> topVerUpFut2 = distributionZoneManager.topVerFutures().get(11L);

        assertFalse(topVerUpFut0.isDone());
        assertFalse(topVerUpFut1.isDone());
        assertFalse(topVerUpFut2.isDone());

        Set<String> threeNodes = Set.of("node0", "node1", "node2");

        topologyWatchListenerOnUpdate(threeNodes, topVer0, dataNodesRevision0);

        assertTrue(waitForCondition(() -> topVerUpFut0.isDone(), 3_000));
        assertTrue(waitForCondition(() -> topVerUpFut1.isDone(), 3_000));
        assertFalse(topVerUpFut2.isDone());

        assertTrue(waitForCondition(() -> {
            DistributionZoneManager.DataNodes dataNodesMeta0 = distributionZoneManager.dataNodes().get(DEFAULT_ZONE_ID);

            if (dataNodesMeta0 == null) {
                return false;
            }

            return dataNodesMeta0.getRevisionScaleUpFutures().size() == 1;
        },
                3_000));

        DistributionZoneManager.DataNodes dataNodesMeta = distributionZoneManager.dataNodes().get(DEFAULT_ZONE_ID);

        CompletableFuture<Void> revision2Fut = dataNodesMeta.getRevisionScaleUpFutures().get((long) dataNodesRevision0);

        assertFalse(revision2Fut.isDone());

        dataNodesWatchListenerOnUpdate(DEFAULT_ZONE_ID, threeNodes, true, dataNodesRevision0, dataNodesRevision0 + 1);

        assertTrue(waitForCondition(() -> revision2Fut.isDone(),
                3_000));

        assertEquals(threeNodes, dataNodesUpFut0.get());
        assertEquals(threeNodes, dataNodesUpFut1.get());
        assertEquals(threeNodes, dataNodesUpFut2.get());
        assertFalse(dataNodesUpFut3.isDone());

        assertTrue(distributionZoneManager.topVerFutures().size() == 1);
        assertTrue(distributionZoneManager.dataNodes().get(DEFAULT_ZONE_ID).getRevisionScaleUpFutures().isEmpty());
        assertTrue(distributionZoneManager.dataNodes().get(DEFAULT_ZONE_ID).getRevisionScaleDownFutures().isEmpty());


        LOG.info("Topology with removed nodes.");

        CompletableFuture<Set<String>> dataNodesDownFut0 = distributionZoneManager.getDataNodes(DEFAULT_ZONE_ID, 4);
        CompletableFuture<Set<String>> dataNodesDownFut1 = distributionZoneManager.getDataNodes(DEFAULT_ZONE_ID, 4);
        CompletableFuture<Set<String>> dataNodesDownFut2 = distributionZoneManager.getDataNodes(DEFAULT_ZONE_ID, 5);
        CompletableFuture<Set<String>> dataNodesDownFut3 = distributionZoneManager.getDataNodes(DEFAULT_ZONE_ID, 6);

        assertTrue(waitForCondition(() -> distributionZoneManager.topVerFutures().size() == 4,
                3_000));

        CompletableFuture<Void> topVerDownFut0 = distributionZoneManager.topVerFutures().get(4L);
        CompletableFuture<Void> topVerDownFut1 = distributionZoneManager.topVerFutures().get(5L);
        CompletableFuture<Void> topVerDownFut2 = distributionZoneManager.topVerFutures().get(6L);

        assertFalse(topVerDownFut0.isDone());
        assertFalse(topVerDownFut1.isDone());
        assertFalse(topVerDownFut2.isDone());

        int topVer1 = 5;

        int dataNodesRevision1 = dataNodesRevision0 + 2;

        Set<String> twoNodes = Set.of("node0", "node1");

        topologyWatchListenerOnUpdate(twoNodes, topVer1, dataNodesRevision1);

        assertTrue(waitForCondition(() -> topVerDownFut0.isDone(), 3_000));
        assertTrue(waitForCondition(() -> topVerDownFut1.isDone(), 3_000));
        assertFalse(waitForCondition(() -> topVerDownFut2.isDone(), 3_000));

        assertTrue(waitForCondition(() -> dataNodesMeta.getRevisionScaleDownFutures().size() == 1, 3_000));

        CompletableFuture<Void> revision5Fut = dataNodesMeta.getRevisionScaleDownFutures().get((long) dataNodesRevision1);

        assertFalse(revision5Fut.isDone());

        dataNodesWatchListenerOnUpdate(DEFAULT_ZONE_ID, twoNodes, false, dataNodesRevision1, dataNodesRevision1 + 1);

        assertTrue(waitForCondition(() -> revision5Fut.isDone(), 3_000));

        assertEquals(twoNodes, dataNodesDownFut0.get());
        assertEquals(twoNodes, dataNodesDownFut1.get());
        assertEquals(twoNodes, dataNodesDownFut2.get());
        assertFalse(dataNodesDownFut3.isDone());

        assertEquals(2, distributionZoneManager.topVerFutures().size());
        assertTrue(distributionZoneManager.dataNodes().get(DEFAULT_ZONE_ID).getRevisionScaleUpFutures().isEmpty());
        assertTrue(distributionZoneManager.dataNodes().get(DEFAULT_ZONE_ID).getRevisionScaleDownFutures().isEmpty());

        int topVer2 = 20;

        int dataNodesRevision2 = dataNodesRevision1 + 2;

        return new TestSeveralScaleUpAndSeveralScaleDownDataObject(
                topVer2,
                dataNodesRevision2,
                topVerUpFut2,
                topVerDownFut2,
                dataNodesMeta,
                dataNodesUpFut3,
                dataNodesDownFut3
        );
    }

    @Test
    void testScaleUpAndThenScaleDown() throws ExecutionException, InterruptedException {
        System.out.println("test1_started");

        System.out.println("Added_nodes");

        CompletableFuture<Set<String>> dataNodesFut = distributionZoneManager.getDataNodes(DEFAULT_ZONE_ID, 5);

        AtomicReference<CompletableFuture<Void>> topVerFut = new AtomicReference<>();

        assertTrue(waitForCondition(() -> {
            topVerFut.set(distributionZoneManager.topVerFutures().get(5L));

            return topVerFut.get() != null;
        },
                3_000));

        assertFalse(topVerFut.get().isDone());

        topologyWatchListenerOnUpdate(Set.of("node0", "node1"), 100, 10);

        assertTrue(waitForCondition(() -> topVerFut.get().isDone(),
                3_000));

        AtomicReference<CompletableFuture<Void>> revisionFut = new AtomicReference<>();

        assertTrue(waitForCondition(() -> {
            DistributionZoneManager.DataNodes dataNodes = distributionZoneManager.dataNodes().get(DEFAULT_ZONE_ID);

            if (dataNodes == null) {
                return false;
            }

            revisionFut.set(dataNodes.getRevisionScaleUpFutures().get(10L));

            return revisionFut.get() != null;
        },
                3_000));

        assertFalse(revisionFut.get().isDone());

        dataNodesWatchListenerOnUpdate(DEFAULT_ZONE_ID, Set.of("node0", "node1"), true, 10, 11);

        assertTrue(waitForCondition(() -> revisionFut.get().isDone(),
                3_000));

        assertEquals(Set.of("node0", "node1"), dataNodesFut.get());

        assertTrue(distributionZoneManager.topVerFutures().isEmpty());
        assertTrue(distributionZoneManager.dataNodes().get(DEFAULT_ZONE_ID).getRevisionScaleUpFutures().isEmpty());
        assertTrue(distributionZoneManager.dataNodes().get(DEFAULT_ZONE_ID).getRevisionScaleDownFutures().isEmpty());

        System.out.println("Removed_nodes");

        dataNodesFut = distributionZoneManager.getDataNodes(DEFAULT_ZONE_ID, 106);

        topVerFut.set(null);

        assertTrue(waitForCondition(() -> {
            topVerFut.set(distributionZoneManager.topVerFutures().get(106L));

            return topVerFut.get() != null;
        },
                3_000));

        assertFalse(topVerFut.get().isDone());

        topologyWatchListenerOnUpdate(Set.of("node0"), 200, 12);

        assertTrue(waitForCondition(() -> topVerFut.get().isDone(),
                3_000));

        revisionFut.set(null);

        assertTrue(waitForCondition(() -> {
            DistributionZoneManager.DataNodes dataNodes = distributionZoneManager.dataNodes().get(DEFAULT_ZONE_ID);

            if (dataNodes == null) {
                return false;
            }

            revisionFut.set(dataNodes.getRevisionScaleDownFutures().get(12L));

            return revisionFut.get() != null;
        },
                3_000));

        assertFalse(revisionFut.get().isDone());

        dataNodesWatchListenerOnUpdate(DEFAULT_ZONE_ID, Set.of("node0"), false, 12, 13);

        assertTrue(waitForCondition(() -> revisionFut.get().isDone(),
                3_000));

        assertEquals(Set.of("node0"), dataNodesFut.get());

        assertTrue(distributionZoneManager.topVerFutures().isEmpty());
        assertTrue(distributionZoneManager.dataNodes().get(DEFAULT_ZONE_ID).getRevisionScaleUpFutures().isEmpty());
        assertTrue(distributionZoneManager.dataNodes().get(DEFAULT_ZONE_ID).getRevisionScaleDownFutures().isEmpty());
    }

    @Test
    void testAwaitingScaleUpOnly() throws Exception {
        distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder("zone1")
                                .dataNodesAutoAdjustScaleUp(0)
                                .dataNodesAutoAdjustScaleDown(Integer.MAX_VALUE)
                                .build()
                )
                .get(5, TimeUnit.SECONDS);

        int zoneId = distributionZoneManager.getZoneId("zone1");

        System.out.println("test1_started");

        System.out.println("Added_nodes");

        CompletableFuture<Set<String>> dataNodesFut = distributionZoneManager.getDataNodes(zoneId, 1);

        Set<String> nodes0 = Set.of("node0", "node1");

        topologyWatchListenerOnUpdate(nodes0, 1, 1);

        dataNodesWatchListenerOnUpdate(zoneId, nodes0, true, 1, 2);

        dataNodesFut.get(3, TimeUnit.SECONDS);

        dataNodesFut = distributionZoneManager.getDataNodes(zoneId, 2);

        Set<String> nodes1 = Set.of("node0");

        topologyWatchListenerOnUpdate(nodes1, 2, 2);

        dataNodesFut.get(3, TimeUnit.SECONDS);
    }

    @Test
    void testAwaitingScaleDownOnly() throws Exception {
        distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder("zone1")
                                .dataNodesAutoAdjustScaleUp(Integer.MAX_VALUE)
                                .dataNodesAutoAdjustScaleDown(0)
                                .build()
                )
                .get(5, TimeUnit.SECONDS);

        int zoneId = distributionZoneManager.getZoneId("zone1");

        CompletableFuture<Set<String>> dataNodesFut = distributionZoneManager.getDataNodes(zoneId, 1);

        Set<String> nodes0 = Set.of("node0", "node1");

        topologyWatchListenerOnUpdate(nodes0, 1, 1);

        dataNodesWatchListenerOnUpdate(zoneId, nodes0, true, 1, 2);

        dataNodesFut.get(3, TimeUnit.SECONDS);

        CompletableFuture<Set<String>> dataNodesFut1 = distributionZoneManager.getDataNodes(zoneId, 2);

        Set<String> nodes1 = Set.of("node0");

        topologyWatchListenerOnUpdate(nodes1, 2, 3);

        dataNodesWatchListenerOnUpdate(zoneId, nodes1, false, 3, 4);

        dataNodesFut1.get(3, TimeUnit.SECONDS);

        CompletableFuture<Set<String>> dataNodesFut2 = distributionZoneManager.getDataNodes(zoneId, 3);

        Set<String> nodes2 = Set.of("node0", "node1");

        topologyWatchListenerOnUpdate(nodes2, 3, 5);

        dataNodesFut2.get(3, TimeUnit.SECONDS);
    }

    @Test
    void testWithOutAwaiting() throws Exception {
        distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder("zone1")
                                .dataNodesAutoAdjustScaleUp(Integer.MAX_VALUE)
                                .dataNodesAutoAdjustScaleDown(Integer.MAX_VALUE)
                                .build()
                )
                .get(5, TimeUnit.SECONDS);

        int zoneId = distributionZoneManager.getZoneId("zone1");

        CompletableFuture<Set<String>> dataNodesFut = distributionZoneManager.getDataNodes(zoneId, 1);

        assertTrue(dataNodesFut.isDone());

        assertEquals(emptySet(), dataNodesFut.get(3, TimeUnit.SECONDS));
    }

    private void topologyWatchListenerOnUpdate(Set<String> nodes, long topVer, long rev) {
        byte[] newLogicalTopology = toBytes(nodes);
        byte[] newTopVer = toBytes(topVer);

        Entry newEntry0 = new EntryImpl(zonesLogicalTopologyKey().bytes(), newLogicalTopology, rev, 1);
        Entry newEntry1 = new EntryImpl(zonesLogicalTopologyVersionKey().bytes(), newTopVer, rev, 1);

        EntryEvent entryEvent0 = new EntryEvent(null, newEntry0);
        EntryEvent entryEvent1 = new EntryEvent(null, newEntry1);

        WatchEvent evt = new WatchEvent(List.of(entryEvent0, entryEvent1), rev);

        topologyWatchListener.onUpdate(evt);
    }

    private void dataNodesWatchListenerOnUpdate(int zoneId, Set<String> nodes, boolean isScaleUp, long scaleRevision, long rev) {
        byte[] newDataNodes = toBytes(nodes);
        byte[] newScaleRevision = toBytes(scaleRevision);

        Entry newEntry0 = new EntryImpl(zoneDataNodesKey(zoneId).bytes(), newDataNodes, rev, 1);
        Entry newEntry1 = new EntryImpl(
                isScaleUp ? zoneScaleUpChangeTriggerKey(zoneId).bytes() : zoneScaleDownChangeTriggerKey(zoneId).bytes(),
                newScaleRevision,
                rev,
                1);

        EntryEvent entryEvent0 = new EntryEvent(null, newEntry0);
        EntryEvent entryEvent1 = new EntryEvent(null, newEntry1);

        WatchEvent evt = new WatchEvent(List.of(entryEvent0, entryEvent1/*, entryEvent2*/), rev);

        dataNodesWatchListener.onUpdate(evt);
    }

    private void mockCmgLocalNodes() {
        when(cmgManager.logicalTopology()).thenReturn(completedFuture(logicalTopology.getLogicalTopology()));
    }
}
