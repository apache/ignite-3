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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.configuration.annotation.ConfigurationType.DISTRIBUTED;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_ZONE_ID;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_ZONE_NAME;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleDownChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleUpChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyVersionKey;
import static org.apache.ignite.internal.distributionzones.util.DistributionZonesTestUtil.assertDataNodesForZone;
import static org.apache.ignite.internal.distributionzones.util.DistributionZonesTestUtil.assertLogicalTopology;
import static org.apache.ignite.internal.distributionzones.util.DistributionZonesTestUtil.assertZoneScaleDownChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.util.DistributionZonesTestUtil.assertZoneScaleUpChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.util.DistributionZonesTestUtil.mockMetaStorageListener;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.util.ByteUtils.longToBytes;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
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
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneChange;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneConfiguration;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneView;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.EntryEvent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.command.MetaStorageCommandsFactory;
import org.apache.ignite.internal.metastorage.command.MultiInvokeCommand;
import org.apache.ignite.internal.metastorage.dsl.Iif;
import org.apache.ignite.internal.metastorage.impl.EntryImpl;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.metastorage.server.raft.MetaStorageListener;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.schema.configuration.TableChange;
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.apache.ignite.internal.schema.configuration.TableView;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

/**
 * Test scenarios for the distribution zone scale up and scale down.
 */
public class DistributionZoneManagerScaleUpTest {
    private static final String ZONE_NAME = "zone1";

    private DistributionZoneManager distributionZoneManager;

    private SimpleInMemoryKeyValueStorage keyValueStorage;

    private ConfigurationManager clusterCfgMgr;

    @Mock
    private LogicalTopologyServiceImpl logicalTopologyService;

    private LogicalTopology topology;

    private ClusterStateStorage clusterStateStorage;

    private VaultManager vaultMgr;

    private MetaStorageManager metaStorageManager;

    private WatchListener topologyWatchListener;

    private DistributionZonesConfiguration zonesConfiguration;

    @Mock
    private ClusterManagementGroupManager cmgManager;

    @Mock
    RaftGroupService metaStorageService;

    @BeforeEach
    public void setUp() {
        clusterCfgMgr = new ConfigurationManager(
                List.of(DistributionZonesConfiguration.KEY),
                Set.of(),
                new TestConfigurationStorage(DISTRIBUTED),
                List.of(),
                List.of()
        );

        zonesConfiguration = clusterCfgMgr.configurationRegistry()
                .getConfiguration(DistributionZonesConfiguration.KEY);

        metaStorageManager = mock(MetaStorageManager.class);

        cmgManager = mock(ClusterManagementGroupManager.class);

        clusterStateStorage = new TestClusterStateStorage();

        topology = new LogicalTopologyImpl(clusterStateStorage);

        LogicalTopologyServiceImpl logicalTopologyService = new LogicalTopologyServiceImpl(topology, cmgManager);

        vaultMgr = mock(VaultManager.class);

        TablesConfiguration tablesConfiguration = mock(TablesConfiguration.class);

        NamedConfigurationTree<TableConfiguration, TableView, TableChange> tables = mock(NamedConfigurationTree.class);

        when(tablesConfiguration.tables()).thenReturn(tables);

        NamedListView<TableView> value = mock(NamedListView.class);

        when(tables.value()).thenReturn(value);

        when(value.namedListKeys()).thenReturn(new ArrayList<>());

        distributionZoneManager = new DistributionZoneManager(
                zonesConfiguration,
                tablesConfiguration,
                metaStorageManager,
                logicalTopologyService,
                vaultMgr,
                "node"
        );

        clusterCfgMgr.start();

        mockVaultAppliedRevision(1);

        when(vaultMgr.get(zonesLogicalTopologyKey())).thenReturn(completedFuture(new VaultEntry(zonesLogicalTopologyKey(), null)));
        when(vaultMgr.put(any(), any())).thenReturn(completedFuture(null));

        doAnswer(invocation -> {
            ByteArray key = invocation.getArgument(0);

            WatchListener watchListener = invocation.getArgument(1);

            if (Arrays.equals(key.bytes(), zonesLogicalTopologyVersionKey().bytes())) {
                topologyWatchListener = watchListener;
            }

            return null;
        }).when(metaStorageManager).registerExactWatch(any(), any());

        AtomicLong raftIndex = new AtomicLong();

        keyValueStorage = spy(new SimpleInMemoryKeyValueStorage("test"));

        MetaStorageListener metaStorageListener = new MetaStorageListener(keyValueStorage);

        metaStorageService = mock(RaftGroupService.class);

        mockMetaStorageListener(raftIndex, metaStorageListener, metaStorageService, metaStorageManager);
    }

    @AfterEach
    public void tearDown() throws Exception {
        distributionZoneManager.stop();

        clusterCfgMgr.stop();

        keyValueStorage.close();

        clusterStateStorage.destroy();
    }

    @Test
    void testDataNodesPropagationAfterScaleUpTriggered() throws Exception {
        ClusterNode node1 = new ClusterNode("1", "name1", new NetworkAddress("localhost", 123));

        topology.putNode(node1);

        Set<ClusterNode> clusterNodes = Set.of(node1);

        mockVaultZonesLogicalTopologyKey(clusterNodes.stream().map(ClusterNode::name).collect(Collectors.toSet()));

        mockCmgLocalNodes();

        distributionZoneManager.start();

        assertDataNodesForZone(DEFAULT_ZONE_ID, clusterNodes.stream().map(ClusterNode::name).collect(Collectors.toSet()), keyValueStorage);

        ClusterNode node2 = new ClusterNode("2", "name2", new NetworkAddress("localhost", 123));

        topology.putNode(node2);

        Set<ClusterNode> clusterNodes2 = Set.of(node1, node2);

        assertLogicalTopology(clusterNodes2, keyValueStorage);

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjustScaleUp(0).build()
        ).get();

        assertDataNodesForZone(1, clusterNodes.stream().map(ClusterNode::name).collect(Collectors.toSet()), keyValueStorage);

        assertZoneScaleUpChangeTriggerKey(1, 1, keyValueStorage);

        watchListenerOnUpdate(topology.getLogicalTopology().nodes().stream().map(ClusterNode::name).collect(Collectors.toSet()), 2);

        assertDataNodesForZone(1, clusterNodes2.stream().map(ClusterNode::name).collect(Collectors.toSet()), keyValueStorage);
    }

    @Test
    void testDataNodesPropagationAfterScaleDownTriggered() throws Exception {
        ClusterNode node1 = new ClusterNode("1", "name1", new NetworkAddress("localhost", 123));

        ClusterNode node2 = new ClusterNode("2", "name2", new NetworkAddress("localhost", 123));

        topology.putNode(node1);

        topology.putNode(node2);

        Set<ClusterNode> clusterNodes = Set.of(node1, node2);

        mockVaultZonesLogicalTopologyKey(clusterNodes.stream().map(ClusterNode::name).collect(Collectors.toSet()));

        mockCmgLocalNodes();

        distributionZoneManager.start();

        assertDataNodesForZone(DEFAULT_ZONE_ID, clusterNodes.stream().map(ClusterNode::name).collect(Collectors.toSet()), keyValueStorage);

        topology.removeNodes(Set.of(node2));

        Set<ClusterNode> clusterNodes2 = Set.of(node1);

        assertLogicalTopology(clusterNodes2, keyValueStorage);

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjustScaleDown(0).build()
        ).get();

        assertDataNodesForZone(1, clusterNodes.stream().map(ClusterNode::name).collect(Collectors.toSet()), keyValueStorage);

        assertZoneScaleDownChangeTriggerKey(1, 1, keyValueStorage);

        watchListenerOnUpdate(topology.getLogicalTopology().nodes().stream().map(ClusterNode::name).collect(Collectors.toSet()), 2);

        assertDataNodesForZone(1, clusterNodes2.stream().map(ClusterNode::name).collect(Collectors.toSet()), keyValueStorage);
    }

    @Test
    void testDataNodesPropagationForDefaultZoneAfterScaleUpTriggered() throws Exception {
        ClusterNode node1 = new ClusterNode("1", "name1", new NetworkAddress("localhost", 123));

        topology.putNode(node1);

        Set<ClusterNode> clusterNodes = Set.of(node1);

        mockVaultZonesLogicalTopologyKey(clusterNodes.stream().map(ClusterNode::name).collect(Collectors.toSet()));

        mockCmgLocalNodes();

        distributionZoneManager.start();

        assertDataNodesForZone(DEFAULT_ZONE_ID, clusterNodes.stream().map(ClusterNode::name).collect(Collectors.toSet()), keyValueStorage);

        ClusterNode node2 = new ClusterNode("2", "name2", new NetworkAddress("localhost", 123));

        topology.putNode(node2);

        Set<ClusterNode> clusterNodes2 = Set.of(node1, node2);

        assertLogicalTopology(clusterNodes2, keyValueStorage);

        distributionZoneManager.alterZone(
                DEFAULT_ZONE_NAME,
                new DistributionZoneConfigurationParameters.Builder(DEFAULT_ZONE_NAME).dataNodesAutoAdjustScaleUp(0).build()
        ).get();

        watchListenerOnUpdate(topology.getLogicalTopology().nodes().stream().map(ClusterNode::name).collect(Collectors.toSet()), 2);

        assertDataNodesForZone(DEFAULT_ZONE_ID, clusterNodes2.stream().map(ClusterNode::name).collect(Collectors.toSet()), keyValueStorage);
    }

    @Test
    void testDataNodesPropagationForDefaultZoneAfterScaleDownTriggered() throws Exception {
        ClusterNode node1 = new ClusterNode("1", "name1", new NetworkAddress("localhost", 123));

        ClusterNode node2 = new ClusterNode("2", "name2", new NetworkAddress("localhost", 123));

        topology.putNode(node1);

        topology.putNode(node2);

        Set<ClusterNode> clusterNodes = Set.of(node1, node2);

        mockVaultZonesLogicalTopologyKey(clusterNodes.stream().map(ClusterNode::name).collect(Collectors.toSet()));

        mockCmgLocalNodes();

        distributionZoneManager.start();

        assertDataNodesForZone(DEFAULT_ZONE_ID, clusterNodes.stream().map(ClusterNode::name).collect(Collectors.toSet()), keyValueStorage);

        topology.removeNodes(Set.of(node2));

        Set<ClusterNode> clusterNodes2 = Set.of(node1);

        assertLogicalTopology(clusterNodes2, keyValueStorage);

        distributionZoneManager.alterZone(
                DEFAULT_ZONE_NAME,
                new DistributionZoneConfigurationParameters.Builder(DEFAULT_ZONE_NAME).dataNodesAutoAdjustScaleDown(0).build()
        ).get();

        watchListenerOnUpdate(topology.getLogicalTopology().nodes().stream().map(ClusterNode::name).collect(Collectors.toSet()), 2);

        assertDataNodesForZone(DEFAULT_ZONE_ID, clusterNodes2.stream().map(ClusterNode::name).collect(Collectors.toSet()), keyValueStorage);
    }

    @Test
    void testDropZoneDoNotPropagateDataNodesAfterScaleUp() throws Exception {
        ClusterNode node1 = new ClusterNode("1", "name1", new NetworkAddress("localhost", 123));

        topology.putNode(node1);

        Set<ClusterNode> clusterNodes = Set.of(node1);

        mockVaultZonesLogicalTopologyKey(clusterNodes.stream().map(ClusterNode::name).collect(Collectors.toSet()));

        mockCmgLocalNodes();

        distributionZoneManager.start();

        ClusterNode node2 = new ClusterNode("2", "name2", new NetworkAddress("localhost", 123));

        topology.putNode(node2);

        Set<ClusterNode> clusterNodes2 = Set.of(node1, node2);

        assertLogicalTopology(clusterNodes2, keyValueStorage);

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjustScaleUp(0).build()
        ).get();

        assertDataNodesForZone(1, clusterNodes.stream().map(ClusterNode::name).collect(Collectors.toSet()), keyValueStorage);

        assertZoneScaleUpChangeTriggerKey(1, 1, keyValueStorage);

        distributionZoneManager.dropZone(ZONE_NAME).get();

        watchListenerOnUpdate(topology.getLogicalTopology().nodes().stream().map(ClusterNode::name).collect(Collectors.toSet()), 2);

        assertNotEqualsDataNodesForZone(1, clusterNodes2.stream().map(ClusterNode::name).collect(Collectors.toSet()));
    }

    @Test
    void testDropZoneDoNotPropagateDataNodesAfterScaleDown() throws Exception {
        ClusterNode node1 = new ClusterNode("1", "name1", new NetworkAddress("localhost", 123));

        ClusterNode node2 = new ClusterNode("2", "name2", new NetworkAddress("localhost", 123));

        topology.putNode(node1);

        topology.putNode(node2);

        Set<ClusterNode> clusterNodes = Set.of(node1, node2);

        mockVaultZonesLogicalTopologyKey(clusterNodes.stream().map(ClusterNode::name).collect(Collectors.toSet()));

        mockCmgLocalNodes();

        distributionZoneManager.start();

        topology.removeNodes(Set.of(node2));

        Set<ClusterNode> clusterNodes2 = Set.of(node1);

        assertLogicalTopology(clusterNodes2, keyValueStorage);

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjustScaleDown(0).build()
        ).get();

        assertDataNodesForZone(1, clusterNodes.stream().map(ClusterNode::name).collect(Collectors.toSet()), keyValueStorage);

        assertZoneScaleDownChangeTriggerKey(1, 1, keyValueStorage);

        distributionZoneManager.dropZone(ZONE_NAME).get();

        watchListenerOnUpdate(topology.getLogicalTopology().nodes().stream().map(ClusterNode::name).collect(Collectors.toSet()), 2);

        assertNotEqualsDataNodesForZone(1, clusterNodes2.stream().map(ClusterNode::name).collect(Collectors.toSet()));
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
        mockVaultZonesLogicalTopologyKey(Set.of());

        mockCmgLocalNodes();

        distributionZoneManager.start();

        assertLogicalTopology(Set.of(), keyValueStorage);

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjustScaleUp(0).build()
        ).get();

        assertDataNodesForZone(1, Set.of(), keyValueStorage);

        assertZoneScaleDownChangeTriggerKey(1, 1, keyValueStorage);

        ClusterNode node1 = new ClusterNode("1", "name1", new NetworkAddress("localhost", 123));

        topology.putNode(node1);

        assertLogicalTopology(Set.of(node1), keyValueStorage);

        watchListenerOnUpdate(topology.getLogicalTopology().nodes().stream().map(ClusterNode::name).collect(Collectors.toSet()), 2);

        assertDataNodesForZone(1, Set.of(node1.name()), keyValueStorage);
    }

    @Test
    void testUpdateZoneScaleUpTriggersDataNodePropagation() throws Exception {
        mockVaultZonesLogicalTopologyKey(Set.of());

        mockCmgLocalNodes();

        distributionZoneManager.start();

        assertLogicalTopology(Set.of(), keyValueStorage);

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjustScaleUp(100).build()
        ).get();

        assertDataNodesForZone(1, Set.of(), keyValueStorage);

        assertZoneScaleDownChangeTriggerKey(1, 1, keyValueStorage);

        ClusterNode node1 = new ClusterNode("1", "name1", new NetworkAddress("localhost", 123));

        topology.putNode(node1);

        assertLogicalTopology(Set.of(node1), keyValueStorage);

        watchListenerOnUpdate(topology.getLogicalTopology().nodes().stream().map(ClusterNode::name).collect(Collectors.toSet()), 2);

        assertDataNodesForZone(1, Set.of(), keyValueStorage);

        distributionZoneManager.alterZone(
                ZONE_NAME,
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjustScaleUp(0).build()
        ).get();

        assertDataNodesForZone(1, Set.of(node1.name()), keyValueStorage);
    }

    @Test
    void testUpdateZoneScaleDownTriggersDataNodePropagation() throws Exception {
        ClusterNode node1 = new ClusterNode("1", "name1", new NetworkAddress("localhost", 123));

        topology.putNode(node1);

        mockVaultZonesLogicalTopologyKey(Set.of(node1.name()));

        mockCmgLocalNodes();

        distributionZoneManager.start();

        assertLogicalTopology(Set.of(node1), keyValueStorage);

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjustScaleDown(100).build()
        ).get();

        assertDataNodesForZone(1, Set.of(node1.name()), keyValueStorage);

        assertZoneScaleDownChangeTriggerKey(1, 1, keyValueStorage);

        topology.removeNodes(Set.of(node1));

        watchListenerOnUpdate(topology.getLogicalTopology().nodes().stream().map(ClusterNode::name).collect(Collectors.toSet()), 2);

        assertDataNodesForZone(1, Set.of(node1.name()), keyValueStorage);

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
        mockVaultZonesLogicalTopologyKey(Set.of());

        mockCmgLocalNodes();

        distributionZoneManager.start();

        assertLogicalTopology(Set.of(), keyValueStorage);

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjustScaleUp(100).build()
        ).get();

        assertDataNodesForZone(1, Set.of(), keyValueStorage);

        assertZoneScaleDownChangeTriggerKey(1, 1, keyValueStorage);

        ClusterNode node1 = new ClusterNode("1", "name1", new NetworkAddress("localhost", 123));

        topology.putNode(node1);

        assertLogicalTopology(Set.of(node1), keyValueStorage);

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        assertNull(zoneState.scaleUpTask());

        watchListenerOnUpdate(topology.getLogicalTopology().nodes().stream().map(ClusterNode::name).collect(Collectors.toSet()), 2);

        assertTrue(waitForCondition(() -> zoneState.scaleUpTask() != null, 1000L));

        distributionZoneManager.alterZone(
                ZONE_NAME,
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjustScaleUp(INFINITE_TIMER_VALUE).build()
        ).get();

        assertTrue(waitForCondition(() -> zoneState.scaleUpTask().isCancelled(), 1000L));
    }

    @Test
    void testScaleDownSetToMaxInt() throws Exception {
        ClusterNode node1 = new ClusterNode("1", "name1", new NetworkAddress("localhost", 123));

        topology.putNode(node1);

        mockVaultZonesLogicalTopologyKey(Set.of(node1.name()));

        mockCmgLocalNodes();

        distributionZoneManager.start();

        assertLogicalTopology(Set.of(node1), keyValueStorage);

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjustScaleDown(100).build()
        ).get();

        assertDataNodesForZone(1, Set.of(node1.name()), keyValueStorage);

        assertZoneScaleDownChangeTriggerKey(1, 1, keyValueStorage);

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        assertNull(zoneState.scaleDownTask());

        topology.removeNodes(Set.of(node1));

        watchListenerOnUpdate(topology.getLogicalTopology().nodes().stream().map(ClusterNode::name).collect(Collectors.toSet()), 2);

        assertTrue(waitForCondition(() -> zoneState.scaleDownTask() != null, 1000L));

        distributionZoneManager.alterZone(
                ZONE_NAME,
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE).build()
        ).get();

        assertTrue(waitForCondition(() -> zoneState.scaleDownTask().isCancelled(), 1000L));
    }

    @Test
    void testScaleUpDidNotChangeDataNodesWhenTriggerKeyWasConcurrentlyChanged() throws Exception {
        mockVaultZonesLogicalTopologyKey(Set.of());

        mockCmgLocalNodes();

        distributionZoneManager.start();

        assertLogicalTopology(Set.of(), keyValueStorage);

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjustScaleUp(0).build()
        ).get();

        assertDataNodesForZone(1, Set.of(), keyValueStorage);

        assertZoneScaleDownChangeTriggerKey(1, 1, keyValueStorage);

        ClusterNode node1 = new ClusterNode("1", "name1", new NetworkAddress("localhost", 123));

        topology.putNode(node1);

        assertLogicalTopology(Set.of(node1), keyValueStorage);

        MetaStorageCommandsFactory commandsFactory = new MetaStorageCommandsFactory();

        lenient().doAnswer(invocationClose -> {
            // First invoke of data nodes propagation on scale up do not pass because of trigger key revision condition.
            // We had scaleUpChangeTriggerKey = 1 from ms, but after that it was changed to 100, so on the next call
            // invoke won't be even triggered because event become stale.
            keyValueStorage.put(zoneScaleUpChangeTriggerKey(1).bytes(), longToBytes(100));

            Iif iif = invocationClose.getArgument(0);

            MultiInvokeCommand multiInvokeCommand = commandsFactory.multiInvokeCommand().iif(iif).build();

            return metaStorageService.run(multiInvokeCommand);
        }).when(metaStorageManager).invoke(any());

        watchListenerOnUpdate(topology.getLogicalTopology().nodes().stream().map(ClusterNode::name).collect(Collectors.toSet()), 2);

        assertZoneScaleUpChangeTriggerKey(100, 1, keyValueStorage);

        assertDataNodesForZone(1, Set.of(), keyValueStorage);
    }

    @Test
    void testScaleDownDidNotChangeDataNodesWhenTriggerKeyWasConcurrentlyChanged() throws Exception {
        ClusterNode node1 = new ClusterNode("1", "name1", new NetworkAddress("localhost", 123));

        topology.putNode(node1);

        mockVaultZonesLogicalTopologyKey(Set.of(node1.name()));

        mockCmgLocalNodes();

        distributionZoneManager.start();

        assertLogicalTopology(Set.of(node1), keyValueStorage);

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjustScaleDown(0).build()
        ).get();

        assertDataNodesForZone(1, Set.of(node1.name()), keyValueStorage);

        assertZoneScaleDownChangeTriggerKey(1, 1, keyValueStorage);

        topology.removeNodes(Set.of(node1));

        MetaStorageCommandsFactory commandsFactory = new MetaStorageCommandsFactory();

        lenient().doAnswer(invocationClose -> {
            // First invoke of data nodes propagation on scale down do not pass because of trigger key revision condition.
            // We had scaleDownChangeTriggerKey = 1 from ms, but after that it was changed to 100, so on the next call
            // invoke won't be even triggered because event become stale.
            keyValueStorage.put(zoneScaleDownChangeTriggerKey(1).bytes(), longToBytes(100));

            Iif iif = invocationClose.getArgument(0);

            MultiInvokeCommand multiInvokeCommand = commandsFactory.multiInvokeCommand().iif(iif).build();

            return metaStorageService.run(multiInvokeCommand);
        }).when(metaStorageManager).invoke(any());

        watchListenerOnUpdate(topology.getLogicalTopology().nodes().stream().map(ClusterNode::name).collect(Collectors.toSet()), 2);

        assertDataNodesForZone(1, Set.of(node1.name()), keyValueStorage);

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
        ClusterNode node1 = new ClusterNode("1", "A", new NetworkAddress("localhost", 123));
        ClusterNode node2 = new ClusterNode("2", "B", new NetworkAddress("localhost", 123));
        ClusterNode node3 = new ClusterNode("3", "C", new NetworkAddress("localhost", 123));

        topology.putNode(node1);
        topology.putNode(node2);
        topology.putNode(node3);

        Set<ClusterNode> clusterNodes = Set.of(node1, node2, node3);

        mockVaultZonesLogicalTopologyKey(clusterNodes.stream().map(ClusterNode::name).collect(Collectors.toSet()));

        mockCmgLocalNodes();

        distributionZoneManager.start();

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME)
                        .dataNodesAutoAdjustScaleUp(0)
                        .dataNodesAutoAdjustScaleDown(0)
                        .build()
        ).get();

        assertDataNodesForZone(1, clusterNodes.stream().map(ClusterNode::name).collect(Collectors.toSet()), keyValueStorage);

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

    private void mockVaultAppliedRevision(long revision) {
        when(metaStorageManager.appliedRevision()).thenReturn(revision);
    }

    private void watchListenerOnUpdate(Set<String> nodes, long rev) {
        byte[] newLogicalTopology = toBytes(nodes);
        byte[] newTopVer = toBytes(1L);

        Entry newEntry0 = new EntryImpl(zonesLogicalTopologyKey().bytes(), newLogicalTopology, rev, 1);
        Entry newEntry1 = new EntryImpl(zonesLogicalTopologyVersionKey().bytes(), newTopVer, rev, 1);

        EntryEvent entryEvent0 = new EntryEvent(null, newEntry0);
        EntryEvent entryEvent1 = new EntryEvent(null, newEntry1);

        WatchEvent evt = new WatchEvent(List.of(entryEvent0, entryEvent1), rev);

        topologyWatchListener.onUpdate(evt);
    }

    private void mockCmgLocalNodes() {
        when(cmgManager.logicalTopology()).thenReturn(completedFuture(topology.getLogicalTopology()));
    }

    private void mockVaultZonesLogicalTopologyKey(Set<String> nodes) {
        byte[] newLogicalTopology = toBytes(nodes);

        when(vaultMgr.get(zonesLogicalTopologyKey()))
                .thenReturn(completedFuture(new VaultEntry(zonesLogicalTopologyKey(), newLogicalTopology)));
    }
}
