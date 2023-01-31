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
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleDownChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleUpChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyKey;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
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
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
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
import org.apache.ignite.internal.metastorage.command.GetAllCommand;
import org.apache.ignite.internal.metastorage.command.GetCommand;
import org.apache.ignite.internal.metastorage.command.MetaStorageCommandsFactory;
import org.apache.ignite.internal.metastorage.command.MultiInvokeCommand;
import org.apache.ignite.internal.metastorage.command.MultipleEntryResponse;
import org.apache.ignite.internal.metastorage.command.SingleEntryResponse;
import org.apache.ignite.internal.metastorage.dsl.Iif;
import org.apache.ignite.internal.metastorage.impl.EntryImpl;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.metastorage.server.raft.MetaStorageListener;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.ReadCommand;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.schema.configuration.TableChange;
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.apache.ignite.internal.schema.configuration.TableView;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteInternalException;
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
public class DistributionZoneManagerScaleUpScaleDownTest {
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

    private WatchListener watchListener;

    private DistributionZonesConfiguration zonesConfiguration;

    @Mock
    private ClusterManagementGroupManager cmgManager;

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
            watchListener = invocation.getArgument(1);

            return null;
        }).when(metaStorageManager).registerExactWatch(any(), any());

        AtomicLong raftIndex = new AtomicLong();

        keyValueStorage = spy(new SimpleInMemoryKeyValueStorage("test"));

        MetaStorageListener metaStorageListener = new MetaStorageListener(keyValueStorage);

        RaftGroupService metaStorageService = mock(RaftGroupService.class);

        // Delegate directly to listener.
        lenient().doAnswer(
                invocationClose -> {
                    Command cmd = invocationClose.getArgument(0);

                    long commandIndex = raftIndex.incrementAndGet();

                    CompletableFuture<Serializable> res = new CompletableFuture<>();

                    CommandClosure<WriteCommand> clo = new CommandClosure<>() {
                        /** {@inheritDoc} */
                        @Override
                        public long index() {
                            return commandIndex;
                        }

                        /** {@inheritDoc} */
                        @Override
                        public WriteCommand command() {
                            return (WriteCommand) cmd;
                        }

                        /** {@inheritDoc} */
                        @Override
                        public void result(@Nullable Serializable r) {
                            if (r instanceof Throwable) {
                                res.completeExceptionally((Throwable) r);
                            } else {
                                res.complete(r);
                            }
                        }
                    };

                    try {
                        metaStorageListener.onWrite(List.of(clo).iterator());
                    } catch (Throwable e) {
                        res.completeExceptionally(new IgniteInternalException(e));
                    }

                    return res;
                }
        ).when(metaStorageService).run(any(WriteCommand.class));

        lenient().doAnswer(
                invocationClose -> {
                    Command cmd = invocationClose.getArgument(0);

                    long commandIndex = raftIndex.incrementAndGet();

                    CompletableFuture<Serializable> res = new CompletableFuture<>();

                    CommandClosure<ReadCommand> clo = new CommandClosure<>() {
                        /** {@inheritDoc} */
                        @Override
                        public long index() {
                            return commandIndex;
                        }

                        /** {@inheritDoc} */
                        @Override
                        public ReadCommand command() {
                            return (ReadCommand) cmd;
                        }

                        /** {@inheritDoc} */
                        @Override
                        public void result(@Nullable Serializable r) {
                            if (r instanceof Throwable) {
                                res.completeExceptionally((Throwable) r);
                            } else {
                                res.complete(r);
                            }
                        }
                    };

                    try {
                        metaStorageListener.onRead(List.of(clo).iterator());
                    } catch (Throwable e) {
                        res.completeExceptionally(new IgniteInternalException(e));
                    }

                    return res;
                }
        ).when(metaStorageService).run(any(ReadCommand.class));

        MetaStorageCommandsFactory commandsFactory = new MetaStorageCommandsFactory();

        lenient().doAnswer(invocationClose -> {
            Iif iif = invocationClose.getArgument(0);

            MultiInvokeCommand multiInvokeCommand = commandsFactory.multiInvokeCommand().iif(iif).build();

            return metaStorageService.run(multiInvokeCommand);
        }).when(metaStorageManager).invoke(any());

        lenient().doAnswer(invocationClose -> {
            Set<ByteArray> keysSet = invocationClose.getArgument(0);

            GetAllCommand getAllCommand = commandsFactory.getAllCommand().keys(
                    keysSet.stream().map(ByteArray::bytes).collect(Collectors.toList())
            ).revision(0).build();

            return metaStorageService.run(getAllCommand).thenApply(bi -> {
                MultipleEntryResponse resp = (MultipleEntryResponse) bi;

                Map<ByteArray, Entry> res = new HashMap<>();

                for (SingleEntryResponse e : resp.entries()) {
                    ByteArray key = new ByteArray(e.key());

                    res.put(key, new EntryImpl(key.bytes(), e.value(), e.revision(), e.updateCounter()));
                }

                return res;
            });
        }).when(metaStorageManager).getAll(any());

        lenient().doAnswer(invocationClose -> {
            ByteArray key = invocationClose.getArgument(0);

            GetCommand getCommand = commandsFactory.getCommand().key(key.bytes()).build();

            return metaStorageService.run(getCommand).thenApply(bi -> {
                SingleEntryResponse resp = (SingleEntryResponse) bi;

                return new EntryImpl(resp.key(), resp.value(), resp.revision(), resp.updateCounter());
            });
        }).when(metaStorageManager).get(any());
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

        mockCmgLocalNodes(1L, clusterNodes);

        distributionZoneManager.start();

        assertDataNodesForZone(DEFAULT_ZONE_ID, clusterNodes.stream().map(ClusterNode::name).collect(Collectors.toSet()));

        ClusterNode node2 = new ClusterNode("2", "name2", new NetworkAddress("localhost", 123));

        topology.putNode(node2);

        Set<ClusterNode> clusterNodes2 = Set.of(node1, node2);

        mockCmgLocalNodes(1L, clusterNodes2);

        assertLogicalTopology(clusterNodes2);

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjustScaleUp(1).build()
        ).get();

        assertDataNodesForZone(1, clusterNodes.stream().map(ClusterNode::name).collect(Collectors.toSet()));

        assertZoneScaleUpChangeTriggerKey(1, 1);

        watchListenerOnUpdate(topology.getLogicalTopology().nodes().stream().map(ClusterNode::name).collect(Collectors.toSet()), 2);

        assertDataNodesForZone(1, clusterNodes2.stream().map(ClusterNode::name).collect(Collectors.toSet()));
    }

    @Test
    void testDataNodesPropagationAfterScaleDownTriggered() throws Exception {
        ClusterNode node1 = new ClusterNode("1", "name1", new NetworkAddress("localhost", 123));

        ClusterNode node2 = new ClusterNode("2", "name2", new NetworkAddress("localhost", 123));

        topology.putNode(node1);

        topology.putNode(node2);

        Set<ClusterNode> clusterNodes = Set.of(node1, node2);

        mockVaultZonesLogicalTopologyKey(clusterNodes.stream().map(ClusterNode::name).collect(Collectors.toSet()));

        mockCmgLocalNodes(2L, clusterNodes);

        distributionZoneManager.start();

        assertDataNodesForZone(DEFAULT_ZONE_ID, clusterNodes.stream().map(ClusterNode::name).collect(Collectors.toSet()));

        topology.removeNodes(Set.of(node2));

        Set<ClusterNode> clusterNodes2 = Set.of(node1);

        mockCmgLocalNodes(3L, clusterNodes2);

        assertLogicalTopology(clusterNodes2);

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjustScaleDown(0).build()
        ).get();

        assertDataNodesForZone(1, clusterNodes.stream().map(ClusterNode::name).collect(Collectors.toSet()));

        assertZoneScaleDownChangeTriggerKey(1, 1);

        watchListenerOnUpdate(topology.getLogicalTopology().nodes().stream().map(ClusterNode::name).collect(Collectors.toSet()), 2);

        assertDataNodesForZone(1, clusterNodes2.stream().map(ClusterNode::name).collect(Collectors.toSet()));
    }

    @Test
    void testDataNodesPropagationForDefaultZoneAfterScaleUpTriggered() throws Exception {
        ClusterNode node1 = new ClusterNode("1", "name1", new NetworkAddress("localhost", 123));

        topology.putNode(node1);

        Set<ClusterNode> clusterNodes = Set.of(node1);

        mockVaultZonesLogicalTopologyKey(clusterNodes.stream().map(ClusterNode::name).collect(Collectors.toSet()));

        mockCmgLocalNodes(1L, clusterNodes);

        distributionZoneManager.start();

        assertDataNodesForZone(DEFAULT_ZONE_ID, clusterNodes.stream().map(ClusterNode::name).collect(Collectors.toSet()));

        ClusterNode node2 = new ClusterNode("2", "name2", new NetworkAddress("localhost", 123));

        topology.putNode(node2);

        Set<ClusterNode> clusterNodes2 = Set.of(node1, node2);

        mockCmgLocalNodes(1L, clusterNodes2);

        assertLogicalTopology(clusterNodes2);

        distributionZoneManager.alterZone(
                DEFAULT_ZONE_NAME,
                new DistributionZoneConfigurationParameters.Builder(DEFAULT_ZONE_NAME).dataNodesAutoAdjustScaleUp(0).build()
        ).get();

        watchListenerOnUpdate(topology.getLogicalTopology().nodes().stream().map(ClusterNode::name).collect(Collectors.toSet()), 2);

        assertDataNodesForZone(DEFAULT_ZONE_ID, clusterNodes2.stream().map(ClusterNode::name).collect(Collectors.toSet()));
    }

    @Test
    void testDataNodesPropagationForDefaultZoneAfterScaleDownTriggered() throws Exception {
        ClusterNode node1 = new ClusterNode("1", "name1", new NetworkAddress("localhost", 123));

        ClusterNode node2 = new ClusterNode("2", "name2", new NetworkAddress("localhost", 123));

        topology.putNode(node1);

        topology.putNode(node2);

        Set<ClusterNode> clusterNodes = Set.of(node1, node2);

        mockVaultZonesLogicalTopologyKey(clusterNodes.stream().map(ClusterNode::name).collect(Collectors.toSet()));

        mockCmgLocalNodes(2L, clusterNodes);

        distributionZoneManager.start();

        assertDataNodesForZone(DEFAULT_ZONE_ID, clusterNodes.stream().map(ClusterNode::name).collect(Collectors.toSet()));

        topology.removeNodes(Set.of(node2));

        Set<ClusterNode> clusterNodes2 = Set.of(node1);

        mockCmgLocalNodes(3L, clusterNodes2);

        assertLogicalTopology(clusterNodes2);

        distributionZoneManager.alterZone(
                DEFAULT_ZONE_NAME,
                new DistributionZoneConfigurationParameters.Builder(DEFAULT_ZONE_NAME).dataNodesAutoAdjustScaleDown(0).build()
        ).get();

        watchListenerOnUpdate(topology.getLogicalTopology().nodes().stream().map(ClusterNode::name).collect(Collectors.toSet()), 2);

        assertDataNodesForZone(DEFAULT_ZONE_ID, clusterNodes2.stream().map(ClusterNode::name).collect(Collectors.toSet()));
    }

    @Test
    void testDropZoneDoNotPropagateDataNodesAfterScaleUp() throws Exception {
        ClusterNode node1 = new ClusterNode("1", "name1", new NetworkAddress("localhost", 123));

        topology.putNode(node1);

        Set<ClusterNode> clusterNodes = Set.of(node1);

        mockVaultZonesLogicalTopologyKey(clusterNodes.stream().map(ClusterNode::name).collect(Collectors.toSet()));

        mockCmgLocalNodes(1L, clusterNodes);

        distributionZoneManager.start();

        ClusterNode node2 = new ClusterNode("2", "name2", new NetworkAddress("localhost", 123));

        topology.putNode(node2);

        Set<ClusterNode> clusterNodes2 = Set.of(node1, node2);

        mockCmgLocalNodes(1L, clusterNodes2);

        assertLogicalTopology(clusterNodes2);

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjustScaleUp(0).build()
        ).get();

        assertDataNodesForZone(1, clusterNodes.stream().map(ClusterNode::name).collect(Collectors.toSet()));

        assertZoneScaleUpChangeTriggerKey(1, 1);

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

        mockCmgLocalNodes(2L, clusterNodes);

        distributionZoneManager.start();

        topology.removeNodes(Set.of(node2));

        Set<ClusterNode> clusterNodes2 = Set.of(node1);

        mockCmgLocalNodes(3L, clusterNodes2);

        assertLogicalTopology(clusterNodes2);

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjustScaleDown(0).build()
        ).get();

        assertDataNodesForZone(1, clusterNodes.stream().map(ClusterNode::name).collect(Collectors.toSet()));

        assertZoneScaleDownChangeTriggerKey(1, 1);

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

        assertZoneScaleUpChangeTriggerKey(3, 1);

        assertDataNodesForZone(1, Set.of("A", "B", "C", "D", "E"));

        out2.countDown();

        in1.countDown();

        //Waiting for the first scheduler ends it work.
        out1.countDown();

        // Assert that nothing has been changed.
        assertZoneScaleUpChangeTriggerKey(3, 1);

        assertDataNodesForZone(1, Set.of("A", "B", "C", "D", "E"));
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

        assertZoneScaleDownChangeTriggerKey(3, 1);

        assertDataNodesForZone(1, Set.of("A"));

        out2.countDown();

        in1.countDown();

        //Waiting for the first scheduler ends it work.
        out1.countDown();

        // Assert that nothing has been changed.
        assertZoneScaleDownChangeTriggerKey(3, 1);

        assertDataNodesForZone(1, Set.of("A"));
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

        assertZoneScaleUpChangeTriggerKey(2, 1);

        assertDataNodesForZone(1, Set.of("A", "B", "C", "D"));

        // Second task is run and we await that data nodes will be changed from ["A", "B", "C", "D"] to ["A", "B", "C", "D", "E"]
        in2.countDown();

        assertZoneScaleUpChangeTriggerKey(3, 1);

        assertDataNodesForZone(1, Set.of("A", "B", "C", "D", "E"));

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

        assertZoneScaleDownChangeTriggerKey(2, 1);

        assertDataNodesForZone(1, Set.of("A", "C"));

        // Second task is run and we await that data nodes will be changed from ["A", "C"] to ["A"]
        in2.countDown();

        assertZoneScaleDownChangeTriggerKey(3, 1);

        assertDataNodesForZone(1, Set.of("A"));

        out1.countDown();
    }

    @Test
    void test1_1() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.addNodesToDataNodes(Set.of("D"), 3);
        zoneState.removeNodesFromDataNodes(Set.of("C"), 7);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 3);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 7);

        assertDataNodesForZone(1, Set.of("A", "B", "D"));
    }

    @Test
    void test1_2() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.addNodesToDataNodes(Set.of("D"), 3);
        zoneState.removeNodesFromDataNodes(Set.of("C"), 7);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 7);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 3);

        assertDataNodesForZone(1, Set.of("A", "B", "D"));
    }

    @Test
    void test1_3() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.removeNodesFromDataNodes(Set.of("C"), 3);
        zoneState.addNodesToDataNodes(Set.of("D"), 7);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 3);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 7);

        assertDataNodesForZone(1, Set.of("A", "B", "D"));
    }

    @Test
    void test1_4() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.removeNodesFromDataNodes(Set.of("C"), 3);
        zoneState.addNodesToDataNodes(Set.of("D"), 7);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 7);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 3);

        assertDataNodesForZone(1, Set.of("A", "B", "D"));
    }

    @Test
    void test2_1() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.addNodesToDataNodes(Set.of("D"), 3);
        zoneState.removeNodesFromDataNodes(Set.of("D"), 7);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 3);
        assertDataNodesForZone(1, Set.of("A", "B", "C", "D"));
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 7);

        assertDataNodesForZone(1, Set.of("A", "B", "C"));
    }

    @Test
    void test2_2() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.addNodesToDataNodes(Set.of("D"), 3);
        zoneState.removeNodesFromDataNodes(Set.of("D"), 7);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 7);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 3);

        assertDataNodesForZone(1, Set.of("A", "B", "C"));
    }

    @Test
    void test2_3() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.removeNodesFromDataNodes(Set.of("C"), 3);
        zoneState.addNodesToDataNodes(Set.of("C"), 7);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 3);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 7);

        assertDataNodesForZone(1, Set.of("A", "B", "C"));
    }

    @Test
    void test2_4() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.removeNodesFromDataNodes(Set.of("C"), 3);
        zoneState.addNodesToDataNodes(Set.of("C"), 7);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 7);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 3);

        assertDataNodesForZone(1, Set.of("A", "B", "C"));
    }

    @Test
    void test3_1() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.addNodesToDataNodes(Set.of("D"), 3);
        zoneState.removeNodesFromDataNodes(Set.of("D"), 7);
        zoneState.addNodesToDataNodes(Set.of("D"), 9);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 9);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 7);

        assertDataNodesForZone(1, Set.of("A", "B", "C", "D"));
    }

    @Test
    void test3_2() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.addNodesToDataNodes(Set.of("D"), 3);
        zoneState.removeNodesFromDataNodes(Set.of("D"), 7);
        zoneState.addNodesToDataNodes(Set.of("D"), 9);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 7);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 9);

        assertDataNodesForZone(1, Set.of("A", "B", "C", "D"));
    }

    @Test
    void test3_3() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.addNodesToDataNodes(Set.of("D"), 3);
        zoneState.removeNodesFromDataNodes(Set.of("D"), 7);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 7);

        zoneState.addNodesToDataNodes(Set.of("D"), 9);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 9);

        assertDataNodesForZone(1, Set.of("A", "B", "C", "D"));
    }

    @Test
    void test3_4() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.removeNodesFromDataNodes(Set.of("C"), 3);
        zoneState.addNodesToDataNodes(Set.of("C"), 7);
        zoneState.removeNodesFromDataNodes(Set.of("C"), 9);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 9);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 7);

        assertDataNodesForZone(1, Set.of("A", "B"));
    }

    @Test
    void test3_5() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.removeNodesFromDataNodes(Set.of("C"), 3);
        zoneState.addNodesToDataNodes(Set.of("C"), 7);
        zoneState.removeNodesFromDataNodes(Set.of("C"), 9);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 7);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 9);

        assertDataNodesForZone(1, Set.of("A", "B"));
    }

    @Test
    void test4_1() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.addNodesToDataNodes(Set.of("D"), 3);
        zoneState.removeNodesFromDataNodes(Set.of("D"), 7);
        zoneState.addNodesToDataNodes(Set.of("E"), 9);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 9);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 7);

        assertDataNodesForZone(1, Set.of("A", "B", "C", "E"));
    }

    @Test
    void test4_2() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.addNodesToDataNodes(Set.of("D"), 3);
        zoneState.removeNodesFromDataNodes(Set.of("D"), 7);
        zoneState.addNodesToDataNodes(Set.of("E"), 9);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 7);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 9);

        assertDataNodesForZone(1, Set.of("A", "B", "C", "E"));
    }

    @Test
    void test4_3() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.addNodesToDataNodes(Set.of("D"), 3);
        zoneState.removeNodesFromDataNodes(Set.of("D"), 7);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 7);

        zoneState.addNodesToDataNodes(Set.of("E"), 9);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 9);

        assertDataNodesForZone(1, Set.of("A", "B", "C", "E"));
    }

    @Test
    void test4_4() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.removeNodesFromDataNodes(Set.of("C"), 3);
        zoneState.addNodesToDataNodes(Set.of("C"), 7);
        zoneState.removeNodesFromDataNodes(Set.of("B"), 9);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 9);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 7);

        assertDataNodesForZone(1, Set.of("A","C"));
    }

    @Test
    void test4_5() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.removeNodesFromDataNodes(Set.of("C"), 3);
        zoneState.addNodesToDataNodes(Set.of("C"), 7);
        zoneState.removeNodesFromDataNodes(Set.of("B"), 9);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 7);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 9);

        assertDataNodesForZone(1, Set.of("A","C"));
    }

    @Test
    void test4_6() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.removeNodesFromDataNodes(Set.of("C"), 3);
        zoneState.addNodesToDataNodes(Set.of("C"), 7);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 7);

        zoneState.removeNodesFromDataNodes(Set.of("B"), 9);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 9);

        assertDataNodesForZone(1, Set.of("A","C"));
    }

    @Test
    void test5_1() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.addNodesToDataNodes(Set.of("D"), 3);
        zoneState.removeNodesFromDataNodes(Set.of("C"), 7);
        zoneState.addNodesToDataNodes(Set.of("C"), 9);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 9);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 7);

        assertDataNodesForZone(1, Set.of("A", "B", "C", "D"));
    }

    @Test
    void test5_2() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.addNodesToDataNodes(Set.of("D"), 3);
        zoneState.removeNodesFromDataNodes(Set.of("C"), 7);
        zoneState.addNodesToDataNodes(Set.of("C"), 9);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 7);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 9);

        assertDataNodesForZone(1, Set.of("A", "B", "C", "D"));
    }

    @Test
    void test5_3() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.addNodesToDataNodes(Set.of("D"), 3);
        zoneState.removeNodesFromDataNodes(Set.of("C"), 7);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 7);

        zoneState.addNodesToDataNodes(Set.of("C"), 9);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 9);

        assertDataNodesForZone(1, Set.of("A", "B", "C", "D"));
    }

    @Test
    void test5_4() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.removeNodesFromDataNodes(Set.of("C"), 3);
        zoneState.addNodesToDataNodes(Set.of("D"), 7);
        zoneState.removeNodesFromDataNodes(Set.of("D"), 9);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 9);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 7);

        assertDataNodesForZone(1, Set.of("A", "B"));
    }

    @Test
    void test5_5() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.removeNodesFromDataNodes(Set.of("C"), 3);
        zoneState.addNodesToDataNodes(Set.of("D"), 7);
        zoneState.removeNodesFromDataNodes(Set.of("D"), 9);

        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 7);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 9);

        assertDataNodesForZone(1, Set.of("A", "B"));
    }

    @Test
    void test5_6() throws Exception {
        preparePrerequisites();

        ZoneState zoneState = distributionZoneManager.zonesTimers().get(1);

        zoneState.removeNodesFromDataNodes(Set.of("C"), 3);
        zoneState.addNodesToDataNodes(Set.of("D"), 7);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(1, 7);

        zoneState.removeNodesFromDataNodes(Set.of("D"), 9);
        distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(1, 9);

        assertDataNodesForZone(1, Set.of("A", "B"));
    }

    @Test
    void test() throws Exception {
        KeyValueStorage storage = new SimpleInMemoryKeyValueStorage("test");

        Node node1 = new Node(storage);

        Node node2 = new Node(storage);

        node1.start();

        node2.start();

        node1.preparePrerequisites();

        node2.preparePrerequisites();
    }

    /**
     * Creates a zone with the auto adjust scale up trigger equals to 0 and the data nodes equals ["A"].
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

        mockCmgLocalNodes(3L, clusterNodes);

        distributionZoneManager.start();

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME)
                        .dataNodesAutoAdjustScaleUp(0)
                        .dataNodesAutoAdjustScaleDown(0)
                        .build()
        ).get();

        assertDataNodesForZone(1, clusterNodes.stream().map(ClusterNode::name).collect(Collectors.toSet()));

        assertZoneScaleUpChangeTriggerKey(1, 1);
        assertZoneScaleDownChangeTriggerKey(1, 1);
    }

    private CompletableFuture<Void> testSaveDataNodesOnScaleUp(int zoneId, long revision) {
        return distributionZoneManager.saveDataNodesToMetaStorageOnScaleUp(zoneId, revision);
    }

    private CompletableFuture<Void> testSaveDataNodesOnScaleDown(int zoneId, long revision) {
        return distributionZoneManager.saveDataNodesToMetaStorageOnScaleDown(zoneId, revision);
    }

    private void assertDataNodesForZone(int zoneId, @Nullable Set<String> clusterNodes) throws InterruptedException {
        assertTrue(waitForCondition(
                () -> {
                    byte[] dataNodes = keyValueStorage.get(zoneDataNodesKey(zoneId).bytes()).value();

                    if (dataNodes == null) {
                        return clusterNodes == null;
                    }

                    Set<String> res = ByteUtils.fromBytes(dataNodes);

                    return res.equals(clusterNodes);
                },
                2000
        ));
    }

    private void assertNotEqualsDataNodesForZone(int zoneId, @Nullable Set<String> clusterNodes) throws InterruptedException {
        assertFalse(waitForCondition(
                () -> {
                    byte[] dataNodes = keyValueStorage.get(zoneDataNodesKey(zoneId).bytes()).value();

                    if (dataNodes == null) {
                        return clusterNodes == null;
                    }

                    Set<String> res = ByteUtils.fromBytes(dataNodes);

                    return res.equals(clusterNodes);
                },
                1000
        ));
    }

    private void assertZoneScaleUpChangeTriggerKey(int revision, int zoneId) throws InterruptedException {
        assertTrue(
                waitForCondition(
                        () -> ByteUtils.bytesToLong(keyValueStorage.get(zoneScaleUpChangeTriggerKey(zoneId).bytes()).value()) == revision,
                        2000
                )
        );
    }

    private void assertZoneScaleDownChangeTriggerKey(int revision, int zoneId) throws InterruptedException {
        assertTrue(
                waitForCondition(
                        () -> ByteUtils.bytesToLong(keyValueStorage.get(zoneScaleDownChangeTriggerKey(zoneId).bytes()).value()) == revision,
                        2000
                )
        );
    }

    private void assertZonesChangeTriggerKey(int revision, int zoneId) throws InterruptedException {
        assertTrue(
                waitForCondition(
                        () -> ByteUtils.bytesToLong(keyValueStorage.get(zonesChangeTriggerKey(zoneId).bytes()).value()) == revision, 1000
                )
        );
    }

    private void mockVaultAppliedRevision(long revision) {
        when(metaStorageManager.appliedRevision()).thenReturn(revision);
    }

    private void watchListenerOnUpdate(Set<String> nodes, long rev) {
        byte[] newLogicalTopology = toBytes(nodes);

        Entry newEntry = new EntryImpl(zonesLogicalTopologyKey().bytes(), newLogicalTopology, rev, 1);

        EntryEvent entryEvent = new EntryEvent(null, newEntry);

        WatchEvent evt = new WatchEvent(entryEvent);

        watchListener.onUpdate(evt);
    }

    private LogicalTopologySnapshot mockCmgLocalNodes(long version, Set<ClusterNode> clusterNodes) {
        LogicalTopologySnapshot logicalTopologySnapshot = new LogicalTopologySnapshot(version, clusterNodes);

        when(cmgManager.logicalTopology()).thenReturn(completedFuture(logicalTopologySnapshot));

        return logicalTopologySnapshot;
    }

    private void assertLogicalTopology(@Nullable Set<ClusterNode> clusterNodes) throws InterruptedException {
        byte[] nodes = clusterNodes == null
                ? null
                : toBytes(clusterNodes.stream().map(ClusterNode::name).collect(Collectors.toSet()));

        assertTrue(waitForCondition(() -> Arrays.equals(keyValueStorage.get(zonesLogicalTopologyKey().bytes()).value(), nodes), 1000));
    }

    private void mockVaultZonesLogicalTopologyKey(Set<String> nodes) {
        byte[] newLogicalTopology = toBytes(nodes);

        when(vaultMgr.get(zonesLogicalTopologyKey()))
                .thenReturn(completedFuture(new VaultEntry(zonesLogicalTopologyKey(), newLogicalTopology)));
    }

    class Node {
        private DistributionZoneManager distributionZoneManager;

        private KeyValueStorage keyValueStorage;

        private ConfigurationManager clusterCfgMgr;

        @Mock
        private LogicalTopologyServiceImpl logicalTopologyService;

        private LogicalTopology topology;

        private ClusterStateStorage clusterStateStorage;

        private VaultManager vaultMgr;

        private MetaStorageManager metaStorageManager;

        private WatchListener watchListener;

        private DistributionZonesConfiguration zonesConfiguration;

        @Mock
        private ClusterManagementGroupManager cmgManager;

        Node(KeyValueStorage storage) {
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
                watchListener = invocation.getArgument(1);

                return null;
            }).when(metaStorageManager).registerExactWatch(any(), any());

            AtomicLong raftIndex = new AtomicLong();

            keyValueStorage = spy(storage);

            MetaStorageListener metaStorageListener = new MetaStorageListener(keyValueStorage);

            RaftGroupService metaStorageService = mock(RaftGroupService.class);

            // Delegate directly to listener.
            lenient().doAnswer(
                    invocationClose -> {
                        Command cmd = invocationClose.getArgument(0);

                        long commandIndex = raftIndex.incrementAndGet();

                        CompletableFuture<Serializable> res = new CompletableFuture<>();

                        CommandClosure<WriteCommand> clo = new CommandClosure<>() {
                            /** {@inheritDoc} */
                            @Override
                            public long index() {
                                return commandIndex;
                            }

                            /** {@inheritDoc} */
                            @Override
                            public WriteCommand command() {
                                return (WriteCommand) cmd;
                            }

                            /** {@inheritDoc} */
                            @Override
                            public void result(@Nullable Serializable r) {
                                if (r instanceof Throwable) {
                                    res.completeExceptionally((Throwable) r);
                                } else {
                                    res.complete(r);
                                }
                            }
                        };

                        try {
                            metaStorageListener.onWrite(List.of(clo).iterator());
                        } catch (Throwable e) {
                            res.completeExceptionally(new IgniteInternalException(e));
                        }

                        return res;
                    }
            ).when(metaStorageService).run(any(WriteCommand.class));

            lenient().doAnswer(
                    invocationClose -> {
                        Command cmd = invocationClose.getArgument(0);

                        long commandIndex = raftIndex.incrementAndGet();

                        CompletableFuture<Serializable> res = new CompletableFuture<>();

                        CommandClosure<ReadCommand> clo = new CommandClosure<>() {
                            /** {@inheritDoc} */
                            @Override
                            public long index() {
                                return commandIndex;
                            }

                            /** {@inheritDoc} */
                            @Override
                            public ReadCommand command() {
                                return (ReadCommand) cmd;
                            }

                            /** {@inheritDoc} */
                            @Override
                            public void result(@Nullable Serializable r) {
                                if (r instanceof Throwable) {
                                    res.completeExceptionally((Throwable) r);
                                } else {
                                    res.complete(r);
                                }
                            }
                        };

                        try {
                            metaStorageListener.onRead(List.of(clo).iterator());
                        } catch (Throwable e) {
                            res.completeExceptionally(new IgniteInternalException(e));
                        }

                        return res;
                    }
            ).when(metaStorageService).run(any(ReadCommand.class));

            MetaStorageCommandsFactory commandsFactory = new MetaStorageCommandsFactory();

            lenient().doAnswer(invocationClose -> {
                Iif iif = invocationClose.getArgument(0);

                MultiInvokeCommand multiInvokeCommand = commandsFactory.multiInvokeCommand().iif(iif).build();

                return metaStorageService.run(multiInvokeCommand);
            }).when(metaStorageManager).invoke(any());

            lenient().doAnswer(invocationClose -> {
                Set<ByteArray> keysSet = invocationClose.getArgument(0);

                GetAllCommand getAllCommand = commandsFactory.getAllCommand().keys(
                        keysSet.stream().map(ByteArray::bytes).collect(Collectors.toList())
                ).revision(0).build();

                return metaStorageService.run(getAllCommand).thenApply(bi -> {
                    MultipleEntryResponse resp = (MultipleEntryResponse) bi;

                    Map<ByteArray, Entry> res = new HashMap<>();

                    for (SingleEntryResponse e : resp.entries()) {
                        ByteArray key = new ByteArray(e.key());

                        res.put(key, new EntryImpl(key.bytes(), e.value(), e.revision(), e.updateCounter()));
                    }

                    return res;
                });
            }).when(metaStorageManager).getAll(any());

            lenient().doAnswer(invocationClose -> {
                ByteArray key = invocationClose.getArgument(0);

                GetCommand getCommand = commandsFactory.getCommand().key(key.bytes()).build();

                return metaStorageService.run(getCommand).thenApply(bi -> {
                    SingleEntryResponse resp = (SingleEntryResponse) bi;

                    return new EntryImpl(resp.key(), resp.value(), resp.revision(), resp.updateCounter());
                });
            }).when(metaStorageManager).get(any());
        }

        public void start() {
            distributionZoneManager.start();
        }

        void stop() throws Exception {
            distributionZoneManager.stop();

            clusterCfgMgr.stop();

            keyValueStorage.close();

            clusterStateStorage.destroy();
        }

        private void preparePrerequisites() throws Exception {
            ClusterNode node1 = new ClusterNode("1", "A", new NetworkAddress("localhost", 123));
            ClusterNode node2 = new ClusterNode("2", "B", new NetworkAddress("localhost", 123));
            ClusterNode node3 = new ClusterNode("3", "C", new NetworkAddress("localhost", 123));

            topology.putNode(node1);
            topology.putNode(node2);
            topology.putNode(node3);

            Set<ClusterNode> clusterNodes = Set.of(node1, node2, node3);

            mockVaultZonesLogicalTopologyKey(clusterNodes.stream().map(ClusterNode::name).collect(Collectors.toSet()));

            mockCmgLocalNodes(3L, clusterNodes);

            distributionZoneManager.start();

            distributionZoneManager.createZone(
                    new DistributionZoneConfigurationParameters.Builder(ZONE_NAME)
                            .dataNodesAutoAdjustScaleUp(0)
                            .dataNodesAutoAdjustScaleDown(0)
                            .build()
            ).get();

            assertDataNodesForZone(1, clusterNodes.stream().map(ClusterNode::name).collect(Collectors.toSet()));

            assertZoneScaleUpChangeTriggerKey(1, 1);
            assertZoneScaleDownChangeTriggerKey(1, 1);
        }
    }
}
