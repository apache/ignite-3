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
import static org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl.LOGICAL_TOPOLOGY_KEY;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyVersionKey;
import static org.apache.ignite.internal.metastorage.client.MetaStorageServiceImpl.toIfInfo;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.client.EntryImpl;
import org.apache.ignite.internal.metastorage.client.If;
import org.apache.ignite.internal.metastorage.client.StatementResult;
import org.apache.ignite.internal.metastorage.common.StatementResultInfo;
import org.apache.ignite.internal.metastorage.common.command.GetCommand;
import org.apache.ignite.internal.metastorage.common.command.MetaStorageCommandsFactory;
import org.apache.ignite.internal.metastorage.common.command.MultiInvokeCommand;
import org.apache.ignite.internal.metastorage.common.command.SingleEntryResponse;
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
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

/**
 * Tests reactions to topology changes in accordance with distribution zones logic.
 */
public class DistributionZoneManagerLogicalTopologyEventsTest {
    @Mock
    private ClusterManagementGroupManager cmgManager;

    private DistributionZoneManager distributionZoneManager;

    private SimpleInMemoryKeyValueStorage keyValueStorage;

    private ConfigurationManager clusterCfgMgr;

    private LogicalTopology topology;

    private ClusterStateStorage clusterStateStorage;

    private DistributionZoneManager prepareDistributionZoneManager() {
        clusterCfgMgr = new ConfigurationManager(
                List.of(DistributionZonesConfiguration.KEY),
                Map.of(),
                new TestConfigurationStorage(DISTRIBUTED),
                List.of(),
                List.of()
        );

        DistributionZonesConfiguration zonesConfiguration = clusterCfgMgr.configurationRegistry()
                .getConfiguration(DistributionZonesConfiguration.KEY);

        MetaStorageManager metaStorageManager = mock(MetaStorageManager.class);

        cmgManager = mock(ClusterManagementGroupManager.class);

        clusterStateStorage = new TestClusterStateStorage();

        topology = new LogicalTopologyImpl(clusterStateStorage);

        LogicalTopologyServiceImpl logicalTopologyService = new LogicalTopologyServiceImpl(topology, cmgManager);

        VaultManager vaultMgr = mock(VaultManager.class);

        when(vaultMgr.get(any())).thenReturn(completedFuture(null));

        when(metaStorageManager.registerWatch(any(ByteArray.class), any())).then(invocation -> completedFuture(null));

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
                vaultMgr
        );

        clusterCfgMgr.start();

        AtomicLong raftIndex = new AtomicLong();

        keyValueStorage = spy(new SimpleInMemoryKeyValueStorage());

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
            If iif = invocationClose.getArgument(0);

            MultiInvokeCommand multiInvokeCommand = commandsFactory.multiInvokeCommand().iif(toIfInfo(iif, commandsFactory)).build();

            return metaStorageService.run(multiInvokeCommand).thenApply(bi -> new StatementResult(((StatementResultInfo) bi).result()));
        }).when(metaStorageManager).invoke(any());

        lenient().doAnswer(invocationClose -> {
            ByteArray key = invocationClose.getArgument(0);

            GetCommand getCommand = commandsFactory.getCommand().key(key.bytes()).build();

            return metaStorageService.run(getCommand).thenApply(bi -> {
                SingleEntryResponse resp = (SingleEntryResponse) bi;

                return new EntryImpl(new ByteArray(resp.key()), resp.value(), resp.revision(), resp.updateCounter());
            });
        }).when(metaStorageManager).get(any());

        return distributionZoneManager;
    }

    @AfterEach
    public void tearDown() throws Exception {
        distributionZoneManager.stop();

        clusterCfgMgr.stop();

        keyValueStorage.close();

        clusterStateStorage.destroy();
    }

    @Test
    void testMetaStorageKeysInitializedOnStartWhenTopVerEmpty() throws Exception {
        DistributionZoneManager distributionZoneManager1 = prepareDistributionZoneManager();

        Set<ClusterNode> clusterNodes = Set.of(new ClusterNode("1", "name1", null));

        mockCmgLocalNodes(1L, clusterNodes);

        distributionZoneManager1.start();

        verify(keyValueStorage, timeout(1000).times(1)).invoke(any());

        assertLogicalTopVer(1L);

        assertLogicalTopology(clusterNodes);
    }

    @Test
    void testMetaStorageKeysInitializedOnStartWhenTopVerIsLessThanCmgTopVer() throws Exception {
        DistributionZoneManager distributionZoneManager1 = prepareDistributionZoneManager();

        Set<ClusterNode> clusterNodes = Set.of(new ClusterNode("1", "name1", null));

        mockCmgLocalNodes(2L, clusterNodes);

        keyValueStorage.put(zonesLogicalTopologyVersionKey().bytes(), ByteUtils.longToBytes(1L));

        distributionZoneManager1.start();

        verify(keyValueStorage, timeout(1000).times(1)).invoke(any());

        assertLogicalTopVer(2L);

        assertLogicalTopology(clusterNodes);
    }

    @Test
    void testMetaStorageKeysInitializedOnStartWhenTopVerEqualsToCmgTopVer() throws Exception {
        DistributionZoneManager distributionZoneManager1 = prepareDistributionZoneManager();

        Set<ClusterNode> clusterNodes = Set.of(new ClusterNode("1", "name1", null));

        mockCmgLocalNodes(2L, clusterNodes);

        keyValueStorage.put(zonesLogicalTopologyVersionKey().bytes(), ByteUtils.longToBytes(2L));

        distributionZoneManager1.start();

        verify(keyValueStorage, after(500).never()).invoke(any());

        assertLogicalTopVer(2L);

        assertLogicalTopology(null);
    }

    @Test
    void testMetaStorageKeysInitializedOnStartWhenTopVerGreaterThanCmgTopVer() throws Exception {
        DistributionZoneManager distributionZoneManager1 = prepareDistributionZoneManager();

        Set<ClusterNode> clusterNodes = Set.of(new ClusterNode("1", "name1", null));

        mockCmgLocalNodes(2L, clusterNodes);

        keyValueStorage.put(zonesLogicalTopologyVersionKey().bytes(), ByteUtils.longToBytes(3L));

        distributionZoneManager1.start();

        verify(keyValueStorage, after(500).never()).invoke(any());

        assertLogicalTopVer(3L);

        assertLogicalTopology(null);
    }

    @Test
    void testNodeAddingUpdatesLogicalTopologyInMetaStorage() throws Exception {
        DistributionZoneManager distributionZoneManager1 = prepareDistributionZoneManager();

        ClusterNode node1 = new ClusterNode("1", "name1", new NetworkAddress("localhost", 123));

        topology.putNode(node1);

        Set<ClusterNode> clusterNodes = Set.of(node1);

        mockCmgLocalNodes(1L, clusterNodes);

        distributionZoneManager1.start();

        ClusterNode node2 = new ClusterNode("2", "name2", new NetworkAddress("localhost", 123));

        topology.putNode(node2);

        var clusterNodes2 = Set.of(node1, node2);

        assertLogicalTopology(clusterNodes2);

        assertLogicalTopVer(2L);
    }

    @Test
    void testNodeStaleAddingDoNotUpdatesLogicalTopologyInMetaStorage() throws Exception {
        DistributionZoneManager distributionZoneManager1 = prepareDistributionZoneManager();

        ClusterNode node1 = new ClusterNode("1", "name1", new NetworkAddress("localhost", 123));

        topology.putNode(node1);

        Set<ClusterNode> clusterNodes = Set.of(node1);

        mockCmgLocalNodes(1L, clusterNodes);

        distributionZoneManager1.start();

        keyValueStorage.put(zonesLogicalTopologyVersionKey().bytes(), ByteUtils.longToBytes(4L));

        ClusterNode node2 = new ClusterNode("2", "name2", new NetworkAddress("localhost", 123));

        topology.putNode(node2);

        assertEquals(2L, topology.getLogicalTopology().version());

        assertLogicalTopology(clusterNodes);

        assertLogicalTopVer(4L);
    }

    @Test
    void testNodeRemovingUpdatesLogicalTopologyInMetaStorage() throws Exception {
        DistributionZoneManager distributionZoneManager1 = prepareDistributionZoneManager();

        ClusterNode node1 = new ClusterNode("1", "name1", new NetworkAddress("localhost", 123));

        ClusterNode node2 = new ClusterNode("2", "name2", new NetworkAddress("localhost", 123));

        topology.putNode(node1);

        topology.putNode(node2);

        Set<ClusterNode> clusterNodes = Set.of(node1, node2);

        mockCmgLocalNodes(2L, clusterNodes);

        distributionZoneManager1.start();

        assertLogicalTopology(clusterNodes);

        topology.removeNodes(Set.of(node2));

        var clusterNodes2 = Set.of(node1);

        assertLogicalTopology(clusterNodes2);

        assertLogicalTopVer(3L);
    }

    @Test
    void testNodeStaleRemovingDoNotUpdatesLogicalTopologyInMetaStorage() throws Exception {
        DistributionZoneManager distributionZoneManager1 = prepareDistributionZoneManager();

        ClusterNode node1 = new ClusterNode("1", "name1", new NetworkAddress("localhost", 123));

        ClusterNode node2 = new ClusterNode("2", "name2", new NetworkAddress("localhost", 123));

        topology.putNode(node1);

        topology.putNode(node2);

        assertEquals(2L, topology.getLogicalTopology().version());

        Set<ClusterNode> clusterNodes = Set.of(node1, node2);

        mockCmgLocalNodes(2L, clusterNodes);

        distributionZoneManager1.start();

        keyValueStorage.put(zonesLogicalTopologyVersionKey().bytes(), ByteUtils.longToBytes(4L));

        topology.removeNodes(Set.of(node2));

        assertLogicalTopology(clusterNodes);

        assertLogicalTopVer(4L);
    }

    @Test
    void testTopologyLeapUpdatesLogicalTopologyInMetaStorage() throws Exception {
        DistributionZoneManager distributionZoneManager1 = prepareDistributionZoneManager();

        ClusterNode node1 = new ClusterNode("1", "name1", new NetworkAddress("localhost", 123));

        ClusterNode node2 = new ClusterNode("2", "name2", new NetworkAddress("localhost", 123));

        topology.putNode(node1);

        Set<ClusterNode> clusterNodes = Set.of(node1);

        mockCmgLocalNodes(2L, clusterNodes);

        distributionZoneManager1.start();

        assertLogicalTopology(clusterNodes);

        var clusterNodes2 = Set.of(node1, node2);

        clusterStateStorage.put(LOGICAL_TOPOLOGY_KEY, ByteUtils.toBytes(new LogicalTopologySnapshot(10L, clusterNodes2)));

        topology.fireTopologyLeap();

        assertLogicalTopology(clusterNodes2);

        assertLogicalTopVer(10L);
    }

    @Test
    void testStaleTopologyLeapDoNotUpdatesLogicalTopologyInMetaStorage() throws Exception {
        DistributionZoneManager distributionZoneManager1 = prepareDistributionZoneManager();

        ClusterNode node1 = new ClusterNode("1", "name1", new NetworkAddress("localhost", 123));

        ClusterNode node2 = new ClusterNode("2", "name2", new NetworkAddress("localhost", 123));

        topology.putNode(node1);

        Set<ClusterNode> clusterNodes = Set.of(node1);

        mockCmgLocalNodes(2L, clusterNodes);

        distributionZoneManager1.start();

        assertLogicalTopology(clusterNodes);

        var clusterNodes2 = Set.of(node1, node2);

        clusterStateStorage.put(LOGICAL_TOPOLOGY_KEY, ByteUtils.toBytes(new LogicalTopologySnapshot(10L, clusterNodes2)));

        keyValueStorage.put(zonesLogicalTopologyVersionKey().bytes(), ByteUtils.longToBytes(11L));

        topology.fireTopologyLeap();

        assertLogicalTopology(clusterNodes);

        assertLogicalTopVer(11L);
    }

    private LogicalTopologySnapshot mockCmgLocalNodes(long version, Set<ClusterNode> clusterNodes) {
        LogicalTopologySnapshot logicalTopologySnapshot = new LogicalTopologySnapshot(version, clusterNodes);

        when(cmgManager.logicalTopology()).thenReturn(completedFuture(logicalTopologySnapshot));

        return logicalTopologySnapshot;
    }

    private void assertLogicalTopVer(long topVer) throws InterruptedException {
        assertTrue(
                waitForCondition(
                        () -> ByteUtils.bytesToLong(keyValueStorage.get(zonesLogicalTopologyVersionKey().bytes()).value()) == topVer, 1000
                )
        );
    }

    private void assertLogicalTopology(@Nullable Set<ClusterNode> clusterNodes) throws InterruptedException {
        byte[] nodes = clusterNodes == null
                ? null
                : ByteUtils.toBytes(clusterNodes.stream().map(ClusterNode::name).collect(Collectors.toSet()));

        assertTrue(waitForCondition(() -> Arrays.equals(keyValueStorage.get(zonesLogicalTopologyKey().bytes()).value(), nodes), 1000));
    }
}
