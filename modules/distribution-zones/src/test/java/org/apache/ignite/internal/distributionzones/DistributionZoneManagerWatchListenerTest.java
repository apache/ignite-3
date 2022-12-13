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
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyKey;
import static org.apache.ignite.internal.metastorage.MetaStorageManager.APPLIED_REV;
import static org.apache.ignite.internal.metastorage.client.MetaStorageServiceImpl.toIfInfo;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.util.ByteUtils.longToBytes;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.ConfigurationValue;
import org.apache.ignite.configuration.NamedConfigurationTree;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneChange;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneConfiguration;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneView;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.client.EntryEvent;
import org.apache.ignite.internal.metastorage.client.If;
import org.apache.ignite.internal.metastorage.client.StatementResult;
import org.apache.ignite.internal.metastorage.client.WatchEvent;
import org.apache.ignite.internal.metastorage.client.WatchListener;
import org.apache.ignite.internal.metastorage.common.StatementResultInfo;
import org.apache.ignite.internal.metastorage.common.command.MetaStorageCommandsFactory;
import org.apache.ignite.internal.metastorage.common.command.MultiInvokeCommand;
import org.apache.ignite.internal.metastorage.server.Entry;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.metastorage.server.raft.MetaStorageListener;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

/**
 * Tests distribution zones configuration changes and reaction to that changes.
 */
public class DistributionZoneManagerWatchListenerTest extends IgniteAbstractTest {
    private static final String ZONE_NAME = "zone1";

    @Mock
    private ClusterManagementGroupManager cmgManager;

    private VaultManager vaultMgr;

    private DistributionZoneManager distributionZoneManager;

    private SimpleInMemoryKeyValueStorage keyValueStorage;

    private ConfigurationManager clusterCfgMgr;

    private WatchListener watchListener;

    private DistributionZonesConfiguration zonesConfiguration;

    private MetaStorageManager metaStorageManager;

    @BeforeEach
    public void setUp() {
        clusterCfgMgr = new ConfigurationManager(
                List.of(DistributionZonesConfiguration.KEY),
                Map.of(),
                new TestConfigurationStorage(DISTRIBUTED),
                List.of(),
                List.of()
        );

        zonesConfiguration = mock(DistributionZonesConfiguration.class);

        metaStorageManager = mock(MetaStorageManager.class);

        cmgManager = mock(ClusterManagementGroupManager.class);

        vaultMgr = mock(VaultManager.class);

        distributionZoneManager = new DistributionZoneManager(
                zonesConfiguration,
                metaStorageManager,
                cmgManager,
                vaultMgr
        );

        clusterCfgMgr.start();

        mockVaultAppliedRevision(1);

        when(vaultMgr.get(zonesLogicalTopologyKey())).thenReturn(completedFuture(new VaultEntry(zonesLogicalTopologyKey(), null)));

        when(metaStorageManager.registerWatch(any(ByteArray.class), any())).then(invocation -> {
            watchListener = invocation.getArgument(1);

            return CompletableFuture.completedFuture(null);
        });

        mockEmptyZonesList();

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
        ).when(metaStorageService).run(any());

        MetaStorageCommandsFactory commandsFactory = new MetaStorageCommandsFactory();

        lenient().doAnswer(invocationClose -> {
            If iif = invocationClose.getArgument(0);

            MultiInvokeCommand multiInvokeCommand = commandsFactory.multiInvokeCommand().iif(toIfInfo(iif, commandsFactory)).build();

            return metaStorageService.run(multiInvokeCommand).thenApply(bi -> new StatementResult(((StatementResultInfo) bi).result()));
        }).when(metaStorageManager).invoke(any());
    }

    @AfterEach
    public void tearDown() throws Exception {
        vaultMgr.stop();

        distributionZoneManager.stop();

        clusterCfgMgr.stop();

        keyValueStorage.close();
    }

    @Test
    void testDataNodesUpdatedOnWatchListenerEvent() {
        distributionZoneManager.start();

        mockCreateZone();

        //first event

        Set<String> nodes = Set.of("node1", "node2");

        watchListenerOnUpdate(nodes, 1);

        verify(keyValueStorage, timeout(1000).times(1)).invoke(any());

        Entry entry = keyValueStorage.get(zoneDataNodesKey(1).bytes());

        Set<String> newDataNodes = ByteUtils.fromBytes(entry.value());

        assertTrue(newDataNodes.containsAll(nodes));
        assertEquals(nodes.size(), newDataNodes.size());

        //second event

        nodes = Set.of("node1", "node3");

        watchListenerOnUpdate(nodes, 2);

        verify(keyValueStorage, timeout(1000).times(3)).invoke(any());

        entry = keyValueStorage.get(zoneDataNodesKey(1).bytes());

        newDataNodes = ByteUtils.fromBytes(entry.value());

        assertTrue(newDataNodes.containsAll(nodes));
        assertEquals(nodes.size(), newDataNodes.size());

        //third event

        nodes = Collections.emptySet();

        watchListenerOnUpdate(nodes, 3);

        verify(keyValueStorage, timeout(1000).times(4)).invoke(any());

        entry = keyValueStorage.get(zoneDataNodesKey(1).bytes());

        newDataNodes = ByteUtils.fromBytes(entry.value());

        assertTrue(newDataNodes.isEmpty());
    }

    @Test
    void testStaleWatchEvent() {
        distributionZoneManager.start();

        mockCreateZone();

        mockVaultAppliedRevision(1);

        long revision = 100;

        keyValueStorage.put(zonesChangeTriggerKey().bytes(), ByteUtils.longToBytes(revision));

        Set<String> nodes = Set.of("node1", "node2");

        watchListenerOnUpdate(nodes, revision);

        verify(keyValueStorage, timeout(1000).times(1)).invoke(any());

        Entry entry = keyValueStorage.get(zoneDataNodesKey(1).bytes());

        assertNull(entry.value());
    }

    @Test
    void testStaleVaultRevisionOnZoneManagerStart() {
        mockCreateZone();

        long revision = 100;

        keyValueStorage.put(zonesChangeTriggerKey().bytes(), ByteUtils.longToBytes(revision));

        Set<String> nodes = Set.of("node1", "node2");

        mockVaultZonesLogicalTopologyKey(nodes);

        mockVaultAppliedRevision(revision);

        distributionZoneManager.start();

        verify(metaStorageManager, timeout(1000).times(1)).invoke(any());

        Entry entry = keyValueStorage.get(zoneDataNodesKey(1).bytes());

        assertNull(entry.value());
    }

    @Test
    void testDataNodesUpdatedOnZoneManagerStart() {
        mockCreateZone();

        mockVaultAppliedRevision(2);

        Set<String> nodes = Set.of("node1", "node2");

        mockVaultZonesLogicalTopologyKey(nodes);

        distributionZoneManager.start();

        verify(keyValueStorage, timeout(1000).times(1)).invoke(any());

        Entry entry = keyValueStorage.get(zoneDataNodesKey(1).bytes());

        Set<String> newDataNodes = ByteUtils.fromBytes(entry.value());

        assertTrue(newDataNodes.containsAll(nodes));
        assertEquals(2, newDataNodes.size());
    }

    private void mockEmptyZonesList() {
        NamedConfigurationTree<DistributionZoneConfiguration, DistributionZoneView, DistributionZoneChange> namedConfigurationTree =
                mock(NamedConfigurationTree.class);
        when(zonesConfiguration.distributionZones()).thenReturn(namedConfigurationTree);

        NamedListView<DistributionZoneView> namedListView = mock(NamedListView.class);
        when(namedConfigurationTree.value()).thenReturn(namedListView);

        when(zonesConfiguration.distributionZones().value().namedListKeys()).thenReturn(Collections.emptyList());
    }

    private void mockCreateZone() {
        NamedConfigurationTree<DistributionZoneConfiguration, DistributionZoneView, DistributionZoneChange> namedConfigurationTree =
                mock(NamedConfigurationTree.class);
        when(zonesConfiguration.distributionZones()).thenReturn(namedConfigurationTree);

        NamedListView<DistributionZoneView> namedListView = mock(NamedListView.class);
        when(namedConfigurationTree.value()).thenReturn(namedListView);

        when(zonesConfiguration.distributionZones().value().namedListKeys()).thenReturn(List.of(ZONE_NAME));

        DistributionZoneConfiguration distributionZoneConfiguration1 = mock(DistributionZoneConfiguration.class);
        when(namedConfigurationTree.get(ZONE_NAME)).thenReturn(distributionZoneConfiguration1);

        ConfigurationValue<Integer> zoneIdValue1 = mock(ConfigurationValue.class);
        when(distributionZoneConfiguration1.zoneId()).thenReturn(zoneIdValue1);
        when(zoneIdValue1.value()).thenReturn(1);

        ConfigurationValue<Integer> dataNodesAutoAdjust1 = mock(ConfigurationValue.class);
        when(distributionZoneConfiguration1.dataNodesAutoAdjust()).thenReturn(dataNodesAutoAdjust1);
        when(dataNodesAutoAdjust1.value()).thenReturn(100);

        ConfigurationValue<Integer> dataNodesAutoAdjustScaleUp1 = mock(ConfigurationValue.class);
        when(distributionZoneConfiguration1.dataNodesAutoAdjustScaleUp()).thenReturn(dataNodesAutoAdjustScaleUp1);
        when(dataNodesAutoAdjustScaleUp1.value()).thenReturn(200);

        ConfigurationValue<Integer> dataNodesAutoAdjustScaleDown1 = mock(ConfigurationValue.class);
        when(distributionZoneConfiguration1.dataNodesAutoAdjustScaleDown()).thenReturn(dataNodesAutoAdjustScaleDown1);
        when(dataNodesAutoAdjustScaleDown1.value()).thenReturn(300);
    }

    private void mockVaultZonesLogicalTopologyKey(Set<String> nodes) {
        byte[] newlogicalTopology = toBytes(nodes);

        when(vaultMgr.get(zonesLogicalTopologyKey()))
                .thenReturn(completedFuture(new VaultEntry(zonesLogicalTopologyKey(), newlogicalTopology)));
    }

    private void watchListenerOnUpdate(Set<String> nodes, long rev) {
        byte[] newlogicalTopology = toBytes(nodes);

        org.apache.ignite.internal.metastorage.client.Entry newEntry =
                new org.apache.ignite.internal.metastorage.client.EntryImpl(null, newlogicalTopology, rev, 1);

        EntryEvent entryEvent = new EntryEvent(null, newEntry);

        WatchEvent evt = new WatchEvent(entryEvent);

        watchListener.onUpdate(evt);
    }

    private void mockVaultAppliedRevision(long revision) {
        when(vaultMgr.get(APPLIED_REV)).thenReturn(completedFuture(new VaultEntry(APPLIED_REV, longToBytes(revision))));
    }

    private LogicalTopologySnapshot mockCmgLocalNodes(Set<ClusterNode> clusterNodes) {
        LogicalTopologySnapshot logicalTopologySnapshot = mock(LogicalTopologySnapshot.class);

        when(cmgManager.logicalTopology()).thenReturn(completedFuture(logicalTopologySnapshot));

        when(logicalTopologySnapshot.nodes()).thenReturn(clusterNodes);

        return logicalTopologySnapshot;
    }

    private void assertDataNodesForZone(int zoneId, @Nullable Set<ClusterNode> clusterNodes) throws InterruptedException {
        byte[] nodes = clusterNodes == null
                ? null
                : ByteUtils.toBytes(clusterNodes.stream().map(ClusterNode::name).collect(Collectors.toSet()));

        assertTrue(waitForCondition(() -> Arrays.equals(keyValueStorage.get(zoneDataNodesKey(zoneId).bytes()).value(), nodes), 1000));
    }

    private void assertZonesChangeTriggerKey(int revision) throws InterruptedException {
        assertTrue(
                waitForCondition(
                        () -> ByteUtils.bytesToLong(keyValueStorage.get(zonesChangeTriggerKey().bytes()).value()) == revision, 1000
                )
        );
    }
}
