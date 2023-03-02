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
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleUpChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyVersionKey;
import static org.apache.ignite.internal.distributionzones.util.DistributionZonesTestUtil.assertDataNodesForZone;
import static org.apache.ignite.internal.distributionzones.util.DistributionZonesTestUtil.mockMetaStorageListener;
import static org.apache.ignite.internal.util.ByteUtils.longToBytes;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
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
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.EntryEvent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.impl.EntryImpl;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.metastorage.server.raft.MetaStorageListener;
import org.apache.ignite.internal.raft.service.RaftGroupService;
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
import org.mockito.Mock;

/**
 * Tests distribution zones logical topology changes and reaction to that changes.
 */
//TODO: IGNITE-18564 Add tests with not default distribution zones, when distributionZones.change.trigger per zone will be created.
public class DistributionZoneManagerWatchListenerTest extends IgniteAbstractTest {
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
        vaultMgr.stop();

        distributionZoneManager.stop();

        clusterCfgMgr.stop();

        keyValueStorage.close();
    }

    @Test
    void testDataNodesOfDefaultZoneUpdatedOnWatchListenerEvent() throws Exception {
        mockVaultZonesLogicalTopologyKey(Set.of());

        mockCmgLocalNodes();

        distributionZoneManager.start();

        distributionZoneManager.alterZone(
                DEFAULT_ZONE_NAME,
                new DistributionZoneConfigurationParameters.Builder(DEFAULT_ZONE_NAME)
                        .dataNodesAutoAdjustScaleUp(0)
                        .dataNodesAutoAdjustScaleDown(Integer.MAX_VALUE)
                        .build()
        ).get();

        //first event

        Set<String> nodes = Set.of("node1", "node2");

        watchListenerOnUpdate(nodes, 2);

        verify(keyValueStorage, timeout(1000).times(3)).invoke(any());

        assertDataNodesForZone(DEFAULT_ZONE_ID, nodes, keyValueStorage);

        //second event

        nodes = Set.of("node1", "node3");

        watchListenerOnUpdate(nodes, 3);

        verify(keyValueStorage, timeout(1000).times(4)).invoke(any());

        nodes = Set.of("node1", "node2", "node3");

        // Scale up just adds node to data nodes
        assertDataNodesForZone(DEFAULT_ZONE_ID, nodes, keyValueStorage);

        //third event

        nodes = Collections.emptySet();

        watchListenerOnUpdate(nodes, 4);

        verify(keyValueStorage, timeout(1000).times(4)).invoke(any());

        nodes = Set.of("node1", "node2", "node3");

        // Scale up wasn't triggered
        assertDataNodesForZone(DEFAULT_ZONE_ID, nodes, keyValueStorage);
    }

    @Test
    void testStaleWatchEvent() throws Exception {
        mockVaultZonesLogicalTopologyKey(Set.of());

        mockCmgLocalNodes();

        distributionZoneManager.start();

        distributionZoneManager.alterZone(
                DEFAULT_ZONE_NAME,
                new DistributionZoneConfigurationParameters.Builder(DEFAULT_ZONE_NAME).dataNodesAutoAdjustScaleUp(0).build()
        ).get();

        long revision = 100;

        keyValueStorage.put(zoneScaleUpChangeTriggerKey(DEFAULT_ZONE_ID).bytes(), longToBytes(revision));

        Set<String> nodes = Set.of("node1", "node2");

        watchListenerOnUpdate(nodes, revision);

        // two invokes on start, and invoke for update scale up won't be triggered, because revision == zoneScaleUpChangeTriggerKey
        verify(keyValueStorage, timeout(1000).times(2)).invoke(any());

        assertDataNodesForZone(DEFAULT_ZONE_ID, Set.of(), keyValueStorage);
    }

    @Test
    void testStaleVaultRevisionOnZoneManagerStart() throws InterruptedException {
        long revision = 100;

        keyValueStorage.put(zonesChangeTriggerKey(DEFAULT_ZONE_ID).bytes(), longToBytes(revision));

        Set<String> nodes = Set.of("node1", "node2");

        mockVaultZonesLogicalTopologyKey(nodes);

        mockVaultAppliedRevision(revision);

        mockCmgLocalNodes();

        distributionZoneManager.start();

        verify(metaStorageManager, timeout(1000).times(2)).invoke(any());

        assertDataNodesForZone(DEFAULT_ZONE_ID, null, keyValueStorage);
    }

    @Test
    void testDataNodesUpdatedOnZoneManagerStart() throws InterruptedException {
        mockVaultAppliedRevision(2);

        Set<String> nodes = Set.of("node1", "node2");

        mockVaultZonesLogicalTopologyKey(nodes);

        mockCmgLocalNodes();

        distributionZoneManager.start();

        verify(keyValueStorage, timeout(1000).times(2)).invoke(any());

        assertDataNodesForZone(DEFAULT_ZONE_ID, nodes, keyValueStorage);
    }

    @Test
    void testLogicalTopologyIsNullOnZoneManagerStart1() {
        mockCmgLocalNodes();

        when(vaultMgr.get(zonesLogicalTopologyKey()))
                .thenReturn(completedFuture(new VaultEntry(zonesLogicalTopologyKey(), null)));

        distributionZoneManager.start();

        // 1 invoke because only invoke to zones logical topology happens
        verify(keyValueStorage, timeout(1000).times(1)).invoke(any());

        assertNull(keyValueStorage.get(zoneDataNodesKey(DEFAULT_ZONE_ID).bytes()).value());
        assertNull(keyValueStorage.get(zoneDataNodesKey(1).bytes()).value());
    }

    private void mockVaultZonesLogicalTopologyKey(Set<String> nodes) {
        byte[] newLogicalTopology = toBytes(nodes);

        when(vaultMgr.get(zonesLogicalTopologyKey()))
                .thenReturn(completedFuture(new VaultEntry(zonesLogicalTopologyKey(), newLogicalTopology)));
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

    private void mockVaultAppliedRevision(long revision) {
        when(metaStorageManager.appliedRevision()).thenReturn(revision);
    }

    private void mockCmgLocalNodes() {
        when(cmgManager.logicalTopology()).thenReturn(completedFuture(topology.getLogicalTopology()));
    }
}
