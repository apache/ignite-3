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
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.configuration.ConfigurationValue;
import org.apache.ignite.configuration.NamedConfigurationTree;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyServiceImpl;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneChange;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneConfiguration;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneView;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesView;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests distribution zones logical topology changes and reaction to that changes.
 */
//TODO: IGNITE-18564 Add tests with not default distribution zones, when distributionZones.change.trigger per zone will be created.
public class DistributionZoneManagerWatchListenerTest extends IgniteAbstractTest {
    private static final String ZONE_NAME_1 = "zone1";

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
                Set.of(),
                new TestConfigurationStorage(DISTRIBUTED),
                List.of(),
                List.of()
        );

        zonesConfiguration = mock(DistributionZonesConfiguration.class);

        metaStorageManager = mock(MetaStorageManager.class);

        LogicalTopologyServiceImpl logicalTopologyService = mock(LogicalTopologyServiceImpl.class);

        LogicalTopologySnapshot topologySnapshot = mock(LogicalTopologySnapshot.class);

        when(topologySnapshot.version()).thenReturn(1L);
        when(topologySnapshot.nodes()).thenReturn(Collections.emptySet());

        when(logicalTopologyService.logicalTopologyOnLeader()).thenReturn(completedFuture(topologySnapshot));

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

        mockDefaultZoneConfiguration();
        mockDefaultZoneView();
        mockEmptyZonesList();

        AtomicLong raftIndex = new AtomicLong();

        keyValueStorage = spy(new SimpleInMemoryKeyValueStorage("test"));

        MetaStorageListener metaStorageListener = new MetaStorageListener(keyValueStorage);

        RaftGroupService metaStorageService = mock(RaftGroupService.class);

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
    void testDataNodesOfDefaultZoneUpdatedOnWatchListenerEvent() throws InterruptedException {
        mockVaultZonesLogicalTopologyKey(Set.of());

        distributionZoneManager.start();

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
    void testStaleWatchEvent() throws InterruptedException {
        mockVaultZonesLogicalTopologyKey(Set.of());

        mockZones(mockZoneWithAutoAdjustScaleUp(0));

        distributionZoneManager.start();

        long revision = 100;

        keyValueStorage.put(zoneScaleUpChangeTriggerKey(1).bytes(), longToBytes(revision));

        Set<String> nodes = Set.of("node1", "node2");

        watchListenerOnUpdate(nodes, revision);

        // two invokes on start, one for scale up
        verify(keyValueStorage, timeout(1000).times(3)).invoke(any());

        assertDataNodesForZone(DEFAULT_ZONE_ID, Collections.emptySet(), keyValueStorage);
    }

    @Test
    void testStaleVaultRevisionOnZoneManagerStart() throws InterruptedException {
        long revision = 100;

        keyValueStorage.put(zonesChangeTriggerKey(0).bytes(), longToBytes(revision));

        Set<String> nodes = Set.of("node1", "node2");

        mockVaultZonesLogicalTopologyKey(nodes);

        mockVaultAppliedRevision(revision);

        distributionZoneManager.start();

        verify(metaStorageManager, timeout(1000).times(2)).invoke(any());

        assertDataNodesForZone(1, null, keyValueStorage);
    }

    @Test
    void testDataNodesUpdatedOnZoneManagerStart() throws InterruptedException {
        mockVaultAppliedRevision(2);

        Set<String> nodes = Set.of("node1", "node2");

        mockVaultZonesLogicalTopologyKey(nodes);

        distributionZoneManager.start();

        verify(keyValueStorage, timeout(1000).times(2)).invoke(any());

        assertDataNodesForZone(DEFAULT_ZONE_ID, nodes, keyValueStorage);
    }

    @Test
    void testLogicalTopologyIsNullOnZoneManagerStart1() {
        mockZones(mockZoneWithAutoAdjust());

        mockVaultAppliedRevision(2);

        when(vaultMgr.get(zonesLogicalTopologyKey()))
                .thenReturn(completedFuture(new VaultEntry(zonesLogicalTopologyKey(), null)));

        distributionZoneManager.start();

        // 1 invoke because only invoke to zones logical topology happens
        verify(keyValueStorage, timeout(1000).times(1)).invoke(any());

        assertNull(keyValueStorage.get(zoneDataNodesKey(DEFAULT_ZONE_ID).bytes()).value());
        assertNull(keyValueStorage.get(zoneDataNodesKey(1).bytes()).value());
    }

    private void mockEmptyZonesList() {
        NamedConfigurationTree<DistributionZoneConfiguration, DistributionZoneView, DistributionZoneChange> namedConfigurationTree =
                mock(NamedConfigurationTree.class);
        when(zonesConfiguration.distributionZones()).thenReturn(namedConfigurationTree);

        NamedListView<DistributionZoneView> namedListView = mock(NamedListView.class);
        when(namedConfigurationTree.value()).thenReturn(namedListView);

        when(zonesConfiguration.distributionZones().value().namedListKeys()).thenReturn(Collections.emptyList());
    }

    private void mockZones(DistributionZoneConfiguration... zones) {
        List<String> names = new ArrayList<>();

        NamedConfigurationTree<DistributionZoneConfiguration, DistributionZoneView, DistributionZoneChange> namedConfigurationTree =
                mock(NamedConfigurationTree.class);
        when(zonesConfiguration.distributionZones()).thenReturn(namedConfigurationTree);

        NamedListView<DistributionZoneView> namedListView = mock(NamedListView.class);
        when(namedConfigurationTree.value()).thenReturn(namedListView);

        for (DistributionZoneConfiguration zone : zones) {
            names.add(zone.name().value());

            when(namedConfigurationTree.get(zone.name().value())).thenReturn(zone);
        }

        when(namedListView.namedListKeys()).thenReturn(names);
    }

    private DistributionZoneConfiguration mockZoneConfiguration(
            Integer zoneId,
            String name,
            Integer dataNodesAutoAdjustTime,
            Integer dataNodesAutoAdjustScaleUpTime,
            Integer dataNodesAutoAdjustScaleDownTime
    ) {
        DistributionZoneConfiguration distributionZoneConfiguration = mock(DistributionZoneConfiguration.class);

        ConfigurationValue<String> nameValue = mock(ConfigurationValue.class);
        when(distributionZoneConfiguration.name()).thenReturn(nameValue);
        when(nameValue.value()).thenReturn(name);

        ConfigurationValue<Integer> zoneIdValue = mock(ConfigurationValue.class);
        when(distributionZoneConfiguration.zoneId()).thenReturn(zoneIdValue);
        when(zoneIdValue.value()).thenReturn(zoneId);

        ConfigurationValue<Integer> dataNodesAutoAdjust = mock(ConfigurationValue.class);
        when(distributionZoneConfiguration.dataNodesAutoAdjust()).thenReturn(dataNodesAutoAdjust);
        when(dataNodesAutoAdjust.value()).thenReturn(dataNodesAutoAdjustTime);

        ConfigurationValue<Integer> dataNodesAutoAdjustScaleUp = mock(ConfigurationValue.class);
        when(distributionZoneConfiguration.dataNodesAutoAdjustScaleUp()).thenReturn(dataNodesAutoAdjustScaleUp);
        when(dataNodesAutoAdjustScaleUp.value()).thenReturn(dataNodesAutoAdjustScaleUpTime);

        ConfigurationValue<Integer> dataNodesAutoAdjustScaleDown = mock(ConfigurationValue.class);
        when(distributionZoneConfiguration.dataNodesAutoAdjustScaleDown()).thenReturn(dataNodesAutoAdjustScaleDown);
        when(dataNodesAutoAdjustScaleDown.value()).thenReturn(dataNodesAutoAdjustScaleDownTime);

        return distributionZoneConfiguration;
    }

    private DistributionZoneView mockZoneView(
            Integer zoneId,
            String name,
            Integer dataNodesAutoAdjustTime,
            Integer dataNodesAutoAdjustScaleUpTime,
            Integer dataNodesAutoAdjustScaleDownTime
    ) {
        DistributionZoneView distributionZoneView = mock(DistributionZoneView.class);

        when(distributionZoneView.name()).thenReturn(name);
        when(distributionZoneView.zoneId()).thenReturn(zoneId);
        when(distributionZoneView.dataNodesAutoAdjust()).thenReturn(dataNodesAutoAdjustTime);
        when(distributionZoneView.dataNodesAutoAdjustScaleUp()).thenReturn(dataNodesAutoAdjustScaleUpTime);
        when(distributionZoneView.dataNodesAutoAdjustScaleDown()).thenReturn(dataNodesAutoAdjustScaleDownTime);

        return distributionZoneView;
    }

    private DistributionZoneConfiguration mockZoneWithAutoAdjust() {
        return mockZoneConfiguration(1, ZONE_NAME_1, 100, Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    private DistributionZoneConfiguration mockDefaultZoneConfiguration() {
        DistributionZoneConfiguration defaultZone = mockZoneConfiguration(DEFAULT_ZONE_ID, DEFAULT_ZONE_NAME, 100, Integer.MAX_VALUE,
                Integer.MAX_VALUE);

        when(zonesConfiguration.defaultDistributionZone()).thenReturn(defaultZone);

        return defaultZone;
    }

    private DistributionZoneView mockDefaultZoneView() {
        DistributionZoneView defaultZone = mockZoneView(DEFAULT_ZONE_ID, DEFAULT_ZONE_NAME, Integer.MAX_VALUE, 0,
                Integer.MAX_VALUE);

        DistributionZonesView zonesView = mock(DistributionZonesView.class);

        when(zonesView.defaultDistributionZone()).thenReturn(defaultZone);
        when(zonesConfiguration.value()).thenReturn(zonesView);

        return defaultZone;
    }

    private DistributionZoneConfiguration mockZoneWithAutoAdjustScaleUp(int scaleUp) {
        return mockZoneConfiguration(1, ZONE_NAME_1, Integer.MAX_VALUE, scaleUp, Integer.MAX_VALUE);
    }


    private void mockVaultZonesLogicalTopologyKey(Set<String> nodes) {
        byte[] newLogicalTopology = toBytes(nodes);

        when(vaultMgr.get(zonesLogicalTopologyKey()))
                .thenReturn(completedFuture(new VaultEntry(zonesLogicalTopologyKey(), newLogicalTopology)));
    }

    private void watchListenerOnUpdate(Set<String> nodes, long rev) {
        byte[] newLogicalTopology = toBytes(nodes);

        Entry newEntry = new EntryImpl(zonesLogicalTopologyKey().bytes(), newLogicalTopology, rev, 1);

        EntryEvent entryEvent = new EntryEvent(null, newEntry);

        WatchEvent evt = new WatchEvent(entryEvent);

        watchListener.onUpdate(evt);
    }

    private void mockVaultAppliedRevision(long revision) {
        when(metaStorageManager.appliedRevision()).thenReturn(revision);
    }
}
