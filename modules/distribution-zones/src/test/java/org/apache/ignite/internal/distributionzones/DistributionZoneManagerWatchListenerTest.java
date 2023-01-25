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
import static org.apache.ignite.internal.metastorage.impl.MetaStorageServiceImpl.toIfInfo;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.util.ByteUtils.fromBytes;
import static org.apache.ignite.internal.util.ByteUtils.longToBytes;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
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
import org.apache.ignite.internal.metastorage.command.GetAllCommand;
import org.apache.ignite.internal.metastorage.command.MetaStorageCommandsFactory;
import org.apache.ignite.internal.metastorage.command.MultiInvokeCommand;
import org.apache.ignite.internal.metastorage.command.MultipleEntryResponse;
import org.apache.ignite.internal.metastorage.command.SingleEntryResponse;
import org.apache.ignite.internal.metastorage.command.info.StatementResultInfo;
import org.apache.ignite.internal.metastorage.dsl.If;
import org.apache.ignite.internal.metastorage.dsl.StatementResult;
import org.apache.ignite.internal.metastorage.impl.EntryImpl;
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
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.Nullable;
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
            Set<ByteArray> keysSet = invocationClose.getArgument(0);

            GetAllCommand getAllCommand = commandsFactory.getAllCommand().keys(
                    keysSet.stream().map(ByteArray::bytes).collect(Collectors.toList())
            ).revision(0).build();

            return metaStorageService.run(getAllCommand).thenApply(bi -> {
                MultipleEntryResponse resp = (MultipleEntryResponse) bi;

                Map<ByteArray, org.apache.ignite.internal.metastorage.Entry> res = new HashMap<>();

                for (SingleEntryResponse e : resp.entries()) {
                    ByteArray key = new ByteArray(e.key());

                    res.put(key, new EntryImpl(key.bytes(), e.value(), e.revision(), e.updateCounter()));
                }

                return res;
            });
        }).when(metaStorageManager).getAll(any());
    }

    @AfterEach
    public void tearDown() throws Exception {
        vaultMgr.stop();

        distributionZoneManager.stop();

        clusterCfgMgr.stop();

        keyValueStorage.close();
    }

    @Test
    void testDataNodesOfDefaultZoneUpdatedOnWatchListenerEvent() {
        mockVaultZonesLogicalTopologyKey(Set.of());

        distributionZoneManager.start();

        //first event

        Set<String> nodes = Set.of("node1", "node2");

        watchListenerOnUpdate(nodes, 2);

        verify(keyValueStorage, timeout(1000).times(2)).invoke(any());

        checkDataNodesOfZone(DEFAULT_ZONE_ID, nodes);

        //second event

        nodes = Set.of("node1", "node3");

        watchListenerOnUpdate(nodes, 3);

        verify(keyValueStorage, timeout(1000).times(3)).invoke(any());

        nodes = Set.of("node1", "node2", "node3");

        // Scale up just adds node to data nodes
        checkDataNodesOfZone(DEFAULT_ZONE_ID, nodes);

        //third event

        nodes = Collections.emptySet();

        watchListenerOnUpdate(nodes, 4);

        verify(keyValueStorage, timeout(1000).times(3)).invoke(any());

        nodes = Set.of("node1", "node2", "node3");

        // Scale up wasn't triggered
        checkDataNodesOfZone(DEFAULT_ZONE_ID, nodes);
    }

    private void assertDataNodesForZone(int zoneId, @Nullable Set<String> clusterNodes) throws InterruptedException {
        byte[] nodes = clusterNodes == null ? null : toBytes(clusterNodes);

        assertTrue(waitForCondition(() -> Arrays.equals(keyValueStorage.get(zoneDataNodesKey(zoneId).bytes()).value(), nodes), 1000));
    }

    @Test
    void testStaleWatchEvent() {
        mockVaultZonesLogicalTopologyKey(Set.of());

        mockZones(mockZoneWithAutoAdjustScaleUp(100));

        distributionZoneManager.start();

        mockVaultAppliedRevision(1);

        long revision = 100;

        keyValueStorage.put(zoneScaleUpChangeTriggerKey(1).bytes(), longToBytes(revision));

        Set<String> nodes = Set.of("node1", "node2");

        watchListenerOnUpdate(nodes, revision);

        verify(keyValueStorage, timeout(1000).times(2)).invoke(any());

        checkDataNodesOfZone(DEFAULT_ZONE_ID, Collections.emptySet());
    }

    @Test
    void testStaleVaultRevisionOnZoneManagerStart() {
        long revision = 100;

        keyValueStorage.put(zonesChangeTriggerKey(0).bytes(), longToBytes(revision));

        Set<String> nodes = Set.of("node1", "node2");

        mockVaultZonesLogicalTopologyKey(nodes);

        mockVaultAppliedRevision(revision);

        distributionZoneManager.start();

        verify(metaStorageManager, timeout(1000).times(1)).invoke(any());

        checkDataNodesOfZone(1, null);
    }

    @Test
    void testDataNodesUpdatedOnZoneManagerStart() {
        mockVaultAppliedRevision(2);

        Set<String> nodes = Set.of("node1", "node2");

        mockVaultZonesLogicalTopologyKey(nodes);

        distributionZoneManager.start();

        verify(keyValueStorage, timeout(1000).times(1)).invoke(any());

        checkDataNodesOfZone(DEFAULT_ZONE_ID, nodes);
    }

    @Test
    void testLogicalTopologyIsNullOnZoneManagerStart1() {
        mockZones(mockZoneWithAutoAdjust());

        mockVaultAppliedRevision(2);

        when(vaultMgr.get(zonesLogicalTopologyKey()))
                .thenReturn(completedFuture(new VaultEntry(zonesLogicalTopologyKey(), null)));

        distributionZoneManager.start();

        verify(keyValueStorage, after(500).never()).invoke(any());

        assertNull(keyValueStorage.get(zoneDataNodesKey(DEFAULT_ZONE_ID).bytes()).value());
        assertNull(keyValueStorage.get(zoneDataNodesKey(1).bytes()).value());
    }

    private void checkDataNodesOfZone(int zoneId, Set<String> nodes) {
        Entry entry = keyValueStorage.get(zoneDataNodesKey(zoneId).bytes());

        if (nodes == null) {
            assertNull(entry.value(), () -> "Actual node list: " + fromBytes(entry.value()));
        } else {
            assertNotNull(entry);
            assertNotNull(entry.value());

            Set<String> newDataNodes = fromBytes(entry.value());

            assertTrue(newDataNodes.containsAll(nodes));
            assertEquals(nodes.size(), newDataNodes.size());
        }
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
