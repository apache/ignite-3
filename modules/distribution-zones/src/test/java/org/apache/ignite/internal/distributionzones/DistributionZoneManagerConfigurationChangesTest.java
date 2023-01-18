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
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleUpChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyKey;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.util.ByteUtils.longToBytes;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.NamedConfigurationTree;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyServiceImpl;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.command.GetAllCommand;
import org.apache.ignite.internal.metastorage.command.MetaStorageCommandsFactory;
import org.apache.ignite.internal.metastorage.command.MultiInvokeCommand;
import org.apache.ignite.internal.metastorage.command.MultipleEntryResponse;
import org.apache.ignite.internal.metastorage.command.SingleEntryResponse;
import org.apache.ignite.internal.metastorage.dsl.Iif;
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
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

/**
 * Tests distribution zones configuration changes and reaction to that changes.
 */
public class DistributionZoneManagerConfigurationChangesTest extends IgniteAbstractTest {
    private static final String ZONE_NAME = "zone1";

    private static final String NEW_ZONE_NAME = "zone2";

    private static final Set<String> nodes = Set.of("name1");

    private DistributionZoneManager distributionZoneManager;

    private SimpleInMemoryKeyValueStorage keyValueStorage;

    private ConfigurationManager clusterCfgMgr;

    @Mock
    private LogicalTopologyServiceImpl logicalTopologyService;

    private VaultManager vaultMgr;

    @BeforeEach
    public void setUp() {
        clusterCfgMgr = new ConfigurationManager(
                List.of(DistributionZonesConfiguration.KEY),
                Set.of(),
                new TestConfigurationStorage(DISTRIBUTED),
                List.of(),
                List.of()
        );

        DistributionZonesConfiguration zonesConfiguration = clusterCfgMgr.configurationRegistry()
                .getConfiguration(DistributionZonesConfiguration.KEY);

        MetaStorageManager metaStorageManager = mock(MetaStorageManager.class);

        logicalTopologyService = mock(LogicalTopologyServiceImpl.class);

        vaultMgr = mock(VaultManager.class);

        when(vaultMgr.get(any())).thenReturn(completedFuture(null));

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

        doNothing().when(logicalTopologyService).addEventListener(any());

        when(logicalTopologyService.logicalTopologyOnLeader()).thenReturn(completedFuture(new LogicalTopologySnapshot(1, Set.of())));

        mockVaultZonesLogicalTopologyKey(nodes);

        distributionZoneManager.start();

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
    }

    @AfterEach
    public void tearDown() throws Exception {
        distributionZoneManager.stop();

        clusterCfgMgr.stop();

        keyValueStorage.close();
    }

    @Test
    void testDataNodesPropagationAfterZoneCreation() throws Exception {
        assertDataNodesForZone(1, null);

        distributionZoneManager.createZone(new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).build()).get();

        assertDataNodesForZone(1, nodes);

        assertZoneScaleUpChangeTriggerKey(1, 1);

        assertZonesChangeTriggerKey(1, 1);
    }

    @Test
    void testZoneDeleteRemovesMetaStorageKey() throws Exception {
        distributionZoneManager.createZone(new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).build()).get();

        assertDataNodesForZone(1, nodes);

        distributionZoneManager.dropZone(ZONE_NAME).get();

        assertTrue(waitForCondition(() -> keyValueStorage.get(zoneDataNodesKey(1).bytes()).value() == null, 5000));
    }

    @Test
    void testSeveralZoneCreationsUpdatesTriggerKey() throws Exception {
        assertNull(keyValueStorage.get(zoneScaleUpChangeTriggerKey(1).bytes()).value());
        assertNull(keyValueStorage.get(zoneScaleUpChangeTriggerKey(2).bytes()).value());

        distributionZoneManager.createZone(new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).build()).get();

        distributionZoneManager.createZone(new DistributionZoneConfigurationParameters.Builder(NEW_ZONE_NAME).build()).get();

        assertZoneScaleUpChangeTriggerKey(1, 1);
        assertZoneScaleUpChangeTriggerKey(2, 2);
        assertZonesChangeTriggerKey(1, 1);
        assertZonesChangeTriggerKey(2, 2);
    }

    @Test
    void testDataNodesNotPropagatedAfterZoneCreation() throws Exception {
        keyValueStorage.put(zonesChangeTriggerKey(1).bytes(), longToBytes(100));

        distributionZoneManager.createZone(new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).build()).get();

        verify(keyValueStorage, timeout(1000).times(1)).invoke(any());

        assertZonesChangeTriggerKey(100, 1);

        assertDataNodesForZone(1, null);
    }

    @Test
    void testZoneDeleteDoNotRemoveMetaStorageKey() throws Exception {
        distributionZoneManager.createZone(new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).build()).get();

        assertDataNodesForZone(1, nodes);

        keyValueStorage.put(zonesChangeTriggerKey(1).bytes(), longToBytes(100));

        distributionZoneManager.dropZone(ZONE_NAME).get();

        verify(keyValueStorage, timeout(1000).times(2)).invoke(any());

        assertDataNodesForZone(1, nodes);
    }

    private void mockVaultZonesLogicalTopologyKey(Set<String> nodes) {
        byte[] newLogicalTopology = toBytes(nodes);

        when(vaultMgr.get(zonesLogicalTopologyKey()))
                .thenReturn(completedFuture(new VaultEntry(zonesLogicalTopologyKey(), newLogicalTopology)));
    }

    private void assertDataNodesForZone(int zoneId, @Nullable Set<String> clusterNodes) throws InterruptedException {
        byte[] nodes = clusterNodes == null ? null : toBytes(clusterNodes);

        assertTrue(waitForCondition(() -> Arrays.equals(keyValueStorage.get(zoneDataNodesKey(zoneId).bytes()).value(), nodes), 1000));
    }

    private void assertZoneScaleUpChangeTriggerKey(int revision, int zoneId) throws InterruptedException {
        assertTrue(
                waitForCondition(
                        () -> ByteUtils.bytesToLong(keyValueStorage.get(zoneScaleUpChangeTriggerKey(zoneId).bytes()).value()) == revision,
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
}
