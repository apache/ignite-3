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
import static org.apache.ignite.internal.metastorage.client.MetaStorageServiceImpl.toIfInfo;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.util.ByteUtils.fromBytes;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyServiceImpl;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.client.If;
import org.apache.ignite.internal.metastorage.client.StatementResult;
import org.apache.ignite.internal.metastorage.common.StatementResultInfo;
import org.apache.ignite.internal.metastorage.common.command.MetaStorageCommandsFactory;
import org.apache.ignite.internal.metastorage.common.command.MultiInvokeCommand;
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

    @Mock
    private ClusterManagementGroupManager cmgManager;

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
                Map.of(),
                new TestConfigurationStorage(DISTRIBUTED),
                List.of(),
                List.of()
        );

        DistributionZonesConfiguration zonesConfiguration = clusterCfgMgr.configurationRegistry()
                .getConfiguration(DistributionZonesConfiguration.KEY);

        MetaStorageManager metaStorageManager = mock(MetaStorageManager.class);

        when(metaStorageManager.registerWatch(any(ByteArray.class), any())).thenReturn(completedFuture(null));

        cmgManager = mock(ClusterManagementGroupManager.class);

        logicalTopologyService = mock(LogicalTopologyServiceImpl.class);

        vaultMgr = mock(VaultManager.class);

        when(vaultMgr.get(any())).thenReturn(completedFuture(null));

        distributionZoneManager = new DistributionZoneManager(
                zonesConfiguration,
                metaStorageManager,
                cmgManager,
                logicalTopologyService,
                vaultMgr
        );

        clusterCfgMgr.start();

        doNothing().when(logicalTopologyService).addEventListener(any());

        when(logicalTopologyService.logicalTopologyOnLeader()).thenReturn(completedFuture(new LogicalTopologySnapshot(1, Set.of())));

        mockVaultZonesLogicalTopologyKey(nodes);

        distributionZoneManager.start();

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
        distributionZoneManager.stop();

        clusterCfgMgr.stop();

        keyValueStorage.close();
    }

    @Test
    void testDataNodesPropagationAfterZoneCreation() throws Exception {
        distributionZoneManager.createZone(new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).build()).get();

        assertDataNodesForZone(1, nodes);

        assertZonesChangeTriggerKey(1);
    }

    @Test
    void testTriggerKeyPropagationAfterZoneUpdate() throws Exception {
        distributionZoneManager.createZone(new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).build()).get();

        assertZonesChangeTriggerKey(1);

        distributionZoneManager.alterZone(
                ZONE_NAME,
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjust(100).build()
        ).get();

        assertZonesChangeTriggerKey(2);

        assertDataNodesForZone(1, nodes);
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
        distributionZoneManager.createZone(new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).build()).get();

        distributionZoneManager.createZone(new DistributionZoneConfigurationParameters.Builder(NEW_ZONE_NAME).build()).get();

        assertZonesChangeTriggerKey(2);
    }

    @Test
    void testSeveralZoneUpdatesUpdatesTriggerKey() throws Exception {
        distributionZoneManager.createZone(new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).build()).get();

        distributionZoneManager.alterZone(
                ZONE_NAME,
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjust(100).build()
        ).get();

        distributionZoneManager.alterZone(
                ZONE_NAME,
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjust(1000).build()
        ).get();

        assertZonesChangeTriggerKey(3);
    }

    @Test
    void testDataNodesNotPropagatedAfterZoneCreation() throws Exception {
        keyValueStorage.put(zonesChangeTriggerKey().bytes(), longToBytes(100));

        distributionZoneManager.createZone(new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).build()).get();

        verify(keyValueStorage, timeout(1000).times(1)).invoke(any());

        assertZonesChangeTriggerKey(100);

        assertDataNodesForZone(1, null);
    }

    @Test
    void testTriggerKeyNotPropagatedAfterZoneUpdate() throws Exception {
        distributionZoneManager.createZone(new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).build()).get();

        assertDataNodesForZone(1, nodes);

        keyValueStorage.put(zonesChangeTriggerKey().bytes(), longToBytes(100));

        distributionZoneManager.alterZone(
                ZONE_NAME,
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjust(100).build()
        ).get();

        verify(keyValueStorage, timeout(1000).times(2)).invoke(any());

        assertZonesChangeTriggerKey(100);

        assertDataNodesForZone(1, nodes);
    }

    @Test
    void testZoneDeleteDoNotRemoveMetaStorageKey() throws Exception {
        distributionZoneManager.createZone(new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).build()).get();

        assertDataNodesForZone(1, nodes);

        keyValueStorage.put(zonesChangeTriggerKey().bytes(), longToBytes(100));

        distributionZoneManager.dropZone(ZONE_NAME).get();

        verify(keyValueStorage, timeout(1000).times(2)).invoke(any());

        assertDataNodesForZone(1, nodes);
    }

    private void mockVaultZonesLogicalTopologyKey(Set<String> nodes) {
        byte[] newLogicalTopology = toBytes(nodes);

        when(vaultMgr.get(zonesLogicalTopologyKey()))
                .thenReturn(completedFuture(new VaultEntry(zonesLogicalTopologyKey(), newLogicalTopology)));
    }

    private void assertDataNodesForZone(int zoneId, @Nullable Set<String> expectedNodes) throws InterruptedException {
        if (expectedNodes == null) {
            assertNull(keyValueStorage.get(zoneDataNodesKey(zoneId).bytes()).value());
        } else {
            Set<String> actual = fromBytes(keyValueStorage.get(zoneDataNodesKey(zoneId).bytes()).value());

            assertTrue(expectedNodes.containsAll(actual));
            assertTrue(expectedNodes.size() == actual.size());
        }
    }

    private void assertZonesChangeTriggerKey(int revision) throws InterruptedException {
        assertTrue(
                waitForCondition(
                        () -> ByteUtils.bytesToLong(keyValueStorage.get(zonesChangeTriggerKey().bytes()).value()) == revision, 1000
                )
        );
    }
}
