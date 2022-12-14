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
import static org.apache.ignite.internal.metastorage.impl.MetaStorageServiceImpl.toIfInfo;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.command.MetaStorageCommandsFactory;
import org.apache.ignite.internal.metastorage.command.MultiInvokeCommand;
import org.apache.ignite.internal.metastorage.command.info.StatementResultInfo;
import org.apache.ignite.internal.metastorage.dsl.If;
import org.apache.ignite.internal.metastorage.dsl.StatementResult;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.metastorage.server.raft.MetaStorageListener;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.util.ByteUtils;
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
public class DistributionZoneManagerConfigurationChangesTest extends IgniteAbstractTest {
    private static final String ZONE_NAME = "zone1";

    private static final String NEW_ZONE_NAME = "zone2";

    @Mock
    private ClusterManagementGroupManager cmgManager;

    private DistributionZoneManager distributionZoneManager;

    private SimpleInMemoryKeyValueStorage keyValueStorage;

    private ConfigurationManager clusterCfgMgr;

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

        cmgManager = mock(ClusterManagementGroupManager.class);

        distributionZoneManager = new DistributionZoneManager(
                zonesConfiguration,
                metaStorageManager,
                cmgManager
        );

        clusterCfgMgr.start();

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
        Set<ClusterNode> clusterNodes = Set.of(new ClusterNode("1", "name1", null));

        mockCmgLocalNodes(clusterNodes);

        distributionZoneManager.createZone(new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).build()).get();

        assertDataNodesForZone(1, clusterNodes);

        assertZonesChangeTriggerKey(1);
    }

    @Test
    void testTriggerKeyPropagationAfterZoneUpdate() throws Exception {
        Set<ClusterNode> clusterNodes = Set.of(new ClusterNode("1", "name1", null));

        LogicalTopologySnapshot logicalTopologySnapshot = mockCmgLocalNodes(clusterNodes);

        distributionZoneManager.createZone(new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).build()).get();

        assertZonesChangeTriggerKey(1);

        var clusterNodes2 = Set.of(
                new ClusterNode("1", "name1", null),
                new ClusterNode("2", "name2", null)
        );

        when(logicalTopologySnapshot.nodes()).thenReturn(clusterNodes2);

        distributionZoneManager.alterZone(
                ZONE_NAME,
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjust(100).build()
        ).get();

        assertZonesChangeTriggerKey(2);

        assertDataNodesForZone(1, clusterNodes);
    }

    @Test
    void testZoneDeleteRemovesMetaStorageKey() throws Exception {
        Set<ClusterNode> clusterNodes = Set.of(new ClusterNode("1", "name1", null));

        mockCmgLocalNodes(clusterNodes);

        distributionZoneManager.createZone(new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).build());

        assertDataNodesForZone(1, clusterNodes);

        distributionZoneManager.dropZone(ZONE_NAME);

        assertTrue(waitForCondition(() -> keyValueStorage.get(zoneDataNodesKey(1).bytes()).value() == null, 5000));
    }

    @Test
    void testSeveralZoneCreationsUpdatesTriggerKey() throws Exception {
        Set<ClusterNode> clusterNodes = Set.of(new ClusterNode("1", "name1", null));

        mockCmgLocalNodes(clusterNodes);

        distributionZoneManager.createZone(new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).build()).get();

        distributionZoneManager.createZone(new DistributionZoneConfigurationParameters.Builder(NEW_ZONE_NAME).build()).get();

        assertZonesChangeTriggerKey(2);
    }

    @Test
    void testSeveralZoneUpdatesUpdatesTriggerKey() throws Exception {
        Set<ClusterNode> clusterNodes = Set.of(new ClusterNode("1", "name1", null));

        mockCmgLocalNodes(clusterNodes);

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
        Set<ClusterNode> clusterNodes = Set.of(new ClusterNode("1", "name1", null));

        mockCmgLocalNodes(clusterNodes);

        keyValueStorage.put(zonesChangeTriggerKey().bytes(), ByteUtils.longToBytes(100));

        distributionZoneManager.createZone(new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).build()).get();

        verify(keyValueStorage, timeout(1000).times(1)).invoke(any());

        assertZonesChangeTriggerKey(100);

        assertDataNodesForZone(1, null);
    }

    @Test
    void testTriggerKeyNotPropagatedAfterZoneUpdate() throws Exception {
        Set<ClusterNode> clusterNodes = Set.of(new ClusterNode("1", "name1", null));

        LogicalTopologySnapshot logicalTopologySnapshot = mockCmgLocalNodes(clusterNodes);

        distributionZoneManager.createZone(new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).build()).get();

        assertDataNodesForZone(1, clusterNodes);

        var clusterNodes2 = Set.of(
                new ClusterNode("1", "name1", null),
                new ClusterNode("2", "name2", null)
        );

        when(logicalTopologySnapshot.nodes()).thenReturn(clusterNodes2);

        keyValueStorage.put(zonesChangeTriggerKey().bytes(), ByteUtils.longToBytes(100));

        distributionZoneManager.alterZone(
                ZONE_NAME,
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjust(100).build()
        ).get();

        verify(keyValueStorage, timeout(1000).times(2)).invoke(any());

        assertZonesChangeTriggerKey(100);

        assertDataNodesForZone(1, clusterNodes);
    }

    @Test
    void testZoneDeleteDoNotRemoveMetaStorageKey() throws Exception {
        Set<ClusterNode> clusterNodes = Set.of(new ClusterNode("1", "name1", null));

        mockCmgLocalNodes(clusterNodes);

        distributionZoneManager.createZone(new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).build());

        assertDataNodesForZone(1, clusterNodes);

        keyValueStorage.put(zonesChangeTriggerKey().bytes(), ByteUtils.longToBytes(100));

        distributionZoneManager.dropZone(ZONE_NAME);

        verify(keyValueStorage, timeout(1000).times(2)).invoke(any());

        assertDataNodesForZone(1, clusterNodes);
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
