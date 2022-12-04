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

package org.apache.ignite.internal.runner.app;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.configuration.annotation.ConfigurationType.DISTRIBUTED;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesChangeTriggerKey;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.storage.DistributedConfigurationStorage;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.distributionzones.DistributionZoneConfigurationParameters;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.distributionzones.DistributionZonesUtil;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.common.command.WatchRangeKeysCommand;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.persistence.PersistentVaultService;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.lang.IgniteUuidGenerator;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.StaticNodeFinder;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.utils.ClusterServiceTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;

@ExtendWith(WorkDirectoryExtension.class)
@ExtendWith(ConfigurationExtension.class)
public class ItDistributionZoneManagerConfigurationChangesTest extends IgniteAbstractTest {
    private static final String ZONE_NAME = "zone1";

    private static final String NEW_ZONE_NAME = "zone2";

    private static final IgniteUuidGenerator uuidGenerator = new IgniteUuidGenerator(UUID.randomUUID(), 0);

    private final ConfigurationRegistry registry = new ConfigurationRegistry(
            List.of(DistributionZonesConfiguration.KEY),
            Map.of(),
            new TestConfigurationStorage(DISTRIBUTED),
            List.of(),
            List.of()
    );

    @InjectConfiguration
    private static RaftConfiguration raftConfiguration;

    /**
     * An emulation of an Ignite node, that only contains components necessary for tests.
     */
    private static class Node {
        private final VaultManager vaultManager;

        private final ClusterService clusterService;

        @Mock
        private final ClusterManagementGroupManager cmgManager;

        private final Loza raftManager;

        private final MetaStorageManager metaStorageManager;

        private final DistributedConfigurationStorage cfgStorage;

        private final ConfigurationManager clusterCfgMgr;

        private final DistributionZoneManager distributionZoneManager;

        /**
         * Constructor that simply creates a subset of components of this node.
         */
        Node(TestInfo testInfo, Path workDir) throws NodeStoppingException {
            var addr = new NetworkAddress("localhost", 10000);

            vaultManager = new VaultManager(new PersistentVaultService(workDir.resolve("vault")));

            clusterService = ClusterServiceTestUtils.clusterService(
                    testInfo,
                    addr.port(),
                    new StaticNodeFinder(List.of(addr))
            );

            raftManager = new Loza(clusterService, raftConfiguration, workDir, new HybridClockImpl());
            //raftManager = mock(Loza.class);

            RaftGroupService raftGroupService = mock(RaftGroupService.class);

            when(raftManager.prepareRaftGroup(any(), any(), any(), any())).thenReturn(completedFuture(raftGroupService));

            when(raftGroupService.run(any(WatchRangeKeysCommand.class))).thenReturn(completedFuture(uuidGenerator.randomUuid()));

            cmgManager = mock(ClusterManagementGroupManager.class);

            //metaStorageManager = spy(MetaStorageManager.class);

            metaStorageManager = new MetaStorageManager(
                    vaultManager,
                    clusterService,
                    cmgManager,
                    raftManager,
                    new SimpleInMemoryKeyValueStorage()
            );

            cfgStorage = new DistributedConfigurationStorage(metaStorageManager, vaultManager);

            clusterCfgMgr = new ConfigurationManager(
                    List.of(DistributionZonesConfiguration.KEY),
                    Map.of(),
                    cfgStorage,
                    List.of(),
                    List.of()
            );

            DistributionZonesConfiguration zonesConfiguration = clusterCfgMgr.configurationRegistry()
                    .getConfiguration(DistributionZonesConfiguration.KEY);

            distributionZoneManager = new DistributionZoneManager(zonesConfiguration, metaStorageManager, cmgManager);
        }

        /**
         * Starts the created components.
         */
        void start() throws Exception {
            vaultManager.start();

            clusterService.start();

            String name = name();

            when(cmgManager.metaStorageNodes()).thenReturn(completedFuture(List.of(name)));

            Stream.of(
                    raftManager,
                    cmgManager,
                    metaStorageManager,
                    clusterCfgMgr,
                    distributionZoneManager
            ).forEach(IgniteComponent::start);

            // deploy watches to propagate data from the metastore into the vault
            metaStorageManager.deployWatches();
        }

        /**
         * Stops the created components.
         */
        void stop() throws Exception {
            var components =
                    List.of(
                            distributionZoneManager,
                            clusterCfgMgr,
                            metaStorageManager,
                            cmgManager,
                            raftManager,
                            clusterService,
                            vaultManager
                    );

            for (IgniteComponent igniteComponent : components) {
                igniteComponent.beforeNodeStop();
            }

            for (IgniteComponent component : components) {
                component.stop();
            }
        }

        String name() {
            return clusterService.topologyService().localMember().name();
        }
    }

    @Test
    void testDataNodesPropagationAfterZoneCreation(@WorkDirectory Path workDir, TestInfo testInfo) throws Exception {
        var node = new Node(testInfo, workDir);

        try {
            node.start();

            List<ClusterNode> clusterNodes = List.of(new ClusterNode("1", "name1", null));

            when(node.cmgManager.logicalTopology()).thenReturn(completedFuture(clusterNodes));

            node.distributionZoneManager.createZone(new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).build());

            assertDataNodesForZone(node, 1, clusterNodes);

            assertZonesChangeTriggerKey(node, 1);
        } finally {
            node.stop();
        }
    }

    @Test
    void testDataNodesPropagationAfterZoneUpdate(@WorkDirectory Path workDir, TestInfo testInfo) throws Exception {
        var node = new Node(testInfo, workDir);

        try {
            node.start();

            List<ClusterNode> clusterNodes = List.of(new ClusterNode("1", "name1", null));

            when(node.cmgManager.logicalTopology()).thenReturn(completedFuture(clusterNodes));

            node.distributionZoneManager.createZone(new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).build()).get();

            //assertDataNodesForZone(node, 1, clusterNodes);

            var clusterNodes2 = List.of(
                    new ClusterNode("1", "name1", null),
                    new ClusterNode("2", "name2", null)
            );

            when(node.cmgManager.logicalTopology()).thenReturn(completedFuture(clusterNodes2));

            node.distributionZoneManager.alterZone(
                    ZONE_NAME,
                    new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).dataNodesAutoAdjust(100).build()
            ).get();

            assertDataNodesForZone(node, 1, clusterNodes2);

            assertZonesChangeTriggerKey(node, 3);
        } finally {
            node.stop();
        }
    }

    @Test
    void testZoneDeleteMetaStorageKeyRemove(@WorkDirectory Path workDir, TestInfo testInfo) throws Exception {
        var node = new Node(testInfo, workDir);

        try {
            node.start();

            List<ClusterNode> clusterNodes = List.of(new ClusterNode("1", "name1", null));

            when(node.cmgManager.logicalTopology()).thenReturn(completedFuture(clusterNodes));

            node.distributionZoneManager.createZone(new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).build());

            assertDataNodesForZone(node, 1, clusterNodes);

            node.distributionZoneManager.dropZone(ZONE_NAME);

            assertTrue(waitForCondition(() -> {
                try {
                    return node.metaStorageManager.get(zoneDataNodesKey(1)).get().value() == null;
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }, 5000));
        } finally {
            node.stop();
        }
    }

    private static void assertDataNodesForZone(Node node, int zoneId, List<ClusterNode> clusterNodes) throws InterruptedException {
        assertTrue(waitForCondition(() -> {
            try {
                return Arrays.equals(
                        node.metaStorageManager.get(zoneDataNodesKey(zoneId)).get().value(),
                        ByteUtils.toBytes(clusterNodes.stream().map(ClusterNode::name).collect(Collectors.toSet()))
                );
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }, 5000));
    }

    private static void assertZonesChangeTriggerKey(Node node, int revision) throws InterruptedException {
        assertTrue(waitForCondition(() -> {
            try {
                return node.metaStorageManager.get(zonesChangeTriggerKey()).get().value() != null;
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }, 5000));

        assertTrue(waitForCondition(() -> {
            try {
                return ByteUtils.bytesToLong(node.metaStorageManager.get(zonesChangeTriggerKey()).get().value()) == revision;
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }, 5000));
    }

    /**
     * Starts the Vault component.
     */
    private static VaultManager createVault(Path workDir) {
        Path vaultPath = workDir.resolve(Paths.get("vault"));

        try {
            Files.createDirectories(vaultPath);
        } catch (IOException e) {
            throw new IgniteInternalException(e);
        }

        return new VaultManager(new PersistentVaultService(vaultPath));
    }
}