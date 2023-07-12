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

package org.apache.ignite.internal.distribution.zones;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_ZONE_ID;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_ZONE_NAME;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertDataNodesFromManager;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleDownChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleUpChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesGlobalStateRevision;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.util.ByteUtils.bytesToLong;
import static org.apache.ignite.utils.ClusterServiceTestUtils.defaultSerializationRegistry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.BaseIgniteRestartTest;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.raft.TestClusterStateStorage;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyServiceImpl;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.ConfigurationModules;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.NodeConfigWriteException;
import org.apache.ignite.internal.configuration.storage.DistributedConfigurationStorage;
import org.apache.ignite.internal.configuration.storage.LocalFileConfigurationStorage;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.validation.ConfigurationValidatorImpl;
import org.apache.ignite.internal.distributionzones.DistributionZoneConfigurationParameters;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager.ZoneState;
import org.apache.ignite.internal.distributionzones.NodeWithAttributes;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.If;
import org.apache.ignite.internal.metastorage.server.TestRocksDbKeyValueStorage;
import org.apache.ignite.internal.metastorage.server.raft.MetaStorageWriteHandler;
import org.apache.ignite.internal.network.configuration.NetworkConfiguration;
import org.apache.ignite.internal.network.recovery.VaultStateIds;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NettyBootstrapFactory;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.scalecube.TestScaleCubeClusterServiceFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for checking {@link DistributionZoneManager} behavior after node's restart.
 */
@ExtendWith(ConfigurationExtension.class)
public class ItIgniteDistributionZoneManagerNodeRestartTest extends BaseIgniteRestartTest {
    private static final LogicalNode A = new LogicalNode(
            new ClusterNode("1", "A", new NetworkAddress("localhost", 123)),
            Map.of("region", "US", "storage", "SSD", "dataRegionSize", "10")
    );

    private static final LogicalNode B = new LogicalNode(
            new ClusterNode("2", "B", new NetworkAddress("localhost", 123)),
            Map.of("region", "EU", "storage", "HHD", "dataRegionSize", "30")
    );

    private static final LogicalNode C = new LogicalNode(
            new ClusterNode("3", "C", new NetworkAddress("localhost", 123)),
            Map.of("region", "CN", "storage", "SSD", "dataRegionSize", "20")
    );

    private static final String ZONE_NAME = "zone1";

    private static final int ZONE_ID = 1;

    /**
     * Start some of Ignite components that are able to serve as Ignite node for test purposes.
     *
     * @param idx Node index.
     * @return Partial node.
     */
    private PartialNode startPartialNode(int idx) {
        String name = testNodeName(testInfo, idx);

        Path dir = workDir.resolve(name);

        List<IgniteComponent> components = new ArrayList<>();

        VaultManager vault = createVault(name, dir);

        ConfigurationModules modules = loadConfigurationModules(log, Thread.currentThread().getContextClassLoader());

        Path configFile = workDir.resolve(TestIgnitionManager.DEFAULT_CONFIG_NAME);
        String configString = configurationString(idx);
        try {
            Files.writeString(configFile, configString);
        } catch (IOException e) {
            throw new NodeConfigWriteException("Failed to write config content to file.", e);
        }

        var localConfigurationGenerator = new ConfigurationTreeGenerator(
                modules.local().rootKeys(),
                modules.local().internalSchemaExtensions(),
                modules.local().polymorphicSchemaExtensions()
        );

        var nodeCfgMgr = new ConfigurationManager(
                modules.local().rootKeys(),
                new LocalFileConfigurationStorage(configFile, localConfigurationGenerator),
                localConfigurationGenerator,
                ConfigurationValidatorImpl.withDefaultValidators(localConfigurationGenerator, modules.local().validators())
        );

        NetworkConfiguration networkConfiguration = nodeCfgMgr.configurationRegistry().getConfiguration(NetworkConfiguration.KEY);

        var nettyBootstrapFactory = new NettyBootstrapFactory(networkConfiguration, name);

        var clusterSvc = new TestScaleCubeClusterServiceFactory().createClusterService(
                name,
                networkConfiguration,
                nettyBootstrapFactory,
                defaultSerializationRegistry(),
                new VaultStateIds(vault)
        );

        var clusterStateStorage = new TestClusterStateStorage();

        var logicalTopology = new LogicalTopologyImpl(clusterStateStorage);

        var cmgManager = mock(ClusterManagementGroupManager.class);

        when(cmgManager.logicalTopology()).thenAnswer(invocation -> completedFuture(logicalTopology.getLogicalTopology()));

        var metaStorageMgr = spy(StandaloneMetaStorageManager.create(
                vault,
                new TestRocksDbKeyValueStorage(name, workDir.resolve("metastorage"))
        ));

        var cfgStorage = new DistributedConfigurationStorage(metaStorageMgr);

        ConfigurationTreeGenerator distributedConfigurationGenerator = new ConfigurationTreeGenerator(
                modules.distributed().rootKeys(),
                modules.distributed().internalSchemaExtensions(),
                modules.distributed().polymorphicSchemaExtensions()
        );

        var clusterCfgMgr = new ConfigurationManager(
                modules.distributed().rootKeys(),
                cfgStorage,
                distributedConfigurationGenerator,
                ConfigurationValidatorImpl.withDefaultValidators(distributedConfigurationGenerator, modules.distributed().validators())
        );

        ConfigurationRegistry clusterConfigRegistry = clusterCfgMgr.configurationRegistry();

        DistributionZonesConfiguration zonesConfiguration = clusterConfigRegistry.getConfiguration(DistributionZonesConfiguration.KEY);

        TablesConfiguration tablesConfiguration = clusterConfigRegistry.getConfiguration(TablesConfiguration.KEY);

        LogicalTopologyServiceImpl logicalTopologyService = new LogicalTopologyServiceImpl(logicalTopology, cmgManager);

        DistributionZoneManager distributionZoneManager = new DistributionZoneManager(
                zonesConfiguration,
                tablesConfiguration,
                metaStorageMgr,
                logicalTopologyService,
                vault,
                name
        );

        // Preparing the result map.

        components.add(vault);
        components.add(nodeCfgMgr);

        // Start.

        vault.start();
        vault.putName(name).join();

        nodeCfgMgr.start();

        // Start the remaining components.
        List<IgniteComponent> otherComponents = List.of(
                nettyBootstrapFactory,
                clusterSvc,
                clusterStateStorage,
                cmgManager,
                metaStorageMgr,
                clusterCfgMgr,
                distributionZoneManager
        );

        for (IgniteComponent component : otherComponents) {
            component.start();

            components.add(component);
        }

        PartialNode partialNode = partialNode(
                nodeCfgMgr,
                clusterCfgMgr,
                metaStorageMgr,
                null,
                components,
                localConfigurationGenerator,
                logicalTopology,
                cfgStorage,
                distributedConfigurationGenerator,
                clusterConfigRegistry
        );

        partialNodes.add(partialNode);

        return partialNode;
    }

    @Test
    public void testNodeAttributesRestoredAfterRestart() throws Exception {
        PartialNode partialNode = startPartialNode(0);

        DistributionZoneManager distributionZoneManager = findComponent(partialNode.startedComponents(), DistributionZoneManager.class);

        partialNode.logicalTopology().putNode(A);
        partialNode.logicalTopology().putNode(B);
        partialNode.logicalTopology().putNode(C);

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME)
                        .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                        .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                        .build()
        ).get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

        // TODO: https://issues.apache.org/jira/browse/IGNITE-19506 change this to the causality versioned call to dataNodes.
        assertDataNodesFromManager(distributionZoneManager, 1, Set.of(A, B, C), TIMEOUT_MILLIS);

        Map<String, Map<String, String>> nodeAttributesBeforeRestart = distributionZoneManager.nodesAttributes();

        partialNode.stop();

        partialNode = startPartialNode(0);

        distributionZoneManager = findComponent(partialNode.startedComponents(), DistributionZoneManager.class);

        Map<String, Map<String, String>> nodeAttributesAfterRestart = distributionZoneManager.nodesAttributes();

        assertEquals(3, nodeAttributesAfterRestart.size());

        assertEquals(nodeAttributesBeforeRestart, nodeAttributesAfterRestart);
    }

    @Test
    public void testLogicalTopologyRestoredAfterRestart() throws Exception {
        PartialNode partialNode = startPartialNode(0);

        partialNode.logicalTopology().putNode(A);
        partialNode.logicalTopology().putNode(B);
        partialNode.logicalTopology().putNode(C);

        Set<NodeWithAttributes> logicalTopology = Set.of(A, B, C).stream()
                .map(n -> new NodeWithAttributes(n.name(), n.id(), n.nodeAttributes()))
                .collect(Collectors.toSet());

        DistributionZoneManager distributionZoneManager = findComponent(partialNode.startedComponents(), DistributionZoneManager.class);

        DistributionZoneManager finalDistributionZoneManager = distributionZoneManager;

        assertTrue(waitForCondition(() -> logicalTopology.equals(finalDistributionZoneManager.logicalTopology()), TIMEOUT_MILLIS));

        partialNode.stop();

        partialNode = startPartialNode(0);

        distributionZoneManager = findComponent(partialNode.startedComponents(), DistributionZoneManager.class);

        assertEquals(logicalTopology, distributionZoneManager.logicalTopology());
    }

    @Test
    public void testGlobalStateRevisionUpdatedCorrectly() throws Exception {
        PartialNode partialNode = startPartialNode(0);

        DistributionZoneManager distributionZoneManager = findComponent(partialNode.startedComponents(), DistributionZoneManager.class);

        partialNode.logicalTopology().putNode(A);
        partialNode.logicalTopology().putNode(B);
        partialNode.logicalTopology().putNode(C);

        // TODO: https://issues.apache.org/jira/browse/IGNITE-19506 change this to the causality versioned call to dataNodes.
        assertDataNodesFromManager(distributionZoneManager, DEFAULT_ZONE_ID, Set.of(A, B, C), TIMEOUT_MILLIS);

        MetaStorageManager metaStorageManager = findComponent(partialNode.startedComponents(), MetaStorageManager.class);

        long scaleUpChangeTriggerRevision = bytesToLong(
                metaStorageManager.get(zoneScaleUpChangeTriggerKey(DEFAULT_ZONE_ID)).join().value()
        );

        VaultManager vaultManager = findComponent(partialNode.startedComponents(), VaultManager.class);

        long globalStateRevision = bytesToLong(vaultManager.get(zonesGlobalStateRevision()).join().value());

        assertEquals(scaleUpChangeTriggerRevision, globalStateRevision);
    }

    @ParameterizedTest
    @MethodSource("provideArgumentsRestartTests")
    public void testLocalDataNodesAreRestoredAfterRestart(int zoneId, String zoneName) throws Exception {
        PartialNode partialNode = startPartialNode(0);

        DistributionZoneManager distributionZoneManager = findComponent(partialNode.startedComponents(), DistributionZoneManager.class);

        createZoneOrAlterDefaultZone(distributionZoneManager, zoneName, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE);

        partialNode.logicalTopology().putNode(A);
        partialNode.logicalTopology().putNode(B);
        partialNode.logicalTopology().removeNodes(Set.of(B));

        assertDataNodesFromManager(distributionZoneManager, zoneId, Set.of(A), TIMEOUT_MILLIS);

        partialNode.stop();

        partialNode = startPartialNode(0);

        distributionZoneManager = findComponent(partialNode.startedComponents(), DistributionZoneManager.class);

        assertDataNodesFromManager(distributionZoneManager, zoneId, Set.of(A), TIMEOUT_MILLIS);
    }

    @ParameterizedTest
    @MethodSource("provideArgumentsRestartTests")
    public void testScaleUpTimerIsRestoredAfterRestart(int zoneId, String zoneName) throws Exception {
        PartialNode partialNode = startPartialNode(0);

        DistributionZoneManager distributionZoneManager = findComponent(partialNode.startedComponents(), DistributionZoneManager.class);

        createZoneOrAlterDefaultZone(distributionZoneManager, zoneName, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE);

        partialNode.logicalTopology().putNode(A);
        partialNode.logicalTopology().putNode(B);

        assertDataNodesFromManager(
                distributionZoneManager,
                zoneId,
                Set.of(A, B),
                TIMEOUT_MILLIS
        );

        MetaStorageManager metaStorageManager = findComponent(partialNode.startedComponents(), MetaStorageManager.class);

        // Block scale up
        blockUpdate(metaStorageManager, zoneScaleUpChangeTriggerKey(zoneId));

        partialNode.logicalTopology().putNode(C);
        partialNode.logicalTopology().removeNodes(Set.of(B));

        assertDataNodesFromManager(
                distributionZoneManager,
                zoneId,
                Set.of(A),
                TIMEOUT_MILLIS
        );

        partialNode.stop();

        partialNode = startPartialNode(0);

        distributionZoneManager = findComponent(partialNode.startedComponents(), DistributionZoneManager.class);

        assertDataNodesFromManager(distributionZoneManager, zoneId, Set.of(A, C), TIMEOUT_MILLIS);
    }

    @ParameterizedTest
    @MethodSource("provideArgumentsRestartTests")
    public void testScaleUpTriggeredByFilterUpdateIsRestoredAfterRestart(int zoneId, String zoneName) throws Exception {
        PartialNode partialNode = startPartialNode(0);

        DistributionZoneManager distributionZoneManager = findComponent(partialNode.startedComponents(), DistributionZoneManager.class);

        createZoneOrAlterDefaultZone(distributionZoneManager, zoneName, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE);

        partialNode.logicalTopology().putNode(A);

        assertDataNodesFromManager(distributionZoneManager, zoneId, Set.of(A), TIMEOUT_MILLIS);

        distributionZoneManager.alterZone(
                zoneName,
                new DistributionZoneConfigurationParameters.Builder(zoneName)
                        .dataNodesAutoAdjustScaleUp(INFINITE_TIMER_VALUE)
                        .build()
        ).get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

        partialNode.logicalTopology().putNode(B);

        assertDataNodesFromManager(distributionZoneManager, zoneId, Set.of(A), TIMEOUT_MILLIS);

        MetaStorageManager metaStorageManager = findComponent(partialNode.startedComponents(), MetaStorageManager.class);

        blockUpdate(metaStorageManager, zoneScaleUpChangeTriggerKey(zoneId));

        // Only Node B passes the filter
        String filter = "$[?(@.dataRegionSize > 10)]";

        distributionZoneManager.alterZone(
                zoneName,
                new DistributionZoneConfigurationParameters.Builder(zoneName)
                        .filter(filter)
                        .build()
        ).get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

        assertDataNodesFromManager(distributionZoneManager, zoneId, Set.of(A), TIMEOUT_MILLIS);

        partialNode.stop();

        partialNode = startPartialNode(0);

        distributionZoneManager = findComponent(partialNode.startedComponents(), DistributionZoneManager.class);

        assertDataNodesFromManager(distributionZoneManager, zoneId, Set.of(B), TIMEOUT_MILLIS);
    }

    @ParameterizedTest
    @MethodSource("provideArgumentsRestartTests")
    public void testScaleUpsTriggeredByFilterUpdateAndNodeJoinAreRestoredAfterRestart(int zoneId, String zoneName) throws Exception {
        PartialNode partialNode = startPartialNode(0);

        DistributionZoneManager distributionZoneManager = findComponent(partialNode.startedComponents(), DistributionZoneManager.class);

        createZoneOrAlterDefaultZone(distributionZoneManager, zoneName, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE);

        partialNode.logicalTopology().putNode(A);

        assertDataNodesFromManager(distributionZoneManager, zoneId, Set.of(A), TIMEOUT_MILLIS);

        MetaStorageManager metaStorageManager = findComponent(partialNode.startedComponents(), MetaStorageManager.class);

        blockUpdate(metaStorageManager, zoneScaleUpChangeTriggerKey(zoneId));

        distributionZoneManager.alterZone(
                zoneName,
                new DistributionZoneConfigurationParameters.Builder(zoneName)
                        .dataNodesAutoAdjustScaleUp(100)
                        .build()
        ).get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

        partialNode.logicalTopology().putNode(B);

        assertDataNodesFromManager(distributionZoneManager, zoneId, Set.of(A), TIMEOUT_MILLIS);

        // Only Node B and C passes the filter
        String filter = "$[?(@.dataRegionSize > 10)]";

        distributionZoneManager.alterZone(
                zoneName,
                new DistributionZoneConfigurationParameters.Builder(zoneName)
                        .filter(filter)
                        .build()
        ).get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

        partialNode.logicalTopology().putNode(C);

        partialNode.logicalTopology().removeNodes(Set.of(A));

        assertDataNodesFromManager(distributionZoneManager, zoneId, Set.of(), TIMEOUT_MILLIS);

        partialNode.stop();

        partialNode = startPartialNode(0);

        distributionZoneManager = findComponent(partialNode.startedComponents(), DistributionZoneManager.class);

        // Immediate timer triggered by filter update
        assertDataNodesFromManager(distributionZoneManager, zoneId, Set.of(B), TIMEOUT_MILLIS);

        ZoneState zoneState = distributionZoneManager.zonesState().get(zoneId);

        // Timer scheduled after join of the node C
        assertNotNull(zoneState.scaleUpTask());

        distributionZoneManager.alterZone(
                zoneName,
                new DistributionZoneConfigurationParameters.Builder(zoneName)
                        .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                        .build()
        ).get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

        assertDataNodesFromManager(distributionZoneManager, zoneId, Set.of(B, C), TIMEOUT_MILLIS);
    }

    @ParameterizedTest
    @MethodSource("provideArgumentsRestartTests")
    public void testScaleDownTimerIsRestoredAfterRestart(int zoneId, String zoneName) throws Exception {
        PartialNode partialNode = startPartialNode(0);

        DistributionZoneManager distributionZoneManager = findComponent(partialNode.startedComponents(), DistributionZoneManager.class);

        createZoneOrAlterDefaultZone(distributionZoneManager, zoneName, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE);

        MetaStorageManager metaStorageManager = findComponent(partialNode.startedComponents(), MetaStorageManager.class);

        // Block scale down
        blockUpdate(metaStorageManager, zoneScaleDownChangeTriggerKey(zoneId));

        partialNode.logicalTopology().putNode(A);
        partialNode.logicalTopology().putNode(B);
        partialNode.logicalTopology().removeNodes(Set.of(B));
        partialNode.logicalTopology().putNode(C);

        assertDataNodesFromManager(
                distributionZoneManager,
                zoneId,
                Set.of(A, B, C),
                TIMEOUT_MILLIS
        );

        partialNode.stop();

        partialNode = startPartialNode(0);

        distributionZoneManager = findComponent(partialNode.startedComponents(), DistributionZoneManager.class);

        assertDataNodesFromManager(distributionZoneManager, zoneId, Set.of(A, C), TIMEOUT_MILLIS);
    }

    private static Stream<Arguments> provideArgumentsRestartTests() {
        List<Arguments> args = new ArrayList<>();

        args.add(Arguments.of(DEFAULT_ZONE_ID, DEFAULT_ZONE_NAME));
        args.add(Arguments.of(ZONE_ID, ZONE_NAME));

        return args.stream();
    }

    private static void createZoneOrAlterDefaultZone(
            DistributionZoneManager distributionZoneManager,
            String zoneName,
            int scaleUp,
            int scaleDown
    ) throws Exception {
        if (zoneName.equals(DEFAULT_ZONE_NAME)) {
            distributionZoneManager.alterZone(
                    DEFAULT_ZONE_NAME,
                    new DistributionZoneConfigurationParameters.Builder(DEFAULT_ZONE_NAME)
                            .dataNodesAutoAdjustScaleUp(scaleUp)
                            .dataNodesAutoAdjustScaleDown(scaleDown)
                            .build()
            ).get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

            ZoneState zoneState = distributionZoneManager.zonesState().get(DEFAULT_ZONE_ID);

            // This is needed because we want to wait for the end of scale up/down triggered by altering delays.
            if (zoneState.scaleUpTask() != null) {
                assertTrue(waitForCondition(() -> zoneState.scaleUpTask().isDone(), TIMEOUT_MILLIS));
            }

            if (zoneState.scaleDownTask() != null) {
                assertTrue(waitForCondition(() -> zoneState.scaleDownTask().isDone(), TIMEOUT_MILLIS));
            }
        } else {
            distributionZoneManager.createZone(
                    new DistributionZoneConfigurationParameters.Builder(ZONE_NAME)
                            .dataNodesAutoAdjustScaleUp(scaleUp)
                            .dataNodesAutoAdjustScaleDown(scaleDown)
                            .build()
            ).get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        }
    }

    private static void blockUpdate(MetaStorageManager metaStorageManager, ByteArray key) {
        when(metaStorageManager.invoke(argThat(iif -> {
            If iif1 = MetaStorageWriteHandler.toIf(iif);

            byte[] keyScaleUp = key.bytes();

            return iif1.andThen().update().operations().stream().anyMatch(op -> Arrays.equals(keyScaleUp, op.key()));
        }))).thenThrow(new RuntimeException("Expected"));
    }
}
