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
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.recovery.ConfigurationCatchUpListener.CONFIGURATION_CATCH_UP_DIFFERENCE_PROPERTY;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.utils.ClusterServiceTestUtils.defaultSerializationRegistry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.ignite.internal.BaseIgniteRestartTest;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.raft.TestClusterStateStorage;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyServiceImpl;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.ConfigurationModules;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.NodeConfigWriteException;
import org.apache.ignite.internal.configuration.storage.DistributedConfigurationStorage;
import org.apache.ignite.internal.configuration.storage.LocalFileConfigurationStorage;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.validation.ConfigurationValidatorImpl;
import org.apache.ignite.internal.distributionzones.DistributionZoneConfigurationParameters;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.distributionzones.NodeWithAttributes;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.TestRocksDbKeyValueStorage;
import org.apache.ignite.internal.network.configuration.NetworkConfiguration;
import org.apache.ignite.internal.network.recovery.VaultStateIds;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NettyBootstrapFactory;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.scalecube.TestScaleCubeClusterServiceFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for checking {@link DistributionZoneManager} behavior after node's restart.
 */
@WithSystemProperty(key = CONFIGURATION_CATCH_UP_DIFFERENCE_PROPERTY, value = "0")
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

        var metaStorageMgr = StandaloneMetaStorageManager.create(
                vault,
                new TestRocksDbKeyValueStorage("test", workDir.resolve("metastorage"))
        );

        var cfgStorage = new DistributedConfigurationStorage(metaStorageMgr, vault);

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

        DistributionZonesConfiguration zonesConfiguration = clusterCfgMgr.configurationRegistry()
                .getConfiguration(DistributionZonesConfiguration.KEY);

        TablesConfiguration tablesConfiguration = clusterCfgMgr.configurationRegistry().getConfiguration(TablesConfiguration.KEY);

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
                null,
                components,
                localConfigurationGenerator,
                logicalTopology,
                cfgStorage,
                distributedConfigurationGenerator
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

        Set<String> nodes = distributionZoneManager.topologyVersionedDataNodes(
                1,
                partialNode.logicalTopology().getLogicalTopology().version()
        ).get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

        assertEquals(Set.of(A, B, C).stream().map(ClusterNode::name).collect(Collectors.toSet()), nodes);

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
}
