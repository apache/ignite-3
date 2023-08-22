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
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_ZONE_NAME;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertDataNodesFromManager;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertValueInStorage;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleDownChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleUpChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesGlobalStateRevision;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.util.ByteUtils.bytesToLong;
import static org.apache.ignite.internal.util.ByteUtils.fromBytes;
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
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.LongFunction;
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
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager.ZoneState;
import org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil;
import org.apache.ignite.internal.distributionzones.DistributionZonesUtil;
import org.apache.ignite.internal.distributionzones.Node;
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
import org.apache.ignite.network.ClusterNodeImpl;
import org.apache.ignite.network.NettyBootstrapFactory;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.scalecube.TestScaleCubeClusterServiceFactory;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for checking {@link DistributionZoneManager} behavior after node's restart.
 */
@ExtendWith(ConfigurationExtension.class)
public class ItIgniteDistributionZoneManagerNodeRestartTest extends BaseIgniteRestartTest {
    private static final LogicalNode A = new LogicalNode(
            new ClusterNodeImpl("1", "A", new NetworkAddress("localhost", 123)),
            Map.of("region", "US", "storage", "SSD", "dataRegionSize", "10")
    );

    private static final LogicalNode B = new LogicalNode(
            new ClusterNodeImpl("2", "B", new NetworkAddress("localhost", 123)),
            Map.of("region", "EU", "storage", "HHD", "dataRegionSize", "30")
    );

    private static final LogicalNode C = new LogicalNode(
            new ClusterNodeImpl("3", "C", new NetworkAddress("localhost", 123)),
            Map.of("region", "CN", "storage", "SSD", "dataRegionSize", "20")
    );

    private static final String ZONE_NAME = "zone1";

    private MetaStorageManager metastore;

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
                modules.local().schemaExtensions(),
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

        metastore = spy(StandaloneMetaStorageManager.create(
                vault,
                new TestRocksDbKeyValueStorage(name, workDir.resolve("metastorage"))
        ));

        Consumer<LongFunction<CompletableFuture<?>>> revisionUpdater = (LongFunction<CompletableFuture<?>> function) ->
                metastore.registerRevisionUpdateListener(function::apply);

        var cfgStorage = new DistributedConfigurationStorage(metastore);

        ConfigurationTreeGenerator distributedConfigurationGenerator = new ConfigurationTreeGenerator(
                modules.distributed().rootKeys(),
                modules.distributed().schemaExtensions(),
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
                revisionUpdater,
                zonesConfiguration,
                tablesConfiguration,
                metastore,
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
                metastore,
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
                metastore,
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
        PartialNode node = startPartialNode(0);

        node.logicalTopology().putNode(A);
        node.logicalTopology().putNode(B);
        node.logicalTopology().putNode(C);

        createZone(node, ZONE_NAME, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE);

        int zoneId = getZoneId(node, ZONE_NAME);

        DistributionZoneManager distributionZoneManager = getDistributionZoneManager(node);

        assertDataNodesFromManager(distributionZoneManager, metastore::appliedRevision, zoneId, Set.of(A, B, C), TIMEOUT_MILLIS);

        Map<String, Map<String, String>> nodeAttributesBeforeRestart = distributionZoneManager.nodesAttributes();

        node.stop();

        node = startPartialNode(0);

        distributionZoneManager = getDistributionZoneManager(node);

        Map<String, Map<String, String>> nodeAttributesAfterRestart = distributionZoneManager.nodesAttributes();

        assertEquals(3, nodeAttributesAfterRestart.size());

        assertEquals(nodeAttributesBeforeRestart, nodeAttributesAfterRestart);
    }

    @Test
    public void testLogicalTopologyRestoredAfterRestart() throws Exception {
        PartialNode node = startPartialNode(0);

        node.logicalTopology().putNode(A);
        node.logicalTopology().putNode(B);
        node.logicalTopology().putNode(C);

        Set<NodeWithAttributes> logicalTopology = Stream.of(A, B, C)
                .map(n -> new NodeWithAttributes(n.name(), n.id(), n.nodeAttributes()))
                .collect(toSet());

        DistributionZoneManager distributionZoneManager = getDistributionZoneManager(node);
        DistributionZoneManager finalDistributionZoneManager = distributionZoneManager;

        assertTrue(waitForCondition(() -> logicalTopology.equals(finalDistributionZoneManager.logicalTopology()), TIMEOUT_MILLIS));

        node.stop();

        node = startPartialNode(0);

        distributionZoneManager = getDistributionZoneManager(node);

        assertEquals(logicalTopology, distributionZoneManager.logicalTopology());
    }

    @Test
    public void testGlobalStateRevisionUpdatedCorrectly() throws Exception {
        PartialNode node = startPartialNode(0);

        node.logicalTopology().putNode(A);
        node.logicalTopology().putNode(B);
        node.logicalTopology().putNode(C);

        int zoneId = getZoneId(node, DEFAULT_ZONE_NAME);

        assertDataNodesFromManager(getDistributionZoneManager(node), metastore::appliedRevision, zoneId, Set.of(A, B, C), TIMEOUT_MILLIS);

        long scaleUpChangeTriggerRevision = bytesToLong(metastore.get(zoneScaleUpChangeTriggerKey(zoneId)).join().value());

        VaultManager vaultManager = getStartedComponent(node, VaultManager.class);

        long globalStateRevision = bytesToLong(vaultManager.get(zonesGlobalStateRevision()).join().value());

        assertEquals(scaleUpChangeTriggerRevision, globalStateRevision);
    }

    @ParameterizedTest
    @MethodSource("provideArgumentsRestartTests")
    public void testLocalDataNodesAreRestoredAfterRestart(String zoneName) throws Exception {
        PartialNode node = startPartialNode(0);

        createZoneOrAlterDefaultZone(node, zoneName, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE);

        node.logicalTopology().putNode(A);
        node.logicalTopology().putNode(B);

        int zoneId = getZoneId(node, zoneName);

        DistributionZoneManager distributionZoneManager = getDistributionZoneManager(node);

        assertDataNodesFromManager(distributionZoneManager, metastore::appliedRevision, zoneId, Set.of(A, B), TIMEOUT_MILLIS);

        node.logicalTopology().removeNodes(Set.of(B));

        assertDataNodesFromManager(distributionZoneManager, metastore::appliedRevision, zoneId, Set.of(A), TIMEOUT_MILLIS);

        long revisionBeforeRestart = metastore.appliedRevision();

        node.stop();

        node = startPartialNode(0);

        distributionZoneManager = getDistributionZoneManager(node);

        assertDataNodesFromManager(distributionZoneManager, metastore::appliedRevision, zoneId, Set.of(A), TIMEOUT_MILLIS);
        assertDataNodesFromManager(distributionZoneManager, () -> revisionBeforeRestart, zoneId, Set.of(A), TIMEOUT_MILLIS);
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-20054")
    @ParameterizedTest
    @MethodSource("provideArgumentsRestartTests")
    public void testScaleUpTimerIsRestoredAfterRestart(String zoneName) throws Exception {
        PartialNode node = startPartialNode(0);

        createZoneOrAlterDefaultZone(node, zoneName, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE);

        node.logicalTopology().putNode(A);
        node.logicalTopology().putNode(B);

        int zoneId = getZoneId(node, zoneName);

        DistributionZoneManager distributionZoneManager = getDistributionZoneManager(node);

        assertDataNodesFromManager(
                distributionZoneManager,
                metastore::appliedRevision,
                zoneId,
                Set.of(A, B),
                TIMEOUT_MILLIS
        );

        // Block scale up
        blockUpdate(metastore, zoneScaleUpChangeTriggerKey(zoneId));

        node.logicalTopology().putNode(C);
        node.logicalTopology().removeNodes(Set.of(B));

        assertDataNodesFromManager(
                distributionZoneManager,
                metastore::appliedRevision,
                zoneId,
                Set.of(A),
                TIMEOUT_MILLIS
        );

        node.stop();

        node = startPartialNode(0);

        distributionZoneManager = getDistributionZoneManager(node);

        assertDataNodesFromManager(distributionZoneManager, metastore::appliedRevision, zoneId, Set.of(A, C), TIMEOUT_MILLIS);
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-20054")
    @ParameterizedTest
    @MethodSource("provideArgumentsRestartTests")
    public void testScaleUpTriggeredByFilterUpdateIsRestoredAfterRestart(String zoneName) throws Exception {
        PartialNode node = startPartialNode(0);

        createZoneOrAlterDefaultZone(node, zoneName, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE);

        node.logicalTopology().putNode(A);

        int zoneId = getZoneId(node, zoneName);

        DistributionZoneManager distributionZoneManager = getDistributionZoneManager(node);

        assertDataNodesFromManager(distributionZoneManager, metastore::appliedRevision, zoneId, Set.of(A), TIMEOUT_MILLIS);

        alterZone(node, zoneName, INFINITE_TIMER_VALUE, null, null);

        node.logicalTopology().putNode(B);

        assertDataNodesFromManager(distributionZoneManager, metastore::appliedRevision, zoneId, Set.of(A), TIMEOUT_MILLIS);

        blockUpdate(metastore, zoneScaleUpChangeTriggerKey(zoneId));

        // Only Node B passes the filter
        String filter = "$[?(@.dataRegionSize > 10)]";

        alterZone(node, zoneName, null, null, filter);

        assertDataNodesFromManager(distributionZoneManager, metastore::appliedRevision, zoneId, Set.of(A), TIMEOUT_MILLIS);

        node.stop();

        node = startPartialNode(0);

        distributionZoneManager = getDistributionZoneManager(node);

        assertDataNodesFromManager(distributionZoneManager, metastore::appliedRevision, zoneId, Set.of(B), TIMEOUT_MILLIS);
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-20054")
    @ParameterizedTest
    @MethodSource("provideArgumentsRestartTests")
    public void testScaleUpsTriggeredByFilterUpdateAndNodeJoinAreRestoredAfterRestart(String zoneName) throws Exception {
        PartialNode node = startPartialNode(0);

        createZoneOrAlterDefaultZone(node, zoneName, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE);

        node.logicalTopology().putNode(A);

        int zoneId = getZoneId(node, zoneName);

        DistributionZoneManager distributionZoneManager = getDistributionZoneManager(node);

        assertDataNodesFromManager(distributionZoneManager, metastore::appliedRevision, zoneId, Set.of(A), TIMEOUT_MILLIS);

        blockUpdate(metastore, zoneScaleUpChangeTriggerKey(zoneId));

        alterZone(node, zoneName, 100, null, null);

        node.logicalTopology().putNode(B);

        assertDataNodesFromManager(distributionZoneManager, metastore::appliedRevision, zoneId, Set.of(A), TIMEOUT_MILLIS);

        // Only Node B and C passes the filter
        String filter = "$[?(@.dataRegionSize > 10)]";

        alterZone(node, zoneName, null, null, filter);

        node.logicalTopology().putNode(C);

        node.logicalTopology().removeNodes(Set.of(A));

        assertDataNodesFromManager(distributionZoneManager, metastore::appliedRevision, zoneId, Set.of(), TIMEOUT_MILLIS);

        node.stop();

        node = startPartialNode(0);

        distributionZoneManager = getDistributionZoneManager(node);

        // Immediate timer triggered by filter update
        assertDataNodesFromManager(distributionZoneManager, metastore::appliedRevision, zoneId, Set.of(B), TIMEOUT_MILLIS);

        ZoneState zoneState = distributionZoneManager.zonesState().get(zoneId);

        // Timer scheduled after join of the node C
        assertNotNull(zoneState.scaleUpTask());

        alterZone(node, zoneName, IMMEDIATE_TIMER_VALUE, null, null);

        assertDataNodesFromManager(distributionZoneManager, metastore::appliedRevision, zoneId, Set.of(B, C), TIMEOUT_MILLIS);
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-20054")
    @ParameterizedTest
    @MethodSource("provideArgumentsRestartTests")
    public void testScaleDownTimerIsRestoredAfterRestart(String zoneName) throws Exception {
        PartialNode node = startPartialNode(0);

        createZoneOrAlterDefaultZone(node, zoneName, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE);

        int zoneId = getZoneId(node, zoneName);

        // Block scale down
        blockUpdate(metastore, zoneScaleDownChangeTriggerKey(zoneId));

        node.logicalTopology().putNode(A);
        node.logicalTopology().putNode(B);
        node.logicalTopology().removeNodes(Set.of(B));
        node.logicalTopology().putNode(C);

        assertValueInStorage(
                metastore,
                zoneDataNodesKey(zoneId),
                (v) -> DistributionZonesUtil.dataNodes(fromBytes(v)).stream().map(Node::nodeName).collect(toSet()),
                Set.of(A.name(), B.name(), C.name()),
                TIMEOUT_MILLIS
        );

        node.stop();

        node = startPartialNode(0);

        assertDataNodesFromManager(
                getDistributionZoneManager(node),
                metastore::appliedRevision,
                zoneId,
                Set.of(A, C),
                TIMEOUT_MILLIS
        );
    }

    private static String[] provideArgumentsRestartTests() {
        return new String[]{DEFAULT_ZONE_NAME, ZONE_NAME};
    }

    private static void createZoneOrAlterDefaultZone(
            PartialNode node,
            String zoneName,
            int scaleUp,
            int scaleDown
    ) throws Exception {
        if (zoneName.equals(DEFAULT_ZONE_NAME)) {
            alterZone(node, DEFAULT_ZONE_NAME, scaleUp, scaleDown, null);

            int defaultZoneId = getZoneId(node, DEFAULT_ZONE_NAME);

            ZoneState zoneState = getDistributionZoneManager(node).zonesState().get(defaultZoneId);

            // This is needed because we want to wait for the end of scale up/down triggered by altering delays.
            if (zoneState.scaleUpTask() != null) {
                assertTrue(waitForCondition(() -> zoneState.scaleUpTask().isDone(), TIMEOUT_MILLIS));
            }

            if (zoneState.scaleDownTask() != null) {
                assertTrue(waitForCondition(() -> zoneState.scaleDownTask().isDone(), TIMEOUT_MILLIS));
            }
        } else {
            createZone(node, ZONE_NAME, scaleUp, scaleDown);
        }
    }

    private static void blockUpdate(MetaStorageManager metaStorageManager, ByteArray key) {
        when(metaStorageManager.invoke(argThat(iif -> {
            If iif1 = MetaStorageWriteHandler.toIf(iif);

            byte[] keyScaleUp = key.bytes();

            return iif1.andThen().update().operations().stream().anyMatch(op -> Arrays.equals(keyScaleUp, op.key()));
        }))).thenThrow(new RuntimeException("Expected"));
    }

    private static <T extends IgniteComponent> T getStartedComponent(PartialNode node, Class<T> componentClass) {
        T component = findComponent(node.startedComponents(), componentClass);

        assertNotNull(component);

        return component;
    }

    private static DistributionZoneManager getDistributionZoneManager(PartialNode node) {
        return getStartedComponent(node, DistributionZoneManager.class);
    }

    private static int getZoneId(PartialNode node, String zoneName) {
        return getDistributionZoneManager(node).getZoneId(zoneName);
    }

    private static void createZone(PartialNode node, String zoneName, int scaleUp, int scaleDown) {
        DistributionZonesTestUtil.createZone(getDistributionZoneManager(node), zoneName, scaleUp, scaleDown, (String) null);
    }

    private static void alterZone(
            PartialNode node,
            String zoneName,
            @Nullable Integer scaleUp,
            @Nullable Integer scaleDown,
            @Nullable String filter
    ) {
        DistributionZonesTestUtil.alterZone(getDistributionZoneManager(node), zoneName, scaleUp, scaleDown, filter);
    }
}
