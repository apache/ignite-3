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

import static java.util.Collections.emptySet;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertDataNodesFromManager;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertLogicalTopologyInMetastorage;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertValueInStorage;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.createZone;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.getDefaultZone;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.dataNodes;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.toDataNodesMap;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleDownChangeTriggerKeyPrefix;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleUpChangeTriggerKeyPrefix;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLastHandledTopology;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesRecoverableStateRevision;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.REBALANCE_SCHEDULER_POOL_SIZE;
import static org.apache.ignite.internal.network.utils.ClusterServiceTestUtils.defaultSerializationRegistry;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.util.ByteUtils.fromBytes;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.apache.ignite.internal.util.IgniteUtils.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.function.Consumer;
import java.util.function.LongFunction;
import java.util.stream.Stream;
import org.apache.ignite.configuration.validation.Validator;
import org.apache.ignite.internal.BaseIgniteRestartTest;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogManagerImpl;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.storage.UpdateLogImpl;
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
import org.apache.ignite.internal.distributionzones.DistributionZoneManager.Augmentation;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager.ZoneState;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.ClockWaiter;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.If;
import org.apache.ignite.internal.metastorage.server.TestRocksDbKeyValueStorage;
import org.apache.ignite.internal.metastorage.server.raft.MetaStorageWriteHandler;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.NettyBootstrapFactory;
import org.apache.ignite.internal.network.configuration.NetworkConfiguration;
import org.apache.ignite.internal.network.recovery.VaultStaleIds;
import org.apache.ignite.internal.network.scalecube.TestScaleCubeClusterServiceFactory;
import org.apache.ignite.internal.security.authentication.validator.AuthenticationProvidersValidatorImpl;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.worker.fixtures.NoOpCriticalWorkerRegistry;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for checking {@link DistributionZoneManager} behavior after node's restart.
 */
@ExtendWith(ConfigurationExtension.class)
public class ItIgniteDistributionZoneManagerNodeRestartTest extends BaseIgniteRestartTest {
    private static final LogicalNode A = new LogicalNode(
            new ClusterNodeImpl("1", "A", new NetworkAddress("localhost", 123)),
            Map.of("region", "US", "storage", "SSD", "dataRegionSize", "10"),
            Map.of(),
            List.of(DEFAULT_STORAGE_PROFILE)
    );

    private static final LogicalNode B = new LogicalNode(
            new ClusterNodeImpl("2", "B", new NetworkAddress("localhost", 123)),
            Map.of("region", "EU", "storage", "HHD", "dataRegionSize", "30"),
            Map.of(),
            List.of(DEFAULT_STORAGE_PROFILE)
    );

    private static final LogicalNode C = new LogicalNode(
            new ClusterNodeImpl("3", "C", new NetworkAddress("localhost", 123)),
            Map.of("region", "CN", "storage", "SSD", "dataRegionSize", "20"),
            Map.of(),
            List.of(DEFAULT_STORAGE_PROFILE)
    );

    private static final String ZONE_NAME = "zone1";

    private MetaStorageManager metastore;

    private volatile boolean startScaleUpBlocking;

    private volatile boolean startScaleDownBlocking;

    private volatile boolean startGlobalStateUpdateBlocking;

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

        VaultManager vault = createVault(dir);

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
                new LocalFileConfigurationStorage(configFile, localConfigurationGenerator, modules.local()),
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
                new VaultStaleIds(vault),
                new NoOpCriticalWorkerRegistry(),
                mock(FailureProcessor.class)
        );

        var clusterStateStorage = new TestClusterStateStorage();

        var logicalTopology = new LogicalTopologyImpl(clusterStateStorage);

        var cmgManager = mock(ClusterManagementGroupManager.class);

        when(cmgManager.logicalTopology()).thenAnswer(invocation -> completedFuture(logicalTopology.getLogicalTopology()));

        metastore = spy(StandaloneMetaStorageManager.create(
                new TestRocksDbKeyValueStorage(name, workDir.resolve("metastorage"))
        ));

        blockMetaStorageUpdates(metastore);

        Consumer<LongFunction<CompletableFuture<?>>> revisionUpdater = (LongFunction<CompletableFuture<?>> function) ->
                metastore.registerRevisionUpdateListener(function::apply);

        var cfgStorage = new DistributedConfigurationStorage("test", metastore);

        ConfigurationTreeGenerator distributedConfigurationGenerator = new ConfigurationTreeGenerator(
                modules.distributed().rootKeys(),
                modules.distributed().schemaExtensions(),
                modules.distributed().polymorphicSchemaExtensions()
        );

        Set<Validator<?, ?>> validators = new HashSet<>(modules.distributed().validators());
        validators.remove(AuthenticationProvidersValidatorImpl.INSTANCE);

        var clusterCfgMgr = new ConfigurationManager(
                modules.distributed().rootKeys(),
                cfgStorage,
                distributedConfigurationGenerator,
                ConfigurationValidatorImpl.withDefaultValidators(distributedConfigurationGenerator, validators)
        );

        ConfigurationRegistry clusterConfigRegistry = clusterCfgMgr.configurationRegistry();

        LogicalTopologyServiceImpl logicalTopologyService = new LogicalTopologyServiceImpl(logicalTopology, cmgManager);

        var clock = new HybridClockImpl();

        var clockWaiter = new ClockWaiter(name, clock);

        var catalogManager = new CatalogManagerImpl(new UpdateLogImpl(metastore), new TestClockService(clock, clockWaiter));

        ScheduledExecutorService rebalanceScheduler = new ScheduledThreadPoolExecutor(REBALANCE_SCHEDULER_POOL_SIZE,
                NamedThreadFactory.create(name, "test-rebalance-scheduler", logger()));

        DistributionZoneManager distributionZoneManager = new DistributionZoneManager(
                name,
                revisionUpdater,
                metastore,
                logicalTopologyService,
                catalogManager,
                rebalanceScheduler
        );

        // Preparing the result map.

        components.add(vault);
        components.add(nodeCfgMgr);

        // Start.

        vault.startAsync();
        vault.putName(name);

        nodeCfgMgr.startAsync();

        // Start the remaining components.
        List<IgniteComponent> otherComponents = List.of(
                nettyBootstrapFactory,
                clusterSvc,
                clusterStateStorage,
                cmgManager,
                metastore,
                clusterCfgMgr,
                clockWaiter,
                catalogManager,
                distributionZoneManager
        );

        for (IgniteComponent component : otherComponents) {
            component.startAsync();

            components.add(component);
        }

        PartialNode partialNode = partialNode(
                name,
                nodeCfgMgr,
                clusterCfgMgr,
                metastore,
                components,
                localConfigurationGenerator,
                logicalTopology,
                cfgStorage,
                distributedConfigurationGenerator,
                clusterConfigRegistry,
                clock
        );

        partialNodes.add(partialNode);

        return partialNode;
    }

    @AfterEach
    public void afterTest() {
        startScaleUpBlocking = false;

        startScaleDownBlocking = false;
    }

    @Test
    public void testNodeAttributesRestoredAfterRestart() throws Exception {
        PartialNode node = startPartialNode(0);

        node.logicalTopology().putNode(A);
        node.logicalTopology().putNode(B);
        node.logicalTopology().putNode(C);

        createZone(getCatalogManager(node), ZONE_NAME, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE, null, DEFAULT_STORAGE_PROFILE);

        int zoneId = getZoneId(node, ZONE_NAME);

        DistributionZoneManager distributionZoneManager = getDistributionZoneManager(node);
        CatalogManager catalogManager = getCatalogManager(node);

        assertDataNodesFromManager(distributionZoneManager, metastore::appliedRevision, catalogManager::latestCatalogVersion, zoneId,
                Set.of(A, B, C), TIMEOUT_MILLIS);

        Map<String, NodeWithAttributes> nodeAttributesBeforeRestart = distributionZoneManager.nodesAttributes();

        node.stop();

        node = startPartialNode(0);

        distributionZoneManager = getDistributionZoneManager(node);

        Map<String, NodeWithAttributes> nodeAttributesAfterRestart = distributionZoneManager.nodesAttributes();

        assertEquals(3, nodeAttributesAfterRestart.size());

        assertEquals(nodeAttributesBeforeRestart, nodeAttributesAfterRestart);
    }

    @ParameterizedTest(name = "defaultZone={0}")
    @ValueSource(booleans = {true, false})
    public void testTopologyAugmentationMapRestoredAfterRestart(boolean defaultZone) throws Exception {
        PartialNode node = startPartialNode(0);

        node.logicalTopology().putNode(A);

        String zoneName = createZoneOrAlterDefaultZone(node, defaultZone, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE);

        node.logicalTopology().putNode(B);
        node.logicalTopology().putNode(C);

        int zoneId = getZoneId(node, zoneName);

        DistributionZoneManager distributionZoneManager = getDistributionZoneManager(node);
        CatalogManager catalogManager = getCatalogManager(node);

        assertDataNodesFromManager(
                distributionZoneManager,
                metastore::appliedRevision,
                catalogManager::latestCatalogVersion,
                zoneId,
                Set.of(A, B, C),
                TIMEOUT_MILLIS
        );

        ConcurrentSkipListMap<Long, Augmentation> nodeAttributesBeforeRestart =
                distributionZoneManager.zonesState().get(zoneId).topologyAugmentationMap();

        node.stop();

        node = startPartialNode(0);

        distributionZoneManager = getDistributionZoneManager(node);

        ConcurrentSkipListMap<Long, Augmentation> nodeAttributesAfterRestart =
                distributionZoneManager.zonesState().get(zoneId).topologyAugmentationMap();

        assertEquals(2, nodeAttributesAfterRestart.size());

        assertEquals(
                nodeAttributesBeforeRestart.values().stream().map(Augmentation::nodes).collect(toSet()),
                nodeAttributesAfterRestart.values().stream().map(Augmentation::nodes).collect(toSet())
        );
    }

    @Test
    public void testLogicalTopologyRestoredAfterRestart() throws Exception {
        PartialNode node = startPartialNode(0);

        node.logicalTopology().putNode(A);
        node.logicalTopology().putNode(B);
        node.logicalTopology().putNode(C);

        Set<NodeWithAttributes> logicalTopology = Stream.of(A, B, C)
                .map(n -> new NodeWithAttributes(n.name(), n.id(), n.userAttributes(), n.storageProfiles()))
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
    public void testLogicalTopologyInterruptedEventRestoredAfterRestart() throws Exception {
        PartialNode node = startPartialNode(0);

        assertValueInStorage(metastore, zonesLastHandledTopology(), (v) -> v, null, TIMEOUT_MILLIS);

        node.logicalTopology().putNode(A);
        node.logicalTopology().putNode(B);

        Set<NodeWithAttributes> logicalTopology = Stream.of(A, B)
                .map(n -> new NodeWithAttributes(n.name(), n.id(), n.userAttributes(), n.storageProfiles()))
                .collect(toSet());

        DistributionZoneManager distributionZoneManager = getDistributionZoneManager(node);
        CatalogManager catalogManager = getCatalogManager(node);
        DistributionZoneManager finalDistributionZoneManager = distributionZoneManager;

        assertTrue(waitForCondition(() -> logicalTopology.equals(finalDistributionZoneManager.logicalTopology()), TIMEOUT_MILLIS));

        assertValueInStorage(metastore, zonesLastHandledTopology(), ByteUtils::fromBytes, logicalTopology, TIMEOUT_MILLIS);

        int zoneId = getDefaultZoneId(node);

        assertDataNodesFromManager(
                distributionZoneManager,
                metastore::appliedRevision,
                catalogManager::latestCatalogVersion,
                zoneId,
                Set.of(A, B),
                TIMEOUT_MILLIS
        );

        metastore = findComponent(node.startedComponents(), MetaStorageManager.class);

        startGlobalStateUpdateBlocking = true;
        startScaleUpBlocking = true;

        node.logicalTopology().putNode(C);

        Set<NodeWithAttributes> newLogicalTopology = Stream.of(A, B, C)
                .map(n -> new NodeWithAttributes(n.name(), n.id(), n.userAttributes(), n.storageProfiles()))
                .collect(toSet());

        assertTrue(waitForCondition(() -> newLogicalTopology.equals(finalDistributionZoneManager.logicalTopology()), TIMEOUT_MILLIS));

        assertValueInStorage(metastore, zonesLastHandledTopology(), ByteUtils::fromBytes, logicalTopology, TIMEOUT_MILLIS);

        node.stop();

        startGlobalStateUpdateBlocking = false;
        startScaleUpBlocking = false;

        node = startPartialNode(0);

        distributionZoneManager = getDistributionZoneManager(node);
        catalogManager = getCatalogManager(node);

        assertEquals(newLogicalTopology, distributionZoneManager.logicalTopology());

        assertValueInStorage(metastore, zonesLastHandledTopology(), ByteUtils::fromBytes, newLogicalTopology, TIMEOUT_MILLIS);

        assertDataNodesFromManager(
                distributionZoneManager,
                metastore::appliedRevision,
                catalogManager::latestCatalogVersion,
                zoneId,
                Set.of(A, B, C),
                TIMEOUT_MILLIS
        );
    }

    @Test
    public void testFirstLogicalTopologyUpdateInterruptedEventRestoredAfterRestart() throws Exception {
        PartialNode node = startPartialNode(0);

        assertValueInStorage(metastore, zonesLastHandledTopology(), (v) -> v, null, TIMEOUT_MILLIS);

        DistributionZoneManager distributionZoneManager = getDistributionZoneManager(node);
        DistributionZoneManager finalDistributionZoneManager = distributionZoneManager;

        assertValueInStorage(metastore, zonesLastHandledTopology(), (v) -> v, null, TIMEOUT_MILLIS);

        metastore = findComponent(node.startedComponents(), MetaStorageManager.class);

        startGlobalStateUpdateBlocking = true;
        startScaleUpBlocking = true;

        node.logicalTopology().putNode(C);

        Set<NodeWithAttributes> newLogicalTopology = Stream.of(C)
                .map(n -> new NodeWithAttributes(n.name(), n.id(), n.userAttributes(), n.storageProfiles()))
                .collect(toSet());

        assertTrue(waitForCondition(() -> newLogicalTopology.equals(finalDistributionZoneManager.logicalTopology()), TIMEOUT_MILLIS));

        assertValueInStorage(metastore, zonesLastHandledTopology(), (v) -> v, null, TIMEOUT_MILLIS);

        node.stop();

        startGlobalStateUpdateBlocking = false;
        startScaleUpBlocking = false;

        node = startPartialNode(0);

        distributionZoneManager = getDistributionZoneManager(node);
        CatalogManager catalogManager = getCatalogManager(node);

        assertEquals(newLogicalTopology, distributionZoneManager.logicalTopology());

        assertValueInStorage(metastore, zonesLastHandledTopology(), ByteUtils::fromBytes, newLogicalTopology, TIMEOUT_MILLIS);

        int zoneId = getDefaultZoneId(node);

        assertDataNodesFromManager(
                distributionZoneManager,
                metastore::appliedRevision,
                catalogManager::latestCatalogVersion,
                zoneId,
                Set.of(C),
                TIMEOUT_MILLIS
        );
    }

    @Test
    public void testCreationZoneWhenDataNodesAreDeletedIsNotSuccessful() throws Exception {
        PartialNode node = startPartialNode(0);

        node.logicalTopology().putNode(A);
        node.logicalTopology().putNode(B);

        node.logicalTopology().putNode(C);

        Set<NodeWithAttributes> logicalTopology = Stream.of(A, B, C)
                .map(n -> new NodeWithAttributes(n.name(), n.id(), n.userAttributes(), n.storageProfiles()))
                .collect(toSet());

        DistributionZoneManager distributionZoneManager = getDistributionZoneManager(node);
        DistributionZoneManager finalDistributionZoneManager = distributionZoneManager;

        assertTrue(waitForCondition(() -> logicalTopology.equals(finalDistributionZoneManager.logicalTopology()), TIMEOUT_MILLIS));

        int zoneId = getDefaultZoneId(node);

        assertValueInStorage(
                metastore,
                zoneDataNodesKey(zoneId),
                (v) -> dataNodes(fromBytes(v)).stream().map(Node::nodeName).collect(toSet()),
                Set.of(A.name(), B.name(), C.name()),
                TIMEOUT_MILLIS
        );

        metastore = findComponent(node.startedComponents(), MetaStorageManager.class);

        byte[][] dataNodeKey = new byte[1][1];

        // In this mock we catch invocation of DistributionZoneManager.initDataNodesAndTriggerKeysInMetaStorage, where condition is based
        // on presence of data node key in ms. After that we make this data node as a tombstone, so when logic of creation of a zone is
        // run, there won't be any initialisation of data nodes keys. We try to imitate concurrent removal of a zone.
        doAnswer(invocation -> {
            ByteArray dataNodeKeyForZone = new ByteArray(dataNodeKey[0]);

            // Here we remove data nodes value for newly created zone, so it is tombstone
            metastore.put(dataNodeKeyForZone, toBytes(toDataNodesMap(emptySet()))).get();

            metastore.remove(dataNodeKeyForZone).get();

            return invocation.callRealMethod();
        }).when(metastore).invoke(argThat(iif -> {
            If iif1 = MetaStorageWriteHandler.toIf(iif);

            byte[][] keysFromIf = iif1.cond().keys();

            Optional<byte[]> dataNodeKeyOptional = Arrays.stream(keysFromIf)
                    .filter(op -> startsWith(op, zoneDataNodesKey().bytes()))
                    .findFirst();

            dataNodeKeyOptional.ifPresent(bytes -> dataNodeKey[0] = bytes);

            return dataNodeKeyOptional.isPresent();
        }));

        createZone(getCatalogManager(node), "zone1", INFINITE_TIMER_VALUE, INFINITE_TIMER_VALUE, null, DEFAULT_STORAGE_PROFILE);

        // Assert that after creation of a zone, data nodes are still tombstone, but not the logical topology, as for default zone.
        assertThat(metastore.get(new ByteArray(dataNodeKey[0])).thenApply(Entry::tombstone), willBe(true));
    }

    @ParameterizedTest(name = "defaultZone={0}")
    @ValueSource(booleans = {true, false})
    public void testLocalDataNodesAreRestoredAfterRestart(boolean defaultZone) throws Exception {
        PartialNode node = startPartialNode(0);

        String zoneName = createZoneOrAlterDefaultZone(node, defaultZone, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE);

        node.logicalTopology().putNode(A);
        node.logicalTopology().putNode(B);

        int zoneId = getZoneId(node, zoneName);

        DistributionZoneManager distributionZoneManager = getDistributionZoneManager(node);
        CatalogManager catalogManager = getCatalogManager(node);

        assertDataNodesFromManager(distributionZoneManager, metastore::appliedRevision, catalogManager::latestCatalogVersion, zoneId,
                Set.of(A, B), TIMEOUT_MILLIS);

        node.logicalTopology().removeNodes(Set.of(B));

        assertDataNodesFromManager(distributionZoneManager, metastore::appliedRevision, catalogManager::latestCatalogVersion, zoneId,
                Set.of(A), TIMEOUT_MILLIS);

        long revisionBeforeRestart = metastore.appliedRevision();

        node.stop();

        node = startPartialNode(0);

        distributionZoneManager = getDistributionZoneManager(node);

        assertDataNodesFromManager(distributionZoneManager, metastore::appliedRevision, catalogManager::latestCatalogVersion, zoneId,
                Set.of(A), TIMEOUT_MILLIS);
        assertDataNodesFromManager(distributionZoneManager, () -> revisionBeforeRestart, catalogManager::latestCatalogVersion, zoneId,
                Set.of(A), TIMEOUT_MILLIS);
    }

    @ParameterizedTest(name = "defaultZone={0}")
    @ValueSource(booleans = {true, false})
    public void testScaleUpTimerIsRestoredAfterRestart(boolean defaultZone) throws Exception {
        PartialNode node = startPartialNode(0);

        node.logicalTopology().putNode(A);
        node.logicalTopology().putNode(B);

        assertLogicalTopologyInMetastorage(Set.of(A, B), metastore);

        String zoneName = createZoneOrAlterDefaultZone(node, defaultZone, 1, 1);

        int zoneId = getZoneId(node, zoneName);

        DistributionZoneManager distributionZoneManager = getDistributionZoneManager(node);
        CatalogManager catalogManager = getCatalogManager(node);

        assertDataNodesFromManager(
                distributionZoneManager,
                metastore::appliedRevision,
                catalogManager::latestCatalogVersion,
                zoneId,
                Set.of(A, B),
                TIMEOUT_MILLIS
        );

        // Block scale up
        CountDownLatch scaleUpLatch = blockScaleUpTaskExecution(zoneId, distributionZoneManager);

        assertTrue(waitForCondition(() -> scaleUpLatch.getCount() == 1, TIMEOUT_MILLIS));

        node.logicalTopology().putNode(C);
        node.logicalTopology().removeNodes(Set.of(B));
        node.logicalTopology().putNode(B);

        assertLogicalTopologyInMetastorage(Set.of(A, B, C), metastore);

        assertDataNodesFromManager(
                distributionZoneManager,
                metastore::appliedRevision,
                catalogManager::latestCatalogVersion,
                zoneId,
                Set.of(A, B),
                TIMEOUT_MILLIS
        );

        node.stop();

        scaleUpLatch.countDown();

        node = startPartialNode(0);

        distributionZoneManager = getDistributionZoneManager(node);

        catalogManager = getCatalogManager(node);

        assertDataNodesFromManager(distributionZoneManager, metastore::appliedRevision, catalogManager::latestCatalogVersion, zoneId,
                Set.of(A, B, C), TIMEOUT_MILLIS);

        metastore = findComponent(node.startedComponents(), MetaStorageManager.class);

        assertValueInStorage(
                metastore,
                zoneDataNodesKey(zoneId),
                (v) -> dataNodes(fromBytes(v)).stream().map(Node::nodeName).collect(toSet()),
                Set.of(A.name(), B.name(), C.name()),
                TIMEOUT_MILLIS
        );
    }

    @ParameterizedTest(name = "defaultZone={0}")
    @ValueSource(booleans = {true, false})
    public void testScaleDownTimerIsRestoredAfterRestart(boolean defaultZone) throws Exception {
        PartialNode node = startPartialNode(0);

        node.logicalTopology().putNode(A);
        node.logicalTopology().putNode(B);

        assertLogicalTopologyInMetastorage(Set.of(A, B), metastore);

        DistributionZoneManager distributionZoneManager = getDistributionZoneManager(node);
        CatalogManager catalogManager = getCatalogManager(node);

        String zoneName = createZoneOrAlterDefaultZone(node, defaultZone, 1, 1);

        int zoneId = getZoneId(node, zoneName);

        // Block scale down
        CountDownLatch scaleDownLatch = blockScaleDownTaskExecution(zoneId, distributionZoneManager);

        assertTrue(waitForCondition(() -> scaleDownLatch.getCount() == 1, TIMEOUT_MILLIS));

        node.logicalTopology().removeNodes(Set.of(B));
        node.logicalTopology().putNode(C);

        assertDataNodesFromManager(
                distributionZoneManager,
                metastore::appliedRevision,
                catalogManager::latestCatalogVersion,
                zoneId,
                Set.of(A, B),
                TIMEOUT_MILLIS
        );

        node.stop();

        scaleDownLatch.countDown();

        node = startPartialNode(0);

        catalogManager = getCatalogManager(node);

        assertDataNodesFromManager(
                getDistributionZoneManager(node),
                metastore::appliedRevision, catalogManager::latestCatalogVersion,
                zoneId,
                Set.of(A, C),
                TIMEOUT_MILLIS
        );

        metastore = findComponent(node.startedComponents(), MetaStorageManager.class);

        assertValueInStorage(
                metastore,
                zoneDataNodesKey(zoneId),
                (v) -> dataNodes(fromBytes(v)).stream().map(Node::nodeName).collect(toSet()),
                Set.of(A.name(), C.name()),
                TIMEOUT_MILLIS
        );
    }

    private static String createZoneOrAlterDefaultZone(
            PartialNode node,
            boolean useDefaultZone,
            int scaleUp,
            int scaleDown
    ) throws Exception {
        String zoneName;

        if (useDefaultZone) {
            CatalogZoneDescriptor defaultZone = getDefaultZone(getCatalogManager(node), node.clock().nowLong());
            zoneName = defaultZone.name();

            alterZone(node, zoneName, scaleUp, scaleDown, null);

            ZoneState zoneState = getDistributionZoneManager(node).zonesState().get(defaultZone.id());

            // This is needed because we want to wait for the end of scale up/down triggered by altering delays.
            if (zoneState.scaleUpTask() != null) {
                assertTrue(waitForCondition(() -> zoneState.scaleUpTask().isDone(), TIMEOUT_MILLIS));
            }

            if (zoneState.scaleDownTask() != null) {
                assertTrue(waitForCondition(() -> zoneState.scaleDownTask().isDone(), TIMEOUT_MILLIS));
            }
        } else {
            zoneName = ZONE_NAME;

            createZone(getCatalogManager(node), zoneName, scaleUp, scaleDown, null, DEFAULT_STORAGE_PROFILE);
        }

        return zoneName;
    }

    private void blockMetaStorageUpdates(MetaStorageManager metaStorageManager) {
        doThrow(new RuntimeException("Expected")).when(metaStorageManager).invoke(argThat(iif -> {
            If iif1 = MetaStorageWriteHandler.toIf(iif);

            byte[] keyScaleUpBytes = zoneScaleUpChangeTriggerKeyPrefix().bytes();
            byte[] keyScaleDownBytes = zoneScaleDownChangeTriggerKeyPrefix().bytes();
            byte[] keyGlobalStateBytes = zonesRecoverableStateRevision().bytes();

            boolean isScaleUpKey = iif1.andThen().update().operations().stream().anyMatch(op -> startsWith(op.key(), keyScaleUpBytes));
            boolean isScaleDownKey = iif1.andThen().update().operations().stream().anyMatch(op -> startsWith(op.key(), keyScaleDownBytes));
            boolean isGlobalStateChangeKey = iif1.andThen().update().operations().stream()
                    .anyMatch(op -> startsWith(op.key(), keyGlobalStateBytes));

            return isScaleUpKey && startScaleUpBlocking
                    || isScaleDownKey && startScaleDownBlocking
                    || isGlobalStateChangeKey && startGlobalStateUpdateBlocking;
        }));
    }

    private static <T extends IgniteComponent> T getStartedComponent(PartialNode node, Class<T> componentClass) {
        T component = findComponent(node.startedComponents(), componentClass);

        assertNotNull(component);

        return component;
    }

    private static DistributionZoneManager getDistributionZoneManager(PartialNode node) {
        return getStartedComponent(node, DistributionZoneManager.class);
    }

    private static CatalogManager getCatalogManager(PartialNode node) {
        return getStartedComponent(node, CatalogManager.class);
    }

    private static int getZoneId(PartialNode node, String zoneName) {
        return DistributionZonesTestUtil.getZoneId(getCatalogManager(node), zoneName, node.clock().nowLong());
    }

    private static int getDefaultZoneId(PartialNode node) {
        return DistributionZonesTestUtil.getDefaultZone(getCatalogManager(node), node.clock().nowLong()).id();
    }

    private static void alterZone(
            PartialNode node,
            String zoneName,
            @Nullable Integer scaleUp,
            @Nullable Integer scaleDown,
            @Nullable String filter
    ) {
        DistributionZonesTestUtil.alterZone(getCatalogManager(node), zoneName, scaleUp, scaleDown, filter);
    }

    /**
     * Schedule a scale up task which block execution of another scale up tasks.
     * Note that because of the executor of scale up/scale down tasks is single-threaded, this
     * method also blocks scale down tasks execution.
     *
     * @return Latch to unblock execution of scale up tasks.
     */
    private static CountDownLatch blockScaleUpTaskExecution(int zoneId, DistributionZoneManager distributionZoneManager) {
        CountDownLatch scaleUpLatch = new CountDownLatch(2);

        Runnable dummyScaleUpTask = () -> {
            try {
                scaleUpLatch.countDown();
                scaleUpLatch.await(2 * TIMEOUT_MILLIS, MILLISECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        distributionZoneManager.zonesState().get(zoneId)
                .rescheduleScaleUp(IMMEDIATE_TIMER_VALUE, dummyScaleUpTask, zoneId);

        return scaleUpLatch;
    }

    /**
     * Schedule a scale down task which block execution of another scale down tasks.
     * Note that because of the executor of scale up/scale down tasks is single-threaded, this
     * method also blocks scale up tasks execution.
     *
     * @return Latch to unblock execution of scale down tasks.
     */
    private static CountDownLatch blockScaleDownTaskExecution(int zoneId, DistributionZoneManager distributionZoneManager) {
        CountDownLatch scaleDownLatch = new CountDownLatch(2);

        Runnable dummyScaleDownTask = () -> {
            try {
                scaleDownLatch.countDown();
                scaleDownLatch.await(TIMEOUT_MILLIS, MILLISECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };

        distributionZoneManager.zonesState().get(zoneId)
                .rescheduleScaleDown(IMMEDIATE_TIMER_VALUE, dummyScaleDownTask, zoneId);

        return scaleDownLatch;
    }
}
