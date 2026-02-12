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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.TEST_DELAY_DURATION;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertDataNodesFromManager;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertLogicalTopologyInMetastorage;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertValueInStorage;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.createZone;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.deserializeLatestDataNodesHistoryEntry;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.getDefaultZone;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.DISTRIBUTION_ZONE_DATA_NODES_HISTORY_PREFIX;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.DISTRIBUTION_ZONE_SCALE_DOWN_TIMER_PREFIX;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.DISTRIBUTION_ZONE_SCALE_UP_TIMER_PREFIX;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.dataNodes;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesHistoryKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLastHandledTopology;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesRecoverableStateRevision;
import static org.apache.ignite.internal.metastorage.dsl.OperationType.NO_OP;
import static org.apache.ignite.internal.network.utils.ClusterServiceTestUtils.defaultChannelTypeRegistry;
import static org.apache.ignite.internal.network.utils.ClusterServiceTestUtils.defaultSerializationRegistry;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.ByteUtils.toByteArray;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.ignite.configuration.validation.Validator;
import org.apache.ignite.internal.BaseIgniteRestartTest;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogManagerImpl;
import org.apache.ignite.internal.catalog.PartitionCountProvider;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.descriptors.ConsistencyMode;
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
import org.apache.ignite.internal.configuration.SystemDistributedExtensionConfiguration;
import org.apache.ignite.internal.configuration.storage.DistributedConfigurationStorage;
import org.apache.ignite.internal.configuration.storage.LocalFileConfigurationStorage;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.validation.ConfigurationValidatorImpl;
import org.apache.ignite.internal.disaster.system.ClusterIdService;
import org.apache.ignite.internal.distributionzones.DataNodesHistory.DataNodesHistorySerializer;
import org.apache.ignite.internal.distributionzones.DataNodesManager.ZoneTimers;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.ClockWaiter;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.MetaStorageRevisionListenerRegistry;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.If;
import org.apache.ignite.internal.metastorage.server.ReadOperationForCompactionTracker;
import org.apache.ignite.internal.metastorage.server.persistence.RocksDbKeyValueStorage;
import org.apache.ignite.internal.metastorage.server.raft.MetaStorageWriteHandler;
import org.apache.ignite.internal.metrics.NoOpMetricManager;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.NettyBootstrapFactory;
import org.apache.ignite.internal.network.configuration.NetworkConfiguration;
import org.apache.ignite.internal.network.configuration.NetworkExtensionConfiguration;
import org.apache.ignite.internal.network.recovery.InMemoryStaleIds;
import org.apache.ignite.internal.network.scalecube.TestScaleCubeClusterService;
import org.apache.ignite.internal.security.authentication.validator.AuthenticationProvidersValidatorImpl;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.version.DefaultIgniteProductVersionSource;
import org.apache.ignite.internal.worker.fixtures.NoOpCriticalWorkerRegistry;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Tests for checking {@link DistributionZoneManager} behavior after node's restart.
 */
@ExtendWith(ConfigurationExtension.class)
@ExtendWith(ExecutorServiceExtension.class)
public class ItIgniteDistributionZoneManagerNodeRestartTest extends BaseIgniteRestartTest {
    private static final LogicalNode A = new LogicalNode(
            new ClusterNodeImpl(randomUUID(), "A", new NetworkAddress("localhost", 123)),
            Map.of("region", "US", "storage", "SSD", "dataRegionSize", "10"),
            Map.of(),
            List.of(DEFAULT_STORAGE_PROFILE)
    );

    private static final LogicalNode B = new LogicalNode(
            new ClusterNodeImpl(randomUUID(), "B", new NetworkAddress("localhost", 123)),
            Map.of("region", "EU", "storage", "HHD", "dataRegionSize", "30"),
            Map.of(),
            List.of(DEFAULT_STORAGE_PROFILE)
    );

    private static final LogicalNode C = new LogicalNode(
            new ClusterNodeImpl(randomUUID(), "C", new NetworkAddress("localhost", 123)),
            Map.of("region", "CN", "storage", "SSD", "dataRegionSize", "20"),
            Map.of(),
            List.of(DEFAULT_STORAGE_PROFILE)
    );

    private static final String ZONE_NAME = "zone1";

    private MetaStorageManager metastore;

    private volatile boolean startScaleUpBlocking;

    private volatile boolean startScaleDownBlocking;

    private volatile boolean startGlobalStateUpdateBlocking;

    @InjectExecutorService
    private ScheduledExecutorService commonScheduledExecutorService;

    /**
     * Start some of Ignite components that are able to serve as Ignite node for test purposes.
     *
     * @param idx Node index.
     * @return Partial node.
     */
    private PartialNode startPartialNode(int idx, Consumer<MetaStorageManager> metaStorageMocker) {
        String name = testNodeName(testInfo, idx);

        Path dir = workDir.resolve(name);

        List<IgniteComponent> components = new ArrayList<>();

        VaultManager vault = createVault(dir);

        var clusterStateStorage = TestClusterStateStorage.initializedClusterStateStorage();

        var clusterIdService = new ClusterIdService(vault);

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

        var componentContext = new ComponentContext();

        // Start local configuration to be able to read all local properties.
        try {
            nodeCfgMgr.startAsync(componentContext).get(30, TimeUnit.SECONDS);
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            throw new AssertionError(e);
        }

        NetworkConfiguration networkConfiguration = nodeCfgMgr.configurationRegistry()
                .getConfiguration(NetworkExtensionConfiguration.KEY).network();

        var nettyBootstrapFactory = new NettyBootstrapFactory(networkConfiguration, name);

        var failureProcessor = mock(FailureProcessor.class);

        var clusterSvc = new TestScaleCubeClusterService(
                name,
                networkConfiguration,
                nettyBootstrapFactory,
                defaultSerializationRegistry(),
                new InMemoryStaleIds(),
                clusterIdService,
                new NoOpCriticalWorkerRegistry(),
                failureProcessor,
                new NoOpMetricManager(),
                defaultChannelTypeRegistry(),
                new DefaultIgniteProductVersionSource()
        );

        var logicalTopology = new LogicalTopologyImpl(clusterStateStorage, failureProcessor);

        var cmgManager = mock(ClusterManagementGroupManager.class);

        when(cmgManager.logicalTopology()).thenAnswer(invocation -> completedFuture(logicalTopology.getLogicalTopology()));

        when(cmgManager.startAsync(any())).thenReturn(nullCompletedFuture());
        when(cmgManager.stopAsync(any())).thenReturn(nullCompletedFuture());

        when(cmgManager.clusterState()).thenReturn(nullCompletedFuture());

        var readOperationForCompactionTracker = new ReadOperationForCompactionTracker();

        var storage = new RocksDbKeyValueStorage(
                name,
                workDir.resolve("metastorage"),
                failureProcessor,
                readOperationForCompactionTracker,
                commonScheduledExecutorService
        );

        var clock = new HybridClockImpl();

        metastore = spy(StandaloneMetaStorageManager.create(storage, clock, readOperationForCompactionTracker));

        metaStorageMocker.accept(metastore);

        blockMetaStorageUpdates(metastore);

        var revisionUpdater = new MetaStorageRevisionListenerRegistry(metastore);

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

        var clockWaiter = new ClockWaiter(name, clock, commonScheduledExecutorService);

        ClockService clockService = new TestClockService(clock, clockWaiter);

        var catalogManager = new CatalogManagerImpl(
                new UpdateLogImpl(metastore, failureProcessor),
                clockService,
                failureProcessor,
                () -> TEST_DELAY_DURATION,
                PartitionCountProvider.defaultPartitionCountProvider()
        );

        LowWatermark lowWatermark = mock(LowWatermark.class);
        when(lowWatermark.getLowWatermark()).thenAnswer(inv -> new HybridTimestamp(System.currentTimeMillis() - 600_000, 0));

        DistributionZoneManager distributionZoneManager = new DistributionZoneManager(
                name,
                () -> clusterSvc.topologyService().localMember().id(),
                revisionUpdater,
                metastore,
                logicalTopologyService,
                catalogManager,
                clusterCfgMgr.configurationRegistry().getConfiguration(SystemDistributedExtensionConfiguration.KEY).system(),
                clockService,
                new NoOpMetricManager(),
                lowWatermark
        );

        // Preparing the result map.

        components.add(vault);
        components.add(nodeCfgMgr);

        // Start.
        assertThat(vault.startAsync(componentContext), willCompleteSuccessfully());
        vault.putName(name);

        assertThat(nodeCfgMgr.startAsync(componentContext), willCompleteSuccessfully());

        // Start the remaining components.
        List<IgniteComponent> otherComponents = List.of(
                clusterStateStorage,
                clusterIdService,
                nettyBootstrapFactory,
                clusterSvc,
                cmgManager,
                metastore,
                clusterCfgMgr,
                clockWaiter,
                catalogManager,
                distributionZoneManager
        );

        for (IgniteComponent component : otherComponents) {
            // TODO: IGNITE-22119 required to be able to wait on this future.
            component.startAsync(componentContext);

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

        assertThat(catalogManager.catalogInitializationFuture(), willCompleteSuccessfully());

        partialNodes.add(partialNode);

        return partialNode;
    }

    private PartialNode startPartialNode(int idx) {
        return startPartialNode(idx, metaStorageManager -> {});
    }

    @AfterEach
    public void afterTest() {
        startScaleUpBlocking = false;

        startScaleDownBlocking = false;
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

        assertValueInStorage(metastore, zonesLastHandledTopology(), this::deserializeLogicalTopologySet, logicalTopology, TIMEOUT_MILLIS);

        createDefaultZone(node);

        int defaultZoneId = getDefaultZoneId(node);

        assertDataNodesFromManager(
                distributionZoneManager,
                metastore::appliedRevision,
                catalogManager::latestCatalogVersion,
                defaultZoneId,
                Set.of(A, B),
                TIMEOUT_MILLIS
        );

        startGlobalStateUpdateBlocking = true;
        startScaleUpBlocking = true;

        node.logicalTopology().putNode(C);

        Set<NodeWithAttributes> newLogicalTopology = Stream.of(A, B, C)
                .map(n -> new NodeWithAttributes(n.name(), n.id(), n.userAttributes(), n.storageProfiles()))
                .collect(toSet());

        assertTrue(waitForCondition(() -> newLogicalTopology.equals(finalDistributionZoneManager.logicalTopology()), TIMEOUT_MILLIS));

        assertValueInStorage(metastore, zonesLastHandledTopology(), this::deserializeLogicalTopologySet, logicalTopology, TIMEOUT_MILLIS);

        node.stop();

        startGlobalStateUpdateBlocking = false;
        startScaleUpBlocking = false;

        node = startPartialNode(0);

        distributionZoneManager = getDistributionZoneManager(node);
        catalogManager = getCatalogManager(node);

        assertEquals(newLogicalTopology, distributionZoneManager.logicalTopology());

        assertValueInStorage(
                metastore,
                zonesLastHandledTopology(),
                this::deserializeLogicalTopologySet,
                newLogicalTopology,
                TIMEOUT_MILLIS
        );

        assertDataNodesFromManager(
                distributionZoneManager,
                metastore::appliedRevision,
                catalogManager::latestCatalogVersion,
                defaultZoneId,
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

        createDefaultZone(node);

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

        assertValueInStorage(
                metastore,
                zonesLastHandledTopology(),
                this::deserializeLogicalTopologySet,
                newLogicalTopology,
                TIMEOUT_MILLIS
        );

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

    @ParameterizedTest
    @EnumSource(ConsistencyMode.class)
    public void testCreationZoneWhenDataNodesAreDeletedIsNotSuccessful(ConsistencyMode consistencyMode) throws Exception {
        var imitateConcurrentRemoval = new AtomicBoolean();

        var dataNodeKey = new AtomicReference<ByteArray>();

        PartialNode node = startPartialNode(0, metaStorageManager -> {
            // In this mock we catch invocation of DataNodesManager.onZoneCreate, where condition is
            // based on presence of data node key in ms. After that we make this data node as a tombstone, so when logic of creation of a
            // zone is run, there won't be any initialisation of data nodes keys. We try to imitate concurrent removal of a zone.
            doAnswer(invocation -> {
                ByteArray dataNodeKeyForZone = dataNodeKey.get();

                // Here we remove data nodes value for newly created zone, so it is tombstone
                assertThat(
                        metaStorageManager.put(dataNodeKeyForZone, DataNodesHistorySerializer.serialize(new DataNodesHistory())),
                        willCompleteSuccessfully()
                );

                assertThat(
                        metaStorageManager.remove(dataNodeKeyForZone),
                        willCompleteSuccessfully()
                );

                return invocation.callRealMethod();
            }).when(metaStorageManager).invoke(argThat(iif -> {
                if (!imitateConcurrentRemoval.get()) {
                    return false;
                }

                If iif1 = MetaStorageWriteHandler.toIf(iif);

                byte[][] keysFromIf = iif1.cond().keys();

                Optional<ByteArray> dataNodeKeyOptional = Arrays.stream(keysFromIf)
                        .filter(op -> startsWith(op, DISTRIBUTION_ZONE_DATA_NODES_HISTORY_PREFIX.getBytes(UTF_8)))
                        .findFirst()
                        .map(ByteArray::new);

                dataNodeKeyOptional.ifPresent(dataNodeKey::set);

                return dataNodeKeyOptional.isPresent();
            }));
        });

        node.logicalTopology().putNode(A);
        node.logicalTopology().putNode(B);
        node.logicalTopology().putNode(C);

        Set<NodeWithAttributes> logicalTopology = Stream.of(A, B, C)
                .map(n -> new NodeWithAttributes(n.name(), n.id(), n.userAttributes(), n.storageProfiles()))
                .collect(toSet());

        DistributionZoneManager distributionZoneManager = getDistributionZoneManager(node);

        assertTrue(waitForCondition(() -> logicalTopology.equals(distributionZoneManager.logicalTopology()), TIMEOUT_MILLIS));

        createDefaultZone(node);
        int defaultZoneId = getDefaultZoneId(node);

        assertValueInStorage(
                metastore,
                zoneDataNodesHistoryKey(defaultZoneId),
                (v) -> dataNodes(deserializeLatestDataNodesHistoryEntry(v)).stream().map(Node::nodeName).collect(toSet()),
                Set.of(A.name(), B.name(), C.name()),
                TIMEOUT_MILLIS
        );

        imitateConcurrentRemoval.set(true);

        createZone(
                getCatalogManager(node),
                "zone1",
                INFINITE_TIMER_VALUE,
                INFINITE_TIMER_VALUE,
                null,
                consistencyMode,
                DEFAULT_STORAGE_PROFILE
        );

        // Assert that after creation of a zone, data nodes are still tombstone, but not the logical topology, as for default zone.
        assertThat(metastore.get(dataNodeKey.get()).thenApply(Entry::tombstone), willBe(true));
    }

    private Set<NodeWithAttributes> deserializeLogicalTopologySet(byte[] bytes) {
        return DistributionZonesUtil.deserializeLogicalTopologySet(bytes);
    }

    @ParameterizedTest(name = "defaultZone={0},consistencyMode={1}")
    @CsvSource({
            "true,",
            "false, HIGH_AVAILABILITY",
            "false, STRONG_CONSISTENCY",
    })
    public void testLocalDataNodesAreRestoredAfterRestart(boolean defaultZone, ConsistencyMode consistencyMode) throws Exception {
        PartialNode node = startPartialNode(0);

        createDefaultZone(node);

        String zoneName = createZoneOrAlterDefaultZone(node, defaultZone, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE, consistencyMode);

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

    @ParameterizedTest(name = "defaultZone={0},consistencyMode={1}")
    @CsvSource({
            "true,",
            "false, HIGH_AVAILABILITY",
            "false, STRONG_CONSISTENCY",
    })
    public void testScaleUpTimerIsRestoredAfterRestart(boolean defaultZone, ConsistencyMode consistencyMode) throws Exception {
        PartialNode node = startPartialNode(0);

        createDefaultZone(node);

        node.logicalTopology().putNode(A);
        node.logicalTopology().putNode(B);

        assertLogicalTopologyInMetastorage(Set.of(A, B), metastore);

        String zoneName = createZoneOrAlterDefaultZone(node, defaultZone, 100, 100, consistencyMode);

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

        log.info("Test: restarting node.");

        node = startPartialNode(0);

        alterZone(node, zoneName, 0, 0, null);

        distributionZoneManager = getDistributionZoneManager(node);

        catalogManager = getCatalogManager(node);

        assertDataNodesFromManager(distributionZoneManager, metastore::appliedRevision, catalogManager::latestCatalogVersion, zoneId,
                Set.of(A, B, C), TIMEOUT_MILLIS);
    }

    @ParameterizedTest(name = "defaultZone={0},consistencyMode={1}")
    @CsvSource({
            "true,",
            "false, HIGH_AVAILABILITY",
            "false, STRONG_CONSISTENCY",
    })
    public void testScaleDownTimerIsRestoredAfterRestart(boolean defaultZone, ConsistencyMode consistencyMode) throws Exception {
        PartialNode node = startPartialNode(0);

        createDefaultZone(node);

        node.logicalTopology().putNode(A);
        node.logicalTopology().putNode(B);

        assertLogicalTopologyInMetastorage(Set.of(A, B), metastore);

        DistributionZoneManager distributionZoneManager = getDistributionZoneManager(node);
        CatalogManager catalogManager = getCatalogManager(node);

        assertDataNodesFromManager(
                distributionZoneManager,
                metastore::appliedRevision,
                catalogManager::latestCatalogVersion,
                getDefaultZoneId(node),
                Set.of(A, B),
                TIMEOUT_MILLIS
        );

        String zoneName = createZoneOrAlterDefaultZone(node, defaultZone, 100, 100, consistencyMode);

        int zoneId = getZoneId(node, zoneName);

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

        log.info("Test: restarting node.");

        node = startPartialNode(0);

        alterZone(node, zoneName, 0, 0, null);

        catalogManager = getCatalogManager(node);

        assertDataNodesFromManager(
                getDistributionZoneManager(node),
                metastore::appliedRevision,
                catalogManager::latestCatalogVersion,
                zoneId,
                Set.of(A, C),
                TIMEOUT_MILLIS
        );
    }

    private static String createZoneOrAlterDefaultZone(
            PartialNode node,
            boolean useDefaultZone,
            int scaleUp,
            int scaleDown,
            ConsistencyMode consistencyMode
    ) throws Exception {
        String zoneName;

        if (useDefaultZone) {
            CatalogZoneDescriptor defaultZone = getDefaultZone(getCatalogManager(node), node.clock().nowLong());
            zoneName = defaultZone.name();

            alterZone(node, zoneName, scaleUp, scaleDown, null);

            ZoneTimers zoneTimers = getDistributionZoneManager(node).dataNodesManager().zoneTimers(defaultZone.id());

            // This is needed because we want to wait for the end of scale up/down triggered by altering delays.
            if (zoneTimers.scaleUp.taskIsScheduled()) {
                assertTrue(waitForCondition(zoneTimers.scaleUp::taskIsDone, TIMEOUT_MILLIS));
            }

            if (zoneTimers.scaleDown.taskIsScheduled()) {
                assertTrue(waitForCondition(zoneTimers.scaleDown::taskIsDone, TIMEOUT_MILLIS));
            }
        } else {
            zoneName = ZONE_NAME;

            createZone(getCatalogManager(node), zoneName, scaleUp, scaleDown, null, consistencyMode, DEFAULT_STORAGE_PROFILE);
        }

        return zoneName;
    }

    private void blockMetaStorageUpdates(MetaStorageManager metaStorageManager) {
        doThrow(new RuntimeException("Expected")).when(metaStorageManager).invoke(argThat(iif -> {
            If iif1 = MetaStorageWriteHandler.toIf(iif);

            byte[] keyScaleUpBytes = DISTRIBUTION_ZONE_SCALE_UP_TIMER_PREFIX.getBytes(UTF_8);
            byte[] keyScaleDownBytes = DISTRIBUTION_ZONE_SCALE_DOWN_TIMER_PREFIX.getBytes(UTF_8);
            byte[] keyGlobalStateBytes = zonesRecoverableStateRevision().bytes();

            boolean isScaleUpKey = iif1.andThen().update().operations().stream()
                    .anyMatch(op -> op.type() != NO_OP && startsWith(toByteArray(op.key()), keyScaleUpBytes));
            boolean isScaleDownKey = iif1.andThen().update().operations().stream()
                    .anyMatch(op -> op.type() != NO_OP && startsWith(toByteArray(op.key()), keyScaleDownBytes));
            boolean isGlobalStateChangeKey = iif1.andThen().update().operations().stream()
                    .anyMatch(op -> op.type() != NO_OP && startsWith(toByteArray(op.key()), keyGlobalStateBytes));

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
        return getDefaultZone(getCatalogManager(node), node.clock().nowLong()).id();
    }

    private static void createDefaultZone(PartialNode node) {
        DistributionZonesTestUtil.createDefaultZone(getCatalogManager(node));
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
}
