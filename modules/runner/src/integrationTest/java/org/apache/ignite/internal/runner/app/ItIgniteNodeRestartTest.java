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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.apache.ignite.utils.ClusterServiceTestUtils.defaultSerializationRegistry;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.InitParameters;
import org.apache.ignite.internal.BaseIgniteRestartTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.baseline.BaselineManager;
import org.apache.ignite.internal.catalog.CatalogManagerImpl;
import org.apache.ignite.internal.catalog.ClockWaiter;
import org.apache.ignite.internal.catalog.storage.UpdateLogImpl;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.NodeAttributesCollector;
import org.apache.ignite.internal.cluster.management.configuration.ClusterManagementConfiguration;
import org.apache.ignite.internal.cluster.management.configuration.NodeAttributesConfiguration;
import org.apache.ignite.internal.cluster.management.raft.RocksDbClusterStateStorage;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyServiceImpl;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.ConfigurationModules;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.NodeConfigWriteException;
import org.apache.ignite.internal.configuration.storage.DistributedConfigurationStorage;
import org.apache.ignite.internal.configuration.storage.LocalFileConfigurationStorage;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.configuration.validation.ConfigurationValidatorImpl;
import org.apache.ignite.internal.configuration.validation.TestConfigurationValidator;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.index.IndexManager;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.configuration.MetaStorageConfiguration;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.metastorage.server.persistence.RocksDbKeyValueStorage;
import org.apache.ignite.internal.metastorage.server.raft.MetastorageGroupId;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.network.configuration.NetworkConfiguration;
import org.apache.ignite.internal.network.recovery.VaultStateIds;
import org.apache.ignite.internal.placementdriver.TestPlacementDriver;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.raft.storage.impl.LocalLogStorageFactory;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.schema.CatalogSchemaManager;
import org.apache.ignite.internal.schema.configuration.GcConfiguration;
import org.apache.ignite.internal.sql.engine.SqlQueryProcessor;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.storage.DataStorageModule;
import org.apache.ignite.internal.storage.DataStorageModules;
import org.apache.ignite.internal.systemview.SystemViewManagerImpl;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.distributed.TableMessageGroup;
import org.apache.ignite.internal.table.distributed.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.table.distributed.schema.SchemaSyncServiceImpl;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.tx.HybridTimestampTracker;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.TransactionIdGenerator;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.message.TxMessageGroup;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.network.NettyBootstrapFactory;
import org.apache.ignite.network.scalecube.TestScaleCubeClusterServiceFactory;
import org.apache.ignite.raft.jraft.RaftGroupService;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.TransactionException;
import org.awaitility.Awaitility;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * These tests check node restart scenarios.
 */
@ExtendWith(ConfigurationExtension.class)
@Timeout(120)
public class ItIgniteNodeRestartTest extends BaseIgniteRestartTest {
    /** Value producer for table data, is used to create data and check it later. */
    private static final IntFunction<String> VALUE_PRODUCER = i -> "val " + i;

    /** Test table name. */
    private static final String TABLE_NAME = "Table1";

    /** Test table name. */
    private static final String TABLE_NAME_2 = "Table2";

    @InjectConfiguration("mock: " + RAFT_CFG)
    private static RaftConfiguration raftConfiguration;

    @InjectConfiguration
    private static ClusterManagementConfiguration clusterManagementConfiguration;

    @InjectConfiguration
    private static NodeAttributesConfiguration nodeAttributes;

    @InjectConfiguration
    private static MetaStorageConfiguration metaStorageConfiguration;

    /**
     * Start some of Ignite components that are able to serve as Ignite node for test purposes.
     *
     * @param idx Node index.
     * @param cfgString Configuration string or {@code null} to use the default configuration.
     * @return Partial node.
     */
    private PartialNode startPartialNode(int idx, @Nullable @Language("HOCON") String cfgString) {
        return startPartialNode(idx, cfgString, null);
    }

    /**
     * Start some of Ignite components that are able to serve as Ignite node for test purposes.
     *
     * @param idx Node index.
     * @param cfgString Configuration string or {@code null} to use the default configuration.
     * @param revisionCallback Callback on storage revision update.
     * @return Partial node.
     */
    private PartialNode startPartialNode(
            int idx,
            @Nullable @Language("HOCON") String cfgString,
            @Nullable Consumer<Long> revisionCallback
    ) {
        String name = testNodeName(testInfo, idx);

        Path dir = workDir.resolve(name);

        List<IgniteComponent> components = new ArrayList<>();

        VaultManager vault = createVault(name, dir);

        ConfigurationModules modules = loadConfigurationModules(log, Thread.currentThread().getContextClassLoader());

        Path configFile = workDir.resolve(TestIgnitionManager.DEFAULT_CONFIG_NAME);
        String configString = cfgString == null ? configurationString(idx) : cfgString;
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

        var hybridClock = new HybridClockImpl();

        var raftGroupEventsClientListener = new RaftGroupEventsClientListener();

        var raftMgr = new Loza(clusterSvc, raftConfiguration, dir, hybridClock, raftGroupEventsClientListener);

        var clusterStateStorage = new RocksDbClusterStateStorage(dir.resolve("cmg"));

        var logicalTopology = new LogicalTopologyImpl(clusterStateStorage);

        var placementDriver = new TestPlacementDriver(name);

        var cmgManager = new ClusterManagementGroupManager(
                vault,
                clusterSvc,
                raftMgr,
                clusterStateStorage,
                logicalTopology,
                clusterManagementConfiguration,
                new NodeAttributesCollector(nodeAttributes),
                new TestConfigurationValidator());

        ReplicaManager replicaMgr = new ReplicaManager(
                name,
                clusterSvc,
                cmgManager,
                hybridClock,
                Set.of(TableMessageGroup.class, TxMessageGroup.class),
                placementDriver
        );

        var replicaService = new ReplicaService(clusterSvc.messagingService(), hybridClock);

        var lockManager = new HeapLockManager();

        ReplicaService replicaSvc = new ReplicaService(clusterSvc.messagingService(), hybridClock);

        var txManager = new TxManagerImpl(replicaService, lockManager, hybridClock, new TransactionIdGenerator(idx),
                () -> clusterSvc.topologyService().localMember().id());

        var logicalTopologyService = new LogicalTopologyServiceImpl(logicalTopology, cmgManager);

        var topologyAwareRaftGroupServiceFactory = new TopologyAwareRaftGroupServiceFactory(
                clusterSvc,
                logicalTopologyService,
                Loza.FACTORY,
                raftGroupEventsClientListener
        );

        var metaStorageMgr = new MetaStorageManagerImpl(
                vault,
                clusterSvc,
                cmgManager,
                logicalTopologyService,
                raftMgr,
                new RocksDbKeyValueStorage(name, dir.resolve("metastorage")),
                hybridClock,
                topologyAwareRaftGroupServiceFactory,
                metaStorageConfiguration
        );

        var cfgStorage = new DistributedConfigurationStorage(metaStorageMgr);

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

        Consumer<LongFunction<CompletableFuture<?>>> registry = (c) -> metaStorageMgr.registerRevisionUpdateListener(c::apply);

        var baselineManager = new BaselineManager(
                clusterCfgMgr,
                metaStorageMgr,
                clusterSvc
        );

        DataStorageModules dataStorageModules = new DataStorageModules(ServiceLoader.load(DataStorageModule.class));

        Path storagePath = getPartitionsStorePath(dir);

        DataStorageManager dataStorageManager = new DataStorageManager(
                dataStorageModules.createStorageEngines(
                        name,
                        clusterConfigRegistry,
                        storagePath,
                        null
                )
        );

        GcConfiguration gcConfig = clusterConfigRegistry.getConfiguration(GcConfiguration.KEY);

        var clockWaiter = new ClockWaiter(name, hybridClock);

        LongSupplier delayDurationMsSupplier = () -> 100L;

        var catalogManager = new CatalogManagerImpl(
                new UpdateLogImpl(metaStorageMgr),
                clockWaiter,
                delayDurationMsSupplier
        );

        CatalogSchemaManager schemaManager = new CatalogSchemaManager(registry, catalogManager, metaStorageMgr);

        DistributionZoneManager distributionZoneManager = new DistributionZoneManager(
                name,
                registry,
                metaStorageMgr,
                logicalTopologyService,
                vault,
                catalogManager
        );

        var schemaSyncService = new SchemaSyncServiceImpl(metaStorageMgr.clusterTime(), delayDurationMsSupplier);

        TableManager tableManager = new TableManager(
                name,
                registry,
                gcConfig,
                clusterSvc,
                raftMgr,
                replicaMgr,
                lockManager,
                replicaService,
                baselineManager,
                clusterSvc.topologyService(),
                txManager,
                dataStorageManager,
                storagePath,
                metaStorageMgr,
                schemaManager,
                view -> new LocalLogStorageFactory(),
                hybridClock,
                new OutgoingSnapshotsManager(clusterSvc.messagingService()),
                topologyAwareRaftGroupServiceFactory,
                vault,
                distributionZoneManager,
                schemaSyncService,
                catalogManager,
                new HybridTimestampTracker(),
                placementDriver
        );

        var indexManager = new IndexManager(schemaManager, tableManager, catalogManager, metaStorageMgr, registry);

        var metricManager = new MetricManager();

        SqlQueryProcessor qryEngine = new SqlQueryProcessor(
                registry,
                clusterSvc,
                logicalTopologyService,
                tableManager,
                schemaManager,
                dataStorageManager,
                () -> dataStorageModules.collectSchemasFields(modules.distributed().polymorphicSchemaExtensions()),
                replicaSvc,
                hybridClock,
                catalogManager,
                metricManager,
                new SystemViewManagerImpl(name, catalogManager)
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
                raftMgr,
                clusterStateStorage,
                cmgManager,
                replicaMgr,
                txManager,
                baselineManager,
                metaStorageMgr,
                clusterCfgMgr,
                dataStorageManager,
                clockWaiter,
                catalogManager,
                schemaManager,
                distributionZoneManager,
                tableManager,
                indexManager,
                qryEngine
        );

        for (IgniteComponent component : otherComponents) {
            component.start();

            components.add(component);
        }

        PartialNode partialNode = partialNode(
                nodeCfgMgr,
                clusterCfgMgr,
                metaStorageMgr,
                revisionCallback,
                components,
                localConfigurationGenerator,
                logicalTopology,
                cfgStorage,
                distributedConfigurationGenerator,
                clusterConfigRegistry,
                hybridClock
        );

        partialNodes.add(partialNode);
        return partialNode;
    }

    /**
     * Returns a path to the partitions store directory. Creates a directory if it doesn't exist.
     *
     * @param workDir Ignite work directory.
     * @return Partitions store path.
     */
    private static Path getPartitionsStorePath(Path workDir) {
        Path partitionsStore = workDir.resolve(Paths.get("db"));

        try {
            Files.createDirectories(partitionsStore);
        } catch (IOException e) {
            throw new IgniteInternalException("Failed to create directory for partitions storage: " + e.getMessage(), e);
        }

        return partitionsStore;
    }

    /**
     * Starts a node with the given parameters.
     *
     * @param idx Node index.
     * @param cfg Configuration string or {@code null} to use the default configuration.
     * @return Created node instance.
     */
    private IgniteImpl startNode(int idx, @Nullable String cfg) {
        boolean initNeeded = CLUSTER_NODES_NAMES.isEmpty();

        CompletableFuture<Ignite> future = startNodeAsync(idx, cfg);

        if (initNeeded) {
            String nodeName = CLUSTER_NODES_NAMES.get(0);

            InitParameters initParameters = InitParameters.builder()
                    .destinationNodeName(nodeName)
                    .metaStorageNodeNames(List.of(nodeName))
                    .clusterName("cluster")
                    .build();
            TestIgnitionManager.init(initParameters);
        }

        assertThat(future, willCompleteSuccessfully());

        Ignite ignite = future.join();

        return (IgniteImpl) ignite;
    }

    /**
     * Starts a node with the given parameters.
     *
     * @param idx Node index.
     * @return Created node instance.
     */
    private IgniteImpl startNode(int idx) {
        return startNode(idx, null);
    }

    /**
     * Starts a node with the given parameters. Does not run the Init command.
     *
     * @param idx Node index.
     * @param cfg Configuration string or {@code null} to use the default configuration.
     * @return Future that completes with a created node instance.
     */
    private CompletableFuture<Ignite> startNodeAsync(int idx, @Nullable String cfg) {
        String nodeName = testNodeName(testInfo, idx);

        String cfgString = cfg == null ? configurationString(idx) : cfg;

        if (CLUSTER_NODES_NAMES.size() == idx) {
            CLUSTER_NODES_NAMES.add(nodeName);
        } else {
            assertNull(CLUSTER_NODES_NAMES.get(idx));

            CLUSTER_NODES_NAMES.set(idx, nodeName);
        }

        return TestIgnitionManager.start(nodeName, cfgString, workDir.resolve(nodeName));
    }

    /**
     * Starts an {@code amount} number of nodes (with sequential indices starting from 0).
     */
    private List<IgniteImpl> startNodes(int amount) {
        boolean initNeeded = CLUSTER_NODES_NAMES.isEmpty();

        List<CompletableFuture<Ignite>> futures = IntStream.range(0, amount)
                .mapToObj(i -> startNodeAsync(i, null))
                .collect(Collectors.toList());

        if (initNeeded) {
            String nodeName = CLUSTER_NODES_NAMES.get(0);

            InitParameters initParameters = InitParameters.builder()
                    .destinationNodeName(nodeName)
                    .metaStorageNodeNames(List.of(nodeName))
                    .clusterName("cluster")
                    .build();
            TestIgnitionManager.init(initParameters);
        }

        return futures.stream()
                .map(future -> {
                    assertThat(future, willCompleteSuccessfully());

                    return (IgniteImpl) future.join();
                })
                .collect(Collectors.toList());
    }

    /**
     * Stop the node with given index.
     *
     * @param idx Node index.
     */
    private void stopNode(int idx) {
        String nodeName = CLUSTER_NODES_NAMES.set(idx, null);

        if (nodeName != null) {
            IgnitionManager.stop(nodeName);
        }
    }

    /**
     * Restarts empty node.
     */
    @Test
    public void emptyNodeTest() {
        IgniteImpl ignite = startNode(0);

        int nodePort = ignite.nodeConfiguration().getConfiguration(NetworkConfiguration.KEY).port().value();

        assertEquals(DEFAULT_NODE_PORT, nodePort);

        stopNode(0);

        ignite = startNode(0);

        nodePort = ignite.nodeConfiguration().getConfiguration(NetworkConfiguration.KEY).port().value();

        assertEquals(DEFAULT_NODE_PORT, nodePort);
    }

    /**
     * Check correctness of return results after node restart.
     * Scenario:
     * <ol>
     *     <li>Start two nodes and fill the data.</li>
     *     <li>Create index.</li>
     *     <li>Check explain contain index scan.</li>
     *     <li>Check return results.</li>
     *     <li>Restart one node.</li>
     *     <li>Run query and compare results.</li>
     * </ol>
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-19091")
    public void testQueryCorrectnessAfterNodeRestart() throws InterruptedException {
        IgniteImpl ignite1 = startNode(0);

        createTableWithoutData(ignite1, TABLE_NAME, 2, 1);

        IgniteImpl ignite2 = startNode(1);

        String sql = "SELECT id FROM " + TABLE_NAME + " WHERE id > 0 ORDER BY id";

        int intRes;

        try (Session session1 = ignite1.sql().createSession(); Session session2 = ignite2.sql().createSession()) {
            session1.execute(null, "CREATE INDEX idx1 ON " + TABLE_NAME + "(id)");

            waitForIndex(List.of(ignite1, ignite2), "idx1");

            createTableWithData(List.of(ignite1), TABLE_NAME, 2, 1);

            ResultSet<SqlRow> plan = session1.execute(null, "EXPLAIN PLAN FOR " + sql);

            String planStr = plan.next().stringValue(0);

            assertTrue(planStr.contains("IndexScan"));

            ResultSet<SqlRow> res1 = session1.execute(null, sql);

            ResultSet<SqlRow> res2 = session2.execute(null, sql);

            intRes = res1.next().intValue(0);

            assertEquals(intRes, res2.next().intValue(0));

            res1.close();

            res2.close();
        }

        stopNode(0);

        ignite1 = startNode(0);

        try (Session session1 = ignite1.sql().createSession()) {
            ResultSet<SqlRow> res3 = session1.execute(null, sql);

            assertEquals(intRes, res3.next().intValue(0));
        }
    }

    /**
     * Restarts a node with changing configuration.
     */
    @Test
    public void changeConfigurationOnStartTest() {
        IgniteImpl ignite = startNode(0);

        int nodePort = ignite.nodeConfiguration().getConfiguration(NetworkConfiguration.KEY).port().value();

        assertEquals(DEFAULT_NODE_PORT, nodePort);

        stopNode(0);

        int newPort = 3322;

        String updateCfg = "network.port=" + newPort;

        ignite = startNode(0, updateCfg);

        nodePort = ignite.nodeConfiguration().getConfiguration(NetworkConfiguration.KEY).port().value();

        assertEquals(newPort, nodePort);
    }

    /**
     * Tests that a new node's attributes configuration is propagated after node restart.
     */
    @Test
    public void changeNodeAttributesConfigurationOnStartTest() {
        IgniteImpl ignite = startNode(0);

        Map<String, String> attributes = new HashMap<>();

        NodeAttributesConfiguration attributesConfiguration = ignite.nodeConfiguration().getConfiguration(NodeAttributesConfiguration.KEY);

        attributesConfiguration.nodeAttributes().value().namedListKeys().forEach(
                key -> attributes.put(key, attributesConfiguration.nodeAttributes().get(key).attribute().value())
        );

        assertEquals(Collections.emptyMap(), attributes);

        stopNode(0);

        String newAttributesCfg = "{\n"
                + "      region.attribute = \"US\"\n"
                + "      storage.attribute = \"SSD\"\n"
                + "}";

        Map<String, String> newAttributesMap = Map.of("region", "US", "storage", "SSD");

        String updateCfg = "nodeAttributes.nodeAttributes=" + newAttributesCfg;

        ignite = startNode(0, updateCfg);

        NodeAttributesConfiguration newAttributesConfiguration =
                ignite.nodeConfiguration().getConfiguration(NodeAttributesConfiguration.KEY);

        Map<String, String> newAttributes = new HashMap<>();

        newAttributesConfiguration.nodeAttributes().value().namedListKeys().forEach(
                key -> newAttributes.put(key, newAttributesConfiguration.nodeAttributes().get(key).attribute().value())
        );

        assertEquals(newAttributesMap, newAttributes);
    }

    /**
     * Restarts the node which stores some data.
     */
    @Test
    public void nodeWithDataTest() throws InterruptedException {
        IgniteImpl ignite = startNode(0);

        createTableWithData(List.of(ignite), TABLE_NAME, 1);

        stopNode(0);

        ignite = startNode(0);

        checkTableWithData(ignite, TABLE_NAME);
    }

    /**
     * Restarts the node which stores some data.
     */
    @ParameterizedTest
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-18733")
    @ValueSource(booleans = {true, false})
    public void metastorageRecoveryTest(boolean useSnapshot) throws InterruptedException {
        List<IgniteImpl> nodes = startNodes(2);
        IgniteImpl main = nodes.get(0);

        createTableWithData(List.of(main), TABLE_NAME, 1);

        stopNode(1);

        MetaStorageManager metaStorageManager = main.metaStorageManager();

        CompletableFuture[] futs = new CompletableFuture[10];

        for (int i = 0; i < 10; i++) {
            // Put some data to the MetaStorage so that there would be new entries to apply to the restarting node.
            ByteArray key = ByteArray.fromString("some-test-key-" + i);
            futs[i] = metaStorageManager.put(key, new byte[]{(byte) i});
        }

        assertThat(CompletableFuture.allOf(futs), willSucceedFast());

        if (useSnapshot) {
            forceSnapshotUsageOnRestart(main);
        }

        IgniteImpl second = startNode(1);

        checkTableWithData(second, TABLE_NAME);

        MetaStorageManager restartedMs = second.metaStorageManager();
        for (int i = 0; i < 10; i++) {
            ByteArray key = ByteArray.fromString("some-test-key-" + i);

            byte[] value = restartedMs.getLocally(key, 100).value();

            assertEquals(1, value.length);
            assertEquals((byte) i, value[0]);
        }
    }

    private static void forceSnapshotUsageOnRestart(IgniteImpl main) throws InterruptedException {
        // Force log truncation, so that restarting node would request a snapshot.
        JraftServerImpl server = (JraftServerImpl) main.raftManager().server();
        List<Peer> peers = server.localPeers(MetastorageGroupId.INSTANCE);

        Peer learnerPeer = peers.stream().filter(peer -> peer.idx() == 0).findFirst().orElseThrow(
                () -> new IllegalStateException(String.format("No leader peer"))
        );

        var nodeId = new RaftNodeId(MetastorageGroupId.INSTANCE, learnerPeer);
        RaftGroupService raftGroupService = server.raftGroupService(nodeId);

        for (int i = 0; i < 2; i++) {
            // Log must be truncated twice.
            CountDownLatch snapshotLatch = new CountDownLatch(1);
            AtomicReference<Status> snapshotStatus = new AtomicReference<>();

            raftGroupService.getRaftNode().snapshot(status -> {
                snapshotStatus.set(status);
                snapshotLatch.countDown();
            });

            assertTrue(snapshotLatch.await(10, TimeUnit.SECONDS), "Snapshot was not finished in time");
            assertTrue(snapshotStatus.get().isOk(), "Snapshot failed: " + snapshotStatus.get());
        }
    }

    /**
     * Restarts the node which stores some data.
     */
    @Test
    public void nodeWithDataAndIndexRebuildTest() {
        IgniteImpl ignite = startNode(0);

        int partitions = 20;

        createTableWithData(List.of(ignite), TABLE_NAME, 1, partitions);

        TableImpl table = (TableImpl) ignite.tables().table(TABLE_NAME);

        InternalTableImpl internalTable = (InternalTableImpl) table.internalTable();

        CompletableFuture[] flushFuts = new CompletableFuture[partitions];

        for (int i = 0; i < partitions; i++) {
            // Flush data on disk, so that we will have a snapshot to read on restart.
            flushFuts[i] = internalTable.storage().getMvPartition(i).flush();
        }

        assertThat(CompletableFuture.allOf(flushFuts), willCompleteSuccessfully());

        // Add more data, so that on restart there will be a index rebuilding operation.
        try (Session session = ignite.sql().createSession()) {
            for (int i = 0; i < 100; i++) {
                session.execute(null, "INSERT INTO " + TABLE_NAME + "(id, name) VALUES (?, ?)",
                        i + 500, VALUE_PRODUCER.apply(i + 500));
            }
        }

        stopNode(0);

        ignite = startNode(0);

        checkTableWithData(ignite, TABLE_NAME);

        table = (TableImpl) ignite.tables().table(TABLE_NAME);

        // Check data that was added after flush.
        for (int i = 0; i < 100; i++) {
            Tuple row = table.keyValueView().get(null, Tuple.create().set("id", i + 500));

            Objects.requireNonNull(row, "row");

            assertEquals(VALUE_PRODUCER.apply(i + 500), row.stringValue("name"));
        }
    }

    /**
     * Starts two nodes and checks that the data are storing through restarts. Nodes restart in the same order when they started at first.
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-18733")
    public void testTwoNodesRestartDirect() throws InterruptedException {
        twoNodesRestart(true);
    }

    /**
     * Starts two nodes and checks that the data are storing through restarts. Nodes restart in reverse order when they started at first.
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-18733")
    public void testTwoNodesRestartReverse() throws InterruptedException {
        twoNodesRestart(false);
    }

    /**
     * Starts two nodes and checks that the data are storing through restarts.
     *
     * @param directOrder When the parameter is true, nodes restart in direct order, otherwise they restart in reverse order.
     */
    private void twoNodesRestart(boolean directOrder) throws InterruptedException {
        List<IgniteImpl> nodes = startNodes(2);

        createTableWithData(nodes, TABLE_NAME, 2);
        createTableWithData(nodes, TABLE_NAME_2, 2);

        stopNode(0);
        stopNode(1);

        Ignite ignite;

        if (directOrder) {
            startNode(0);
            ignite = startNode(1);
        } else {
            // Since the first node is the CMG leader, the second node can't be started synchronously (it won't be able to join the cluster
            // and the future will never resolve).
            CompletableFuture<Ignite> future = startNodeAsync(1, null);

            startNode(0);

            assertThat(future, willCompleteSuccessfully());

            ignite = future.join();
        }

        checkTableWithData(ignite, TABLE_NAME);
        checkTableWithData(ignite, TABLE_NAME_2);
    }

    /**
     * Check that the table with given name is present in TableManager.
     *
     * @param tableManager Table manager.
     * @param tableName Table name.
     */
    private void assertTablePresent(TableManager tableManager, String tableName) {
        Collection<TableImpl> tables = tableManager.latestTables().values();

        boolean isPresent = false;

        for (TableImpl table : tables) {
            if (table.name().equals(tableName)) {
                isPresent = true;

                break;
            }
        }

        assertTrue(isPresent, "tableName=" + tableName + ", tables=" + tables);
    }

    /**
     * Checks that one node in a cluster of 2 nodes is able to restart and recover a table that was created when this node was absent. Also
     * checks that the table created before node stop, is not available when majority if lost.
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-20137")
    public void testOneNodeRestartWithGap() throws InterruptedException {
        IgniteImpl ignite = startNode(0);

        startNode(1);

        createTableWithData(List.of(ignite), TABLE_NAME, 2);

        stopNode(1);

        Table table = ignite.tables().table(TABLE_NAME);

        assertNotNull(table);

        assertThrows(TransactionException.class, () -> table.keyValueView().get(null, Tuple.create().set("id", 0)));

        createTableWithoutData(ignite, TABLE_NAME_2, 1, 1);

        IgniteImpl ignite1 = startNode(1);

        TableManager tableManager = (TableManager) ignite1.tables();

        assertNotNull(tableManager);

        assertTablePresent(tableManager, TABLE_NAME.toUpperCase());
        assertTablePresent(tableManager, TABLE_NAME_2.toUpperCase());
    }

    /**
     * Checks that the table created in cluster of 2 nodes, is recovered on a node after restart of this node.
     */
    @Test
    public void testRecoveryOnOneNode() {
        IgniteImpl ignite = startNode(0);

        IgniteImpl node = startNode(1);

        createTableWithData(List.of(ignite), TABLE_NAME, 2, 1);

        stopNode(1);

        node = startNode(1);

        TableManager tableManager = (TableManager) node.tables();

        assertNotNull(tableManager);

        assertTablePresent(tableManager, TABLE_NAME.toUpperCase());
    }

    /**
     * Checks that a cluster is able to restart when some changes were made in configuration.
     */
    @Test
    public void testRestartDiffConfig() throws InterruptedException {
        List<IgniteImpl> ignites = startNodes(2);

        createTableWithData(ignites, TABLE_NAME, 2);
        createTableWithData(ignites, TABLE_NAME_2, 2);

        stopNode(0);
        stopNode(1);

        startNode(0);

        @Language("HOCON") String cfgString = IgniteStringFormatter.format(NODE_BOOTSTRAP_CFG,
                DEFAULT_NODE_PORT + 11,
                "[\"localhost:" + DEFAULT_NODE_PORT + "\"]",
                DEFAULT_CLIENT_PORT + 11
        );

        IgniteImpl node1 = startNode(1, cfgString);

        TableManager tableManager = (TableManager) node1.tables();

        assertTablePresent(tableManager, TABLE_NAME.toUpperCase());
    }

    /**
     * The test for node restart when there is a gap between the node local configuration and distributed configuration.
     */
    @Test
    public void testCfgGapWithoutData() throws InterruptedException {
        List<IgniteImpl> nodes = startNodes(3);

        createTableWithData(nodes, TABLE_NAME, nodes.size());

        log.info("Stopping the node.");

        stopNode(nodes.size() - 1);

        nodes.set(nodes.size() - 1, null);

        createTableWithData(nodes, TABLE_NAME_2, nodes.size());
        createTableWithData(nodes, TABLE_NAME_2 + "0", nodes.size());

        log.info("Starting the node.");

        IgniteImpl node = startNode(nodes.size() - 1, null);

        log.info("After starting the node.");

        TableManager tableManager = (TableManager) node.tables();

        assertTablePresent(tableManager, TABLE_NAME.toUpperCase());
        assertTablePresent(tableManager, TABLE_NAME_2.toUpperCase());
    }

    /**
     * The test for node restart when there is a gap between the node local configuration and distributed configuration, and metastorage
     * group stops for some time while restarting node is being recovered. The recovery process should continue and eventually succeed after
     * metastorage group starts again.
     */
    @Test
    @Disabled(value = "https://issues.apache.org/jira/browse/IGNITE-18919")
    public void testMetastorageStop() {
        int cfgGap = 4;

        List<IgniteImpl> nodes = startNodes(3);

        log.info("Stopping the node.");

        stopNode(nodes.size() - 1);

        nodes.set(nodes.size() - 1, null);

        for (int i = 0; i < cfgGap; i++) {
            createTableWithData(nodes, "t" + i, nodes.size(), 1);
        }

        log.info("Starting the node.");

        PartialNode partialNode = startPartialNode(
                nodes.size() - 1,
                configurationString(nodes.size() - 1),
                rev -> {
                    log.info("Partially started node: applying revision: " + rev);

                    if (rev == cfgGap / 2) {
                        log.info("Stopping METASTORAGE");

                        stopNode(0);

                        log.info("Starting METASTORAGE");

                        startNode(0);

                        log.info("Restarted METASTORAGE");
                    }
                }
        );

        TableManager tableManager = findComponent(partialNode.startedComponents(), TableManager.class);

        for (int i = 0; i < cfgGap; i++) {
            assertTablePresent(tableManager, "T" + i);
        }
    }

    /**
     * The test for node restart when there is a gap between the node local configuration and distributed configuration.
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-18733")
    public void testCfgGap() throws InterruptedException {
        List<IgniteImpl> nodes = startNodes(4);

        createTableWithData(nodes, "t1", nodes.size());

        log.info("Stopping the node.");

        stopNode(nodes.size() - 1);

        nodes.set(nodes.size() - 1, null);

        checkTableWithData(nodes.get(0), "t1");

        createTableWithData(nodes, "t2", nodes.size());

        log.info("Starting the node.");

        IgniteImpl newNode = startNode(nodes.size() - 1);

        checkTableWithData(nodes.get(0), "t1");
        checkTableWithData(nodes.get(0), "t2");

        checkTableWithData(newNode, "t1");
        checkTableWithData(newNode, "t2");
    }

    /**
     * The test for updating cluster configuration with the default value.
     * Check that new nodes will be able to synchronize the local cluster configuration.
     */
    @Test
    public void updateClusterCfgWithDefaultValue() {
        IgniteImpl ignite = startNode(0);

        GcConfiguration gcConfiguration = ignite.clusterConfiguration()
                .getConfiguration(GcConfiguration.KEY);
        int defaultValue = gcConfiguration.onUpdateBatchSize().value();
        CompletableFuture<Void> update = gcConfiguration.onUpdateBatchSize().update(defaultValue);
        assertThat(update, willCompleteSuccessfully());

        stopNode(0);

        startNodes(3);
    }

    /**
     * Checks the table exists and validates all data in it.
     *
     * @param ignite Ignite.
     * @param name Table name.
     */
    private static void checkTableWithData(Ignite ignite, String name) {
        Table table = ignite.tables().table(name);

        assertNotNull(table);

        for (int i = 0; i < 100; i++) {
            int fi = i;

            Awaitility.with()
                    .await()
                    .pollInterval(100, TimeUnit.MILLISECONDS)
                    .pollDelay(0, TimeUnit.MILLISECONDS)
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> {
                        try {
                            Tuple row = table.keyValueView().get(null, Tuple.create().set("id", fi));

                            if (row == null) {
                                return false;
                            } else {
                                assertEquals(VALUE_PRODUCER.apply(fi), row.stringValue("name"));
                                return true;
                            }
                        } catch (TransactionException te) {
                            // There may be an exception if the primary replica node was stopped. We should wait for new primary to appear.
                            return false;
                        }
                    });
        }
    }

    /**
     * Creates a table and load data to it.
     *
     * @param nodes Ignite nodes.
     * @param name Table name.
     * @param replicas Replica factor.
     */
    private void createTableWithData(List<IgniteImpl> nodes, String name, int replicas) {
        createTableWithData(nodes, name, replicas, 2);
    }

    /**
     * Creates a table and load data to it.
     *
     * @param nodes Ignite nodes.
     * @param name Table name.
     * @param replicas Replica factor.
     * @param partitions Partitions count.
     */
    private void createTableWithData(List<IgniteImpl> nodes, String name, int replicas, int partitions) {
        try (Session session = nodes.get(0).sql().createSession()) {
            session.execute(null,
                    String.format("CREATE ZONE IF NOT EXISTS ZONE_%s WITH REPLICAS=%d, PARTITIONS=%d", name, replicas, partitions));
            session.execute(null, "CREATE TABLE IF NOT EXISTS " + name
                    + "(id INT PRIMARY KEY, name VARCHAR) WITH PRIMARY_ZONE='ZONE_" + name.toUpperCase() + "';");

            for (int i = 0; i < 100; i++) {
                session.execute(null, "INSERT INTO " + name + "(id, name) VALUES (?, ?)",
                        i, VALUE_PRODUCER.apply(i));
            }
        }
    }

    private void waitForIndex(Collection<IgniteImpl> nodes, String indexName) throws InterruptedException {
        // FIXME: Wait for the index to be created on all nodes,
        //  this is a workaround for https://issues.apache.org/jira/browse/IGNITE-18733 to avoid missed updates to the PK index.
        assertTrue(waitForCondition(
                () -> nodes.stream()
                        .map(nodeImpl -> nodeImpl.catalogManager().index(indexName.toUpperCase(), nodeImpl.clock().nowLong()))
                        .allMatch(Objects::nonNull),
                TIMEOUT_MILLIS
        ));
    }

    /**
     * Creates a table.
     *
     * @param ignite Ignite.
     * @param name Table name.
     * @param replicas Replica factor.
     * @param partitions Partitions count.
     */
    private static Table createTableWithoutData(Ignite ignite, String name, int replicas, int partitions) {
        try (Session session = ignite.sql().createSession()) {
            session.execute(null,
                    String.format("CREATE ZONE IF NOT EXISTS ZONE_%s WITH REPLICAS=%d, PARTITIONS=%d", name, replicas, partitions));
            session.execute(null, "CREATE TABLE " + name
                    + "(id INT PRIMARY KEY, name VARCHAR) WITH PRIMARY_ZONE='ZONE_" + name.toUpperCase() + "';");
        }

        return ignite.tables().table(name);
    }
}
