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

import static java.util.Collections.emptySet;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.TestWrappers.unwrapTableImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapTableManager;
import static org.apache.ignite.internal.TestWrappers.unwrapTableViewInternal;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.configuration.IgnitePaths.partitionsPath;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.alterZone;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.REBALANCE_SCHEDULER_POOL_SIZE;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.STABLE_ASSIGNMENTS_PREFIX;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.stablePartAssignmentsKey;
import static org.apache.ignite.internal.network.utils.ClusterServiceTestUtils.defaultSerializationRegistry;
import static org.apache.ignite.internal.table.NodeUtils.transferPrimary;
import static org.apache.ignite.internal.table.TableTestUtils.getTableIdStrict;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.bypassingThreadAssertions;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.bypassingThreadAssertionsAsync;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runInExecutor;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.apache.ignite.internal.util.ByteUtils.toByteArray;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteServer;
import org.apache.ignite.internal.BaseIgniteRestartTest;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.affinity.Assignments;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.app.ThreadPoolsManager;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogManagerImpl;
import org.apache.ignite.internal.catalog.commands.AlterZoneCommand;
import org.apache.ignite.internal.catalog.commands.AlterZoneCommandBuilder;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.CreateTableCommand;
import org.apache.ignite.internal.catalog.commands.TableHashPrimaryKey;
import org.apache.ignite.internal.catalog.configuration.SchemaSynchronizationConfiguration;
import org.apache.ignite.internal.catalog.storage.UpdateLogImpl;
import org.apache.ignite.internal.cluster.management.ClusterIdService;
import org.apache.ignite.internal.cluster.management.ClusterInitializer;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.NodeAttributesCollector;
import org.apache.ignite.internal.cluster.management.configuration.NodeAttributesConfiguration;
import org.apache.ignite.internal.cluster.management.raft.RocksDbClusterStateStorage;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyServiceImpl;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.configuration.ComponentWorkingDir;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.ConfigurationModules;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.NodeConfigWriteException;
import org.apache.ignite.internal.configuration.RaftGroupOptionsConfigHelper;
import org.apache.ignite.internal.configuration.SystemLocalConfiguration;
import org.apache.ignite.internal.configuration.storage.DistributedConfigurationStorage;
import org.apache.ignite.internal.configuration.storage.LocalFileConfigurationStorage;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.configuration.validation.ConfigurationValidatorImpl;
import org.apache.ignite.internal.configuration.validation.TestConfigurationValidator;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.failure.NoOpFailureProcessor;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.ClockServiceImpl;
import org.apache.ignite.internal.hlc.ClockWaiter;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.index.IndexManager;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.lowwatermark.LowWatermarkImpl;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.configuration.MetaStorageConfiguration;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.metastorage.server.persistence.RocksDbKeyValueStorage;
import org.apache.ignite.internal.metastorage.server.raft.MetastorageGroupId;
import org.apache.ignite.internal.metrics.MetricManagerImpl;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NettyBootstrapFactory;
import org.apache.ignite.internal.network.NettyWorkersRegistrar;
import org.apache.ignite.internal.network.configuration.NetworkConfiguration;
import org.apache.ignite.internal.network.recovery.VaultStaleIds;
import org.apache.ignite.internal.network.scalecube.TestScaleCubeClusterServiceFactory;
import org.apache.ignite.internal.network.wrapper.JumpToExecutorByConsistentIdAfterSend;
import org.apache.ignite.internal.partition.replicator.PartitionReplicaLifecycleManager;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessageGroup;
import org.apache.ignite.internal.placementdriver.PlacementDriverManager;
import org.apache.ignite.internal.placementdriver.PrimaryReplicaAwaitTimeoutException;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftGroupOptionsConfigurer;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.TestLozaFactory;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.raft.storage.LogStorageFactory;
import org.apache.ignite.internal.raft.storage.impl.LocalLogStorageFactory;
import org.apache.ignite.internal.raft.util.SharedLogStorageFactoryUtils;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.configuration.GcConfiguration;
import org.apache.ignite.internal.schema.configuration.StorageUpdateConfiguration;
import org.apache.ignite.internal.sql.api.IgniteSqlImpl;
import org.apache.ignite.internal.sql.configuration.distributed.SqlDistributedConfiguration;
import org.apache.ignite.internal.sql.configuration.local.SqlLocalConfiguration;
import org.apache.ignite.internal.sql.engine.SqlQueryProcessor;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.storage.DataStorageModule;
import org.apache.ignite.internal.storage.DataStorageModules;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.systemview.SystemViewManagerImpl;
import org.apache.ignite.internal.table.RecordBinaryViewImpl;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.distributed.index.IndexMetaStorage;
import org.apache.ignite.internal.table.distributed.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.table.distributed.schema.SchemaSyncServiceImpl;
import org.apache.ignite.internal.table.distributed.schema.ThreadLocalPartitionCommandsMarshaller;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.internal.test.WatchListenerInhibitor;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.thread.ThreadOperation;
import org.apache.ignite.internal.tx.HybridTimestampTracker;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.RemotelyTriggeredResourceRegistry;
import org.apache.ignite.internal.tx.impl.ResourceVacuumManager;
import org.apache.ignite.internal.tx.impl.TransactionIdGenerator;
import org.apache.ignite.internal.tx.impl.TransactionInflights;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.message.TxMessageGroup;
import org.apache.ignite.internal.tx.test.TestLocalRwTxCounter;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.worker.CriticalWorkerWatchdog;
import org.apache.ignite.internal.worker.configuration.CriticalWorkersConfiguration;
import org.apache.ignite.internal.wrapper.Wrappers;
import org.apache.ignite.raft.jraft.RaftGroupService;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.TransactionException;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
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

    /** Assume that the table id will always be 8 for the test table. There is an assertion to check if this is true. */
    // TODO: IGNITE-22251 Get rid of it
    private static final int TABLE_ID = 8;

    /** Test table name. */
    private static final String TABLE_NAME_2 = "Table2";

    @InjectConfiguration("mock: " + RAFT_CFG)
    private static RaftConfiguration raftConfiguration;

    @InjectConfiguration
    private static SystemLocalConfiguration systemConfiguration;

    @InjectConfiguration
    private static NodeAttributesConfiguration nodeAttributes;

    @InjectConfiguration
    private static StorageConfiguration storageConfiguration;

    @InjectConfiguration
    private static MetaStorageConfiguration metaStorageConfiguration;

    @InjectConfiguration
    private static TransactionConfiguration txConfiguration;

    @InjectConfiguration
    private static StorageUpdateConfiguration storageUpdateConfiguration;

    @InjectConfiguration
    private CriticalWorkersConfiguration workersConfiguration;

    @InjectConfiguration
    private ReplicationConfiguration replicationConfiguration;

    /**
     * Interceptor of {@link MetaStorageManager#invoke(Condition, Collection, Collection)}.
     */
    private final Map<Integer, InvokeInterceptor> metaStorageInvokeInterceptorByNode = new ConcurrentHashMap<>();

    /**
     * Mocks the data nodes returned by {@link DistributionZoneManager#dataNodes(long, int, int)} method on different nodes.
     */
    private final Map<Integer, Supplier<CompletableFuture<Set<String>>>> dataNodesMockByNode = new ConcurrentHashMap<>();

    private final ExecutorService storageExecutor = Executors.newSingleThreadExecutor(
            IgniteThreadFactory.create("test", "storage-test-pool-iinrt", log, ThreadOperation.STORAGE_READ)
    );

    @BeforeEach
    public void beforeTest() {
        metaStorageInvokeInterceptorByNode.clear();
        dataNodesMockByNode.clear();
        partialNodes.clear();
    }

    /**
     * Start some of Ignite components that are able to serve as Ignite node for test purposes.
     *
     * @param idx Node index.
     * @param cfgString Configuration string or {@code null} to use the default configuration.
     * @return Partial node.
     */
    private PartialNode startPartialNode(
            int idx,
            @Nullable @Language("HOCON") String cfgString
    ) {
        String name = testNodeName(testInfo, idx);

        Path dir = workDir.resolve(name);

        List<IgniteComponent> components = new ArrayList<>();

        VaultManager vault = createVault(dir);

        var clusterStateStorage = new RocksDbClusterStateStorage(dir.resolve("cmg"), name);

        var clusterIdService = new ClusterIdService(clusterStateStorage);

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
                new LocalFileConfigurationStorage(configFile, localConfigurationGenerator, modules.local()),
                localConfigurationGenerator,
                ConfigurationValidatorImpl.withDefaultValidators(localConfigurationGenerator, modules.local().validators())
        );

        NetworkConfiguration networkConfiguration = nodeCfgMgr.configurationRegistry().getConfiguration(NetworkConfiguration.KEY);

        var threadPoolsManager = new ThreadPoolsManager(name);

        var failureProcessor = new NoOpFailureProcessor();

        var workerRegistry = new CriticalWorkerWatchdog(workersConfiguration, threadPoolsManager.commonScheduler(), failureProcessor);

        var nettyBootstrapFactory = new NettyBootstrapFactory(networkConfiguration, name);
        var nettyWorkersRegistrar = new NettyWorkersRegistrar(
                workerRegistry,
                threadPoolsManager.commonScheduler(),
                nettyBootstrapFactory,
                workersConfiguration,
                failureProcessor
        );

        var clusterSvc = new TestScaleCubeClusterServiceFactory().createClusterService(
                name,
                networkConfiguration,
                nettyBootstrapFactory,
                defaultSerializationRegistry(),
                new VaultStaleIds(vault),
                clusterIdService,
                workerRegistry,
                failureProcessor
        );

        var hybridClock = new HybridClockImpl();

        var raftGroupEventsClientListener = new RaftGroupEventsClientListener();

        ComponentWorkingDir partitionsWorkDir = partitionsPath(systemConfiguration, dir);

        LogStorageFactory partitionsLogStorageFactory =
                SharedLogStorageFactoryUtils.create(clusterSvc.nodeName(), partitionsWorkDir.raftLogPath());

        RaftGroupOptionsConfigurer partitionRaftConfigurer =
                RaftGroupOptionsConfigHelper.configureProperties(partitionsLogStorageFactory, partitionsWorkDir.metaPath());

        var raftMgr = TestLozaFactory.create(
                clusterSvc,
                raftConfiguration,
                hybridClock,
                raftGroupEventsClientListener
        );

        var logicalTopology = new LogicalTopologyImpl(clusterStateStorage);

        var clusterInitializer = new ClusterInitializer(
                clusterSvc,
                hocon -> hocon,
                new TestConfigurationValidator()
        );

        ComponentWorkingDir cmgWorkDir = new ComponentWorkingDir(workDir.resolve("cmg"));

        LogStorageFactory cmgLogStorageFactory =
                SharedLogStorageFactoryUtils.create(clusterSvc.nodeName(), cmgWorkDir.raftLogPath());

        RaftGroupOptionsConfigurer cmgRaftConfigurer =
                RaftGroupOptionsConfigHelper.configureProperties(cmgLogStorageFactory, cmgWorkDir.metaPath());

        var cmgManager = new ClusterManagementGroupManager(
                vault,
                clusterSvc,
                clusterInitializer,
                raftMgr,
                clusterStateStorage,
                logicalTopology,
                new NodeAttributesCollector(nodeAttributes,
                        nodeCfgMgr.configurationRegistry().getConfiguration(StorageConfiguration.KEY)),
                failureProcessor,
                clusterIdService,
                cmgRaftConfigurer
        );

        LongSupplier partitionIdleSafeTimePropagationPeriodMsSupplier
                = () -> TestIgnitionManager.DEFAULT_PARTITION_IDLE_SYNC_TIME_INTERVAL_MS;

        MessagingService messagingServiceReturningToStorageOperationsPool = new JumpToExecutorByConsistentIdAfterSend(
                clusterSvc.messagingService(),
                name,
                message -> threadPoolsManager.partitionOperationsExecutor()
        );

        var replicaService = new ReplicaService(
                messagingServiceReturningToStorageOperationsPool,
                hybridClock,
                threadPoolsManager.partitionOperationsExecutor(),
                replicationConfiguration,
                threadPoolsManager.commonScheduler()
        );

        var lockManager = new HeapLockManager();

        var logicalTopologyService = new LogicalTopologyServiceImpl(logicalTopology, cmgManager);

        var metricManager = new MetricManagerImpl();

        var topologyAwareRaftGroupServiceFactory = new TopologyAwareRaftGroupServiceFactory(
                clusterSvc,
                logicalTopologyService,
                Loza.FACTORY,
                raftGroupEventsClientListener
        );

        var metaStorage = new RocksDbKeyValueStorage(name, dir.resolve("metastorage"), new NoOpFailureProcessor());

        InvokeInterceptor metaStorageInvokeInterceptor = metaStorageInvokeInterceptorByNode.get(idx);

        CompletableFuture<LongSupplier> maxClockSkewFuture = new CompletableFuture<>();

        ComponentWorkingDir metastorageWorkDir = new ComponentWorkingDir(workDir.resolve("metastorage"));

        LogStorageFactory msLogStorageFactory =
                SharedLogStorageFactoryUtils.create(clusterSvc.nodeName(), metastorageWorkDir.raftLogPath());

        RaftGroupOptionsConfigurer msRaftConfigurer =
                RaftGroupOptionsConfigHelper.configureProperties(msLogStorageFactory, metastorageWorkDir.metaPath());

        var metaStorageMgr = new MetaStorageManagerImpl(
                clusterSvc,
                cmgManager,
                logicalTopologyService,
                raftMgr,
                metaStorage,
                hybridClock,
                topologyAwareRaftGroupServiceFactory,
                metricManager,
                metaStorageConfiguration,
                msRaftConfigurer
        ) {
            @Override
            public CompletableFuture<Boolean> invoke(Condition condition, Collection<Operation> success, Collection<Operation> failure) {
                if (metaStorageInvokeInterceptor != null) {
                    var res = metaStorageInvokeInterceptor.invoke(condition, success, failure);

                    if (res != null) {
                        return completedFuture(res);
                    }
                }

                return super.invoke(condition, success, failure);
            }
        };

        var cfgStorage = new DistributedConfigurationStorage("test", metaStorageMgr);

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

        var clockWaiter = new ClockWaiter(name, hybridClock);

        SchemaSynchronizationConfiguration schemaSyncConfiguration = clusterConfigRegistry.getConfiguration(
                SchemaSynchronizationConfiguration.KEY
        );

        ClockService clockService = new ClockServiceImpl(
                hybridClock,
                clockWaiter,
                () -> schemaSyncConfiguration.maxClockSkew().value()
        );

        maxClockSkewFuture.complete(clockService::maxClockSkewMillis);

        var placementDriverManager = new PlacementDriverManager(
                name,
                metaStorageMgr,
                MetastorageGroupId.INSTANCE,
                clusterSvc,
                cmgManager::metaStorageNodes,
                logicalTopologyService,
                raftMgr,
                topologyAwareRaftGroupServiceFactory,
                clockService
        );

        ScheduledExecutorService rebalanceScheduler = new ScheduledThreadPoolExecutor(REBALANCE_SCHEDULER_POOL_SIZE,
                NamedThreadFactory.create(name, "test-rebalance-scheduler", logger()));

        ReplicaManager replicaMgr = new ReplicaManager(
                name,
                clusterSvc,
                cmgManager,
                clockService,
                Set.of(PartitionReplicationMessageGroup.class, TxMessageGroup.class),
                placementDriverManager.placementDriver(),
                threadPoolsManager.partitionOperationsExecutor(),
                partitionIdleSafeTimePropagationPeriodMsSupplier,
                failureProcessor,
                new ThreadLocalPartitionCommandsMarshaller(clusterSvc.serializationRegistry()),
                topologyAwareRaftGroupServiceFactory,
                raftMgr,
                partitionRaftConfigurer,
                view -> new LocalLogStorageFactory(),
                threadPoolsManager.tableIoExecutor()
        );

        var resourcesRegistry = new RemotelyTriggeredResourceRegistry();

        GcConfiguration gcConfig = clusterConfigRegistry.getConfiguration(GcConfiguration.KEY);

        var lowWatermark = new LowWatermarkImpl(
                name,
                gcConfig.lowWatermark(),
                clockService,
                vault,
                failureProcessor,
                clusterSvc.messagingService()
        );

        TransactionInflights transactionInflights = new TransactionInflights(placementDriverManager.placementDriver(), clockService);

        var txManager = new TxManagerImpl(
                name,
                txConfiguration,
                messagingServiceReturningToStorageOperationsPool,
                clusterSvc.topologyService(),
                replicaService,
                lockManager,
                clockService,
                new TransactionIdGenerator(idx),
                placementDriverManager.placementDriver(),
                partitionIdleSafeTimePropagationPeriodMsSupplier,
                new TestLocalRwTxCounter(),
                threadPoolsManager.partitionOperationsExecutor(),
                resourcesRegistry,
                transactionInflights,
                lowWatermark
        );

        ResourceVacuumManager resourceVacuumManager = new ResourceVacuumManager(
                name,
                resourcesRegistry,
                clusterSvc.topologyService(),
                clusterSvc.messagingService(),
                transactionInflights,
                txManager
        );

        Consumer<LongFunction<CompletableFuture<?>>> registry = (c) -> metaStorageMgr.registerRevisionUpdateListener(c::apply);

        DataStorageModules dataStorageModules = new DataStorageModules(
                ServiceLoader.load(DataStorageModule.class)
        );

        Path storagePath = getPartitionsStorePath(dir);

        LogSyncer logSyncer = partitionsLogStorageFactory;

        DataStorageManager dataStorageManager = new DataStorageManager(
                dataStorageModules.createStorageEngines(
                        name,
                        nodeCfgMgr.configurationRegistry(),
                        storagePath,
                        null,
                        failureProcessor,
                        logSyncer,
                        hybridClock
                ),
                nodeCfgMgr.configurationRegistry().getConfiguration(StorageConfiguration.KEY)
        );

        TransactionConfiguration txConfiguration = clusterConfigRegistry.getConfiguration(TransactionConfiguration.KEY);

        LongSupplier delayDurationMsSupplier = () -> TestIgnitionManager.DEFAULT_DELAY_DURATION_MS;

        var catalogManager = new CatalogManagerImpl(
                new UpdateLogImpl(metaStorageMgr),
                clockService,
                delayDurationMsSupplier,
                partitionIdleSafeTimePropagationPeriodMsSupplier
        );

        var indexMetaStorage = new IndexMetaStorage(catalogManager, lowWatermark, metaStorageMgr);

        SchemaManager schemaManager = new SchemaManager(registry, catalogManager);

        var dataNodesMock = dataNodesMockByNode.get(idx);

        DistributionZoneManager distributionZoneManager = new DistributionZoneManager(
                name,
                registry,
                metaStorageMgr,
                logicalTopologyService,
                catalogManager,
                rebalanceScheduler
        ) {
            @Override
            public CompletableFuture<Set<String>> dataNodes(long causalityToken, int catalogVersion, int zoneId) {
                if (dataNodesMock != null) {
                    return dataNodesMock.get();
                }

                return super.dataNodes(causalityToken, catalogVersion, zoneId);
            }
        };

        var schemaSyncService = new SchemaSyncServiceImpl(metaStorageMgr.clusterTime(), delayDurationMsSupplier);

        var sqlRef = new AtomicReference<IgniteSqlImpl>();

        TableManager tableManager = new TableManager(
                name,
                registry,
                gcConfig,
                txConfiguration,
                storageUpdateConfiguration,
                messagingServiceReturningToStorageOperationsPool,
                clusterSvc.topologyService(),
                clusterSvc.serializationRegistry(),
                replicaMgr,
                lockManager,
                replicaService,
                txManager,
                dataStorageManager,
                storagePath,
                metaStorageMgr,
                schemaManager,
                threadPoolsManager.tableIoExecutor(),
                threadPoolsManager.partitionOperationsExecutor(),
                rebalanceScheduler,
                hybridClock,
                clockService,
                new OutgoingSnapshotsManager(clusterSvc.messagingService()),
                distributionZoneManager,
                schemaSyncService,
                catalogManager,
                new HybridTimestampTracker(),
                placementDriverManager.placementDriver(),
                sqlRef::get,
                resourcesRegistry,
                lowWatermark,
                transactionInflights,
                indexMetaStorage,
                logSyncer,
                new PartitionReplicaLifecycleManager(
                        catalogManager,
                        replicaMgr,
                        distributionZoneManager,
                        metaStorageMgr,
                        clusterSvc.topologyService(),
                        lowWatermark,
                        threadPoolsManager.tableIoExecutor(),
                        rebalanceScheduler,
                        threadPoolsManager.partitionOperationsExecutor(),
                        clockService,
                        placementDriverManager.placementDriver(),
                        schemaSyncService
                )
        );

        var indexManager = new IndexManager(
                schemaManager,
                tableManager,
                catalogManager,
                threadPoolsManager.tableIoExecutor(),
                registry,
                lowWatermark
        );

        SqlQueryProcessor qryEngine = new SqlQueryProcessor(
                clusterSvc,
                logicalTopologyService,
                tableManager,
                schemaManager,
                dataStorageManager,
                replicaService,
                clockService,
                schemaSyncService,
                catalogManager,
                metricManager,
                new SystemViewManagerImpl(name, catalogManager),
                failureProcessor,
                partitionIdleSafeTimePropagationPeriodMsSupplier,
                placementDriverManager.placementDriver(),
                clusterConfigRegistry.getConfiguration(SqlDistributedConfiguration.KEY),
                nodeCfgMgr.configurationRegistry().getConfiguration(SqlLocalConfiguration.KEY),
                transactionInflights,
                txManager,
                threadPoolsManager.commonScheduler()
        );

        sqlRef.set(new IgniteSqlImpl(qryEngine, new HybridTimestampTracker()));

        // Preparing the result map.

        components.add(vault);
        components.add(nodeCfgMgr);

        // Start.

        assertThat(vault.startAsync(new ComponentContext()), willCompleteSuccessfully());
        vault.putName(name);

        assertThat(nodeCfgMgr.startAsync(new ComponentContext()), willCompleteSuccessfully());

        // Start the remaining components.
        List<IgniteComponent> otherComponents = List.of(
                threadPoolsManager,
                failureProcessor,
                workerRegistry,
                clusterStateStorage,
                clusterIdService,
                nettyBootstrapFactory,
                nettyWorkersRegistrar,
                clusterSvc,
                partitionsLogStorageFactory,
                cmgLogStorageFactory,
                msLogStorageFactory,
                raftMgr,
                cmgManager,
                replicaMgr,
                txManager,
                resourceVacuumManager,
                lowWatermark,
                metaStorageMgr,
                clusterCfgMgr,
                dataStorageManager,
                clockWaiter,
                catalogManager,
                indexMetaStorage,
                schemaManager,
                distributionZoneManager,
                tableManager,
                indexManager,
                qryEngine,
                sqlRef.get()
        );

        for (IgniteComponent component : otherComponents) {
            // TODO: IGNITE-22119 required to be able to wait on this future.
            component.startAsync(new ComponentContext());

            components.add(component);
        }

        lowWatermark.scheduleUpdates();

        PartialNode partialNode = partialNode(
                name,
                nodeCfgMgr,
                clusterCfgMgr,
                metaStorageMgr,
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
    public void testQueryCorrectnessAfterNodeRestart() {
        IgniteImpl ignite1 = startNode(0);

        createTableWithoutData(ignite1, TABLE_NAME, 2, 1);

        IgniteImpl ignite2 = startNode(1);

        String sql = "SELECT id FROM " + TABLE_NAME + " WHERE id > 0 ORDER BY id";

        int intRes;

        IgniteSql sql1 = ignite1.sql();
        IgniteSql sql2 = ignite2.sql();

        createTableWithData(List.of(ignite1), TABLE_NAME, 2, 1);

        sql1.execute(null, "CREATE INDEX idx1 ON " + TABLE_NAME + "(id)");

        ResultSet<SqlRow> plan = sql1.execute(null, "EXPLAIN PLAN FOR " + sql);

        String planStr = plan.next().stringValue(0);

        assertTrue(planStr.contains("IndexScan"));

        ResultSet<SqlRow> res1 = sql1.execute(null, sql);

        ResultSet<SqlRow> res2 = sql2.execute(null, sql);

        intRes = res1.next().intValue(0);

        assertEquals(intRes, res2.next().intValue(0));

        res1.close();

        res2.close();

        stopNode(0);

        ignite1 = startNode(0);

        sql1 = ignite1.sql();

        ResultSet<SqlRow> res3 = sql1.execute(null, sql);

        assertEquals(intRes, res3.next().intValue(0));
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
    public void nodeWithDataTest() {
        IgniteImpl ignite = startNode(0);

        createTableWithData(List.of(ignite), TABLE_NAME, 1);

        stopNode(0);

        ignite = startNode(0);

        checkTableWithData(ignite, TABLE_NAME);
    }

    @Test
    public void testNodeSeesItselfInLocalLogicalTopology() {
        List<IgniteImpl> nodes = startNodes(3);

        // Here we check that node sees itself in local logical topology.
        nodes.forEach(node -> assertTrue(node.logicalTopologyService().localLogicalTopology().nodes().stream().map(LogicalNode::id)
                .collect(toSet()).contains(node.id())));

        // Actually we have stronger guarantees because of awaiting all nodes to start inside startNodes.
        // On one node (cmg leader) we will see all three nodes in local logical topology.
        // On the node that started second we will see at least two nodes.
        // On the third node we will see all three nodes.
        // All in all that means that in total we will see at least (3 + 2 + 3) nodes.
        Integer sumOfLogicalTopologyProjectionSizes =
                nodes.stream().map(node -> node.logicalTopologyService().localLogicalTopology().nodes().size())
                        .reduce(0, Integer::sum);

        assertTrue(sumOfLogicalTopologyProjectionSizes >= 3 + 2 + 3);
    }

    /**
     * Restarts the node which stores some data.
     */
    @ParameterizedTest
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

    /**
     * Checks that logical topology version is maintained after nodes restart.
     */
    @Test
    public void logicalTopologyVersionMaintainedTest() {
        IgniteImpl main = startNodes(3).get(0);

        stopNode(1);
        IgniteImpl restarted = startNode(1);

        stopNode(2);
        IgniteImpl secondRestarted = startNode(2);

        long mainVersion = main.logicalTopologyService().localLogicalTopology().version();
        long restartedVersion = restarted.logicalTopologyService().localLogicalTopology().version();
        long secondRestartedVersion = secondRestarted.logicalTopologyService().localLogicalTopology().version();

        assertEquals(mainVersion, restartedVersion);
        assertEquals(mainVersion, secondRestartedVersion);
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

        TableViewInternal table = unwrapTableViewInternal(ignite.tables().table(TABLE_NAME));

        InternalTableImpl internalTable = (InternalTableImpl) table.internalTable();

        CompletableFuture[] flushFuts = new CompletableFuture[partitions];

        for (int i = 0; i < partitions; i++) {
            int finalI = i;
            // Flush data on disk, so that we will have a snapshot to read on restart.
            flushFuts[i] = bypassingThreadAssertionsAsync(() -> internalTable.storage().getMvPartition(finalI).flush());
        }

        assertThat(CompletableFuture.allOf(flushFuts), willCompleteSuccessfully());

        // Add more data, so that on restart there will be a index rebuilding operation.
        for (int i = 0; i < 100; i++) {
            ignite.sql().execute(null, "INSERT INTO " + TABLE_NAME + "(id, name) VALUES (?, ?)",
                    i + 500, VALUE_PRODUCER.apply(i + 500));
        }

        stopNode(0);

        ignite = startNode(0);

        checkTableWithData(ignite, TABLE_NAME);

        TableViewInternal tableAfterRestart = unwrapTableViewInternal(ignite.tables().table(TABLE_NAME));

        // Check data that was added after flush.
        bypassingThreadAssertions(() -> {
            for (int i = 0; i < 100; i++) {
                Tuple row = tableAfterRestart.keyValueView().get(null, Tuple.create().set("id", i + 500));

                Objects.requireNonNull(row, "row");

                assertEquals(VALUE_PRODUCER.apply(i + 500), row.stringValue("name"));
            }
        });
    }

    /**
     * Starts two nodes and checks that the data are storing through restarts. Nodes restart in the same order when they started at first.
     */
    @Test
    public void testTwoNodesRestartDirect() {
        twoNodesRestart(true);
    }

    /**
     * Starts two nodes and checks that the data are storing through restarts. Nodes restart in reverse order when they started at first.
     */
    @Test
    public void testTwoNodesRestartReverse() {
        twoNodesRestart(false);
    }

    /**
     * Starts two nodes and checks that the data are storing through restarts.
     *
     * @param directOrder When the parameter is true, nodes restart in direct order, otherwise they restart in reverse order.
     */
    private void twoNodesRestart(boolean directOrder) {
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
            IgniteServer node = startEmbeddedNode(1, null);

            startNode(0);

            assertThat(node.waitForInitAsync(), willCompleteSuccessfully());

            ignite = node.api();
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
        Collection<TableImpl> tables = tableManager.startedTables().values();

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
    public void testOneNodeRestartWithGap() throws InterruptedException {
        IgniteImpl ignite = startNode(0);

        IgniteImpl ignite1 = startNode(1);

        createTableWithData(List.of(ignite), TABLE_NAME, 2, 1);

        TableImpl table = unwrapTableImpl(ignite.tables().table(TABLE_NAME));

        assertNotNull(table);

        ReplicationGroupId groupId = new TablePartitionId(table.tableId(), 0);

        CompletableFuture<ReplicaMeta> primaryFut =
                ignite.placementDriver().awaitPrimaryReplica(groupId, ignite.clock().now(), 30, TimeUnit.SECONDS);

        assertThat(
                primaryFut,
                willCompleteSuccessfully()
        );

        String leaseholderId = primaryFut.join().getLeaseholderId();

        if (!ignite1.id().equals(leaseholderId)) {
            transferPrimary(List.of(ignite, ignite1), groupId, ignite1.name());
        }

        log.info("Test: stopping the node.");

        stopNode(1);

        assertThrowsWithCause(() -> table.keyValueView().get(null, Tuple.create().set("id", 0)), PrimaryReplicaAwaitTimeoutException.class);

        createTableWithoutData(ignite, TABLE_NAME_2, 1, 1);

        ignite1 = startNode(1);

        TableManager tableManager = unwrapTableManager(ignite1.tables());

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

        TableManager tableManager = unwrapTableManager(node.tables());

        assertNotNull(tableManager);

        assertTablePresent(tableManager, TABLE_NAME.toUpperCase());
    }

    /**
     * Checks that a cluster is able to restart when some changes were made in configuration.
     */
    @Test
    public void testRestartDiffConfig() {
        List<IgniteImpl> ignites = startNodes(2);

        createTableWithData(ignites, TABLE_NAME, 2);
        createTableWithData(ignites, TABLE_NAME_2, 2);

        stopNode(0);
        stopNode(1);

        startNode(0);

        @Language("HOCON") String cfgString = IgniteStringFormatter.format(NODE_BOOTSTRAP_CFG,
                DEFAULT_NODE_PORT + 11,
                "[\"localhost:" + DEFAULT_NODE_PORT + "\"]",
                DEFAULT_CLIENT_PORT + 11,
                DEFAULT_HTTP_PORT + 11,
                DEFAULT_HTTPS_PORT + 11,
                "{}"
        );

        IgniteImpl node1 = startNode(1, cfgString);

        TableManager tableManager = unwrapTableManager(node1.tables());

        assertTablePresent(tableManager, TABLE_NAME.toUpperCase());
    }

    /**
     * The test for node restart when there is a gap between the node local configuration and distributed configuration.
     */
    @Test
    public void testCfgGapWithoutData() {
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

        TableManager tableManager = unwrapTableManager(node.tables());

        assertTablePresent(tableManager, TABLE_NAME.toUpperCase());
        assertTablePresent(tableManager, TABLE_NAME_2.toUpperCase());
    }

    /**
     * The test for node restart when there is a gap between the node local configuration and distributed configuration, and metastorage
     * group stops for some time while restarting node is being recovered. The recovery process should continue and eventually succeed after
     * metastorage group starts again.
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-18919")
    public void testMetastorageStop() throws NodeStoppingException {
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
                configurationString(nodes.size() - 1)
        // TODO IGNITE-18919 here the revision callback was removed, because meta storage recovery process was changed.
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
    public void testCfgGap() {
        List<IgniteImpl> nodes = startNodes(4);

        createTableWithData(nodes, "t1", nodes.size(), 1);

        log.info("Stopping the node.");

        stopNode(nodes.size() - 1);

        nodes.set(nodes.size() - 1, null);

        checkTableWithData(nodes.get(0), "t1");

        createTableWithData(nodes, "t2", nodes.size(), 1);

        log.info("Starting the node.");

        IgniteImpl newNode = startNode(nodes.size() - 1);

        checkTableWithData(nodes.get(0), "t1");
        checkTableWithData(nodes.get(0), "t2");

        checkTableWithDataOnSpecificNode(newNode, "t1", 100, 0);
        checkTableWithDataOnSpecificNode(newNode, "t2", 100, 0);
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
        int defaultValue = gcConfiguration.batchSize().value();
        CompletableFuture<Void> update = gcConfiguration.batchSize().update(defaultValue);
        assertThat(update, willCompleteSuccessfully());

        stopNode(0);

        startNodes(3);
    }

    @Test
    public void destroyObsoleteStoragesOnRestart() throws InterruptedException {
        int nodesCount = 3;
        List<IgniteImpl> nodes = startNodes(nodesCount);

        int partitions = nodesCount;
        int replicas = nodesCount;
        createTableWithData(nodes, TABLE_NAME, replicas, partitions);

        int restartedNodeIndex = nodesCount - 1;

        WatchListenerInhibitor inhibitor = WatchListenerInhibitor.metastorageEventsInhibitor(nodes.get(restartedNodeIndex));

        inhibitor.startInhibit();

        // Change the zone - one replica per partition.
        alterZone(nodes.get(0).catalogManager(), String.format("ZONE_%s", TABLE_NAME.toUpperCase()), 1);

        stopNode(restartedNodeIndex);

        inhibitor.stopInhibit();

        IgniteImpl restartedNode = startNode(restartedNodeIndex);

        TableImpl table = unwrapTableImpl(restartedNode.tables().table(TABLE_NAME));

        boolean success = waitForCondition(() -> {
            // Check that only storage for 1 partition left on the restarted node.
            return IntStream.range(0, partitions)
                    .mapToObj(i -> table.internalTable().storage().getMvPartition(i))
                    .filter(Objects::nonNull)
                    .count() == 1;
        }, 10_000);

        assertTrue(success);
    }

    @Test
    public void testCorrectPartitionRecoveryOnSnapshot() throws InterruptedException {
        int nodesCount = 3;
        List<IgniteImpl> nodes = startNodes(nodesCount);

        int partitions = nodesCount;
        int replicas = nodesCount;
        createTableWithData(nodes, TABLE_NAME, replicas, partitions);

        int restartedNodeIndex = nodesCount - 1;

        WatchListenerInhibitor inhibitor = WatchListenerInhibitor.metastorageEventsInhibitor(nodes.get(restartedNodeIndex));

        inhibitor.startInhibit();

        alterZone(nodes.get(0).catalogManager(), String.format("ZONE_%s", TABLE_NAME.toUpperCase()), 1);

        stopNode(restartedNodeIndex);

        inhibitor.stopInhibit();

        forceSnapshotUsageOnRestart(nodes.get(0));

        IgniteImpl restartedNode = startNode(restartedNodeIndex);

        TableImpl table = unwrapTableImpl(restartedNode.tables().table(TABLE_NAME));

        long recoveryRevision = restartedNode.metaStorageManager().recoveryFinishedFuture().join();

        PeersAndLearners configuration = PeersAndLearners.fromConsistentIds(nodes.stream().map(IgniteImpl::name)
                .collect(toSet()), Set.of());

        for (int p = 0; p < partitions; p++) {
            TablePartitionId tablePartitionId = new TablePartitionId(table.tableId(), p);

            Entry e = restartedNode.metaStorageManager().getLocally(stablePartAssignmentsKey(tablePartitionId), recoveryRevision);

            Set<Assignment> assignment = Assignments.fromBytes(e.value()).nodes();

            boolean shouldBe = assignment.stream().anyMatch(n -> n.consistentId().equals(restartedNode.name()));

            Peer peer = configuration.peer(restartedNode.name());

            boolean isStarted = restartedNode.raftManager().isStarted(new RaftNodeId(tablePartitionId, peer));

            assertEquals(shouldBe, isStarted);
        }
    }

    /**
     * Checks that after create table call there is the same value of stable assignments in every node's meta storage, and it was
     * changed only once.
     *
     * @param populateStableAssignmentsBeforeTableCreation Whether to populate stable assignments before table creation.
     * @param restart Whether to restart one of the nodes while creating the table.
     * @throws InterruptedException If interrupted.
     */
    @ParameterizedTest
    @CsvSource({
            "true,true",
            "true,false",
            "false,true",
            "false,false"
    })
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-22251")
    public void createTableCallOnMultipleNodesTest(boolean populateStableAssignmentsBeforeTableCreation, boolean restart)
            throws InterruptedException {
        int nodesCount = 3;

        var nodes = startNodes(3);

        var node = nodes.get(0);

        Map<Integer, AtomicInteger> stableAssignmentsChangeCounters = new ConcurrentHashMap<>();
        Map<Integer, AtomicBoolean> lateChangeFlag = new ConcurrentHashMap<>();

        // Prefix that will be updated after the table creation.
        String testPrefix = "testPrefix";

        for (int i = 0; i < nodesCount; i++) {
            stableAssignmentsChangeCounters.put(i, new AtomicInteger());
            lateChangeFlag.put(i, new AtomicBoolean());

            final int fi = i;
            createWatchListener(
                    nodes.get(i).metaStorageManager(),
                    STABLE_ASSIGNMENTS_PREFIX,
                    e -> stableAssignmentsChangeCounters.get(fi).incrementAndGet()
            );

            createWatchListener(
                    nodes.get(i).metaStorageManager(),
                    testPrefix,
                    e -> lateChangeFlag.get(fi).set(true)
            );
        }

        var partId = new TablePartitionId(TABLE_ID, 0);

        // Populate the stable assignments before calling table create, if needed.
        if (populateStableAssignmentsBeforeTableCreation) {
            int catalogVersion = node.catalogManager().latestCatalogVersion();
            long timestamp = node.catalogManager().catalog(catalogVersion).time();

            node.metaStorageManager().put(
                    stablePartAssignmentsKey(partId),
                    Assignments.toBytes(Set.of(Assignment.forPeer(node.name())), timestamp)
            );

            waitForCondition(() -> lateChangeFlag.values().stream().allMatch(AtomicBoolean::get), 5_000);

            lateChangeFlag.values().forEach(v -> v.set(false));
        }

        String zoneName = "TEST_ZONE";

        if (restart) {
            stopNode(nodesCount - 2);
        }

        IgniteSql sql = node.sql();

        sql.execute(null, String.format("CREATE ZONE IF NOT EXISTS %s WITH REPLICAS=%d, PARTITIONS=%d, STORAGE_PROFILES='%s'",
                zoneName, nodesCount, 1, DEFAULT_STORAGE_PROFILE));

        sql.execute(null, "CREATE TABLE " + TABLE_NAME
                + "(id INT PRIMARY KEY, name VARCHAR) WITH PRIMARY_ZONE='" + zoneName + "';");

        assertEquals(TABLE_ID, tableId(node, TABLE_NAME));

        node.metaStorageManager().put(new ByteArray(testPrefix.getBytes(StandardCharsets.UTF_8)), new byte[0]);

        if (restart) {
            IgniteImpl restartedNode = startNode(nodesCount - 2);
            nodes.set(nodesCount - 2, restartedNode);
        }

        // Waiting for late prefix on all nodes.
        waitForCondition(() -> lateChangeFlag.values().stream().allMatch(AtomicBoolean::get), 5_000);

        var assignmentsKey = stablePartAssignmentsKey(partId).bytes();

        Set<Assignment> expectedAssignments = getAssignmentsFromMetaStorage(node.metaStorageManager(), assignmentsKey);

        // Checking that the stable assignments are same and were changed only once on every node.
        for (int i = 0; i < nodesCount; i++) {
            if (!restart) {
                // TODO IGNITE-21194 return this check
                assertEquals(1, stableAssignmentsChangeCounters.get(i).get(), "node index=" + i);
            }

            assertEquals(
                    expectedAssignments,
                    getAssignmentsFromMetaStorage(nodes.get(i).metaStorageManager(), assignmentsKey),
                    "node index=" + i
            );
        }
    }

    private void createWatchListener(MetaStorageManager metaStorageManager, String prefix, Consumer<WatchEvent> listener) {
        metaStorageManager.registerPrefixWatch(
                new ByteArray(prefix.getBytes(StandardCharsets.UTF_8)),
                new WatchListener() {
                    @Override
                    public CompletableFuture<Void> onUpdate(WatchEvent event) {
                        listener.accept(event);

                        return nullCompletedFuture();
                    }

                    @Override
                    public void onError(Throwable e) {
                        log.error("Error in test watch listener", e);
                    }
                }
        );
    }

    /**
     * Creates the table on a cluster of 3 nodes, delays the table start processing, stops 2 nodes before the assignments are applied
     * to meta storage. The remaining node is hanging, so it doesn't write the updates to meta storage as well. Then the stopped nodes
     * are restarted, and receive different value of data nodes from distribution zones manager. We check that the calculated assignments
     * for the table are eventually equal on every node's local meta storages, and they are equal to expected - to the assignments of
     * the first node that is able to initialize them during the table start.
     *
     * @param nodeThatWrittenAssignments The index of the restarted node the has written the calculated assignments successfully.
     * @param nodeThatPicksUpAssignments The index of the restarted node that picks up assignments from meta storage.
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @CsvSource({
            "1,2",
            "2,1"
    })
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-22251")
    public void tableRecoveryOnMultipleRestartingNodes(int nodeThatWrittenAssignments, int nodeThatPicksUpAssignments) throws Exception {
        var node0 = startNode(0);

        int idx1 = 1;
        int idx2 = 2;

        var node1 = startPartialNode(idx1, configurationString(idx1));
        var node2 = startPartialNode(idx2, configurationString(idx2));

        String tableName = "TEST";
        String zoneName = "ZONE_TEST";

        var assignmentsKey = stablePartAssignmentsKey(new TablePartitionId(TABLE_ID, 0));

        var metaStorageInterceptorFut = new CompletableFuture<>();
        var metaStorageInterceptorInnerFut = new CompletableFuture<>();

        metaStorageInvokeInterceptorByNode.put(nodeThatPicksUpAssignments,
                (cond, success, failure) -> {
                    if (checkMetaStorageInvoke(success, assignmentsKey)) {
                        metaStorageInterceptorInnerFut.complete(null);

                        metaStorageInterceptorFut.join();
                    }

                    return null;
                }
        );

        MetaStorageManager msManager1 = findComponent(node1.startedComponents(), MetaStorageManager.class);
        MetaStorageManager msManager2 = findComponent(node2.startedComponents(), MetaStorageManager.class);

        WatchListenerInhibitor nodeInhibitor0 = WatchListenerInhibitor.metastorageEventsInhibitor(node0.metaStorageManager());
        WatchListenerInhibitor nodeInhibitor1 = WatchListenerInhibitor.metastorageEventsInhibitor(msManager1);
        WatchListenerInhibitor nodeInhibitor2 = WatchListenerInhibitor.metastorageEventsInhibitor(msManager2);

        // Create table, all nodes are lagging.
        IgniteSql sql = node0.sql();

        sql.execute(null, String.format("CREATE ZONE IF NOT EXISTS %s WITH REPLICAS=%d, PARTITIONS=%d, STORAGE_PROFILES='%s'",
                zoneName, 2, 1, DEFAULT_STORAGE_PROFILE));

        nodeInhibitor0.startInhibit();
        nodeInhibitor1.startInhibit();
        nodeInhibitor2.startInhibit();

        sql.executeAsync(null, "CREATE TABLE " + tableName
                + "(id INT PRIMARY KEY, name VARCHAR) WITH PRIMARY_ZONE='" + zoneName + "';");

        // Stopping 2 of 3 nodes.
        node1.stop();
        node2.stop();

        Set<String> dataNodesOnNode1 = Set.of(node0.name(), node1.name());
        Set<String> dataNodesOnNode2 = Set.of(node1.name(), node2.name());

        dataNodesMockByNode.put(idx1, () -> completedFuture(dataNodesOnNode1));
        dataNodesMockByNode.put(idx2, () -> completedFuture(dataNodesOnNode2));

        AtomicReference<PartialNode> nodeWnRef = new AtomicReference<>();
        AtomicReference<PartialNode> nodePnRef = new AtomicReference<>();

        // Restarting 2 nodes.
        var nodePnFut = runAsync(() ->
                nodePnRef.set(startPartialNode(nodeThatPicksUpAssignments, configurationString(nodeThatPicksUpAssignments))));

        assertThat(metaStorageInterceptorInnerFut, willCompleteSuccessfully());

        var nodeWnFut = runAsync(() ->
                nodeWnRef.set(startPartialNode(nodeThatWrittenAssignments, configurationString(nodeThatWrittenAssignments))));

        assertThat(nodeWnFut, willCompleteSuccessfully());

        var msManagerRestartedW = findComponent(nodeWnRef.get().startedComponents(), MetaStorageManager.class);
        waitForValueInLocalMs(msManagerRestartedW, assignmentsKey);

        metaStorageInterceptorFut.complete(null);

        assertThat(nodePnFut, willCompleteSuccessfully());

        var msManagerRestartedP = findComponent(nodePnRef.get().startedComponents(), MetaStorageManager.class);

        waitForValueInLocalMs(msManagerRestartedP, assignmentsKey);

        // Node 0 stops hanging.
        nodeInhibitor0.stopInhibit();
        waitForValueInLocalMs(node0.metaStorageManager(), assignmentsKey);

        assertEquals(TABLE_ID, tableId(node0, tableName));

        Set<Assignment> expectedAssignments = dataNodesMockByNode.get(nodeThatWrittenAssignments).get().join()
                .stream().map(Assignment::forPeer).collect(toSet());

        checkAssignmentsInMetaStorage(node0.metaStorageManager(), assignmentsKey.bytes(), expectedAssignments);
        checkAssignmentsInMetaStorage(msManagerRestartedW, assignmentsKey.bytes(), expectedAssignments);
        checkAssignmentsInMetaStorage(msManagerRestartedP, assignmentsKey.bytes(), expectedAssignments);
    }

    @Test
    public void testSequentialAsyncTableCreationThenAlterZoneThenRestartOnMsSnapshot() throws InterruptedException {
        IgniteImpl node0 = startNode(0);
        IgniteImpl node1 = startNode(1);

        String tableName = "TEST";
        String zoneName = "ZONE_TEST";

        node0.sql().execute(null,
                String.format("CREATE ZONE IF NOT EXISTS %s WITH REPLICAS=%d, PARTITIONS=%d, STORAGE_PROFILES='%s'",
                        zoneName, 2, 1, DEFAULT_STORAGE_PROFILE));

        int catalogVersionBeforeTable = node0.catalogManager().latestCatalogVersion();

        WatchListenerInhibitor nodeInhibitor0 = WatchListenerInhibitor.metastorageEventsInhibitor(node0);
        WatchListenerInhibitor nodeInhibitor1 = WatchListenerInhibitor.metastorageEventsInhibitor(node1);

        nodeInhibitor0.startInhibit();
        nodeInhibitor1.startInhibit();

        CompletableFuture<?> createTableInCatalogFuture = createTableInCatalog(node0.catalogManager(), tableName, zoneName);

        stopNode(1);

        CompletableFuture<?> alterZoneInCatalogFuture = alterZoneInCatalogAsync(node0.catalogManager(), zoneName, 1);

        // Wait for the next catalog version: table creation.
        // The next catalog update (alter zone) can't be processed until the table creation is completed.
        assertTrue(waitForCondition(() -> {
            int ver = latestCatalogVersionInMs(node0.metaStorageManager());

            return ver == catalogVersionBeforeTable + 1;
        }, 10_000));

        // Double check that the version is exactly as expected, if it's not, the synchronous sequential catalog update processing may be
        // broken.
        assertEquals(catalogVersionBeforeTable + 1, latestCatalogVersionInMs(node0.metaStorageManager()));

        Thread.sleep(1000);

        forceSnapshotUsageOnRestart(node0);

        log.info("Restarting node 1.");

        node1 = startNode(1);

        ByteArray assignmentsKey = stablePartAssignmentsKey(new TablePartitionId(tableId(node1, tableName), 0));

        waitForValueInLocalMs(node1.metaStorageManager(), assignmentsKey);

        nodeInhibitor0.stopInhibit();

        assertThat(createTableInCatalogFuture, willCompleteSuccessfully());
        assertThat(alterZoneInCatalogFuture, willCompleteSuccessfully());

        waitForValueInLocalMs(node0.metaStorageManager(), assignmentsKey);

        IgniteImpl finalNode1 = node1;

        // Restart is followed by rebalance, because data nodes are recalculated after full table creation that is completed after restart.
        boolean success = waitForCondition(() -> {
            Set<Assignment> assignments0 = getAssignmentsFromMetaStorage(node0.metaStorageManager(), assignmentsKey.bytes());
            Set<Assignment> assignments1 = getAssignmentsFromMetaStorage(finalNode1.metaStorageManager(), assignmentsKey.bytes());

            return assignments0.size() == 1 && assignments0.equals(assignments1);
        }, 10_000);

        if (!success) {
            log.info("Test: assignment on node0:" + getAssignmentsFromMetaStorage(node0.metaStorageManager(), assignmentsKey.bytes()));
            log.info("Test: assignment on node1:" + getAssignmentsFromMetaStorage(finalNode1.metaStorageManager(), assignmentsKey.bytes()));
        }

        assertTrue(success);
    }

    private int latestCatalogVersionInMs(MetaStorageManager metaStorageManager) {
        var e = metaStorageManager.getLocally(new ByteArray("catalog.version".getBytes(StandardCharsets.UTF_8)), 1000);

        if (e == null || e.empty()) {
            return -1;
        }

        return ByteUtils.bytesToInt(e.value());
    }

    private static CompletableFuture<?> createTableInCatalog(CatalogManager catalogManager, String tableName, String zoneName) {
        var tableColumn = ColumnParams.builder().name("id").type(INT32).build();

        TableHashPrimaryKey primaryKey = TableHashPrimaryKey.builder()
                .columns(List.of("id"))
                .build();

        var createTableCommand = CreateTableCommand.builder()
                .schemaName("PUBLIC")
                .tableName(tableName)
                .columns(List.of(tableColumn))
                .primaryKey(primaryKey)
                .zone(zoneName)
                .build();

        return catalogManager.execute(createTableCommand);
    }

    private static CompletableFuture<?> alterZoneInCatalogAsync(
            CatalogManager catalogManager,
            String zoneName,
            @Nullable Integer replicas
    ) {
        AlterZoneCommandBuilder builder = AlterZoneCommand.builder().zoneName(zoneName);

        builder.replicas(replicas);

        return catalogManager.execute(builder.build());
    }

    private void waitForValueInLocalMs(MetaStorageManager metaStorageManager, ByteArray key) throws InterruptedException {
        assertTrue(waitForCondition(() -> {
            var e = metaStorageManager.getLocally(key, metaStorageManager.appliedRevision());

            return !e.empty();
        }, 10_000));
    }

    private boolean checkMetaStorageInvoke(Collection<Operation> ops, ByteArray key) {
        var k = new String(key.bytes(), StandardCharsets.UTF_8);

        return ops.stream().anyMatch(op -> new String(toByteArray(op.key()), StandardCharsets.UTF_8).equals(k));
    }

    private void checkAssignmentsInMetaStorage(MetaStorageManager metaStorageManager, byte[] assignmentsKey, Set<Assignment> expected) {
        Set<Assignment> a = getAssignmentsFromMetaStorage(metaStorageManager, assignmentsKey);

        assertEquals(expected, a);
    }

    private Set<Assignment> getAssignmentsFromMetaStorage(MetaStorageManager metaStorageManager, byte[] assignmentsKey) {
        var e = metaStorageManager.getLocally(new ByteArray(assignmentsKey), metaStorageManager.appliedRevision());

        return e == null || e.tombstone() || e.empty()
                ? emptySet()
                : Assignments.fromBytes(e.value()).nodes();
    }

    private static int tableId(IgniteImpl node, String tableName) {
        return getTableIdStrict(node.catalogManager(), tableName, node.clock().nowLong());
    }

    /**
     * Checks the table exists and validates all data in the storage of specific node.
     *
     * @param ignite Ignite.
     * @param name Table name.
     * @param recordsCount Total count of records to check.
     * @param partNum Partition to check.
     */
    private void checkTableWithDataOnSpecificNode(Ignite ignite, String name, int recordsCount, int partNum) {
        TableImpl table = unwrapTableImpl(ignite.tables().table(name));
        RecordBinaryViewImpl view = Wrappers.unwrap(table.recordView(), RecordBinaryViewImpl.class);

        AtomicInteger foundRecords = new AtomicInteger();

        Supplier<Boolean> allDataPresent = () -> {
            foundRecords.set(0);

            try (Cursor<ReadResult> cursor = table.internalTable().storage().getMvPartition(partNum).scan(HybridTimestamp.MAX_VALUE)) {
                while (cursor.hasNext()) {
                    ReadResult rr = cursor.next();

                    if (!rr.isWriteIntent()) {
                        Tuple tuple = view.binaryRowToTuple(null, rr.binaryRow()).join();
                        if (tuple == null) {
                            return false;
                        } else {
                            int id = tuple.intValue("id");
                            assertEquals(VALUE_PRODUCER.apply(id), tuple.stringValue("name"));
                            foundRecords.incrementAndGet();
                        }
                    }
                }
            }

            return foundRecords.get() == recordsCount;
        };

        try {
            Awaitility.with()
                    .await()
                    .pollInterval(100, TimeUnit.MILLISECONDS)
                    .pollDelay(0, TimeUnit.MILLISECONDS)
                    .atMost(3, TimeUnit.SECONDS)
                    .until(() -> runInExecutor(storageExecutor, allDataPresent));
        } catch (AssertionError | ConditionTimeoutException e) {
            log.error("Found records in partitions: " + foundRecords.get());

            throw e;
        }
    }

    /**
     * Checks the table exists and validates all data in it. Important: it checks availability of the data, not the presence of
     * the data in a storage of specific node.
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
        IgniteSql sql = nodes.get(0).sql();

        sql.execute(null,
                String.format("CREATE ZONE IF NOT EXISTS ZONE_%s WITH REPLICAS=%d, PARTITIONS=%d, STORAGE_PROFILES='%s'",
                        name, replicas, partitions, DEFAULT_STORAGE_PROFILE));
        sql.execute(null, "CREATE TABLE IF NOT EXISTS " + name
                + "(id INT PRIMARY KEY, name VARCHAR) WITH PRIMARY_ZONE='ZONE_" + name.toUpperCase() + "';");

        for (int i = 0; i < 100; i++) {
            sql.execute(null, "INSERT INTO " + name + "(id, name) VALUES (?, ?)",
                    i, VALUE_PRODUCER.apply(i));
        }
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
        IgniteSql sql = ignite.sql();

        sql.execute(null,
                String.format("CREATE ZONE IF NOT EXISTS ZONE_%s WITH REPLICAS=%d, PARTITIONS=%d, STORAGE_PROFILES='%s'",
                        name, replicas, partitions, DEFAULT_STORAGE_PROFILE));
        sql.execute(null, "CREATE TABLE " + name
                + "(id INT PRIMARY KEY, name VARCHAR) WITH PRIMARY_ZONE='ZONE_" + name.toUpperCase() + "';");

        return ignite.tables().table(name);
    }

    private interface InvokeInterceptor {
        Boolean invoke(Condition condition, Collection<Operation> success, Collection<Operation> failure);
    }
}
