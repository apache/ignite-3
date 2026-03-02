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

package org.apache.ignite.internal.partition.replicator.fixtures;

import static java.util.Collections.reverse;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.BaseIgniteRestartTest.createVault;
import static org.apache.ignite.internal.configuration.IgnitePaths.partitionsPath;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.pendingPartAssignmentsQueueKey;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.stopAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.internal.util.MockUtil.isMock;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.LongSupplier;
import org.apache.ignite.internal.app.NodePropertiesImpl;
import org.apache.ignite.internal.app.ThreadPoolsManager;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogManagerImpl;
import org.apache.ignite.internal.catalog.PartitionCountProvider;
import org.apache.ignite.internal.catalog.compaction.CatalogCompactionRunner;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.storage.UpdateLogImpl;
import org.apache.ignite.internal.cluster.management.ClusterIdHolder;
import org.apache.ignite.internal.cluster.management.ClusterInitializer;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.NodeAttributesCollector;
import org.apache.ignite.internal.cluster.management.configuration.NodeAttributesConfiguration;
import org.apache.ignite.internal.cluster.management.raft.TestClusterStateStorage;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyServiceImpl;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.configuration.ClusterConfiguration;
import org.apache.ignite.internal.configuration.ComponentWorkingDir;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.NodeConfiguration;
import org.apache.ignite.internal.configuration.RaftGroupOptionsConfigHelper;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.configuration.SystemDistributedExtensionConfiguration;
import org.apache.ignite.internal.configuration.SystemDistributedExtensionConfigurationSchema;
import org.apache.ignite.internal.configuration.SystemLocalConfiguration;
import org.apache.ignite.internal.configuration.SystemLocalExtensionConfigurationSchema;
import org.apache.ignite.internal.configuration.storage.DistributedConfigurationStorage;
import org.apache.ignite.internal.configuration.storage.LocalFileConfigurationStorage;
import org.apache.ignite.internal.configuration.validation.TestConfigurationValidator;
import org.apache.ignite.internal.disaster.system.SystemDisasterRecoveryStorage;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.distributionzones.rebalance.RebalanceMinimumRequiredTimeProviderImpl;
import org.apache.ignite.internal.eventlog.api.EventLog;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.failure.NoOpFailureManager;
import org.apache.ignite.internal.hlc.ClockServiceImpl;
import org.apache.ignite.internal.hlc.ClockWaiter;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.index.IndexBuildingManager;
import org.apache.ignite.internal.index.IndexManager;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.lowwatermark.LowWatermarkImpl;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.metastorage.impl.MetaStorageRevisionListenerRegistry;
import org.apache.ignite.internal.metastorage.server.ReadOperationForCompactionTracker;
import org.apache.ignite.internal.metastorage.server.persistence.RocksDbKeyValueStorage;
import org.apache.ignite.internal.metastorage.server.raft.MetastorageGroupId;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.metrics.NoOpMetricManager;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.NodeFinder;
import org.apache.ignite.internal.network.configuration.MulticastNodeFinderConfigurationSchema;
import org.apache.ignite.internal.network.configuration.NetworkExtensionConfigurationSchema;
import org.apache.ignite.internal.network.configuration.StaticNodeFinderConfigurationSchema;
import org.apache.ignite.internal.network.recovery.InMemoryStaleIds;
import org.apache.ignite.internal.network.utils.ClusterServiceTestUtils;
import org.apache.ignite.internal.partition.replicator.PartitionReplicaLifecycleManager;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessageGroup;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.partition.replicator.schema.CatalogValidationSchemasSource;
import org.apache.ignite.internal.partition.replicator.schema.ValidationSchemasSource;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.placementdriver.PlacementDriverManager;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.RaftGroupOptionsConfigurer;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.storage.LogStorageManager;
import org.apache.ignite.internal.raft.storage.impl.VolatileLogStorageManagerCreator;
import org.apache.ignite.internal.raft.util.SharedLogStorageManagerUtils;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.VersionedAssignments;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.replicator.configuration.ReplicationExtensionConfigurationSchema;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.SchemaSafeTimeTrackerImpl;
import org.apache.ignite.internal.schema.SchemaSyncService;
import org.apache.ignite.internal.schema.configuration.GcConfiguration;
import org.apache.ignite.internal.schema.configuration.GcExtensionConfigurationSchema;
import org.apache.ignite.internal.sql.api.IgniteSqlImpl;
import org.apache.ignite.internal.sql.api.PublicApiThreadingIgniteSql;
import org.apache.ignite.internal.sql.configuration.distributed.SqlDistributedConfiguration;
import org.apache.ignite.internal.sql.configuration.local.SqlLocalConfiguration;
import org.apache.ignite.internal.sql.engine.SqlQueryProcessor;
import org.apache.ignite.internal.sql.engine.exec.kill.KillCommandHandler;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.storage.DataStorageModules;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.storage.configurations.StorageExtensionConfigurationSchema;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryDataStorageModule;
import org.apache.ignite.internal.storage.pagememory.VolatilePageMemoryDataStorageModule;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryProfileConfigurationSchema;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryStorageEngineExtensionConfigurationSchema;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.VolatilePageMemoryProfileConfigurationSchema;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.VolatilePageMemoryStorageEngineExtensionConfigurationSchema;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbProfileConfigurationSchema;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbStorageEngineExtensionConfigurationSchema;
import org.apache.ignite.internal.systemview.SystemViewManagerImpl;
import org.apache.ignite.internal.systemview.api.SystemViewManager;
import org.apache.ignite.internal.table.StreamerReceiverRunner;
import org.apache.ignite.internal.table.TableTestUtils;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.distributed.index.IndexMetaStorage;
import org.apache.ignite.internal.table.distributed.raft.MinimumRequiredTimeCollectorService;
import org.apache.ignite.internal.table.distributed.raft.MinimumRequiredTimeCollectorServiceImpl;
import org.apache.ignite.internal.table.distributed.raft.PartitionSafeTimeValidator;
import org.apache.ignite.internal.table.distributed.schema.CheckCatalogVersionOnActionRequest;
import org.apache.ignite.internal.table.distributed.schema.CheckCatalogVersionOnAppendEntries;
import org.apache.ignite.internal.table.distributed.schema.SchemaSyncServiceImpl;
import org.apache.ignite.internal.table.distributed.schema.ThreadLocalPartitionCommandsMarshaller;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.IgniteTransactionsImpl;
import org.apache.ignite.internal.tx.impl.PublicApiThreadingIgniteTransactions;
import org.apache.ignite.internal.tx.impl.RemotelyTriggeredResourceRegistry;
import org.apache.ignite.internal.tx.impl.ResourceVacuumManager;
import org.apache.ignite.internal.tx.impl.TransactionIdGenerator;
import org.apache.ignite.internal.tx.impl.TransactionInflights;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.impl.VolatileTxStateMetaStorage;
import org.apache.ignite.internal.tx.message.TxMessageGroup;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.apache.ignite.internal.tx.storage.state.rocksdb.TxStateRocksDbSharedStorage;
import org.apache.ignite.internal.tx.test.TestLocalRwTxCounter;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.tx.IgniteTransactions;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.TestInfo;

/**
 * Fake node used in colocation tests.
 */
public class Node {
    private static final IgniteLogger LOG = Loggers.forClass(Node.class);

    private static final int DELAY_DURATION_MS = 100;

    /** The interval between two consecutive MS idle safe time syncs. */
    public static final int METASTORAGE_IDLE_SYNC_TIME_INTERVAL_MS = DELAY_DURATION_MS / 2;

    public final String name;

    public final Loza raftManager;

    private final ThreadPoolsManager threadPoolsManager;

    private final VolatileLogStorageManagerCreator volatileLogStorageManagerCreator;

    public final ReplicaManager replicaManager;

    public final MetaStorageManagerImpl metaStorageManager;

    private final VaultManager vaultManager;

    private final NodePropertiesImpl nodeProperties;

    public final ClusterService clusterService;

    private final LockManager lockManager;

    private final TxManager txManager;

    private final DataStorageManager dataStorageMgr;

    private final TxStateRocksDbSharedStorage sharedTxStateStorage;

    public final TableManager tableManager;

    public final DistributionZoneManager distributionZoneManager;

    private final ConfigurationRegistry nodeConfigRegistry;

    private final ConfigurationRegistry clusterConfigRegistry;

    public final ClusterManagementGroupManager cmgManager;

    private final SchemaManager schemaManager;

    private final SchemaSafeTimeTrackerImpl schemaSafeTimeTracker;

    public final CatalogManager catalogManager;

    public final PartitionReplicaLifecycleManager partitionReplicaLifecycleManager;

    private final SchemaSyncService schemaSyncService;

    private final ClockWaiter clockWaiter;

    private final List<IgniteComponent> nodeComponents = new CopyOnWriteArrayList<>();

    private final ConfigurationTreeGenerator nodeCfgGenerator;

    private final ConfigurationTreeGenerator clusterCfgGenerator;

    public final LowWatermarkImpl lowWatermark;

    public final RemotelyTriggeredResourceRegistry resourcesRegistry;

    private final OutgoingSnapshotsManager outgoingSnapshotsManager;

    /** Cleanup manager for tx resources. */
    private final ResourceVacuumManager resourceVacuumManager;

    /** Hybrid clock. */
    public final HybridClock hybridClock = new HybridClockImpl();

    /** Index manager. */
    private final IndexManager indexManager;

    /** Failure processor. */
    private final FailureManager failureManager;

    private final LogStorageManager partitionsLogStorageManager;

    private final LogStorageManager msLogStorageManager;

    private final LogStorageManager cmgLogStorageManager;

    private final IndexMetaStorage indexMetaStorage;

    private final HybridTimestampTracker observableTimestampTracker = HybridTimestampTracker.atomicTracker(null);

    public final PlacementDriverManager placementDriverManager;

    @Nullable
    private volatile InvokeInterceptor invokeInterceptor;

    private final CatalogCompactionRunner catalogCompactionRunner;

    private final SystemViewManager systemViewManager;

    private final SqlQueryProcessor sqlQueryProcessor;

    /** Index building manager. */
    private final IndexBuildingManager indexBuildingManager;

    /** Interceptor for {@link MetaStorageManager#invoke} calls. */
    @FunctionalInterface
    public interface InvokeInterceptor {
        @Nullable
        Boolean invoke(Condition condition, Collection<Operation> success, Collection<Operation> failure);
    }

    /** Constructor. */
    public Node(
            TestInfo testInfo,
            NetworkAddress address,
            NodeFinder nodeFinder,
            Path workDir,
            SystemLocalConfiguration systemLocalConfiguration,
            RaftConfiguration raftConfiguration,
            NodeAttributesConfiguration nodeAttributesConfiguration,
            StorageConfiguration storageConfiguration,
            SystemDistributedConfiguration systemConfiguration,
            ReplicationConfiguration replicationConfiguration,
            TransactionConfiguration transactionConfiguration,
            ScheduledExecutorService scheduledExecutorService,
            @Nullable InvokeInterceptor invokeInterceptor,
            GcConfiguration gcConfiguration,
            SqlLocalConfiguration sqlLocalConfiguration,
            SqlDistributedConfiguration sqlDistributedConfiguration
    ) {
        this.invokeInterceptor = invokeInterceptor;

        name = testNodeName(testInfo, address.port());

        Path dir = workDir.resolve(name);

        vaultManager = createVault(dir);

        nodeProperties = new NodePropertiesImpl(vaultManager);

        nodeCfgGenerator = new ConfigurationTreeGenerator(
                List.of(NodeConfiguration.KEY),
                List.of(
                        NetworkExtensionConfigurationSchema.class,
                        StorageExtensionConfigurationSchema.class,
                        SystemLocalExtensionConfigurationSchema.class,
                        PersistentPageMemoryStorageEngineExtensionConfigurationSchema.class,
                        VolatilePageMemoryStorageEngineExtensionConfigurationSchema.class,
                        RocksDbStorageEngineExtensionConfigurationSchema.class
                ),
                List.of(
                        PersistentPageMemoryProfileConfigurationSchema.class,
                        VolatilePageMemoryProfileConfigurationSchema.class,
                        RocksDbProfileConfigurationSchema.class,
                        StaticNodeFinderConfigurationSchema.class,
                        MulticastNodeFinderConfigurationSchema.class
                )
        );

        Path configPath = dir.resolve("config");
        TestIgnitionManager.writeConfigurationFileApplyingTestDefaults(configPath);

        nodeConfigRegistry = new ConfigurationRegistry(
                List.of(NodeConfiguration.KEY),
                new LocalFileConfigurationStorage(configPath, nodeCfgGenerator, null),
                nodeCfgGenerator,
                new TestConfigurationValidator()
        );

        var clusterIdHolder = new ClusterIdHolder();

        clusterService = ClusterServiceTestUtils.clusterService(
                testInfo,
                address.port(),
                nodeFinder,
                new InMemoryStaleIds(),
                clusterIdHolder
        );

        lockManager = HeapLockManager.smallInstance();

        var raftGroupEventsClientListener = new RaftGroupEventsClientListener();

        ComponentWorkingDir partitionsWorkDir = partitionsPath(systemLocalConfiguration, dir);

        partitionsLogStorageManager = SharedLogStorageManagerUtils.create(clusterService.nodeName(), partitionsWorkDir.raftLogPath());

        LogSyncer partitionsLogSyncer = partitionsLogStorageManager.logSyncer();

        RaftGroupOptionsConfigurer partitionRaftConfigurer =
                RaftGroupOptionsConfigHelper.configureProperties(partitionsLogStorageManager, partitionsWorkDir.metaPath());

        MetricManager metricManager = new NoOpMetricManager();

        raftManager = new Loza(
                clusterService,
                metricManager,
                raftConfiguration,
                systemLocalConfiguration,
                hybridClock,
                raftGroupEventsClientListener,
                new NoOpFailureManager()
        );

        failureManager = new NoOpFailureManager();

        var clusterStateStorage = new TestClusterStateStorage();
        var logicalTopology = new LogicalTopologyImpl(clusterStateStorage, failureManager);

        var clusterInitializer = new ClusterInitializer(
                clusterService,
                hocon -> hocon,
                new TestConfigurationValidator()
        );

        ComponentWorkingDir cmgWorkDir = new ComponentWorkingDir(dir.resolve("cmg"));

        cmgLogStorageManager =
                SharedLogStorageManagerUtils.create(clusterService.nodeName(), cmgWorkDir.raftLogPath());

        RaftGroupOptionsConfigurer cmgRaftConfigurer =
                RaftGroupOptionsConfigHelper.configureProperties(cmgLogStorageManager, cmgWorkDir.metaPath());

        cmgManager = new ClusterManagementGroupManager(
                vaultManager,
                new SystemDisasterRecoveryStorage(vaultManager),
                clusterService,
                clusterInitializer,
                raftManager,
                clusterStateStorage,
                logicalTopology,
                new NodeAttributesCollector(nodeAttributesConfiguration, storageConfiguration),
                failureManager,
                clusterIdHolder,
                cmgRaftConfigurer,
                metricManager
        );

        LogicalTopologyServiceImpl logicalTopologyService = new LogicalTopologyServiceImpl(logicalTopology, cmgManager);

        var readOperationForCompactionTracker = new ReadOperationForCompactionTracker();

        var keyValueStorage = new RocksDbKeyValueStorage(
                name,
                resolveDir(dir, "metaStorageTestKeyValue"),
                failureManager,
                readOperationForCompactionTracker,
                scheduledExecutorService
        );

        var topologyAwareRaftGroupServiceFactory = new TopologyAwareRaftGroupServiceFactory(
                clusterService,
                logicalTopologyService,
                Loza.FACTORY,
                raftGroupEventsClientListener
        );

        ComponentWorkingDir metastorageWorkDir = new ComponentWorkingDir(dir.resolve("metastorage"));

        msLogStorageManager =
                SharedLogStorageManagerUtils.create(clusterService.nodeName(), metastorageWorkDir.raftLogPath());

        RaftGroupOptionsConfigurer msRaftConfigurer =
                RaftGroupOptionsConfigHelper.configureProperties(msLogStorageManager, metastorageWorkDir.metaPath());

        metaStorageManager = new MetaStorageManagerImpl(
                clusterService,
                cmgManager,
                logicalTopologyService,
                raftManager,
                keyValueStorage,
                hybridClock,
                topologyAwareRaftGroupServiceFactory,
                new NoOpMetricManager(),
                systemConfiguration,
                msRaftConfigurer,
                readOperationForCompactionTracker
        ) {
            @Override
            public CompletableFuture<Boolean> invoke(
                    Condition condition,
                    List<Operation> success,
                    List<Operation> failure
            ) {
                InvokeInterceptor invokeInterceptor = Node.this.invokeInterceptor;

                if (invokeInterceptor != null) {
                    Boolean res = invokeInterceptor.invoke(condition, success, failure);

                    if (res != null) {
                        return completedFuture(res);
                    }
                }

                return super.invoke(condition, success, failure);
            }

            @Override
            public CompletableFuture<Boolean> invoke(Condition condition, Operation success, Operation failure) {
                InvokeInterceptor invokeInterceptor = Node.this.invokeInterceptor;

                if (invokeInterceptor != null) {
                    Boolean res = invokeInterceptor.invoke(condition, List.of(success), List.of(failure));

                    if (res != null) {
                        return completedFuture(res);
                    }
                }

                return super.invoke(condition, success, failure);
            }
        };

        threadPoolsManager = new ThreadPoolsManager(name, metricManager);

        LongSupplier partitionIdleSafeTimePropagationPeriodMsSupplier = () -> 10L;

        clockWaiter = new ClockWaiter(name, hybridClock, threadPoolsManager.commonScheduler());

        var clockService = new ClockServiceImpl(
                hybridClock,
                clockWaiter,
                () -> TestIgnitionManager.DEFAULT_MAX_CLOCK_SKEW_MS,
                skew -> {}
        );

        ReplicaService replicaSvc = new ReplicaService(
                clusterService.messagingService(),
                clockService,
                threadPoolsManager.partitionOperationsExecutor(),
                replicationConfiguration,
                threadPoolsManager.commonScheduler()
        );

        resourcesRegistry = new RemotelyTriggeredResourceRegistry();

        placementDriverManager = new PlacementDriverManager(
                name,
                metaStorageManager,
                MetastorageGroupId.INSTANCE,
                clusterService,
                cmgManager::metaStorageNodes,
                logicalTopologyService,
                raftManager,
                topologyAwareRaftGroupServiceFactory,
                clockService,
                failureManager,
                replicationConfiguration,
                Runnable::run,
                metricManager,
                zoneId -> completedFuture(Set.of())
        );

        VolatileTxStateMetaStorage txStateVolatileStorage = VolatileTxStateMetaStorage.createStarted();

        var transactionInflights = new TransactionInflights(placementDriverManager.placementDriver(), clockService, txStateVolatileStorage);

        var cfgStorage = new DistributedConfigurationStorage("test", metaStorageManager);

        clusterCfgGenerator = new ConfigurationTreeGenerator(
                List.of(ClusterConfiguration.KEY),
                List.of(
                        GcExtensionConfigurationSchema.class,
                        ReplicationExtensionConfigurationSchema.class,
                        SystemDistributedExtensionConfigurationSchema.class
                ),
                List.of()
        );

        clusterConfigRegistry = new ConfigurationRegistry(
                List.of(ClusterConfiguration.KEY),
                cfgStorage,
                clusterCfgGenerator,
                new TestConfigurationValidator()
        );

        var registry = new MetaStorageRevisionListenerRegistry(metaStorageManager);

        DataStorageModules dataStorageModules = new DataStorageModules(List.of(
                new PersistentPageMemoryDataStorageModule(),
                new NonVolatileTestDataStorageModule(),
                new VolatilePageMemoryDataStorageModule()
        ));

        Path storagePath = dir.resolve("storage");

        dataStorageMgr = new DataStorageManager(
                dataStorageModules.createStorageEngines(
                        name,
                        new NoOpMetricManager(),
                        nodeConfigRegistry,
                        dir.resolve("storage"),
                        null,
                        failureManager,
                        partitionsLogSyncer,
                        hybridClock,
                        scheduledExecutorService
                ),
                storageConfiguration
        );

        lowWatermark = new LowWatermarkImpl(
                name,
                gcConfiguration.lowWatermark(),
                clockService,
                vaultManager,
                failureManager,
                clusterService.messagingService()
        );

        txManager = new TxManagerImpl(
                transactionConfiguration,
                systemConfiguration,
                clusterService,
                replicaSvc,
                lockManager,
                txStateVolatileStorage,
                clockService,
                new TransactionIdGenerator(address.port()),
                placementDriverManager.placementDriver(),
                partitionIdleSafeTimePropagationPeriodMsSupplier,
                new TestLocalRwTxCounter(),
                resourcesRegistry,
                transactionInflights,
                lowWatermark,
                threadPoolsManager.commonScheduler(),
                metricManager
        );

        volatileLogStorageManagerCreator = new VolatileLogStorageManagerCreator(name, workDir.resolve("volatile-log-spillout-" + name));

        schemaSafeTimeTracker = new SchemaSafeTimeTrackerImpl(metaStorageManager.clusterTime());
        metaStorageManager.registerNotificationEnqueuedListener(schemaSafeTimeTracker);

        LongSupplier delayDurationMsSupplier = () -> DELAY_DURATION_MS;

        schemaSyncService = new SchemaSyncServiceImpl(schemaSafeTimeTracker, delayDurationMsSupplier);

        catalogManager = new CatalogManagerImpl(
                new UpdateLogImpl(metaStorageManager, failureManager),
                clockService,
                failureManager,
                delayDurationMsSupplier,
                PartitionCountProvider.defaultPartitionCountProvider()
        );

        schemaManager = new SchemaManager(registry, catalogManager);

        ValidationSchemasSource validationSchemasSource = new CatalogValidationSchemasSource(catalogManager, schemaManager);

        replicaManager = new ReplicaManager(
                name,
                clusterService,
                cmgManager,
                groupId -> completedFuture(Assignments.EMPTY),
                clockService,
                Set.of(PartitionReplicationMessageGroup.class, TxMessageGroup.class),
                placementDriverManager.placementDriver(),
                threadPoolsManager.partitionOperationsExecutor(),
                partitionIdleSafeTimePropagationPeriodMsSupplier,
                new NoOpFailureManager(),
                new ThreadLocalPartitionCommandsMarshaller(clusterService.serializationRegistry()),
                new PartitionSafeTimeValidator(validationSchemasSource, catalogManager, schemaSyncService),
                topologyAwareRaftGroupServiceFactory,
                raftManager,
                partitionRaftConfigurer,
                volatileLogStorageManagerCreator,
                threadPoolsManager.tableIoExecutor(),
                replicaGrpId -> metaStorageManager.get(pendingPartAssignmentsQueueKey((ZonePartitionId) replicaGrpId))
                        .thenApply(entry -> new VersionedAssignments(entry.value(), entry.revision())),
                threadPoolsManager.commonScheduler()
        );

        raftManager.appendEntriesRequestInterceptor(new CheckCatalogVersionOnAppendEntries(catalogManager));
        raftManager.actionRequestInterceptor(new CheckCatalogVersionOnActionRequest(catalogManager));

        indexMetaStorage = new IndexMetaStorage(catalogManager, lowWatermark, metaStorageManager);

        MinimumRequiredTimeCollectorService minTimeCollectorService = new MinimumRequiredTimeCollectorServiceImpl();

        catalogCompactionRunner = new CatalogCompactionRunner(
                name,
                (CatalogManagerImpl) catalogManager,
                clusterService.messagingService(),
                logicalTopologyService,
                placementDriverManager.placementDriver(),
                replicaSvc,
                clockService,
                schemaSyncService,
                clusterService.topologyService(),
                lowWatermark,
                clockService::nowLong,
                minTimeCollectorService,
                new RebalanceMinimumRequiredTimeProviderImpl(metaStorageManager, catalogManager));

        metaStorageManager.addElectionListener(catalogCompactionRunner::updateCoordinator);

        SystemDistributedConfiguration systemDistributedConfiguration =
                clusterConfigRegistry.getConfiguration(SystemDistributedExtensionConfiguration.KEY).system();

        distributionZoneManager = new DistributionZoneManager(
                name,
                () -> clusterService.topologyService().localMember().id(),
                registry,
                metaStorageManager,
                logicalTopologyService,
                catalogManager,
                systemDistributedConfiguration,
                clockService,
                metricManager,
                lowWatermark
        );

        sharedTxStateStorage = new TxStateRocksDbSharedStorage(
                name,
                storagePath.resolve("tx-state"),
                threadPoolsManager.commonScheduler(),
                threadPoolsManager.tableIoExecutor(),
                partitionsLogSyncer,
                failureManager
        );

        outgoingSnapshotsManager = new OutgoingSnapshotsManager(name, clusterService.messagingService(), failureManager);

        partitionReplicaLifecycleManager = new PartitionReplicaLifecycleManager(
                catalogManager,
                replicaManager,
                distributionZoneManager,
                metaStorageManager,
                clusterService.topologyService(),
                lowWatermark,
                failureManager,
                threadPoolsManager.tableIoExecutor(),
                threadPoolsManager.rebalanceScheduler(),
                threadPoolsManager.partitionOperationsExecutor(),
                clockService,
                placementDriverManager.placementDriver(),
                schemaSyncService,
                systemDistributedConfiguration,
                sharedTxStateStorage,
                txManager,
                schemaManager,
                dataStorageMgr,
                outgoingSnapshotsManager,
                metricManager,
                clusterService.messagingService(),
                replicaSvc
        );

        resourceVacuumManager = new ResourceVacuumManager(
                name,
                resourcesRegistry,
                clusterService.topologyService(),
                clusterService.messagingService(),
                transactionInflights,
                txManager,
                lowWatermark,
                failureManager,
                metricManager
        );

        tableManager = new TableManager(
                name,
                registry,
                gcConfiguration,
                replicationConfiguration,
                clusterService.messagingService(),
                clusterService.topologyService(),
                lockManager,
                replicaSvc,
                txManager,
                dataStorageMgr,
                metaStorageManager,
                schemaManager,
                validationSchemasSource,
                threadPoolsManager.tableIoExecutor(),
                threadPoolsManager.partitionOperationsExecutor(),
                clockService,
                outgoingSnapshotsManager,
                schemaSyncService,
                catalogManager,
                failureManager,
                observableTimestampTracker,
                placementDriverManager.placementDriver(),
                () -> mock(IgniteSql.class),
                resourcesRegistry,
                lowWatermark,
                transactionInflights,
                indexMetaStorage,
                partitionReplicaLifecycleManager,
                minTimeCollectorService,
                systemDistributedConfiguration,
                metricManager,
                TableTestUtils.NOOP_PARTITION_MODIFICATION_COUNTER_FACTORY
        ) {

            @Override
            protected MvTableStorage createTableStorage(CatalogTableDescriptor tableDescriptor, CatalogZoneDescriptor zoneDescriptor) {
                MvTableStorage storage = createSpy(super.createTableStorage(tableDescriptor, zoneDescriptor));

                var partitionStorages = new ConcurrentHashMap<Integer, MvPartitionStorage>();

                doAnswer(invocation -> {
                    Integer partitionId = invocation.getArgument(0);

                    return partitionStorages.computeIfAbsent(partitionId, id -> {
                        try {
                            return (MvPartitionStorage) createSpy(invocation.callRealMethod());
                        } catch (Throwable e) {
                            throw new RuntimeException(e);
                        }
                    });
                }).when(storage).getMvPartition(anyInt());

                return storage;
            }
        };

        tableManager.setStreamerReceiverRunner(mock(StreamerReceiverRunner.class));

        indexManager = new IndexManager(
                schemaManager,
                tableManager,
                catalogManager,
                threadPoolsManager.tableIoExecutor(),
                registry,
                lowWatermark
        );

        indexBuildingManager = new IndexBuildingManager(
                name,
                replicaSvc,
                catalogManager,
                metaStorageManager,
                indexManager,
                indexMetaStorage,
                placementDriverManager.placementDriver(),
                clusterService,
                logicalTopologyService,
                clockService,
                failureManager,
                lowWatermark,
                txManager,
                partitionReplicaLifecycleManager,
                metricManager
        );

        systemViewManager = new SystemViewManagerImpl(name, catalogManager, failureManager);

        sqlQueryProcessor = new SqlQueryProcessor(
                clusterService,
                logicalTopologyService,
                tableManager,
                schemaManager,
                dataStorageMgr,
                replicaSvc,
                clockService,
                schemaSyncService,
                catalogManager,
                new NoOpMetricManager(),
                systemViewManager,
                failureManager,
                placementDriverManager.placementDriver(),
                sqlDistributedConfiguration,
                sqlLocalConfiguration,
                transactionInflights,
                txManager,
                lowWatermark,
                threadPoolsManager.commonScheduler(),
                new KillCommandHandler(name, logicalTopologyService, clusterService.messagingService()),
                mock(EventLog.class)
        );
    }

    public IgniteTransactions transactions() {
        IgniteTransactionsImpl transactions = new IgniteTransactionsImpl(txManager, observableTimestampTracker);
        return new PublicApiThreadingIgniteTransactions(transactions, ForkJoinPool.commonPool());
    }

    public IgniteSql sql() {
        IgniteSqlImpl igniteSql = new IgniteSqlImpl(sqlQueryProcessor, observableTimestampTracker, threadPoolsManager.commonScheduler());
        return new PublicApiThreadingIgniteSql(igniteSql, ForkJoinPool.commonPool());
    }

    public void waitForMetadataCompletenessAtNow() {
        assertThat(schemaSyncService.waitForMetadataCompleteness(hybridClock.now()), willCompleteSuccessfully());
    }

    /**
     * Starts the created components.
     */
    public CompletableFuture<Void> start() {
        ComponentContext componentContext = new ComponentContext();

        IgniteComponent[] componentsToStartBeforeJoin = {
                threadPoolsManager,
                vaultManager,
                nodeProperties,
                nodeConfigRegistry,
                failureManager,
                clusterService,
                partitionsLogStorageManager,
                msLogStorageManager,
                cmgLogStorageManager,
                raftManager,
                cmgManager,
                lowWatermark
        };

        IgniteComponent[] componentsToStartAfterJoin = {
                metaStorageManager,
                clusterConfigRegistry,
                placementDriverManager,
                clockWaiter,
                catalogManager,
                catalogCompactionRunner,
                indexMetaStorage,
                distributionZoneManager,
                volatileLogStorageManagerCreator,
                replicaManager,
                txManager,
                dataStorageMgr,
                schemaManager,
                sharedTxStateStorage,
                outgoingSnapshotsManager,
                partitionReplicaLifecycleManager,
                tableManager,
                indexManager,
                indexBuildingManager,
                resourceVacuumManager,
                systemViewManager,
                sqlQueryProcessor
        };

        return startComponentsAsync(componentContext, componentsToStartBeforeJoin)
                .thenCompose(v -> cmgManager.joinFuture())
                .thenComposeAsync(v -> {
                    CompletableFuture<Void> componentsStartAfterJoin = startComponentsAsync(componentContext, componentsToStartAfterJoin);

                    return metaStorageManager.recoveryFinishedFuture()
                            .thenCompose(rev -> allOf(
                                    metaStorageManager.notifyRevisionUpdateListenerOnStart(),
                                    componentsStartAfterJoin
                            ));
                })
                .thenCompose(v -> metaStorageManager.deployWatches())
                .thenCompose(v -> cmgManager.onJoinReady())
                .thenCompose(v -> catalogManager.catalogInitializationFuture())
                .thenRun(lowWatermark::scheduleUpdates);
    }

    private CompletableFuture<Void> startComponentsAsync(ComponentContext componentContext, IgniteComponent... components) {
        var componentStartFutures = new CompletableFuture[components.length];

        for (int compIdx = 0; compIdx < components.length; compIdx++) {
            IgniteComponent component = components[compIdx];
            componentStartFutures[compIdx] = component.startAsync(componentContext);
            nodeComponents.add(component);
        }

        return allOf(componentStartFutures);
    }

    /**
     * Stops the created components.
     */
    public void stop() {
        invokeInterceptor = null;

        List<IgniteComponent> components = new ArrayList<>(nodeComponents);
        reverse(components);

        for (IgniteComponent component : components) {
            try {
                component.beforeNodeStop();
            } catch (Exception e) {
                LOG.error("Unable to execute before node stop [component={}]", e, component);
            }
        }

        assertThat(stopAsync(new ComponentContext(), components), willCompleteSuccessfully());

        nodeCfgGenerator.close();
        clusterCfgGenerator.close();
    }

    public void setInvokeInterceptor(@Nullable InvokeInterceptor invokeInterceptor) {
        this.invokeInterceptor = invokeInterceptor;
    }

    private static Path resolveDir(Path workDir, String dirName) {
        Path newDirPath = workDir.resolve(dirName);

        try {
            return Files.createDirectories(newDirPath);
        } catch (IOException e) {
            throw new IgniteInternalException(e);
        }
    }

    public @Nullable TxStatePartitionStorage txStatePartitionStorage(int zoneId, int partitionId) {
        return partitionReplicaLifecycleManager.txStatePartitionStorage(zoneId, partitionId);
    }

    public HybridTimestamp currentSafeTimeForZonePartition(int zoneId, int partId) {
        return partitionReplicaLifecycleManager.currentSafeTimeForZonePartition(zoneId, partId);
    }

    public DataStorageManager dataStorageManager() {
        return dataStorageMgr;
    }

    public TxManager txManager() {
        return txManager;
    }

    /**
     * Returns the primary replica for given zone's partition 0. If there is no primary yet, waits for it.
     *
     * @param zoneId ID of the zone.
     */
    public ReplicaMeta getPrimaryReplica(int zoneId) throws InterruptedException {
        assertTrue(waitForCondition(() -> getNullablePrimaryReplica(zoneId) != null, SECONDS.toMillis(10)));

        ReplicaMeta primaryReplica = getNullablePrimaryReplica(zoneId);
        assertThat(primaryReplica, is(notNullValue()));
        return primaryReplica;
    }

    private @Nullable ReplicaMeta getNullablePrimaryReplica(int zoneId) {
        CompletableFuture<ReplicaMeta> primaryReplicaFuture = placementDriverManager.placementDriver().getPrimaryReplica(
                new ZonePartitionId(zoneId, 0),
                hybridClock.now()
        );

        assertThat(primaryReplicaFuture, willCompleteSuccessfully());
        return primaryReplicaFuture.join();
    }

    @Contract("null -> null")
    private static <T> @Nullable T createSpy(@Nullable T object) {
        if (object == null) {
            return null;
        }

        return isMock(object) ? object : spy(object);
    }
}
