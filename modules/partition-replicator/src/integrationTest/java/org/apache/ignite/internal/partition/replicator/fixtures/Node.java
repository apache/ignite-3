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
import static org.apache.ignite.internal.BaseIgniteRestartTest.createVault;
import static org.apache.ignite.internal.configuration.IgnitePaths.partitionsPath;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.REBALANCE_SCHEDULER_POOL_SIZE;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.pendingPartAssignmentsKey;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedIn;
import static org.apache.ignite.internal.util.IgniteUtils.stopAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.LongSupplier;
import org.apache.ignite.internal.app.ThreadPoolsManager;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogManagerImpl;
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
import org.apache.ignite.internal.configuration.ClusterConfiguration;
import org.apache.ignite.internal.configuration.ComponentWorkingDir;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.NodeConfiguration;
import org.apache.ignite.internal.configuration.RaftGroupOptionsConfigHelper;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.configuration.SystemDistributedExtensionConfiguration;
import org.apache.ignite.internal.configuration.SystemDistributedExtensionConfigurationSchema;
import org.apache.ignite.internal.configuration.SystemLocalConfiguration;
import org.apache.ignite.internal.configuration.storage.DistributedConfigurationStorage;
import org.apache.ignite.internal.configuration.storage.LocalFileConfigurationStorage;
import org.apache.ignite.internal.configuration.validation.TestConfigurationValidator;
import org.apache.ignite.internal.disaster.system.SystemDisasterRecoveryStorage;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.failure.NoOpFailureManager;
import org.apache.ignite.internal.hlc.ClockServiceImpl;
import org.apache.ignite.internal.hlc.ClockWaiter;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.index.IndexManager;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.lowwatermark.LowWatermarkImpl;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.configuration.MetaStorageConfiguration;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.metastorage.impl.MetaStorageRevisionListenerRegistry;
import org.apache.ignite.internal.metastorage.server.ReadOperationForCompactionTracker;
import org.apache.ignite.internal.metastorage.server.persistence.RocksDbKeyValueStorage;
import org.apache.ignite.internal.metrics.NoOpMetricManager;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.NodeFinder;
import org.apache.ignite.internal.network.configuration.NetworkExtensionConfigurationSchema;
import org.apache.ignite.internal.network.recovery.InMemoryStaleIds;
import org.apache.ignite.internal.network.utils.ClusterServiceTestUtils;
import org.apache.ignite.internal.pagememory.configuration.schema.PersistentPageMemoryProfileConfigurationSchema;
import org.apache.ignite.internal.pagememory.configuration.schema.VolatilePageMemoryProfileConfigurationSchema;
import org.apache.ignite.internal.partition.replicator.PartitionReplicaLifecycleManager;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessageGroup;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.RaftGroupOptionsConfigurer;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.storage.LogStorageFactory;
import org.apache.ignite.internal.raft.storage.impl.LocalLogStorageFactory;
import org.apache.ignite.internal.raft.util.SharedLogStorageFactoryUtils;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.SchemaSyncService;
import org.apache.ignite.internal.schema.configuration.GcConfiguration;
import org.apache.ignite.internal.schema.configuration.GcExtensionConfiguration;
import org.apache.ignite.internal.schema.configuration.GcExtensionConfigurationSchema;
import org.apache.ignite.internal.schema.configuration.StorageUpdateConfiguration;
import org.apache.ignite.internal.schema.configuration.StorageUpdateExtensionConfiguration;
import org.apache.ignite.internal.schema.configuration.StorageUpdateExtensionConfigurationSchema;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.storage.DataStorageModules;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.storage.configurations.StorageExtensionConfigurationSchema;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryDataStorageModule;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryStorageEngineExtensionConfigurationSchema;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.VolatilePageMemoryStorageEngineExtensionConfigurationSchema;
import org.apache.ignite.internal.table.StreamerReceiverRunner;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.distributed.index.IndexMetaStorage;
import org.apache.ignite.internal.table.distributed.raft.MinimumRequiredTimeCollectorService;
import org.apache.ignite.internal.table.distributed.raft.MinimumRequiredTimeCollectorServiceImpl;
import org.apache.ignite.internal.table.distributed.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.table.distributed.schema.SchemaSyncServiceImpl;
import org.apache.ignite.internal.table.distributed.schema.ThreadLocalPartitionCommandsMarshaller;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.IgniteTransactionsImpl;
import org.apache.ignite.internal.tx.impl.PublicApiThreadingIgniteTransactions;
import org.apache.ignite.internal.tx.impl.RemotelyTriggeredResourceRegistry;
import org.apache.ignite.internal.tx.impl.TransactionIdGenerator;
import org.apache.ignite.internal.tx.impl.TransactionInflights;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.message.TxMessageGroup;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.tx.test.TestLocalRwTxCounter;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.tx.IgniteTransactions;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.TestInfo;

/**
 * Fake node used in colocation tests.
 */
public class Node {
    private static final IgniteLogger LOG = Loggers.forClass(Node.class);

    public final String name;

    private final Loza raftManager;

    private final ThreadPoolsManager threadPoolsManager;

    public final ReplicaManager replicaManager;

    public final MetaStorageManager metaStorageManager;

    private final VaultManager vaultManager;

    public final ClusterService clusterService;

    private final LockManager lockManager;

    private final TxManager txManager;

    private final DataStorageManager dataStorageMgr;

    public final TableManager tableManager;

    public final DistributionZoneManager distributionZoneManager;

    private final ConfigurationManager nodeCfgMgr;

    private final ConfigurationManager clusterCfgMgr;

    public final ClusterManagementGroupManager cmgManager;

    private final SchemaManager schemaManager;

    public final CatalogManager catalogManager;

    private final PartitionReplicaLifecycleManager partitionReplicaLifecycleManager;

    private final SchemaSyncService schemaSyncService;

    private final ClockWaiter clockWaiter;

    private final List<IgniteComponent> nodeComponents = new CopyOnWriteArrayList<>();

    private final ConfigurationTreeGenerator nodeCfgGenerator;

    private final ConfigurationTreeGenerator clusterCfgGenerator;

    private final Map<TablePartitionId, CompletableFuture<Void>> finishHandleChangeStableAssignmentEventFutures
            = new ConcurrentHashMap<>();

    private final LowWatermarkImpl lowWatermark;

    /** The future have to be complete after the node start and all Meta storage watches are deployd. */
    private CompletableFuture<Void> deployWatchesFut;

    /** Hybrid clock. */
    public final HybridClock hybridClock = new HybridClockImpl();

    /** Index manager. */
    private final IndexManager indexManager;

    /** Failure processor. */
    private final FailureManager failureManager;

    public final AtomicReference<Function<ReplicaRequest, ReplicationGroupId>> converter =
            new AtomicReference<>(request -> request.groupId().asReplicationGroupId());

    private final LogStorageFactory partitionsLogStorageFactory;

    private final LogStorageFactory msLogStorageFactory;

    private final LogStorageFactory cmgLogStorageFactory;

    private final IndexMetaStorage indexMetaStorage;

    private final HybridTimestampTracker observableTimestampTracker = HybridTimestampTracker.atomicTracker(null);

    private volatile MvTableStorage mvTableStorage;

    private volatile TxStateStorage txStateStorage;

    @Nullable
    private volatile InvokeInterceptor invokeInterceptor;

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
            PlacementDriver placementDriver,
            SystemLocalConfiguration systemLocalConfiguration,
            RaftConfiguration raftConfiguration,
            NodeAttributesConfiguration nodeAttributesConfiguration,
            StorageConfiguration storageConfiguration,
            MetaStorageConfiguration metaStorageConfiguration,
            ReplicationConfiguration replicationConfiguration,
            TransactionConfiguration transactionConfiguration,
            ScheduledExecutorService scheduledExecutorService,
            @Nullable InvokeInterceptor invokeInterceptor
    ) {
        this.invokeInterceptor = invokeInterceptor;

        name = testNodeName(testInfo, address.port());

        Path dir = workDir.resolve(name);

        vaultManager = createVault(dir);

        nodeCfgGenerator = new ConfigurationTreeGenerator(
                List.of(NodeConfiguration.KEY),
                List.of(
                        NetworkExtensionConfigurationSchema.class,
                        StorageExtensionConfigurationSchema.class,
                        PersistentPageMemoryStorageEngineExtensionConfigurationSchema.class,
                        VolatilePageMemoryStorageEngineExtensionConfigurationSchema.class
                ),
                List.of(
                        PersistentPageMemoryProfileConfigurationSchema.class,
                        VolatilePageMemoryProfileConfigurationSchema.class
                )
        );

        Path configPath = dir.resolve("config");
        TestIgnitionManager.addDefaultsToConfigurationFile(configPath);

        nodeCfgMgr = new ConfigurationManager(
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

        partitionsLogStorageFactory = SharedLogStorageFactoryUtils.create(clusterService.nodeName(), partitionsWorkDir.raftLogPath());

        RaftGroupOptionsConfigurer partitionRaftConfigurer =
                RaftGroupOptionsConfigHelper.configureProperties(partitionsLogStorageFactory, partitionsWorkDir.metaPath());

        raftManager = new Loza(
                clusterService,
                new NoOpMetricManager(),
                raftConfiguration,
                hybridClock,
                raftGroupEventsClientListener,
                new NoOpFailureManager()
        );

        var clusterStateStorage = new TestClusterStateStorage();
        var logicalTopology = new LogicalTopologyImpl(clusterStateStorage);

        var clusterInitializer = new ClusterInitializer(
                clusterService,
                hocon -> hocon,
                new TestConfigurationValidator()
        );

        failureManager = new NoOpFailureManager();

        ComponentWorkingDir cmgWorkDir = new ComponentWorkingDir(dir.resolve("cmg"));

        cmgLogStorageFactory =
                SharedLogStorageFactoryUtils.create(clusterService.nodeName(), cmgWorkDir.raftLogPath());

        RaftGroupOptionsConfigurer cmgRaftConfigurer =
                RaftGroupOptionsConfigHelper.configureProperties(cmgLogStorageFactory, cmgWorkDir.metaPath());

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
                cmgRaftConfigurer
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

        msLogStorageFactory =
                SharedLogStorageFactoryUtils.create(clusterService.nodeName(), metastorageWorkDir.raftLogPath());

        RaftGroupOptionsConfigurer msRaftConfigurer =
                RaftGroupOptionsConfigHelper.configureProperties(msLogStorageFactory, metastorageWorkDir.metaPath());

        metaStorageManager = new MetaStorageManagerImpl(
                clusterService,
                cmgManager,
                logicalTopologyService,
                raftManager,
                keyValueStorage,
                hybridClock,
                topologyAwareRaftGroupServiceFactory,
                new NoOpMetricManager(),
                metaStorageConfiguration,
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
        };

        threadPoolsManager = new ThreadPoolsManager(name);

        LongSupplier partitionIdleSafeTimePropagationPeriodMsSupplier = () -> 10L;

        ReplicaService replicaSvc = new ReplicaService(
                clusterService.messagingService(),
                hybridClock,
                threadPoolsManager.partitionOperationsExecutor(),
                replicationConfiguration,
                threadPoolsManager.commonScheduler()
        );

        var resourcesRegistry = new RemotelyTriggeredResourceRegistry();

        clockWaiter = new ClockWaiter(name, hybridClock, threadPoolsManager.commonScheduler());

        var clockService = new ClockServiceImpl(
                hybridClock,
                clockWaiter,
                () -> TestIgnitionManager.DEFAULT_MAX_CLOCK_SKEW_MS
        );

        var transactionInflights = new TransactionInflights(placementDriver, clockService);

        var cfgStorage = new DistributedConfigurationStorage("test", metaStorageManager);

        clusterCfgGenerator = new ConfigurationTreeGenerator(
                List.of(ClusterConfiguration.KEY),
                List.of(
                        GcExtensionConfigurationSchema.class,
                        StorageUpdateExtensionConfigurationSchema.class,
                        SystemDistributedExtensionConfigurationSchema.class
                ),
                List.of()
        );

        clusterCfgMgr = new ConfigurationManager(
                List.of(ClusterConfiguration.KEY),
                cfgStorage,
                clusterCfgGenerator,
                new TestConfigurationValidator()
        );

        ConfigurationRegistry clusterConfigRegistry = clusterCfgMgr.configurationRegistry();

        var registry = new MetaStorageRevisionListenerRegistry(metaStorageManager);

        GcConfiguration gcConfig = clusterConfigRegistry.getConfiguration(GcExtensionConfiguration.KEY).gc();

        DataStorageModules dataStorageModules = new DataStorageModules(List.of(
                new PersistentPageMemoryDataStorageModule()
        ));

        Path storagePath = dir.resolve("storage");

        dataStorageMgr = new DataStorageManager(
                dataStorageModules.createStorageEngines(
                        name,
                        nodeCfgMgr.configurationRegistry(),
                        dir.resolve("storage"),
                        null,
                        failureManager,
                        partitionsLogStorageFactory,
                        hybridClock,
                        scheduledExecutorService
                ),
                storageConfiguration
        );

        lowWatermark = new LowWatermarkImpl(
                name,
                gcConfig.lowWatermark(),
                clockService,
                vaultManager,
                failureManager,
                clusterService.messagingService()
        );

        txManager = new TxManagerImpl(
                transactionConfiguration,
                clusterService,
                replicaSvc,
                lockManager,
                clockService,
                new TransactionIdGenerator(address.port()),
                placementDriver,
                partitionIdleSafeTimePropagationPeriodMsSupplier,
                new TestLocalRwTxCounter(),
                resourcesRegistry,
                transactionInflights,
                lowWatermark,
                threadPoolsManager.commonScheduler()
        );

        replicaManager = new ReplicaManager(
                name,
                clusterService,
                cmgManager,
                clockService,
                Set.of(PartitionReplicationMessageGroup.class, TxMessageGroup.class),
                placementDriver,
                threadPoolsManager.partitionOperationsExecutor(),
                partitionIdleSafeTimePropagationPeriodMsSupplier,
                new NoOpFailureManager(),
                new ThreadLocalPartitionCommandsMarshaller(clusterService.serializationRegistry()),
                topologyAwareRaftGroupServiceFactory,
                raftManager,
                partitionRaftConfigurer,
                view -> new LocalLogStorageFactory(),
                ForkJoinPool.commonPool(),
                t -> converter.get().apply(t),
                replicaGrpId -> metaStorageManager.get(pendingPartAssignmentsKey((ZonePartitionId) replicaGrpId))
                        .thenApply(Entry::value)
        );

        LongSupplier delayDurationMsSupplier = () -> 10L;

        catalogManager = new CatalogManagerImpl(
                new UpdateLogImpl(metaStorageManager),
                clockService,
                delayDurationMsSupplier
        );

        indexMetaStorage = new IndexMetaStorage(catalogManager, lowWatermark, metaStorageManager);

        schemaManager = new SchemaManager(registry, catalogManager);

        schemaSyncService = new SchemaSyncServiceImpl(metaStorageManager.clusterTime(), delayDurationMsSupplier);

        ScheduledExecutorService rebalanceScheduler = Executors.newScheduledThreadPool(
                REBALANCE_SCHEDULER_POOL_SIZE,
                NamedThreadFactory.create(name, "test-rebalance-scheduler", LOG)
        );

        SystemDistributedConfiguration systemDistributedConfiguration =
                clusterConfigRegistry.getConfiguration(SystemDistributedExtensionConfiguration.KEY).system();

        distributionZoneManager = new DistributionZoneManager(
                name,
                registry,
                metaStorageManager,
                logicalTopologyService,
                catalogManager,
                rebalanceScheduler,
                systemDistributedConfiguration
        );

        partitionReplicaLifecycleManager = new PartitionReplicaLifecycleManager(
                catalogManager,
                replicaManager,
                distributionZoneManager,
                metaStorageManager,
                clusterService.topologyService(),
                lowWatermark,
                threadPoolsManager.tableIoExecutor(),
                rebalanceScheduler,
                threadPoolsManager.partitionOperationsExecutor(),
                clockService,
                placementDriver,
                schemaSyncService,
                systemDistributedConfiguration);

        StorageUpdateConfiguration storageUpdateConfiguration = clusterConfigRegistry
                .getConfiguration(StorageUpdateExtensionConfiguration.KEY).storageUpdate();

        MinimumRequiredTimeCollectorService minTimeCollectorService = new MinimumRequiredTimeCollectorServiceImpl();

        tableManager = new TableManager(
                name,
                registry,
                gcConfig,
                transactionConfiguration,
                storageUpdateConfiguration,
                clusterService.messagingService(),
                clusterService.topologyService(),
                clusterService.serializationRegistry(),
                replicaManager,
                lockManager,
                replicaSvc,
                txManager,
                dataStorageMgr,
                storagePath,
                metaStorageManager,
                schemaManager,
                threadPoolsManager.tableIoExecutor(),
                threadPoolsManager.partitionOperationsExecutor(),
                rebalanceScheduler,
                threadPoolsManager.commonScheduler(),
                clockService,
                new OutgoingSnapshotsManager(clusterService.messagingService()),
                distributionZoneManager,
                schemaSyncService,
                catalogManager,
                observableTimestampTracker,
                placementDriver,
                () -> mock(IgniteSql.class),
                resourcesRegistry,
                lowWatermark,
                transactionInflights,
                indexMetaStorage,
                partitionsLogStorageFactory,
                partitionReplicaLifecycleManager,
                minTimeCollectorService,
                systemDistributedConfiguration
        ) {

            @Override
            protected MvTableStorage createTableStorage(CatalogTableDescriptor tableDescriptor, CatalogZoneDescriptor zoneDescriptor) {
                mvTableStorage = spy(super.createTableStorage(tableDescriptor, zoneDescriptor));

                return mvTableStorage;
            }

            @Override
            protected TxStateStorage createTxStateTableStorage(
                    CatalogTableDescriptor tableDescriptor,
                    CatalogZoneDescriptor zoneDescriptor
            ) {
                txStateStorage = spy(super.createTxStateTableStorage(tableDescriptor, zoneDescriptor));

                return txStateStorage;
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
    }

    public IgniteTransactions transactions() {
        IgniteTransactionsImpl transactions = new IgniteTransactionsImpl(txManager, observableTimestampTracker);
        return new PublicApiThreadingIgniteTransactions(transactions, ForkJoinPool.commonPool());
    }

    public void waitForMetadataCompletenessAtNow() {
        assertThat(schemaSyncService.waitForMetadataCompleteness(hybridClock.now()), willCompleteSuccessfully());
    }

    /**
     * Starts the created components.
     */
    public void start() {
        ComponentContext componentContext = new ComponentContext();

        deployWatchesFut = startComponentsAsync(
                componentContext,
                threadPoolsManager,
                vaultManager,
                nodeCfgMgr,
                failureManager,
                clusterService,
                partitionsLogStorageFactory,
                msLogStorageFactory,
                cmgLogStorageFactory,
                raftManager,
                cmgManager,
                lowWatermark
        ).thenComposeAsync(
                v -> cmgManager.joinFuture()
        ).thenApplyAsync(v -> startComponentsAsync(
                componentContext,
                metaStorageManager,
                clusterCfgMgr,
                clockWaiter,
                catalogManager,
                indexMetaStorage,
                distributionZoneManager,
                replicaManager,
                txManager,
                dataStorageMgr,
                schemaManager,
                partitionReplicaLifecycleManager,
                tableManager,
                indexManager
        )).thenComposeAsync(componentFuts -> {
            CompletableFuture<Void> configurationNotificationFut = metaStorageManager.recoveryFinishedFuture()
                    .thenCompose(rev -> allOf(
                            nodeCfgMgr.configurationRegistry().notifyCurrentConfigurationListeners(),
                            clusterCfgMgr.configurationRegistry().notifyCurrentConfigurationListeners(),
                            ((MetaStorageManagerImpl) metaStorageManager).notifyRevisionUpdateListenerOnStart(),
                            componentFuts
                    ));

            assertThat(configurationNotificationFut, willSucceedIn(1, TimeUnit.MINUTES));

            lowWatermark.scheduleUpdates();

            return metaStorageManager.deployWatches();
        });
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
     * Waits for watches deployed.
     */
    public void waitWatches() {
        assertThat("Watches were not deployed", deployWatchesFut, willCompleteSuccessfully());
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
}
