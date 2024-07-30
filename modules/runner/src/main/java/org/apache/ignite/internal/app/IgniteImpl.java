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

package org.apache.ignite.internal.app;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.REBALANCE_SCHEDULER_POOL_SIZE;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_READ;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_WRITE;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteServer;
import org.apache.ignite.catalog.IgniteCatalog;
import org.apache.ignite.client.handler.ClientHandlerMetricSource;
import org.apache.ignite.client.handler.ClientHandlerModule;
import org.apache.ignite.client.handler.configuration.ClientConnectorConfiguration;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.configuration.ConfigurationDynamicDefaultsPatcher;
import org.apache.ignite.configuration.ConfigurationModule;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogManagerImpl;
import org.apache.ignite.internal.catalog.compaction.CatalogCompactionRunner;
import org.apache.ignite.internal.catalog.configuration.SchemaSynchronizationConfiguration;
import org.apache.ignite.internal.catalog.sql.IgniteCatalogSqlImpl;
import org.apache.ignite.internal.catalog.storage.UpdateLogImpl;
import org.apache.ignite.internal.cluster.management.ClusterInitializer;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.ClusterState;
import org.apache.ignite.internal.cluster.management.NodeAttributesCollector;
import org.apache.ignite.internal.cluster.management.configuration.ClusterManagementConfiguration;
import org.apache.ignite.internal.cluster.management.configuration.NodeAttributesConfiguration;
import org.apache.ignite.internal.cluster.management.raft.ClusterStateStorage;
import org.apache.ignite.internal.cluster.management.raft.ClusterStateStorageManager;
import org.apache.ignite.internal.cluster.management.raft.RocksDbClusterStateStorage;
import org.apache.ignite.internal.cluster.management.raft.ValidationManager;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyServiceImpl;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.components.LongJvmPauseDetector;
import org.apache.ignite.internal.compute.AntiHijackIgniteCompute;
import org.apache.ignite.internal.compute.ComputeComponent;
import org.apache.ignite.internal.compute.ComputeComponentImpl;
import org.apache.ignite.internal.compute.IgniteComputeImpl;
import org.apache.ignite.internal.compute.IgniteComputeInternal;
import org.apache.ignite.internal.compute.configuration.ComputeConfiguration;
import org.apache.ignite.internal.compute.executor.ComputeExecutorImpl;
import org.apache.ignite.internal.compute.loader.JobClassLoaderFactory;
import org.apache.ignite.internal.compute.loader.JobContextManager;
import org.apache.ignite.internal.compute.state.InMemoryComputeStateMachine;
import org.apache.ignite.internal.configuration.ConfigurationDynamicDefaultsPatcherImpl;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.ConfigurationModules;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.JdbcPortProviderImpl;
import org.apache.ignite.internal.configuration.ServiceLoaderModulesProvider;
import org.apache.ignite.internal.configuration.hocon.HoconConverter;
import org.apache.ignite.internal.configuration.storage.ConfigurationStorage;
import org.apache.ignite.internal.configuration.storage.DistributedConfigurationStorage;
import org.apache.ignite.internal.configuration.storage.LocalFileConfigurationStorage;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.configuration.validation.ConfigurationValidator;
import org.apache.ignite.internal.configuration.validation.ConfigurationValidatorImpl;
import org.apache.ignite.internal.deployunit.DeploymentManagerImpl;
import org.apache.ignite.internal.deployunit.IgniteDeployment;
import org.apache.ignite.internal.deployunit.configuration.DeploymentConfiguration;
import org.apache.ignite.internal.deployunit.metastore.DeploymentUnitStoreImpl;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.eventlog.config.schema.EventLogConfiguration;
import org.apache.ignite.internal.eventlog.impl.EventLogImpl;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.failure.configuration.FailureProcessorConfiguration;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.ClockServiceImpl;
import org.apache.ignite.internal.hlc.ClockWaiter;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.index.IndexBuildingManager;
import org.apache.ignite.internal.index.IndexManager;
import org.apache.ignite.internal.index.IndexNodeFinishedRwTransactionsChecker;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.lowwatermark.LowWatermarkImpl;
import org.apache.ignite.internal.lowwatermark.event.ChangeLowWatermarkEventParameters;
import org.apache.ignite.internal.lowwatermark.event.LowWatermarkEvent;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.cache.IdempotentCacheVacuumizer;
import org.apache.ignite.internal.metastorage.configuration.MetaStorageConfiguration;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.metastorage.server.persistence.RocksDbKeyValueStorage;
import org.apache.ignite.internal.metastorage.server.raft.MetastorageGroupId;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.metrics.MetricManagerImpl;
import org.apache.ignite.internal.metrics.configuration.MetricConfiguration;
import org.apache.ignite.internal.metrics.sources.JvmMetricSource;
import org.apache.ignite.internal.metrics.sources.OsMetricSource;
import org.apache.ignite.internal.network.ChannelType;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.DefaultMessagingService;
import org.apache.ignite.internal.network.MessageSerializationRegistryImpl;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NettyBootstrapFactory;
import org.apache.ignite.internal.network.NettyWorkersRegistrar;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.configuration.NetworkConfiguration;
import org.apache.ignite.internal.network.recovery.VaultStaleIds;
import org.apache.ignite.internal.network.scalecube.ScaleCubeClusterServiceFactory;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.internal.network.serialization.SerializationRegistryServiceLoader;
import org.apache.ignite.internal.network.wrapper.JumpToExecutorByConsistentIdAfterSend;
import org.apache.ignite.internal.partition.replicator.PartitionReplicaLifecycleManager;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessageGroup;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.PlacementDriverManager;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.Marshaller;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.storage.LogStorageFactory;
import org.apache.ignite.internal.raft.storage.impl.VolatileLogStorageFactoryCreator;
import org.apache.ignite.internal.raft.util.SharedLogStorageFactoryUtils;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.rest.RestComponent;
import org.apache.ignite.internal.rest.RestFactory;
import org.apache.ignite.internal.rest.RestManager;
import org.apache.ignite.internal.rest.RestManagerFactory;
import org.apache.ignite.internal.rest.authentication.AuthenticationProviderFactory;
import org.apache.ignite.internal.rest.cluster.ClusterManagementRestFactory;
import org.apache.ignite.internal.rest.compute.ComputeRestFactory;
import org.apache.ignite.internal.rest.configuration.PresentationsFactory;
import org.apache.ignite.internal.rest.configuration.RestConfiguration;
import org.apache.ignite.internal.rest.deployment.CodeDeploymentRestFactory;
import org.apache.ignite.internal.rest.metrics.MetricRestFactory;
import org.apache.ignite.internal.rest.node.NodeManagementRestFactory;
import org.apache.ignite.internal.rest.recovery.DisasterRecoveryFactory;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.configuration.GcConfiguration;
import org.apache.ignite.internal.schema.configuration.StorageUpdateConfiguration;
import org.apache.ignite.internal.security.authentication.AuthenticationManager;
import org.apache.ignite.internal.security.authentication.AuthenticationManagerImpl;
import org.apache.ignite.internal.security.configuration.SecurityConfiguration;
import org.apache.ignite.internal.sql.api.IgniteSqlImpl;
import org.apache.ignite.internal.sql.api.PublicApiThreadingIgniteSql;
import org.apache.ignite.internal.sql.configuration.distributed.SqlDistributedConfiguration;
import org.apache.ignite.internal.sql.configuration.local.SqlLocalConfiguration;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.SqlQueryProcessor;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.storage.DataStorageModule;
import org.apache.ignite.internal.storage.DataStorageModules;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.engine.ThreadAssertingStorageEngine;
import org.apache.ignite.internal.systemview.SystemViewManagerImpl;
import org.apache.ignite.internal.systemview.api.SystemViewManager;
import org.apache.ignite.internal.table.distributed.PublicApiThreadingIgniteTables;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.distributed.disaster.DisasterRecoveryManager;
import org.apache.ignite.internal.table.distributed.index.IndexMetaStorage;
import org.apache.ignite.internal.table.distributed.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.table.distributed.schema.CheckCatalogVersionOnActionRequest;
import org.apache.ignite.internal.table.distributed.schema.CheckCatalogVersionOnAppendEntries;
import org.apache.ignite.internal.table.distributed.schema.SchemaSyncService;
import org.apache.ignite.internal.table.distributed.schema.SchemaSyncServiceImpl;
import org.apache.ignite.internal.table.distributed.schema.ThreadLocalPartitionCommandsMarshaller;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.threading.PublicApiThreadingIgniteCatalog;
import org.apache.ignite.internal.tx.HybridTimestampTracker;
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
import org.apache.ignite.internal.tx.message.TxMessageGroup;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.VaultService;
import org.apache.ignite.internal.vault.persistence.PersistentVaultService;
import org.apache.ignite.internal.worker.CriticalWorkerWatchdog;
import org.apache.ignite.internal.worker.ThreadAssertions;
import org.apache.ignite.internal.worker.configuration.CriticalWorkersConfiguration;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NodeMetadata;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.IgniteTables;
import org.apache.ignite.tx.IgniteTransactions;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Ignite internal implementation.
 */
public class IgniteImpl implements Ignite {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(IgniteImpl.class);

    /**
     * Path to the persistent storage used by the {@link VaultService} component.
     */
    private static final Path VAULT_DB_PATH = Paths.get("vault");

    /**
     * Path to the persistent storage used by the {@link MetaStorageManager} component.
     */
    private static final Path METASTORAGE_DB_PATH = Paths.get("metastorage");

    /**
     * Path to the persistent storage used by the {@link ClusterManagementGroupManager} component.
     */
    private static final Path CMG_DB_PATH = Paths.get("cmg");

    /**
     * Path for the partitions persistent storage.
     */
    private static final Path PARTITIONS_STORE_PATH = Paths.get("db");

    /** Ignite node name. */
    private final String name;

    /** Lifecycle manager. */
    private final LifecycleManager lifecycleManager;

    private final ThreadPoolsManager threadPoolsManager;

    /** Vault manager. */
    private final VaultManager vaultMgr;

    /** Sql query engine. */
    private final SqlQueryProcessor qryEngine;

    /** Sql API facade. */
    private final IgniteSqlImpl sql;

    /** Configuration manager that handles node (local) configuration. */
    private final ConfigurationManager nodeCfgMgr;

    /** Cluster service (cluster network manager). */
    private final ClusterService clusterSvc;

    private final ComputeComponent computeComponent;

    private final CriticalWorkerWatchdog criticalWorkerRegistry;

    /** Failure processor. */
    private final FailureProcessor failureProcessor;

    /** Netty bootstrap factory. */
    private final NettyBootstrapFactory nettyBootstrapFactory;

    private final NettyWorkersRegistrar nettyWorkersRegistrar;

    /** Raft manager. */
    private final Loza raftMgr;

    /** Meta storage manager. */
    private final MetaStorageManagerImpl metaStorageMgr;

    /** Placement driver manager. */
    private final PlacementDriverManager placementDriverMgr;

    /** Configuration manager that handles cluster (distributed) configuration. */
    private final ConfigurationManager clusterCfgMgr;

    /** Cluster initializer. */
    private final ClusterInitializer clusterInitializer;

    /** Replica manager. */
    private final ReplicaManager replicaMgr;

    /** Transactions manager. */
    private final TxManager txManager;

    /** Distributed table manager. */
    private final TableManager distributedTblMgr;

    /** Disaster recovery manager. */
    private final DisasterRecoveryManager disasterRecoveryManager;

    private final IndexManager indexManager;

    /** Rest module. */
    private final RestComponent restComponent;

    private final ClusterStateStorage clusterStateStorage;

    private final ClusterManagementGroupManager cmgMgr;

    private final LogicalTopologyService logicalTopologyService;

    /** Client handler module. */
    private final ClientHandlerModule clientHandlerModule;

    /** Distributed configuration storage. */
    private final ConfigurationStorage cfgStorage;

    /** Compute. */
    private final IgniteComputeInternal compute;

    /** JVM pause detector. */
    private final LongJvmPauseDetector longJvmPauseDetector;

    /** Data storage manager. */
    private final DataStorageManager dataStorageMgr;

    /** Schema manager. */
    private final SchemaManager schemaManager;

    /** Metric manager. */
    private final MetricManager metricManager;

    private final IgniteDeployment deploymentManager;

    private final DistributionZoneManager distributionZoneManager;

    private final PartitionReplicaLifecycleManager partitionReplicaLifecycleManager;

    /** Creator for volatile {@link LogStorageFactory} instances. */
    private final VolatileLogStorageFactoryCreator volatileLogStorageFactoryCreator;

    /** A hybrid logical clock. */
    private final HybridClock clock;

    private final ClockWaiter clockWaiter;

    private final ClockService clockService;

    private final LowWatermarkImpl lowWatermark;

    private final OutgoingSnapshotsManager outgoingSnapshotsManager;

    private final CatalogManager catalogManager;

    private final CatalogCompactionRunner catalogCompactionRunner;

    private final AuthenticationManager authenticationManager;

    /** Timestamp tracker for embedded transactions. */
    private final HybridTimestampTracker observableTimestampTracker = new HybridTimestampTracker();

    /** System views manager. */
    private final SystemViewManagerImpl systemViewManager;

    /** Index building manager. */
    private final IndexBuildingManager indexBuildingManager;

    /** Local node RW transaction completion checker for indexes. */
    private final IndexNodeFinishedRwTransactionsChecker indexNodeFinishedRwTransactionsChecker;

    /** Cleanup manager for tx resources. */
    private final ResourceVacuumManager resourceVacuumManager;

    /** Remote triggered resources registry. */
    private final RemotelyTriggeredResourceRegistry resourcesRegistry;

    private final Executor asyncContinuationExecutor = ForkJoinPool.commonPool();

    /** Default log storage factory for raft. */
    private final LogStorageFactory logStorageFactory;

    private IndexMetaStorage indexMetaStorage;

    /**
     * The Constructor.
     *
     * @param node Embedded node.
     * @param configPath Path to node configuration in the HOCON format.
     * @param workDir Work directory for the started node. Must not be {@code null}.
     * @param serviceProviderClassLoader The class loader to be used to load provider-configuration files and provider classes, or
     *         {@code null} if the system class loader (or, failing that the bootstrap class loader) is to be used.
     */
    IgniteImpl(IgniteServer node, Path configPath, Path workDir, @Nullable ClassLoader serviceProviderClassLoader) {
        this.name = node.name();

        longJvmPauseDetector = new LongJvmPauseDetector(name);

        lifecycleManager = new LifecycleManager(name);

        threadPoolsManager = new ThreadPoolsManager(name);

        vaultMgr = createVault(workDir);

        metricManager = new MetricManagerImpl();

        ConfigurationModules modules = loadConfigurationModules(serviceProviderClassLoader);

        ConfigurationTreeGenerator localConfigurationGenerator = new ConfigurationTreeGenerator(
                modules.local().rootKeys(),
                modules.local().schemaExtensions(),
                modules.local().polymorphicSchemaExtensions()
        );

        LocalFileConfigurationStorage localFileConfigurationStorage = new LocalFileConfigurationStorage(
                name,
                configPath,
                localConfigurationGenerator,
                modules.local()
        );

        ConfigurationValidator localConfigurationValidator =
                ConfigurationValidatorImpl.withDefaultValidators(localConfigurationGenerator, modules.local().validators());

        nodeCfgMgr = new ConfigurationManager(
                modules.local().rootKeys(),
                localFileConfigurationStorage,
                localConfigurationGenerator,
                localConfigurationValidator
        );

        ConfigurationRegistry nodeConfigRegistry = nodeCfgMgr.configurationRegistry();

        NetworkConfiguration networkConfiguration = nodeConfigRegistry.getConfiguration(NetworkConfiguration.KEY);

        MessageSerializationRegistry serializationRegistry = createSerializationRegistry(serviceProviderClassLoader);

        failureProcessor = new FailureProcessor(node::shutdown, nodeConfigRegistry.getConfiguration(FailureProcessorConfiguration.KEY));

        CriticalWorkersConfiguration criticalWorkersConfiguration = nodeConfigRegistry.getConfiguration(CriticalWorkersConfiguration.KEY);

        criticalWorkerRegistry = new CriticalWorkerWatchdog(
                criticalWorkersConfiguration,
                threadPoolsManager.commonScheduler(),
                failureProcessor
        );

        nettyBootstrapFactory = new NettyBootstrapFactory(networkConfiguration, name);
        nettyWorkersRegistrar = new NettyWorkersRegistrar(
                criticalWorkerRegistry,
                threadPoolsManager.commonScheduler(),
                nettyBootstrapFactory,
                criticalWorkersConfiguration,
                failureProcessor
        );

        clusterSvc = new ScaleCubeClusterServiceFactory().createClusterService(
                name,
                networkConfiguration,
                nettyBootstrapFactory,
                serializationRegistry,
                new VaultStaleIds(vaultMgr),
                criticalWorkerRegistry,
                failureProcessor
        );

        clock = new HybridClockImpl();

        clockWaiter = new ClockWaiter(name, clock);

        RaftConfiguration raftConfiguration = nodeConfigRegistry.getConfiguration(RaftConfiguration.KEY);

        // TODO https://issues.apache.org/jira/browse/IGNITE-19051
        RaftGroupEventsClientListener raftGroupEventsClientListener = new RaftGroupEventsClientListener();

        logStorageFactory = SharedLogStorageFactoryUtils.create(clusterSvc.nodeName(), workDir, raftConfiguration);

        raftMgr = new Loza(
                clusterSvc,
                metricManager,
                raftConfiguration,
                workDir,
                clock,
                raftGroupEventsClientListener,
                logStorageFactory
        );

        LockManager lockMgr = new HeapLockManager();

        MessagingService messagingServiceReturningToStorageOperationsPool = new JumpToExecutorByConsistentIdAfterSend(
                clusterSvc.messagingService(),
                name,
                message -> threadPoolsManager.partitionOperationsExecutor()
        );

        // TODO: IGNITE-16841 - use common RocksDB instance to store cluster state as well.
        clusterStateStorage = new RocksDbClusterStateStorage(workDir.resolve(CMG_DB_PATH), name);

        var logicalTopology = new LogicalTopologyImpl(clusterStateStorage);

        ConfigurationTreeGenerator distributedConfigurationGenerator = new ConfigurationTreeGenerator(
                modules.distributed().rootKeys(),
                modules.distributed().schemaExtensions(),
                modules.distributed().polymorphicSchemaExtensions()
        );

        ConfigurationValidator distributedCfgValidator = ConfigurationValidatorImpl.withDefaultValidators(
                distributedConfigurationGenerator,
                modules.distributed().validators()
        );

        ConfigurationDynamicDefaultsPatcher clusterCfgDynamicDefaultsPatcher = new ConfigurationDynamicDefaultsPatcherImpl(
                modules.distributed(),
                distributedConfigurationGenerator
        );

        clusterInitializer = new ClusterInitializer(
                clusterSvc,
                clusterCfgDynamicDefaultsPatcher,
                distributedCfgValidator
        );

        NodeAttributesCollector nodeAttributesCollector =
                new NodeAttributesCollector(
                        nodeConfigRegistry.getConfiguration(NodeAttributesConfiguration.KEY),
                        nodeConfigRegistry.getConfiguration(StorageConfiguration.KEY)
                );

        var clusterStateStorageMgr =  new ClusterStateStorageManager(clusterStateStorage);
        var validationManager = new ValidationManager(clusterStateStorageMgr, logicalTopology);

        cmgMgr = new ClusterManagementGroupManager(
                vaultMgr,
                clusterSvc,
                clusterInitializer,
                raftMgr,
                clusterStateStorageMgr,
                logicalTopology,
                validationManager,
                nodeConfigRegistry.getConfiguration(ClusterManagementConfiguration.KEY),
                nodeAttributesCollector,
                failureProcessor
        );

        logicalTopologyService = new LogicalTopologyServiceImpl(logicalTopology, cmgMgr);

        var topologyAwareRaftGroupServiceFactory = new TopologyAwareRaftGroupServiceFactory(
                clusterSvc,
                logicalTopologyService,
                Loza.FACTORY,
                raftGroupEventsClientListener
        );

        CompletableFuture<LongSupplier> maxClockSkewMillisFuture = new CompletableFuture<>();

        metaStorageMgr = new MetaStorageManagerImpl(
                clusterSvc,
                cmgMgr,
                logicalTopologyService,
                raftMgr,
                new RocksDbKeyValueStorage(name, workDir.resolve(METASTORAGE_DB_PATH), failureProcessor),
                clock,
                topologyAwareRaftGroupServiceFactory,
                metricManager
        );

        this.cfgStorage = new DistributedConfigurationStorage(name, metaStorageMgr);

        clusterCfgMgr = new ConfigurationManager(
                modules.distributed().rootKeys(),
                cfgStorage,
                distributedConfigurationGenerator,
                distributedCfgValidator
        );

        ConfigurationRegistry clusterConfigRegistry = clusterCfgMgr.configurationRegistry();

        metaStorageMgr.configure(clusterConfigRegistry.getConfiguration(MetaStorageConfiguration.KEY));

        SchemaSynchronizationConfiguration schemaSyncConfig = clusterConfigRegistry.getConfiguration(
                SchemaSynchronizationConfiguration.KEY
        );

        clockService = new ClockServiceImpl(clock, clockWaiter, new SameValueLongSupplier(() -> schemaSyncConfig.maxClockSkew().value()));

        metaStorageMgr.addElectionListener(
                new IdempotentCacheVacuumizer(
                        name,
                        threadPoolsManager.commonScheduler(),
                        metaStorageMgr::evictIdempotentCommandsCache,
                        raftConfiguration.retryTimeout().value(),
                        clockService,
                        1,
                        1,
                        MINUTES
                )
        );

        // TODO remove.
        maxClockSkewMillisFuture.complete(clockService::maxClockSkewMillis);

        Consumer<LongFunction<CompletableFuture<?>>> registry = c -> metaStorageMgr.registerRevisionUpdateListener(c::apply);

        placementDriverMgr = new PlacementDriverManager(
                name,
                metaStorageMgr,
                MetastorageGroupId.INSTANCE,
                clusterSvc,
                cmgMgr::metaStorageNodes,
                logicalTopologyService,
                raftMgr,
                topologyAwareRaftGroupServiceFactory,
                clockService
        );

        ReplicationConfiguration replicationConfig = clusterConfigRegistry.getConfiguration(ReplicationConfiguration.KEY);

        ReplicaService replicaSvc = new ReplicaService(
                messagingServiceReturningToStorageOperationsPool,
                clock,
                threadPoolsManager.partitionOperationsExecutor(),
                replicationConfig,
                threadPoolsManager.commonScheduler()
        );

        LongSupplier partitionIdleSafeTimePropagationPeriodMsSupplier = partitionIdleSafeTimePropagationPeriodMsSupplier(replicationConfig);

        ScheduledExecutorService rebalanceScheduler = new ScheduledThreadPoolExecutor(REBALANCE_SCHEDULER_POOL_SIZE,
                NamedThreadFactory.create(name, "rebalance-scheduler", LOG));

        // TODO: IGNITE-22222 this instantiation should be moved inside ReplicaManager's constructor
        Marshaller raftMarshaller = new ThreadLocalPartitionCommandsMarshaller(clusterSvc.serializationRegistry());

        volatileLogStorageFactoryCreator = new VolatileLogStorageFactoryCreator(name, workDir.resolve("volatile-log-spillout"));

        replicaMgr = new ReplicaManager(
                name,
                clusterSvc,
                cmgMgr,
                clockService,
                Set.of(PartitionReplicationMessageGroup.class, TxMessageGroup.class),
                placementDriverMgr.placementDriver(),
                threadPoolsManager.partitionOperationsExecutor(),
                partitionIdleSafeTimePropagationPeriodMsSupplier,
                failureProcessor,
                raftMarshaller,
                topologyAwareRaftGroupServiceFactory,
                raftMgr,
                volatileLogStorageFactoryCreator,
                threadPoolsManager.tableIoExecutor()
        );

        metricManager.configure(clusterConfigRegistry.getConfiguration(MetricConfiguration.KEY));

        DataStorageModules dataStorageModules = new DataStorageModules(
                ServiceLoader.load(DataStorageModule.class, serviceProviderClassLoader)
        );

        Path storagePath = getPartitionsStorePath(workDir);

        GcConfiguration gcConfig = clusterConfigRegistry.getConfiguration(GcConfiguration.KEY);

        LogSyncer logSyncer = logStorageFactory;

        Map<String, StorageEngine> storageEngines = dataStorageModules.createStorageEngines(
                name,
                nodeConfigRegistry,
                storagePath,
                longJvmPauseDetector,
                failureProcessor,
                logSyncer,
                clock
        );

        dataStorageMgr = new DataStorageManager(
                applyThreadAssertionsIfNeeded(storageEngines),
                nodeConfigRegistry.getConfiguration(StorageConfiguration.KEY)
        );

        outgoingSnapshotsManager = new OutgoingSnapshotsManager(name, clusterSvc.messagingService());

        LongSupplier delayDurationMsSupplier = delayDurationMsSupplier(schemaSyncConfig);

        CatalogManagerImpl catalogManager = new CatalogManagerImpl(
                new UpdateLogImpl(metaStorageMgr),
                clockService,
                delayDurationMsSupplier,
                partitionIdleSafeTimePropagationPeriodMsSupplier
        );

        CatalogCompactionRunner catalogCompactionRunner = new CatalogCompactionRunner(
                name,
                catalogManager,
                clusterSvc.messagingService(),
                logicalTopologyService,
                placementDriverMgr.placementDriver(),
                clockService,
                threadPoolsManager.commonScheduler()
        );

        metaStorageMgr.addElectionListener(catalogCompactionRunner::updateCoordinator);

        systemViewManager = new SystemViewManagerImpl(name, catalogManager);
        nodeAttributesCollector.register(systemViewManager);
        logicalTopology.addEventListener(systemViewManager);
        systemViewManager.register(catalogManager);

        this.catalogManager = catalogManager;
        this.catalogCompactionRunner = catalogCompactionRunner;

        lowWatermark = new LowWatermarkImpl(
                name,
                gcConfig.lowWatermark(),
                clockService,
                vaultMgr,
                failureProcessor,
                clusterSvc.messagingService()
        );

        lowWatermark.listen(LowWatermarkEvent.LOW_WATERMARK_CHANGED,
                params -> catalogCompactionRunner.onLowWatermarkChanged(((ChangeLowWatermarkEventParameters) params).newLowWatermark()));

        this.indexMetaStorage = new IndexMetaStorage(catalogManager, lowWatermark, metaStorageMgr);

        raftMgr.appendEntriesRequestInterceptor(new CheckCatalogVersionOnAppendEntries(catalogManager));
        raftMgr.actionRequestInterceptor(new CheckCatalogVersionOnActionRequest(catalogManager));

        SchemaSyncService schemaSyncService = new SchemaSyncServiceImpl(metaStorageMgr.clusterTime(), delayDurationMsSupplier);

        schemaManager = new SchemaManager(registry, catalogManager);

        distributionZoneManager = new DistributionZoneManager(
                name,
                registry,
                metaStorageMgr,
                logicalTopologyService,
                catalogManager,
                rebalanceScheduler
        );

        partitionReplicaLifecycleManager = new PartitionReplicaLifecycleManager(
                catalogManager,
                replicaMgr,
                distributionZoneManager,
                metaStorageMgr,
                clusterSvc.topologyService(),
                lowWatermark,
                threadPoolsManager.tableIoExecutor(),
                rebalanceScheduler,
                threadPoolsManager.partitionOperationsExecutor()
        );

        TransactionConfiguration txConfig = clusterConfigRegistry.getConfiguration(TransactionConfiguration.KEY);

        indexNodeFinishedRwTransactionsChecker = new IndexNodeFinishedRwTransactionsChecker(
                catalogManager,
                clusterSvc.messagingService(),
                clock
        );

        resourcesRegistry = new RemotelyTriggeredResourceRegistry();

        var transactionInflights = new TransactionInflights(placementDriverMgr.placementDriver(), clockService);

        // TODO: IGNITE-19344 - use nodeId that is validated on join (and probably generated differently).
        txManager = new TxManagerImpl(
                name,
                txConfig,
                messagingServiceReturningToStorageOperationsPool,
                clusterSvc.topologyService(),
                replicaSvc,
                lockMgr,
                clockService,
                new TransactionIdGenerator(() -> clusterSvc.nodeName().hashCode()),
                placementDriverMgr.placementDriver(),
                partitionIdleSafeTimePropagationPeriodMsSupplier,
                indexNodeFinishedRwTransactionsChecker,
                threadPoolsManager.partitionOperationsExecutor(),
                resourcesRegistry,
                transactionInflights,
                lowWatermark
        );

        resourceVacuumManager = new ResourceVacuumManager(
                name,
                resourcesRegistry,
                clusterSvc.topologyService(),
                messagingServiceReturningToStorageOperationsPool,
                transactionInflights,
                txManager
        );

        StorageUpdateConfiguration storageUpdateConfiguration = clusterConfigRegistry.getConfiguration(StorageUpdateConfiguration.KEY);

        distributedTblMgr = new TableManager(
                name,
                registry,
                gcConfig,
                txConfig,
                storageUpdateConfiguration,
                messagingServiceReturningToStorageOperationsPool,
                clusterSvc.topologyService(),
                clusterSvc.serializationRegistry(),
                replicaMgr,
                lockMgr,
                replicaSvc,
                txManager,
                dataStorageMgr,
                storagePath,
                metaStorageMgr,
                schemaManager,
                threadPoolsManager.tableIoExecutor(),
                threadPoolsManager.partitionOperationsExecutor(),
                rebalanceScheduler,
                clock,
                clockService,
                outgoingSnapshotsManager,
                distributionZoneManager,
                schemaSyncService,
                catalogManager,
                observableTimestampTracker,
                placementDriverMgr.placementDriver(),
                this::bareSql,
                resourcesRegistry,
                lowWatermark,
                transactionInflights,
                indexMetaStorage,
                logSyncer,
                partitionReplicaLifecycleManager
        );

        disasterRecoveryManager = new DisasterRecoveryManager(
                threadPoolsManager.tableIoExecutor(),
                messagingServiceReturningToStorageOperationsPool,
                metaStorageMgr,
                catalogManager,
                distributionZoneManager,
                raftMgr,
                clusterSvc.topologyService(),
                distributedTblMgr,
                metricManager
        );

        systemViewManager.register(disasterRecoveryManager);

        indexManager = new IndexManager(
                schemaManager,
                distributedTblMgr,
                catalogManager,
                threadPoolsManager.tableIoExecutor(),
                registry,
                lowWatermark
        );

        indexBuildingManager = new IndexBuildingManager(
                name,
                replicaSvc,
                catalogManager,
                metaStorageMgr,
                indexManager,
                indexMetaStorage,
                placementDriverMgr.placementDriver(),
                clusterSvc,
                logicalTopologyService,
                clockService
        );

        qryEngine = new SqlQueryProcessor(
                clusterSvc,
                logicalTopologyService,
                distributedTblMgr,
                schemaManager,
                dataStorageMgr,
                replicaSvc,
                clockService,
                schemaSyncService,
                catalogManager,
                metricManager,
                systemViewManager,
                failureProcessor,
                partitionIdleSafeTimePropagationPeriodMsSupplier,
                placementDriverMgr.placementDriver(),
                clusterConfigRegistry.getConfiguration(SqlDistributedConfiguration.KEY),
                nodeConfigRegistry.getConfiguration(SqlLocalConfiguration.KEY),
                transactionInflights,
                txManager,
                threadPoolsManager.commonScheduler()
        );

        sql = new IgniteSqlImpl(qryEngine, observableTimestampTracker);

        var deploymentManagerImpl = new DeploymentManagerImpl(
                clusterSvc,
                new DeploymentUnitStoreImpl(metaStorageMgr),
                logicalTopologyService,
                workDir,
                nodeConfigRegistry.getConfiguration(DeploymentConfiguration.KEY),
                cmgMgr,
                name
        );
        deploymentManager = deploymentManagerImpl;

        ComputeConfiguration computeCfg = nodeConfigRegistry.getConfiguration(ComputeConfiguration.KEY);
        InMemoryComputeStateMachine stateMachine = new InMemoryComputeStateMachine(computeCfg, name);
        computeComponent = new ComputeComponentImpl(
                name,
                clusterSvc.messagingService(),
                clusterSvc.topologyService(),
                logicalTopologyService,
                new JobContextManager(deploymentManagerImpl, deploymentManagerImpl.deploymentUnitAccessor(), new JobClassLoaderFactory()),
                new ComputeExecutorImpl(this, stateMachine, computeCfg),
                computeCfg
        );

        compute = new IgniteComputeImpl(
                placementDriverMgr.placementDriver(),
                clusterSvc.topologyService(),
                distributedTblMgr,
                computeComponent,
                clock
        );

        authenticationManager = createAuthenticationManager();

        ClientConnectorConfiguration clientConnectorConfiguration = nodeConfigRegistry.getConfiguration(ClientConnectorConfiguration.KEY);

        clientHandlerModule = new ClientHandlerModule(
                qryEngine,
                distributedTblMgr,
                // TODO: IGNITE-20232 The observable timestamp should be different for each client.
                new IgniteTransactionsImpl(txManager, new HybridTimestampTracker()),
                compute,
                clusterSvc,
                nettyBootstrapFactory,
                () -> cmgMgr.clusterState().thenApply(ClusterState::clusterTag),
                metricManager,
                new ClientHandlerMetricSource(),
                authenticationManager,
                clockService,
                schemaSyncService,
                catalogManager,
                placementDriverMgr.placementDriver(),
                clientConnectorConfiguration,
                lowWatermark
        );

        restComponent = createRestComponent(name);
    }

    private static Map<String, StorageEngine> applyThreadAssertionsIfNeeded(Map<String, StorageEngine> storageEngines) {
        boolean enabled = ThreadAssertions.enabled();

        LOG.info("Thread assertions enablement status: {}", enabled);

        if (!enabled) {
            return storageEngines;
        }

        Map<String, StorageEngine> decoratedEngines = new HashMap<>();

        for (Entry<String, StorageEngine> entry : storageEngines.entrySet()) {
            decoratedEngines.put(entry.getKey(), new ThreadAssertingStorageEngine(entry.getValue()));
        }

        return Map.copyOf(decoratedEngines);
    }

    private static SameValueLongSupplier delayDurationMsSupplier(SchemaSynchronizationConfiguration schemaSyncConfig) {
        return new SameValueLongSupplier(() -> schemaSyncConfig.delayDuration().value());
    }

    private static LongSupplier partitionIdleSafeTimePropagationPeriodMsSupplier(ReplicationConfiguration replicationConfig) {
        return new SameValueLongSupplier(() -> replicationConfig.idleSafeTimePropagationDuration().value());
    }

    private AuthenticationManager createAuthenticationManager() {
        SecurityConfiguration securityConfiguration = clusterCfgMgr.configurationRegistry()
                .getConfiguration(SecurityConfiguration.KEY);
        EventLogConfiguration eventLogConfiguration = clusterCfgMgr.configurationRegistry()
                .getConfiguration(EventLogConfiguration.KEY);

        return new AuthenticationManagerImpl(securityConfiguration, new EventLogImpl(eventLogConfiguration));
    }

    private RestComponent createRestComponent(String name) {
        RestManager restManager = new RestManager();
        Supplier<RestFactory> presentationsFactory = () -> new PresentationsFactory(nodeCfgMgr, clusterCfgMgr);
        Supplier<RestFactory> clusterManagementRestFactory = () -> new ClusterManagementRestFactory(clusterSvc, clusterInitializer, cmgMgr);
        Supplier<RestFactory> nodeManagementRestFactory = () -> new NodeManagementRestFactory(lifecycleManager, () -> name,
                new JdbcPortProviderImpl(nodeCfgMgr.configurationRegistry()));
        Supplier<RestFactory> nodeMetricRestFactory = () -> new MetricRestFactory(metricManager);
        Supplier<RestFactory> authProviderFactory = () -> new AuthenticationProviderFactory(authenticationManager);
        Supplier<RestFactory> deploymentCodeRestFactory = () -> new CodeDeploymentRestFactory(deploymentManager);
        Supplier<RestFactory> restManagerFactory = () -> new RestManagerFactory(restManager);
        Supplier<RestFactory> computeRestFactory = () -> new ComputeRestFactory(compute);
        Supplier<RestFactory> disasterRecoveryFactory = () -> new DisasterRecoveryFactory(disasterRecoveryManager);

        RestConfiguration restConfiguration = nodeCfgMgr.configurationRegistry().getConfiguration(RestConfiguration.KEY);

        return new RestComponent(
                List.of(presentationsFactory,
                        clusterManagementRestFactory,
                        nodeManagementRestFactory,
                        nodeMetricRestFactory,
                        deploymentCodeRestFactory,
                        authProviderFactory,
                        restManagerFactory,
                        computeRestFactory,
                        disasterRecoveryFactory
                ),
                restManager,
                restConfiguration
        );
    }

    private static MessageSerializationRegistry createSerializationRegistry(@Nullable ClassLoader classLoader) {
        var serviceLoader = new SerializationRegistryServiceLoader(classLoader);

        var serializationRegistry = new MessageSerializationRegistryImpl();

        serviceLoader.registerSerializationFactories(serializationRegistry);

        return serializationRegistry;
    }

    private static ConfigurationModules loadConfigurationModules(@Nullable ClassLoader classLoader) {
        var modulesProvider = new ServiceLoaderModulesProvider();
        List<ConfigurationModule> modules = modulesProvider.modules(classLoader);

        if (modules.isEmpty()) {
            throw new IllegalStateException("No configuration modules were loaded, this means Ignite cannot start. "
                    + "Please make sure that the classloader for loading services is correct.");
        }

        var configModules = new ConfigurationModules(modules);

        LOG.info("Configuration modules loaded [modules={}, localRoots={}, distRoots={}]",
                modules, configModules.local().rootKeys(), configModules.distributed().rootKeys());

        return configModules;
    }

    /**
     * Starts ignite node.
     *
     * <p>When this method returns, the node is partially started and ready to accept the init command (that is, its
     * REST endpoint is functional).
     *
     * @return Future that will be completed when the node is started.
     */
    CompletableFuture<Void> startAsync() {
        ExecutorService startupExecutor = Executors.newSingleThreadExecutor(
                IgniteThreadFactory.create(name, "start", LOG, STORAGE_READ, STORAGE_WRITE)
        );
        ComponentContext componentContext = new ComponentContext(startupExecutor);

        try {
            JvmMetricSource jvmMetrics = new JvmMetricSource();
            metricManager.registerSource(jvmMetrics);
            metricManager.enable(jvmMetrics);

            OsMetricSource osMetrics = new OsMetricSource();
            metricManager.registerSource(osMetrics);
            metricManager.enable(osMetrics);

            // Start the components that are required to join the cluster.
            // TODO https://issues.apache.org/jira/browse/IGNITE-22570
            return lifecycleManager.startComponentsAsync(
                    componentContext,
                    longJvmPauseDetector,
                    vaultMgr,
                    nodeCfgMgr,
                    threadPoolsManager,
                    clockWaiter,
                    failureProcessor,
                    criticalWorkerRegistry,
                    nettyBootstrapFactory,
                    nettyWorkersRegistrar,
                    clusterSvc,
                    restComponent,
                    logStorageFactory,
                    raftMgr,
                    clusterStateStorage,
                    cmgMgr,
                    lowWatermark
            ).thenRun(() -> {
                try {
                    vaultMgr.putName(name);

                    clusterSvc.updateMetadata(
                            new NodeMetadata(restComponent.hostName(), restComponent.httpPort(), restComponent.httpsPort()));

                } catch (Throwable e) {
                    startupExecutor.shutdownNow();

                    throw handleStartException(e);
                }
                LOG.info("Components started");
            });
        } catch (Throwable e) {
            startupExecutor.shutdownNow();

            throw handleStartException(e);
        }
    }

    /**
     * Continues the join process, returns the Ignite API future which will be completed when node is joined the logical topology.
     *
     * @return Ignite API future.
     */
    CompletableFuture<Ignite> joinClusterAsync() {
        LOG.info("Joining the cluster");

        ExecutorService joinExecutor = Executors.newSingleThreadExecutor(
                IgniteThreadFactory.create(name, "join", LOG, STORAGE_READ, STORAGE_WRITE)
        );
        ComponentContext componentContext = new ComponentContext(joinExecutor);

        return cmgMgr.joinFuture()
                // Disable REST component during initialization.
                .thenAcceptAsync(unused -> restComponent.disable(), joinExecutor)
                .thenComposeAsync(unused -> {
                    LOG.info("Join complete, starting MetaStorage");

                    try {
                        lifecycleManager.startComponentAsync(metaStorageMgr, componentContext);
                    } catch (NodeStoppingException e) {
                        throw new CompletionException(e);
                    }

                    return metaStorageMgr.recoveryFinishedFuture();
                }, joinExecutor)
                .thenComposeAsync(unused -> initializeClusterConfiguration(joinExecutor), joinExecutor)
                .thenRunAsync(() -> {
                    LOG.info("MetaStorage started, starting the remaining components");

                    // Start all other components after the join request has completed and the node has been validated.
                    try {
                        lifecycleManager.startComponentsAsync(
                                componentContext,
                                catalogManager,
                                catalogCompactionRunner,
                                indexMetaStorage,
                                clusterCfgMgr,
                                authenticationManager,
                                placementDriverMgr,
                                metricManager,
                                distributionZoneManager,
                                computeComponent,
                                replicaMgr,
                                indexNodeFinishedRwTransactionsChecker,
                                txManager,
                                dataStorageMgr,
                                schemaManager,
                                volatileLogStorageFactoryCreator,
                                outgoingSnapshotsManager,
                                partitionReplicaLifecycleManager,
                                distributedTblMgr,
                                disasterRecoveryManager,
                                indexManager,
                                indexBuildingManager,
                                qryEngine,
                                clientHandlerModule,
                                deploymentManager,
                                sql,
                                resourceVacuumManager
                        );

                        // The system view manager comes last because other components
                        // must register system views before it starts.
                        lifecycleManager.startComponentAsync(systemViewManager, componentContext);
                    } catch (NodeStoppingException e) {
                        throw new CompletionException(e);
                    }
                }, joinExecutor)
                .thenComposeAsync(v -> {
                    LOG.info("Components started, performing recovery");

                    return recoverComponentsStateOnStart(joinExecutor, lifecycleManager.allComponentsStartFuture());
                }, joinExecutor)
                .thenComposeAsync(v -> clusterCfgMgr.configurationRegistry().onDefaultsPersisted(), joinExecutor)
                // Signal that local recovery is complete and the node is ready to join the cluster.
                .thenComposeAsync(v -> {
                    LOG.info("Recovery complete, finishing join");

                    return cmgMgr.onJoinReady();
                }, joinExecutor)
                .thenComposeAsync(ignored -> awaitSelfInLocalLogicalTopology(), joinExecutor)
                .thenCompose(ignored -> catalogManager.catalogInitializationFuture())
                .thenRunAsync(() -> {
                    try {
                        // Enable watermark events.
                        lowWatermark.scheduleUpdates();

                        // Enable client requests handling on start complete.
                        clientHandlerModule.enable();

                        // Enable REST component on start complete.
                        restComponent.enable();
                        // Transfer the node to the STARTED state.
                        lifecycleManager.onStartComplete();
                    } catch (NodeStoppingException e) {
                        throw new CompletionException(e);
                    }
                }, joinExecutor)
                .handleAsync((v, e) -> {
                    if (e != null) {
                        throw handleStartException(e);
                    }

                    return (Ignite) this;
                }, joinExecutor)
                // Moving to the common pool on purpose to close the join pool and proceed user's code in the common pool.
                .whenCompleteAsync((res, ex) -> joinExecutor.shutdownNow());
    }

    private CompletableFuture<Void> awaitSelfInLocalLogicalTopology() {
        CompletableFuture<Void> awaitSelfInLogicalTopologyFuture = new CompletableFuture<>();

        LogicalTopologyEventListener awaitSelfListener = new LogicalTopologyEventListener() {
            @Override
            public void onNodeJoined(LogicalNode joinedNode, LogicalTopologySnapshot newTopology) {
                checkSelfInTopology(newTopology, awaitSelfInLogicalTopologyFuture, this);
            }

            @Override
            public void onTopologyLeap(LogicalTopologySnapshot newTopology) {
                checkSelfInTopology(newTopology, awaitSelfInLogicalTopologyFuture, this);
            }
        };

        logicalTopologyService.addEventListener(awaitSelfListener);

        checkSelfInTopology(logicalTopologyService.localLogicalTopology(), awaitSelfInLogicalTopologyFuture, awaitSelfListener);

        return awaitSelfInLogicalTopologyFuture;
    }

    private void checkSelfInTopology(
            LogicalTopologySnapshot logicalTopologySnapshot,
            CompletableFuture<Void> awaitSelfInLogicalTopologyFuture,
            LogicalTopologyEventListener awaitSelfListener
    ) {
        if (logicalTopologySnapshot.nodes().stream().map(LogicalNode::id).collect(toSet()).contains(id())) {
            awaitSelfInLogicalTopologyFuture.complete(null);
            logicalTopologyService.removeEventListener(awaitSelfListener);
        }
    }

    private RuntimeException handleStartException(Throwable e) {
        String errMsg = "Unable to start [node=" + name + "]";

        LOG.error(errMsg, e);

        IgniteException igniteException = new IgniteException(errMsg, e);

        ExecutorService lifecycleExecutor = stopExecutor();

        try {
            lifecycleManager.stopNode(new ComponentContext(lifecycleExecutor)).get();
        } catch (Exception ex) {
            igniteException.addSuppressed(ex);
        } finally {
            lifecycleExecutor.shutdownNow();
        }

        return igniteException;
    }

    /**
     * Synchronously stops ignite node.
     */
    public void stop() throws ExecutionException, InterruptedException {
        stopAsync().get();
    }

    /**
     * Asynchronously stops ignite node.
     */
    public CompletableFuture<Void> stopAsync() {
        ExecutorService lifecycleExecutor = stopExecutor();

        // TODO https://issues.apache.org/jira/browse/IGNITE-22570
        return lifecycleManager.stopNode(new ComponentContext(lifecycleExecutor))
                // Moving to the common pool on purpose to close the stop pool and proceed user's code in the common pool.
                .whenCompleteAsync((res, ex) -> lifecycleExecutor.shutdownNow());
    }

    private ExecutorService stopExecutor() {
        return Executors.newSingleThreadExecutor(
                IgniteThreadFactory.create(name, "stop", LOG, STORAGE_READ, STORAGE_WRITE)
        );
    }

    /** {@inheritDoc} */
    @Override
    public IgniteTables tables() {
        return new PublicApiThreadingIgniteTables(distributedTblMgr, asyncContinuationExecutor);
    }

    public DisasterRecoveryManager disasterRecoveryManager() {
        return disasterRecoveryManager;
    }

    @TestOnly
    public QueryProcessor queryEngine() {
        return qryEngine;
    }

    @TestOnly
    public SystemViewManager systemViewManager() {
        return systemViewManager;
    }

    @TestOnly
    public MetaStorageManager metaStorageManager() {
        return metaStorageMgr;
    }

    @TestOnly
    public FailureProcessor failureProcessor() {
        return failureProcessor;
    }

    @TestOnly
    public MetricManager metricManager() {
        return metricManager;
    }

    /** {@inheritDoc} */
    @Override
    public IgniteTransactions transactions() {
        IgniteTransactionsImpl transactions = new IgniteTransactionsImpl(txManager, observableTimestampTracker);
        return new PublicApiThreadingIgniteTransactions(transactions, asyncContinuationExecutor);
    }

    private IgniteSql bareSql() {
        return sql;
    }

    /** {@inheritDoc} */
    @Override
    public IgniteSql sql() {
        return new PublicApiThreadingIgniteSql(sql, asyncContinuationExecutor);
    }

    /** {@inheritDoc} */
    @Override
    public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override
    public IgniteCompute compute() {
        return new AntiHijackIgniteCompute(compute, asyncContinuationExecutor);
    }

    /** {@inheritDoc} */
    @Override
    public Collection<ClusterNode> clusterNodes() {
        return clusterSvc.topologyService().allMembers();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Collection<ClusterNode>> clusterNodesAsync() {
        return completedFuture(clusterNodes());
    }

    @Override
    public IgniteCatalog catalog() {
        return new PublicApiThreadingIgniteCatalog(new IgniteCatalogSqlImpl(sql, distributedTblMgr), asyncContinuationExecutor);
    }

    /**
     * Returns node configuration.
     */
    public ConfigurationRegistry nodeConfiguration() {
        return nodeCfgMgr.configurationRegistry();
    }

    /**
     * Returns cluster configuration.
     */
    public ConfigurationRegistry clusterConfiguration() {
        return clusterCfgMgr.configurationRegistry();
    }

    public HybridTimestampTracker observableTimeTracker() {
        return observableTimestampTracker;
    }

    /**
     * Returns the id of the current node.
     */
    // TODO: should be encapsulated in local properties, see https://issues.apache.org/jira/browse/IGNITE-15131
    public String id() {
        return clusterSvc.topologyService().localMember().id();
    }

    /**
     * Returns the local HTTP address of REST endpoints.
     *
     * @return address or null if HTTP is not enabled.
     * @throws IgniteInternalException if the REST module is not started.
     */
    // TODO: should be encapsulated in local properties, see https://issues.apache.org/jira/browse/IGNITE-15131
    @Nullable
    public NetworkAddress restHttpAddress() {
        String host = restComponent.hostName();
        int port = restComponent.httpPort();
        if (port != -1) {
            return new NetworkAddress(host, port);
        } else {
            return null;
        }
    }

    /**
     * Returns the local HTTPS address of REST endpoints.
     *
     * @return address or null if HTTPS is not enabled.
     * @throws IgniteInternalException if the REST module is not started.
     */
    // TODO: should be encapsulated in local properties, see https://issues.apache.org/jira/browse/IGNITE-15131
    @Nullable
    public NetworkAddress restHttpsAddress() {
        String host = restComponent.hostName();
        int port = restComponent.httpsPort();
        if (port != -1) {
            return new NetworkAddress(host, port);
        } else {
            return null;
        }
    }

    /**
     * Returns the local address of the Thin Client.
     *
     * @throws IgniteInternalException if the Client module is not started.
     */
    // TODO: should be encapsulated in local properties, see https://issues.apache.org/jira/browse/IGNITE-15131
    public NetworkAddress clientAddress() {
        return NetworkAddress.from(clientHandlerModule.localAddress());
    }

    /**
     * Initializes the cluster that this node is present in.
     *
     * @param metaStorageNodeNames names of nodes that will host the Meta Storage.
     * @param cmgNodeNames names of nodes that will host the CMG.
     * @param clusterName Human-readable name of a cluster.
     * @param clusterConfiguration cluster configuration, that will be applied after init.
     * @return Completable future which completes when the cluster is initialized.
     * @throws NodeStoppingException If node stopping intention was detected.
     */
    CompletableFuture<Void> initClusterAsync(
            Collection<String> metaStorageNodeNames,
            Collection<String> cmgNodeNames,
            String clusterName,
            String clusterConfiguration
    ) throws NodeStoppingException {
        return cmgMgr.initClusterAsync(metaStorageNodeNames, cmgNodeNames, clusterName, clusterConfiguration);
    }

    /**
     * Checks if the local revision is {@code 0} and initializes the cluster configuration with the specified user-provided configuration
     * upon cluster initialization. If the local revision is not {@code 0}, does nothing.
     */
    private CompletableFuture<Void> initializeClusterConfiguration(ExecutorService startupExecutor) {
        return cfgStorage.localRevision()
                .thenComposeAsync(appliedRevision -> {
                    if (appliedRevision != 0) {
                        return nullCompletedFuture();
                    } else {
                        return cmgMgr.initialClusterConfigurationFuture()
                                .thenAcceptAsync(initialConfigHocon -> {
                                    if (initialConfigHocon == null) {
                                        return;
                                    }

                                    Config config = ConfigFactory.parseString(initialConfigHocon);
                                    ConfigurationSource hoconSource = HoconConverter.hoconSource(config.root());
                                    clusterCfgMgr.configurationRegistry().initializeConfigurationWith(hoconSource);
                                }, startupExecutor);
                    }
                }, startupExecutor);
    }

    /**
     * Recovers components state on start by invoking configuration listeners ({@link #notifyConfigurationListeners()} and deploying watches
     * after that.
     */
    private CompletableFuture<?> recoverComponentsStateOnStart(ExecutorService startupExecutor, CompletableFuture<Void> startFuture) {
        CompletableFuture<Void> startupConfigurationUpdate = notifyConfigurationListeners();
        CompletableFuture<Void> startupRevisionUpdate = metaStorageMgr.notifyRevisionUpdateListenerOnStart();

        return CompletableFuture.allOf(startupConfigurationUpdate, startupRevisionUpdate, startFuture)
                .thenComposeAsync(t -> {
                    // Deploy all registered watches because all components are ready and have registered their listeners.
                    return metaStorageMgr.deployWatches();
                }, startupExecutor);
    }

    /**
     * Notify all listeners of current configurations.
     */
    private CompletableFuture<Void> notifyConfigurationListeners() {
        return CompletableFuture.allOf(
                nodeConfiguration().notifyCurrentConfigurationListeners(),
                clusterConfiguration().notifyCurrentConfigurationListeners()
        );
    }

    /**
     * Starts the Vault component.
     */
    private static VaultManager createVault(Path workDir) {
        Path vaultPath = workDir.resolve(VAULT_DB_PATH);

        try {
            Files.createDirectories(vaultPath);
        } catch (IOException e) {
            throw new IgniteInternalException(e);
        }

        return new VaultManager(new PersistentVaultService(vaultPath));
    }

    /**
     * Returns a path to the partitions store directory. Creates a directory if it doesn't exist.
     *
     * @param workDir Ignite work directory.
     * @return Partitions store path.
     */
    private static Path getPartitionsStorePath(Path workDir) {
        Path partitionsStore = workDir.resolve(PARTITIONS_STORE_PATH);

        try {
            Files.createDirectories(partitionsStore);
        } catch (IOException e) {
            throw new IgniteInternalException("Failed to create directory for partitions storage: " + e.getMessage(), e);
        }

        return partitionsStore;
    }

    @TestOnly
    public Loza raftManager() {
        return raftMgr;
    }

    @TestOnly
    public ClusterNode node() {
        return clusterSvc.topologyService().localMember();
    }

    @TestOnly
    public DistributionZoneManager distributionZoneManager() {
        return distributionZoneManager;
    }

    @TestOnly
    public LogicalTopologyService logicalTopologyService() {
        return logicalTopologyService;
    }

    @TestOnly
    public IgniteDeployment deployment() {
        return deploymentManager;
    }

    // TODO: IGNITE-18493 - remove/move this
    @TestOnly
    public void dropMessages(BiPredicate<@Nullable String, NetworkMessage> predicate) {
        ((DefaultMessagingService) clusterSvc.messagingService()).dropMessages(predicate);
    }

    // TODO: IGNITE-18493 - remove/move this
    @TestOnly
    @Nullable
    public BiPredicate<String, NetworkMessage> dropMessagesPredicate() {
        return ((DefaultMessagingService) clusterSvc.messagingService()).dropMessagesPredicate();
    }

    // TODO IGNITE-18493 - remove/move this
    @TestOnly
    public void sendFakeMessage(String recipientConsistentId, NetworkMessage msg) {
        clusterSvc.messagingService().send(recipientConsistentId, ChannelType.DEFAULT, msg);
    }

    // TODO: IGNITE-18493 - remove/move this
    @TestOnly
    public void stopDroppingMessages() {
        ((DefaultMessagingService) clusterSvc.messagingService()).stopDroppingMessages();
    }

    /** Returns the node's hybrid clock. */
    @TestOnly
    public HybridClock clock() {
        return clock;
    }

    @TestOnly
    public ClockService clockService() {
        return clockService;
    }

    /** Returns the node's transaction manager. */
    @TestOnly
    public TxManager txManager() {
        return txManager;
    }

    /** Returns the node's placement driver service. */
    @TestOnly
    public PlacementDriver placementDriver() {
        return placementDriverMgr.placementDriver();
    }

    /** Returns the node's catalog manager. */
    @TestOnly
    public CatalogManager catalogManager() {
        return catalogManager;
    }

    /** Returns the node's catalog compaction runner. */
    @TestOnly
    public CatalogCompactionRunner catalogCompactionRunner() {
        return catalogCompactionRunner;
    }

    /** Returns {@link NettyBootstrapFactory}. */
    @TestOnly
    public NettyBootstrapFactory nettyBootstrapFactory() {
        return nettyBootstrapFactory;
    }

    /** Returns cluster service (cluster network manager). */
    @TestOnly
    public ClusterService clusterService() {
        return clusterSvc;
    }

    /** Returns resources registry. */
    @TestOnly
    public RemotelyTriggeredResourceRegistry resourcesRegistry() {
        return resourcesRegistry;
    }

    /** Returns low watermark. */
    @TestOnly
    public LowWatermarkImpl lowWatermark() {
        return lowWatermark;
    }
}
