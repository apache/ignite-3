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

import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_READ;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_WRITE;
import static org.apache.ignite.internal.util.CompletableFutures.copyStateTo;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.extractCodeFrom;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapRootCause;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import io.micronaut.context.ApplicationContext;
import io.micronaut.inject.qualifiers.Qualifiers;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiPredicate;
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteServer;
import org.apache.ignite.catalog.IgniteCatalog;
import org.apache.ignite.client.handler.ClientHandlerModule;
import org.apache.ignite.client.handler.ClientInboundMessageHandler;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.internal.app.di.NodeSeedParams;
import org.apache.ignite.internal.app.di.PostConstructionWiring;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogManagerImpl;
import org.apache.ignite.internal.catalog.PartitionCountCalculator;
import org.apache.ignite.internal.catalog.PartitionCountCalculatorWrapper;
import org.apache.ignite.internal.catalog.compaction.CatalogCompactionRunner;
import org.apache.ignite.internal.catalog.sql.IgniteCatalogSqlImpl;
import org.apache.ignite.internal.cluster.management.ClusterInitializer;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.ClusterState;
import org.apache.ignite.internal.cluster.management.InvalidNodeConfigurationException;
import org.apache.ignite.internal.cluster.management.raft.ClusterStateStorage;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.components.NodeProperties;
import org.apache.ignite.internal.compute.AntiHijackIgniteCompute;
import org.apache.ignite.internal.compute.ComputeComponentImpl;
import org.apache.ignite.internal.compute.IgniteComputeInternal;
import org.apache.ignite.internal.configuration.ComponentWorkingDir;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.JdbcPortProviderImpl;
import org.apache.ignite.internal.configuration.hocon.HoconConverter;
import org.apache.ignite.internal.configuration.storage.ConfigurationStorage;
import org.apache.ignite.internal.configuration.storage.DistributedConfigurationStorage;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.deployunit.DeploymentManagerImpl;
import org.apache.ignite.internal.deployunit.IgniteDeployment;
import org.apache.ignite.internal.di.IgniteComponentLifecycleManager;
import org.apache.ignite.internal.di.IgniteDiContext;
import org.apache.ignite.internal.di.StartupPhase;
import org.apache.ignite.internal.disaster.system.ClusterIdService;
import org.apache.ignite.internal.disaster.system.ServerRestarter;
import org.apache.ignite.internal.disaster.system.SystemDisasterRecoveryManager;
import org.apache.ignite.internal.disaster.system.SystemDisasterRecoveryManagerImpl;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.eventlog.impl.EventLogImpl;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.ClockServiceImpl;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.lowwatermark.LowWatermarkImpl;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.metrics.MetricManagerImpl;
import org.apache.ignite.internal.metrics.messaging.MetricMessaging;
import org.apache.ignite.internal.network.ChannelType;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.DefaultMessagingService;
import org.apache.ignite.internal.network.IgniteClusterImpl;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.NettyBootstrapFactory;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.PublicApiThreadingIgniteCluster;
import org.apache.ignite.internal.partition.replicator.PartitionReplicaLifecycleManager;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.PlacementDriverManager;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.storage.LogStorageManager;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.rest.RestComponent;
import org.apache.ignite.internal.rest.RestFactory;
import org.apache.ignite.internal.rest.RestManager;
import org.apache.ignite.internal.rest.RestManagerFactory;
import org.apache.ignite.internal.rest.authentication.AuthenticationProviderFactory;
import org.apache.ignite.internal.rest.cluster.ClusterManagementRestFactory;
import org.apache.ignite.internal.rest.cluster.DataNodesRestFactory;
import org.apache.ignite.internal.rest.compute.ComputeRestFactory;
import org.apache.ignite.internal.rest.configuration.PresentationsFactory;
import org.apache.ignite.internal.rest.configuration.RestConfiguration;
import org.apache.ignite.internal.rest.configuration.RestExtensionConfiguration;
import org.apache.ignite.internal.rest.deployment.CodeDeploymentRestFactory;
import org.apache.ignite.internal.rest.events.RestEventsFactory;
import org.apache.ignite.internal.rest.metrics.MetricRestFactory;
import org.apache.ignite.internal.rest.node.NodeManagementRestFactory;
import org.apache.ignite.internal.rest.node.NodePropertiesFactory;
import org.apache.ignite.internal.rest.recovery.DisasterRecoveryFactory;
import org.apache.ignite.internal.rest.recovery.system.SystemDisasterRecoveryFactory;
import org.apache.ignite.internal.rest.sql.SqlQueryRestFactory;
import org.apache.ignite.internal.schema.SchemaSafeTimeTrackerImpl;
import org.apache.ignite.internal.security.authentication.AuthenticationManager;
import org.apache.ignite.internal.sql.api.IgniteSqlImpl;
import org.apache.ignite.internal.sql.api.PublicApiThreadingIgniteSql;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.SqlQueryProcessor;
import org.apache.ignite.internal.sql.engine.exec.kill.KillCommandHandler;
import org.apache.ignite.internal.systemview.SystemViewManagerImpl;
import org.apache.ignite.internal.systemview.api.SystemViewManager;
import org.apache.ignite.internal.table.distributed.PublicApiThreadingIgniteTables;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.distributed.disaster.DisasterRecoveryManager;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.threading.PublicApiThreadingIgniteCatalog;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.IgniteTransactionsImpl;
import org.apache.ignite.internal.tx.impl.PublicApiThreadingIgniteTransactions;
import org.apache.ignite.internal.tx.impl.RemotelyTriggeredResourceRegistry;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.storage.state.rocksdb.TxStateRocksDbSharedStorage;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.network.IgniteCluster;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NodeMetadata;
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

    /** Ignite node name. */
    private final String name;

    private final Path workDir;

    /** Lifecycle manager. */
    private final LifecycleManager lifecycleManager;

    /** Micronaut DI context for component construction and dependency injection. */
    private final ApplicationContext diContext;

    /** Lifecycle manager for DI-managed components. */
    private final IgniteComponentLifecycleManager componentLifecycleManager;

    /** Vault manager. */
    private final VaultManager vaultMgr;

    private final NodePropertiesImpl nodeProperties;

    /** Sql query engine. */
    private final SqlQueryProcessor qryEngine;

    /** Sql API facade. */
    private final IgniteSqlImpl sql;

    /** Configuration registry that handles node (local) configuration. */
    private final ConfigurationRegistry nodeConfigRegistry;

    private final ClusterIdService clusterIdService;

    /** Cluster service (cluster network manager). */
    private final ClusterService clusterSvc;

    private final ComputeComponentImpl computeComponent;

    /** Failure processor. */
    private final FailureManager failureManager;

    /** Netty bootstrap factory. */
    private final NettyBootstrapFactory nettyBootstrapFactory;

    /** Raft manager. */
    private final Loza raftMgr;

    /** Meta storage manager. */
    private final MetaStorageManagerImpl metaStorageMgr;

    /** Placement driver manager. */
    private final PlacementDriverManager placementDriverMgr;

    /** Configuration registry that handles cluster (distributed) configuration. */
    private final ConfigurationRegistry clusterConfigRegistry;

    /** Cluster initializer. */
    private final ClusterInitializer clusterInitializer;

    /** Replica manager. */
    private final ReplicaManager replicaMgr;

    /** Transactions manager. */
    private final TxManagerImpl txManager;

    /** Distributed table manager. */
    private final TableManager distributedTblMgr;

    private final TxStateRocksDbSharedStorage sharedTxStateStorage;

    /** Disaster recovery manager. */
    private final DisasterRecoveryManager disasterRecoveryManager;

    /** Rest module. */
    private final RestComponent restComponent;

    private final ClusterStateStorage clusterStateStorage;

    private final ClusterManagementGroupManager cmgMgr;

    private final LogicalTopologyService logicalTopologyService;

    private final ComponentWorkingDir partitionsWorkDir;

    private final ComponentWorkingDir metastorageWorkDir;

    /** Client handler module. */
    private final ClientHandlerModule clientHandlerModule;

    /** Distributed configuration storage. */
    private final ConfigurationStorage cfgStorage;

    /** Compute. */
    private final IgniteComputeInternal compute;

    private final SchemaSafeTimeTrackerImpl schemaSafeTimeTracker;

    /** Metric manager. */
    private final MetricManagerImpl metricManager;

    /** Metric messaging. */
    private final MetricMessaging metricMessaging;

    private final DeploymentManagerImpl deploymentManager;

    private final DistributionZoneManager distributionZoneManager;

    private final PartitionReplicaLifecycleManager partitionReplicaLifecycleManager;

    /** A hybrid logical clock. */
    private final HybridClock clock;

    private final ClockService clockService;

    private final LowWatermarkImpl lowWatermark;

    private final CatalogManager catalogManager;

    private final CatalogCompactionRunner catalogCompactionRunner;

    private final AuthenticationManager authenticationManager;

    /** Timestamp tracker for embedded transactions. */
    private final HybridTimestampTracker observableTimestampTracker;

    /** System views manager. */
    private final SystemViewManagerImpl systemViewManager;

    /** Remote triggered resources registry. */
    private final RemotelyTriggeredResourceRegistry resourcesRegistry;

    private final SystemDisasterRecoveryManagerImpl systemDisasterRecoveryManager;

    private final IgniteTables publicTables;
    private final IgniteTransactions publicTransactions;
    private final IgniteSql publicSql;
    private final IgniteCompute publicCompute;
    private final IgniteCatalog publicCatalog;
    private final IgniteCluster publicCluster;

    /** Partitions log storage manager for raft. */
    private final LogStorageManager partitionsLogStorageManager;

    private final EventLogImpl eventLog;

    private final KillCommandHandler killCommandHandler;

    private final AtomicBoolean stopGuard = new AtomicBoolean();

    private final CompletableFuture<Void> stopFuture = new CompletableFuture<>();

    @Nullable
    private volatile ClusterState clusterState;

    private final PartitionCountCalculatorWrapper partitionCountCalculatorWrapper;

    /** Future that completes when the node has joined the cluster. */
    private final CompletableFuture<Ignite> joinFuture = new CompletableFuture<>();

    /**
     * The Constructor.
     *
     * @param node Embedded node.
     * @param configPath Path to node configuration in the HOCON format.
     * @param workDir Work directory for the started node. Must not be {@code null}.
     * @param serviceProviderClassLoader The class loader to be used to load provider-configuration files and provider classes, or
     *         {@code null} if the system class loader (or, failing that the bootstrap class loader) is to be used.
     * @param asyncContinuationExecutor Executor in which user-facing futures will be completed.
     */
    IgniteImpl(
            IgniteServer node,
            ServerRestarter restarter,
            Path configPath,
            Path workDir,
            @Nullable ClassLoader serviceProviderClassLoader,
            Executor asyncContinuationExecutor
    ) {
        this.name = node.name();
        this.workDir = workDir;

        lifecycleManager = new LifecycleManager(name);

        // Build the Micronaut DI context with seed singletons.
        Supplier<UUID> clusterIdSupplier = () -> clusterState().clusterTag().clusterId();

        NodeSeedParams seedParams = new NodeSeedParams(
                node, restarter, configPath, workDir, serviceProviderClassLoader, asyncContinuationExecutor,
                clusterIdSupplier
        );

        diContext = IgniteDiContext.builder()
                .withSingleton(seedParams)
                .withSingleton(this)
                .withPackages("org.apache.ignite.internal.app.di")
                .withExcludedPackages("org.apache.ignite.internal.rest")
                .build();

        // Start local configuration early — factory methods read config values during bean construction.
        nodeConfigRegistry = diContext.getBean(ConfigurationRegistry.class, Qualifiers.byName("nodeConfig"));

        try {
            lifecycleManager.startComponentsAsync(new ComponentContext(), nodeConfigRegistry);
        } catch (NodeStoppingException e) {
            throw new AssertionError(String.format("Unexpected exception: [nodeName=%s, configPath=%s]", name, configPath), e);
        }

        LOG.info("Starting node: [name={}, workDir={}, configPath={}]", name, workDir, configPath);
        LOG.info("Local node configuration: {}", convertToHoconString(nodeConfigRegistry));

        // Resolve all DI-managed components. Bean creation is lazy — requesting the first bean triggers
        // its construction plus all transitive dependencies. By the time we're done, all beans are created.

        // Phase 1 components (pre-join). Only fields still used outside the constructor are resolved.
        metricManager = diContext.getBean(MetricManagerImpl.class);
        vaultMgr = diContext.getBean(VaultManager.class);
        nodeProperties = diContext.getBean(NodePropertiesImpl.class);
        failureManager = diContext.getBean(FailureManager.class);
        clusterIdService = diContext.getBean(ClusterIdService.class);
        nettyBootstrapFactory = diContext.getBean(NettyBootstrapFactory.class);
        clusterSvc = diContext.getBean(ClusterService.class);
        clock = diContext.getBean(HybridClock.class);
        clusterStateStorage = diContext.getBean(ClusterStateStorage.class);
        systemDisasterRecoveryManager = diContext.getBean(SystemDisasterRecoveryManagerImpl.class);
        partitionsLogStorageManager = diContext.getBean(LogStorageManager.class, Qualifiers.byName("partitions"));
        partitionsWorkDir = diContext.getBean(ComponentWorkingDir.class, Qualifiers.byName("partitions"));
        metastorageWorkDir = diContext.getBean(ComponentWorkingDir.class, Qualifiers.byName("metastorage"));
        raftMgr = diContext.getBean(Loza.class);
        cmgMgr = diContext.getBean(ClusterManagementGroupManager.class);
        logicalTopologyService = diContext.getBean(LogicalTopologyService.class);
        clusterInitializer = diContext.getBean(ClusterInitializer.class);
        lowWatermark = diContext.getBean(LowWatermarkImpl.class);

        // Phase 2 components (post-join).
        metaStorageMgr = diContext.getBean(MetaStorageManagerImpl.class);
        clusterConfigRegistry = diContext.getBean(ConfigurationRegistry.class, Qualifiers.byName("clusterConfig"));
        cfgStorage = diContext.getBean(DistributedConfigurationStorage.class);
        eventLog = diContext.getBean(EventLogImpl.class);
        clockService = diContext.getBean(ClockServiceImpl.class);
        catalogManager = diContext.getBean(CatalogManagerImpl.class);
        catalogCompactionRunner = diContext.getBean(CatalogCompactionRunner.class);
        placementDriverMgr = diContext.getBean(PlacementDriverManager.class);
        schemaSafeTimeTracker = diContext.getBean(SchemaSafeTimeTrackerImpl.class);
        replicaMgr = diContext.getBean(ReplicaManager.class);
        txManager = diContext.getBean(TxManagerImpl.class);
        distributionZoneManager = diContext.getBean(DistributionZoneManager.class);
        sharedTxStateStorage = diContext.getBean(TxStateRocksDbSharedStorage.class);
        partitionReplicaLifecycleManager = diContext.getBean(PartitionReplicaLifecycleManager.class);
        distributedTblMgr = diContext.getBean(TableManager.class);
        disasterRecoveryManager = diContext.getBean(DisasterRecoveryManager.class);
        qryEngine = diContext.getBean(SqlQueryProcessor.class);
        sql = diContext.getBean(IgniteSqlImpl.class);
        killCommandHandler = diContext.getBean(KillCommandHandler.class);
        computeComponent = diContext.getBean(ComputeComponentImpl.class);
        compute = diContext.getBean(IgniteComputeInternal.class);
        deploymentManager = diContext.getBean(DeploymentManagerImpl.class);
        authenticationManager = diContext.getBean(AuthenticationManager.class);
        clientHandlerModule = diContext.getBean(ClientHandlerModule.class);
        resourcesRegistry = diContext.getBean(RemotelyTriggeredResourceRegistry.class);
        metricMessaging = diContext.getBean(MetricMessaging.class);
        partitionCountCalculatorWrapper = diContext.getBean(PartitionCountCalculatorWrapper.class);
        systemViewManager = diContext.getBean(SystemViewManagerImpl.class);
        observableTimestampTracker = diContext.getBean(HybridTimestampTracker.class);

        // Initialize the DI component lifecycle manager. Exclude components with special startup ordering:
        // nodeConfigRegistry — started early in constructor, before other beans are created
        // clusterConfigRegistry — started manually in joinClusterAsync() (after MetaStorage recovery)
        // metaStorageMgr — started manually in joinClusterAsync() (must start before cluster config init)
        // systemViewManager — must start last, after all system view registrations
        componentLifecycleManager = new IgniteComponentLifecycleManager(diContext);
        componentLifecycleManager.exclude(nodeConfigRegistry, clusterConfigRegistry, metaStorageMgr, systemViewManager);

        // nodeConfigRegistry was already started above; register it for proper reverse-order shutdown.
        componentLifecycleManager.markAsStarted(nodeConfigRegistry);

        // Post-construction wiring: cross-component hookups that cannot be expressed as constructor injection.
        // These are all listener/callback registrations that just store references; they don't require
        // components to be started yet.
        PostConstructionWiring wiring = diContext.getBean(PostConstructionWiring.class);
        wiring.wirePhase2();

        // REST component (depends on joinFuture and lifecycle state, created manually).
        restComponent = createRestComponent(name);

        // Public API wrappers.
        publicTables = new PublicApiThreadingIgniteTables(distributedTblMgr, asyncContinuationExecutor);
        publicTransactions = new PublicApiThreadingIgniteTransactions(
                new IgniteTransactionsImpl(txManager, observableTimestampTracker), asyncContinuationExecutor
        );
        publicSql = new PublicApiThreadingIgniteSql(sql, asyncContinuationExecutor);
        publicCompute = new AntiHijackIgniteCompute(compute, asyncContinuationExecutor);
        publicCatalog = new PublicApiThreadingIgniteCatalog(new IgniteCatalogSqlImpl(sql, distributedTblMgr), asyncContinuationExecutor);
        publicCluster = new PublicApiThreadingIgniteCluster(new IgniteClusterImpl(clusterSvc.topologyService(), clusterIdService));
    }

    private ClusterState clusterState() {
        ClusterState localClusterState = clusterState;

        assert localClusterState != null : "Cluster has not been initialized yet.";

        return localClusterState;
    }

    private RestComponent createRestComponent(String name) {
        RestManager restManager = new RestManager();
        Supplier<RestFactory> presentationsFactory = () -> new PresentationsFactory(nodeConfigRegistry, clusterConfigRegistry);
        Supplier<RestFactory> clusterManagementRestFactory = () -> new ClusterManagementRestFactory(
                clusterSvc,
                clusterInitializer,
                cmgMgr,
                () -> joinFuture
        );
        Supplier<RestFactory> nodeManagementRestFactory = () -> new NodeManagementRestFactory(lifecycleManager, () -> name,
                new JdbcPortProviderImpl(nodeConfigRegistry));
        Supplier<RestFactory> metricRestFactory = () -> new MetricRestFactory(metricManager, metricMessaging);
        Supplier<RestFactory> authProviderFactory = () -> new AuthenticationProviderFactory(authenticationManager);
        Supplier<RestFactory> deploymentCodeRestFactory =
                () -> new CodeDeploymentRestFactory(deploymentManager, deploymentManager.tempStorageProvider());
        Supplier<RestFactory> restManagerFactory = () -> new RestManagerFactory(restManager);
        Supplier<RestFactory> computeRestFactory = () -> new ComputeRestFactory(compute);
        Supplier<RestFactory> disasterRecoveryFactory = () -> new DisasterRecoveryFactory(disasterRecoveryManager);
        Supplier<RestFactory> systemDisasterRecoveryFactory = () -> new SystemDisasterRecoveryFactory(systemDisasterRecoveryManager);
        Supplier<RestFactory> sqlQueryRestFactory = () -> new SqlQueryRestFactory(sql, killCommandHandler);
        Supplier<RestFactory> nodePropertiesRestFactory = () -> new NodePropertiesFactory(nodeProperties);
        Supplier<RestFactory> dataNodesRestFactory = () -> new DataNodesRestFactory(distributionZoneManager);
        Supplier<RestFactory> restEventsFactory = () -> new RestEventsFactory(eventLog, name);

        RestConfiguration restConfiguration = nodeConfigRegistry.getConfiguration(RestExtensionConfiguration.KEY).rest();

        return new RestComponent(
                List.of(presentationsFactory,
                        clusterManagementRestFactory,
                        nodeManagementRestFactory,
                        metricRestFactory,
                        deploymentCodeRestFactory,
                        authProviderFactory,
                        restManagerFactory,
                        computeRestFactory,
                        disasterRecoveryFactory,
                        systemDisasterRecoveryFactory,
                        sqlQueryRestFactory,
                        nodePropertiesRestFactory,
                        dataNodesRestFactory,
                        restEventsFactory
                ),
                restManager,
                restConfiguration
        );
    }

    /**
     * Starts ignite node.
     *
     * <p>When the returned future completes, the node is partially started and ready to accept the init command (that is, its
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
            // Pre-start wiring: register metric sources and start partition modification counters.
            PostConstructionWiring wiring = diContext.getBean(PostConstructionWiring.class);
            wiring.wirePhase1();

            // Start REST component separately (not DI-managed, depends on joinFuture).
            // Register before PHASE_1 so it's stopped after PHASE_1 components during reverse-order shutdown.
            CompletableFuture<Void> restStartFuture = lifecycleManager.startComponentAsync(restComponent, componentContext);
            componentLifecycleManager.markAsStarted(restComponent);

            // Start all PHASE_1 DI-managed components.
            CompletableFuture<Void> phase1Future = componentLifecycleManager.startPhase(StartupPhase.PHASE_1, componentContext);

            return CompletableFuture.allOf(restStartFuture, phase1Future)
                    .thenRunAsync(() -> {
                        vaultMgr.putName(name);

                        clusterSvc.updateMetadata(
                                new NodeMetadata(restComponent.hostName(), restComponent.httpPort(), restComponent.httpsPort()));

                        LOG.info("Components started");
                    }, startupExecutor)
                    .handleAsync((v, e) -> {
                        if (e != null) {
                            throw handleStartException(e);
                        }

                        return v;
                    }, startupExecutor)
                    // Moving to the common pool on purpose to close the join pool and proceed with user's code in the common pool.
                    .whenCompleteAsync((res, ex) -> startupExecutor.shutdownNow());
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

        cmgMgr.joinFuture()
                .thenComposeAsync(unused -> cmgMgr.clusterState(), joinExecutor)
                .thenAcceptAsync(clusterState -> {
                    this.clusterState = clusterState;

                    systemDisasterRecoveryManager.saveClusterState(clusterState);
                }, joinExecutor)
                // Disable REST component during initialization.
                .thenRunAsync(restComponent::disable, joinExecutor)
                .thenComposeAsync(unused -> {
                    LOG.info("Join complete, starting MetaStorage");

                    // Register with componentLifecycleManager BEFORE starting, so that stopAll()
                    // will know about this component even if a concurrent stop races with the start.
                    componentLifecycleManager.markAsStarted(metaStorageMgr);
                    try {
                        lifecycleManager.startComponentAsync(metaStorageMgr, componentContext);
                    } catch (NodeStoppingException e) {
                        throw new CompletionException(e);
                    }

                    return metaStorageMgr.recoveryFinishedFuture();
                }, joinExecutor)
                .thenComposeAsync(unused -> initializeClusterConfiguration(joinExecutor), joinExecutor)
                .thenRunAsync(() -> {
                    // Start cluster config registry after MetaStorage recovery (it depends on DistributedConfigurationStorage).
                    componentLifecycleManager.markAsStarted(clusterConfigRegistry);
                    try {
                        lifecycleManager.startComponentAsync(clusterConfigRegistry, componentContext);
                    } catch (NodeStoppingException e) {
                        throw new CompletionException(e);
                    }
                }, joinExecutor)
                .<CompletableFuture<Void>>thenApplyAsync(unused -> {
                    LOG.info("MetaStorage started, starting the remaining components");

                    // Fire-and-forget: start all PHASE_2 DI-managed components.
                    // We don't wait for them here because some components' startAsync() may need
                    // MetaStorage revision notifications (delivered by notifyRevisionUpdateListenerOnStart).
                    // Waiting here would deadlock on restart.
                    CompletableFuture<Void> phase2Future = componentLifecycleManager.startPhase(
                            StartupPhase.PHASE_2, componentContext
                    );

                    // The system view manager comes last because other components
                    // must register system views before it starts.
                    componentLifecycleManager.markAsStarted(systemViewManager);
                    try {
                        lifecycleManager.startComponentAsync(systemViewManager, componentContext);
                    } catch (NodeStoppingException e) {
                        throw new CompletionException(e);
                    }

                    return phase2Future;
                }, joinExecutor)
                .thenComposeAsync(phase2Future -> {
                    LOG.info("Components started, performing recovery");

                    LOG.info("Cluster configuration: {}", convertToHoconString(clusterConfigRegistry));

                    // Wait for ALL component starts (both DI-managed PHASE_2 and manually-started)
                    // plus revision update before deploying watches.
                    CompletableFuture<Void> allStartsFuture = CompletableFuture.allOf(
                            phase2Future,
                            lifecycleManager.allComponentsStartFuture(joinExecutor)
                    );

                    return recoverComponentsStateOnStart(joinExecutor, allStartsFuture);
                }, joinExecutor)
                .thenComposeAsync(v -> clusterConfigRegistry.onDefaultsPersisted(), joinExecutor)
                // Signal that local recovery is complete and the node is ready to join the cluster.
                .thenComposeAsync(v -> {
                    LOG.info("Recovery complete, finishing join");

                    return cmgMgr.onJoinReady();
                }, joinExecutor)
                .thenComposeAsync(ignored -> awaitSelfInLocalLogicalTopology(), joinExecutor)
                .thenCompose(ignored -> catalogManager.catalogInitializationFuture())
                .thenCompose(ignored -> systemViewManager.completeRegistration())
                .thenRunAsync(() -> {
                    try {
                        // Enable watermark events.
                        lowWatermark.scheduleUpdates();

                        // Enable client requests handling on start complete.
                        clientHandlerModule.enable();

                        // Enable REST component on start complete.
                        restComponent.enable();

                        // Enable compute messages handling
                        computeComponent.enable();

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
                // Moving to the common pool on purpose to close the join pool and proceed with user's code in the common pool.
                .whenCompleteAsync((res, ex) -> {
                    joinExecutor.shutdownNow();
                    if (ex != null) {
                        joinFuture.completeExceptionally(ex);
                    } else {
                        joinFuture.complete(res);
                    }
                });

        return joinFuture;
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
        if (logicalTopologySnapshot.hasNode(id())) {
            awaitSelfInLogicalTopologyFuture.complete(null);
            logicalTopologyService.removeEventListener(awaitSelfListener);
        }
    }

    private RuntimeException handleStartException(Throwable e) {
        String errMsg = "Unable to start [node=" + name + "]";

        var igniteException = new IgniteException(extractCodeFrom(e), errMsg, e);

        Throwable rootEx = unwrapRootCause(e);
        if (rootEx instanceof InvalidNodeConfigurationException) {
            LOG.error("{}. Reason: {}", errMsg,  rootEx.getMessage());
        } else {
            // We log the exception as soon as possible to minimize the probability that it gets lost due to something like an OOM later.
            LOG.error(errMsg, igniteException);
        }

        try {
            stopAsync().get();
        } catch (Throwable ex) {
            // We add ex as a suppressed subexception, but we don't know how the caller will handle it, so we also log it ourselves.
            LOG.error("Node stop failed after node start failure", ex);

            igniteException.addSuppressed(ex);
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
        if (!stopGuard.compareAndSet(false, true)) {
            return stopFuture;
        }

        ExecutorService lifecycleExecutor = stopExecutor();
        ComponentContext componentContext = new ComponentContext(lifecycleExecutor);

        lifecycleManager.markAsStopping();
        cmgMgr.markAsStopping();
        metaStorageMgr.markAsStopping();

        // Stop ALL components in reverse start order. The componentLifecycleManager tracks both
        // DI-managed and manually-started components (via markAsStarted) in the correct order.
        componentLifecycleManager.stopAll(componentContext)
                .thenRunAsync(diContext::close)
                // Moving to the common pool on purpose to close the stop pool and proceed user's code in the common pool.
                .whenCompleteAsync((res, ex) -> lifecycleExecutor.shutdownNow())
                .whenCompleteAsync(copyStateTo(stopFuture));

        return stopFuture;
    }

    private ExecutorService stopExecutor() {
        return Executors.newSingleThreadExecutor(
                IgniteThreadFactory.create(name, "stop", LOG, STORAGE_READ, STORAGE_WRITE)
        );
    }

    /** {@inheritDoc} */
    @Override
    public IgniteTables tables() {
        return publicTables;
    }

    public DisasterRecoveryManager disasterRecoveryManager() {
        return disasterRecoveryManager;
    }

    @TestOnly
    public VaultManager vault() {
        return vaultMgr;
    }

    @TestOnly
    public ClusterStateStorage clusterStateStorage() {
        return clusterStateStorage;
    }

    @TestOnly
    public ClusterManagementGroupManager clusterManagementGroupManager() {
        return cmgMgr;
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
    public FailureManager failureProcessor() {
        return failureManager;
    }

    @TestOnly
    public MetricManager metricManager() {
        return metricManager;
    }

    @TestOnly
    public TableManager distributedTableManager() {
        return distributedTblMgr;
    }

    /** {@inheritDoc} */
    @Override
    public IgniteTransactions transactions() {
        return publicTransactions;
    }

    /** {@inheritDoc} */
    @Override
    public IgniteSql sql() {
        return publicSql;
    }

    /** {@inheritDoc} */
    @Override
    public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override
    public IgniteCompute compute() {
        return publicCompute;
    }

    @Override
    public IgniteCatalog catalog() {
        return publicCatalog;
    }

    @Override
    public IgniteCluster cluster() {
        return publicCluster;
    }

    public Path workDir() {
        return workDir;
    }

    /**
     * Returns node configuration.
     */
    public ConfigurationRegistry nodeConfiguration() {
        return nodeConfigRegistry;
    }

    /**
     * Returns cluster configuration.
     */
    public ConfigurationRegistry clusterConfiguration() {
        return clusterConfigRegistry;
    }

    public HybridTimestampTracker observableTimeTracker() {
        return observableTimestampTracker;
    }

    /**
     * Returns the id of the current node.
     */
    // TODO: should be encapsulated in local properties, see https://issues.apache.org/jira/browse/IGNITE-15131
    public UUID id() {
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
                                    clusterConfigRegistry.initializeConfigurationWith(hoconSource);
                                }, startupExecutor);
                    }
                }, startupExecutor)
                .thenRunAsync(systemDisasterRecoveryManager::markInitConfigApplied, startupExecutor);
    }

    private CompletableFuture<?> recoverComponentsStateOnStart(ExecutorService startupExecutor, CompletableFuture<Void> startFuture) {
        CompletableFuture<Void> startupRevisionUpdate = metaStorageMgr.notifyRevisionUpdateListenerOnStart();

        return CompletableFuture.allOf(startupRevisionUpdate, startFuture)
                .thenComposeAsync(unused -> {
                    // Deploy all registered watches because all components are ready and have registered their listeners.
                    return metaStorageMgr.deployWatches();
                }, startupExecutor);
    }

    @TestOnly
    public Loza raftManager() {
        return raftMgr;
    }

    @TestOnly
    public InternalClusterNode node() {
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

    @TestOnly
    public ComponentWorkingDir partitionsWorkDir() {
        return partitionsWorkDir;
    }

    @TestOnly
    public ComponentWorkingDir metastorageWorkDir() {
        return metastorageWorkDir;
    }

    @TestOnly
    public LogStorageManager partitionsLogStorageManager() {
        return partitionsLogStorageManager;
    }

    /** Returns the node's transaction manager. */
    @TestOnly
    public TxManager txManager() {
        return txManager;
    }

    @TestOnly
    public ClientInboundMessageHandler clientInboundMessageHandler() {
        return clientHandlerModule.handler();
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

    @TestOnly
    public ClusterIdService clusterIdService() {
        return clusterIdService;
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

    /** Returns replicas manager. */
    @TestOnly
    public ReplicaManager replicaManager() {
        return replicaMgr;
    }

    /** Returns disaster recovery manager for system groups. */
    @TestOnly
    public SystemDisasterRecoveryManager systemDisasterRecoveryManager() {
        return systemDisasterRecoveryManager;
    }

    @TestOnly
    public PartitionReplicaLifecycleManager partitionReplicaLifecycleManager() {
        return partitionReplicaLifecycleManager;
    }

    @TestOnly
    public TxStateRocksDbSharedStorage sharedTxStateStorage() {
        return sharedTxStateStorage;
    }

    @TestOnly
    public NodeProperties nodeProperties() {
        return nodeProperties;
    }

    @TestOnly
    public SchemaSafeTimeTrackerImpl schemaSafeTimeTracker() {
        return schemaSafeTimeTracker;
    }

    @TestOnly
    public PartitionCountCalculator partitionCountCalculator() {
        return partitionCountCalculatorWrapper;
    }

    @TestOnly
    public void useStaticPartitionCountCalculator(int partitions) {
        partitionCountCalculatorWrapper.setPartitionCountCalculator(PartitionCountCalculator.staticPartitionCountCalculator(partitions));
    }

    /** Triggers dumping node components state. This method is used for debugging purposes only. */
    @TestOnly
    public void dumpClusterState() {
        IgniteStringBuilder sb = new IgniteStringBuilder()
                .app("Dumping cluster state for node ").app(node().name()).app(":")
                .nl();

        lifecycleManager.dumpState(sb, "");

        LOG.info(sb.toString());
    }

    /**
     * Converts the entire configuration from the registry to a HOCON string without spaces, comments and quotes. For example,
     * "ignite{clientConnector{connectTimeoutMillis=5000,idleTimeoutMillis=0}}".
     */
    private static String convertToHoconString(ConfigurationRegistry configRegistry) {
        return HoconConverter.represent(configRegistry.superRoot(), List.of()).render(ConfigRenderOptions.concise().setJson(false));
    }
}
