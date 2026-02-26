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

import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_READ;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_WRITE;
import static org.apache.ignite.internal.util.CompletableFutures.copyStateTo;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.extractCodeFrom;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapRootCause;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiPredicate;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.catalog.IgniteCatalog;
import org.apache.ignite.client.handler.ClientHandlerModule;
import org.apache.ignite.client.handler.ClientInboundMessageHandler;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.configuration.ConfigurationModule;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.compaction.CatalogCompactionRunner;
import org.apache.ignite.internal.catalog.configuration.SchemaSynchronizationConfiguration;
import org.apache.ignite.internal.cluster.management.ClusterInitializer;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.ClusterState;
import org.apache.ignite.internal.cluster.management.CmgGroupId;
import org.apache.ignite.internal.cluster.management.InvalidNodeConfigurationException;
import org.apache.ignite.internal.cluster.management.raft.ClusterStateStorage;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.components.LongJvmPauseDetector;
import org.apache.ignite.internal.components.NodeProperties;
import org.apache.ignite.internal.compute.ComputeComponentImpl;
import org.apache.ignite.internal.compute.IgniteComputeInternal;
import org.apache.ignite.internal.configuration.ComponentWorkingDir;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.ConfigurationModules;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.JdbcPortProviderImpl;
import org.apache.ignite.internal.configuration.ServiceLoaderModulesProvider;
import org.apache.ignite.internal.configuration.hocon.HoconConverter;
import org.apache.ignite.internal.configuration.storage.ConfigurationStorage;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.deployunit.DeploymentManagerImpl;
import org.apache.ignite.internal.deployunit.IgniteDeployment;
import org.apache.ignite.internal.disaster.system.ClusterIdService;
import org.apache.ignite.internal.disaster.system.SystemDisasterRecoveryManager;
import org.apache.ignite.internal.disaster.system.SystemDisasterRecoveryManagerImpl;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.eventlog.impl.EventLogImpl;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.ClockWaiter;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.index.IndexBuildingManager;
import org.apache.ignite.internal.index.IndexManager;
import org.apache.ignite.internal.index.IndexNodeFinishedRwTransactionsChecker;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.lowwatermark.LowWatermarkImpl;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.cache.IdempotentCacheVacuumizer;
import org.apache.ignite.internal.metastorage.impl.MetaStorageCompactionTrigger;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.metastorage.server.raft.MetastorageGroupId;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.metrics.messaging.MetricMessaging;
import org.apache.ignite.internal.metrics.sources.ClockServiceMetricSource;
import org.apache.ignite.internal.metrics.sources.JvmMetricSource;
import org.apache.ignite.internal.metrics.sources.OsMetricSource;
import org.apache.ignite.internal.network.ChannelType;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.DefaultMessagingService;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.LogicalTopologyEventsListener;
import org.apache.ignite.internal.network.MessageSerializationRegistryImpl;
import org.apache.ignite.internal.network.NettyBootstrapFactory;
import org.apache.ignite.internal.network.NettyWorkersRegistrar;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.internal.network.serialization.SerializationRegistryServiceLoader;
import org.apache.ignite.internal.partition.replicator.PartitionReplicaLifecycleManager;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.PlacementDriverManager;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.server.impl.GroupStoragesContextResolver;
import org.apache.ignite.internal.raft.storage.LogStorageManager;
import org.apache.ignite.internal.raft.storage.impl.VolatileLogStorageManagerCreator;
import org.apache.ignite.internal.replicator.PartitionGroupId;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
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
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.SchemaSafeTimeTrackerImpl;
import org.apache.ignite.internal.security.authentication.AuthenticationManager;
import org.apache.ignite.internal.security.authentication.AuthenticationManagerImpl;
import org.apache.ignite.internal.security.configuration.SecurityConfiguration;
import org.apache.ignite.internal.security.configuration.SecurityExtensionConfiguration;
import org.apache.ignite.internal.sql.api.IgniteSqlImpl;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.SqlQueryProcessor;
import org.apache.ignite.internal.sql.engine.api.kill.CancellableOperationType;
import org.apache.ignite.internal.sql.engine.api.kill.OperationKillHandler;
import org.apache.ignite.internal.sql.engine.exec.kill.KillCommandHandler;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.engine.ThreadAssertingStorageEngine;
import org.apache.ignite.internal.systemview.SystemViewManagerImpl;
import org.apache.ignite.internal.systemview.api.SystemViewManager;
import org.apache.ignite.internal.table.distributed.PartitionModificationCounterFactory;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.distributed.disaster.DisasterRecoveryManager;
import org.apache.ignite.internal.table.distributed.index.IndexMetaStorage;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.RemotelyTriggeredResourceRegistry;
import org.apache.ignite.internal.tx.impl.ResourceVacuumManager;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.storage.state.rocksdb.TxStateRocksDbSharedStorage;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.worker.CriticalWorkerWatchdog;
import org.apache.ignite.internal.worker.ThreadAssertions;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.network.IgniteCluster;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.IgniteTables;
import org.apache.ignite.tx.IgniteTransactions;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Ignite internal implementation.
 */
@Singleton
public class IgniteImpl implements Ignite {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(IgniteImpl.class);

    /** Used for durable destruction purposes. */
    private static final String PARTITION_GROUP_NAME = "partition";

    /** Name of a transaction state directory. */
    private static final String TX_STATE_DIR = "tx-state";

    /** Ignite node name. */
    @Value("${node-name}")
    private String name;

    @Value("${work-dir}")
    private Path workDir;

    /** Lifecycle manager. */
    @Inject
    private LifecycleManager lifecycleManager;

    @Inject
    private ThreadPoolsManager threadPoolsManager;

    /** Vault manager. */
    @Inject
    private VaultManager vaultMgr;

    @Inject
    private NodePropertiesImpl nodeProperties;

    /** Sql query engine. */
    @Inject
    private SqlQueryProcessor qryEngine;

    /** Sql API facade. */
    private final IgniteSqlImpl sql;

    /** Configuration manager that handles node (local) configuration. */
    private final ConfigurationManager nodeCfgMgr;

    private final ClusterIdService clusterIdService;

    /** Cluster service (cluster network manager). */
    private final ClusterService clusterSvc;

    private final ComputeComponentImpl computeComponent;

    private final CriticalWorkerWatchdog criticalWorkerRegistry;

    /** Failure processor. */
    private final FailureManager failureManager;

    /** Netty bootstrap factory. */
    private final NettyBootstrapFactory nettyBootstrapFactory;

    private final NettyWorkersRegistrar nettyWorkersRegistrar;

    /** Raft manager. */
    private final Loza raftMgr;

    /** Meta storage manager. */
    private final MetaStorageManagerImpl metaStorageMgr;

    /** Metastorage compaction trigger. */
    private final MetaStorageCompactionTrigger metaStorageCompactionTrigger;

    /** Placement driver manager. */
    private final PlacementDriverManager placementDriverMgr;

    /** Configuration manager that handles cluster (distributed) configuration. */
    private final ConfigurationManager clusterCfgMgr;

    /** Idempotent cache vacuumizer. */
    private final IdempotentCacheVacuumizer idempotentCacheVacuumizer;

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

    private final IndexManager indexManager;

    /** Rest module. */
    // private final RestComponent restComponent;

    private final ClusterStateStorage clusterStateStorage;

    private final ClusterManagementGroupManager cmgMgr;

    private final LogicalTopologyService logicalTopologyService;

    private final ComponentWorkingDir partitionsWorkDir;

    private final ComponentWorkingDir metastorageWorkDir;

    private final ComponentWorkingDir cmgWorkDir;

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

    private final SchemaSafeTimeTrackerImpl schemaSafeTimeTracker;

    /** Schema manager. */
    private final SchemaManager schemaManager;

    /** Metric manager. */
    private final MetricManager metricManager;

    /** Metric messaging. */
    private final MetricMessaging metricMessaging;

    private final DeploymentManagerImpl deploymentManager;

    private final DistributionZoneManager distributionZoneManager;

    private final PartitionReplicaLifecycleManager partitionReplicaLifecycleManager;

    /** Creator for volatile {@link LogStorageManager} instances. */
    private final VolatileLogStorageManagerCreator volatileLogStorageManagerCreator;

    private final SystemPropertiesComponent systemPropertiesComponent;

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
    private final HybridTimestampTracker observableTimestampTracker = HybridTimestampTracker.atomicTracker(null);

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

    private final SystemDisasterRecoveryManagerImpl systemDisasterRecoveryManager;

    private final IgniteTables publicTables;
    private final IgniteTransactions publicTransactions;
    private final IgniteSql publicSql;
    private final IgniteCompute publicCompute;
    private final IgniteCatalog publicCatalog;
    private final IgniteCluster publicCluster;

    /** Partitions log storage manager for raft. */
    private final LogStorageManager partitionsLogStorageManager;

    private final LogStorageManager msLogStorageManager;

    private final LogStorageManager cmgLogStorageManager;

    private final IndexMetaStorage indexMetaStorage;

    private final EventLogImpl eventLog;

    private final KillCommandHandler killCommandHandler;

    private final AtomicBoolean stopGuard = new AtomicBoolean();

    private final CompletableFuture<Void> stopFuture = new CompletableFuture<>();

    @Nullable
    private volatile ClusterState clusterState;

    private final ClockServiceMetricSource clockServiceMetricSource;

    private final PartitionModificationCounterFactory partitionModificationCounterFactory;

    /** Future that completes when the node has joined the cluster. */
    private final CompletableFuture<Ignite> joinFuture = new CompletableFuture<>();

    private GroupStoragesContextResolver createGroupStoragesContextResolver() {
        Map<String, LogStorageManager> logStorageManagerByGroupName = Map.of(
                PARTITION_GROUP_NAME, partitionsLogStorageManager,
                MetastorageGroupId.INSTANCE.toString(), msLogStorageManager,
                CmgGroupId.INSTANCE.toString(), cmgLogStorageManager
        );

        Map<String, Path> serverDataPathByGroupName = Map.of(
                PARTITION_GROUP_NAME, partitionsWorkDir.metaPath(),
                MetastorageGroupId.INSTANCE.toString(), metastorageWorkDir.metaPath(),
                CmgGroupId.INSTANCE.toString(), cmgWorkDir.metaPath()
        );

        return new GroupStoragesContextResolver(
                replicationGroupId -> replicationGroupId instanceof PartitionGroupId ? PARTITION_GROUP_NAME : replicationGroupId.toString(),
                serverDataPathByGroupName,
                logStorageManagerByGroupName
        );
    }

    private static LogicalTopologyEventListener logicalTopologyJoinedNodesListener(LogicalTopologyEventsListener listener) {
        return new LogicalTopologyEventListener() {
            @Override
            public void onNodeJoined(LogicalNode joinedNode, LogicalTopologySnapshot newTopology) {
                listener.onJoined(joinedNode, newTopology.version());
            }

            @Override
            public void onNodeLeft(LogicalNode leftNode, LogicalTopologySnapshot newTopology) {
                listener.onLeft(leftNode, newTopology.version());
            }
        };
    }

    private ClusterState clusterState() {
        ClusterState localClusterState = clusterState;

        assert localClusterState != null : "Cluster has not been initialized yet.";

        return localClusterState;
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

    private static LongSupplier delayDurationMsSupplier(SchemaSynchronizationConfiguration schemaSyncConfig) {
        return () -> schemaSyncConfig.delayDurationMillis().value();
    }

    private static LongSupplier partitionIdleSafeTimePropagationPeriodMsSupplier(ReplicationConfiguration replicationConfig) {
        return () -> replicationConfig.idleSafeTimePropagationDurationMillis().value();
    }

    private AuthenticationManager createAuthenticationManager() {
        SecurityConfiguration securityConfiguration = clusterCfgMgr.configurationRegistry()
                .getConfiguration(SecurityExtensionConfiguration.KEY).security();

        return new AuthenticationManagerImpl(securityConfiguration, eventLog);
    }

    private RestComponent createRestComponent(String name) {
        RestManager restManager = new RestManager();
        Supplier<RestFactory> presentationsFactory = () -> new PresentationsFactory(nodeCfgMgr, clusterCfgMgr);
        Supplier<RestFactory> clusterManagementRestFactory = () -> new ClusterManagementRestFactory(
                clusterSvc,
                clusterInitializer,
                cmgMgr,
                () -> joinFuture
        );
        Supplier<RestFactory> nodeManagementRestFactory = () -> new NodeManagementRestFactory(lifecycleManager, () -> name,
                new JdbcPortProviderImpl(nodeCfgMgr.configurationRegistry()));
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

        RestConfiguration restConfiguration = nodeCfgMgr.configurationRegistry().getConfiguration(RestExtensionConfiguration.KEY).rest();

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

    private static MessageSerializationRegistry createSerializationRegistry(@Nullable ClassLoader classLoader) {
        var serviceLoader = new SerializationRegistryServiceLoader(classLoader);

        var serializationRegistry = new MessageSerializationRegistryImpl();

        serviceLoader.registerSerializationFactories(serializationRegistry);

        return serializationRegistry;
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
            JvmMetricSource jvmMetrics = new JvmMetricSource();
            metricManager.registerSource(jvmMetrics);
            metricManager.enable(jvmMetrics);

            OsMetricSource osMetrics = new OsMetricSource();
            metricManager.registerSource(osMetrics);
            metricManager.enable(osMetrics);

            metricManager.registerSource(clockServiceMetricSource);
            metricManager.enable(clockServiceMetricSource);

            partitionModificationCounterFactory.start();

            // Start the components that are required to join the cluster.
            // TODO https://issues.apache.org/jira/browse/IGNITE-22570
            CompletableFuture<Void> componentsStartFuture = lifecycleManager.startComponentsAsync(
                    componentContext,
                    longJvmPauseDetector,
                    vaultMgr,
                    nodeProperties,
                    threadPoolsManager,
                    clockWaiter,
                    failureManager,
                    clusterStateStorage,
                    clusterIdService,
                    systemDisasterRecoveryManager,
                    criticalWorkerRegistry,
                    nettyBootstrapFactory,
                    nettyWorkersRegistrar,
                    clusterSvc,
                    // restComponent,
                    partitionsLogStorageManager,
                    msLogStorageManager,
                    cmgLogStorageManager,
                    raftMgr,
                    cmgMgr,
                    lowWatermark
            );

            return componentsStartFuture
                    .thenRunAsync(() -> {
                        vaultMgr.putName(name);

                        // clusterSvc.updateMetadata(
                        //         new NodeMetadata(restComponent.hostName(), restComponent.httpPort(), restComponent.httpsPort()));

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
                // .thenRunAsync(restComponent::disable, joinExecutor)
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
                                new LowWatermarkRectifier(lowWatermark, catalogManager),
                                catalogCompactionRunner,
                                indexMetaStorage,
                                clusterCfgMgr,
                                idempotentCacheVacuumizer,
                                authenticationManager,
                                placementDriverMgr,
                                metricManager,
                                metricMessaging,
                                distributionZoneManager,
                                computeComponent,
                                volatileLogStorageManagerCreator,
                                replicaMgr,
                                indexNodeFinishedRwTransactionsChecker,
                                txManager,
                                dataStorageMgr,
                                schemaSafeTimeTracker,
                                schemaManager,
                                outgoingSnapshotsManager,
                                sharedTxStateStorage,
                                partitionReplicaLifecycleManager,
                                distributedTblMgr,
                                disasterRecoveryManager,
                                indexManager,
                                indexBuildingManager,
                                qryEngine,
                                clientHandlerModule,
                                deploymentManager,
                                sql,
                                systemPropertiesComponent,
                                resourceVacuumManager,
                                metaStorageCompactionTrigger,
                                eventLog
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

                    LOG.info("Cluster configuration: {}", convertToHoconString(clusterCfgMgr.configurationRegistry()));

                    return recoverComponentsStateOnStart(joinExecutor, lifecycleManager.allComponentsStartFuture(joinExecutor));
                }, joinExecutor)
                .thenComposeAsync(v -> clusterCfgMgr.configurationRegistry().onDefaultsPersisted(), joinExecutor)
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
                        // restComponent.enable();

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
        if (logicalTopologySnapshot.nodes().stream().map(LogicalNode::id).collect(toSet()).contains(id())) {
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

        ExecutorService lifecycleExecutor = stopExecutor();

        try {
            lifecycleManager.stopNode(new ComponentContext(lifecycleExecutor)).get();
        } catch (Throwable ex) {
            // We add ex as a suppressed subexception, but we don't know how the caller will handle it, so we also log it ourselves.
            LOG.error("Node stop failed after node start failure", ex);

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
        if (!stopGuard.compareAndSet(false, true)) {
            return stopFuture;
        }

        ExecutorService lifecycleExecutor = stopExecutor();

        cmgMgr.markAsStopping();
        metaStorageMgr.markAsStopping();

        // TODO https://issues.apache.org/jira/browse/IGNITE-22570
        lifecycleManager.stopNode(new ComponentContext(lifecycleExecutor))
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

    private IgniteSql bareSql() {
        return sql;
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
        String host = ""; // restComponent.hostName();
        int port = 0; // restComponent.httpPort();
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
        String host = ""; // restComponent.hostName();
        int port = 0; // restComponent.httpsPort();
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

    /** Returns a {@link OperationKillHandler kill handler} for the compute job. */
    private static OperationKillHandler computeKillHandler(IgniteComputeInternal compute) {
        return new OperationKillHandler() {
            @Override
            public CompletableFuture<Boolean> cancelAsync(String operationId) {
                UUID jobId = UUID.fromString(operationId);

                return compute.cancelAsync(jobId)
                        .thenApply(res -> res != null ? res : Boolean.FALSE);
            }

            @Override
            public boolean local() {
                return false;
            }

            @Override
            public CancellableOperationType type() {
                return CancellableOperationType.COMPUTE;
            }
        };
    }

    /** Returns a {@link OperationKillHandler kill handler} for the transaction. */
    private static OperationKillHandler transactionKillHandler(TxManager txManager) {
        return new OperationKillHandler() {
            @Override
            public CompletableFuture<Boolean> cancelAsync(String operationId) {
                UUID transactionId = UUID.fromString(operationId);

                return txManager.kill(transactionId);
            }

            @Override
            public boolean local() {
                return true;
            }

            @Override
            public CancellableOperationType type() {
                return CancellableOperationType.TRANSACTION;
            }
        };
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
