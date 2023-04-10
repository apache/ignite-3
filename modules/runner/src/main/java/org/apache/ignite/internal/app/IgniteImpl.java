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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.client.handler.ClientHandlerMetricSource;
import org.apache.ignite.client.handler.ClientHandlerModule;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.configuration.ConfigurationModule;
import org.apache.ignite.internal.baseline.BaselineManager;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogServiceImpl;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.DistributedConfigurationUpdater;
import org.apache.ignite.internal.cluster.management.configuration.ClusterManagementConfiguration;
import org.apache.ignite.internal.cluster.management.configuration.NodeAttributesConfiguration;
import org.apache.ignite.internal.cluster.management.raft.ClusterStateStorage;
import org.apache.ignite.internal.cluster.management.raft.RocksDbClusterStateStorage;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyServiceImpl;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.component.RestAddressReporter;
import org.apache.ignite.internal.components.LongJvmPauseDetector;
import org.apache.ignite.internal.compute.ComputeComponent;
import org.apache.ignite.internal.compute.ComputeComponentImpl;
import org.apache.ignite.internal.compute.IgniteComputeImpl;
import org.apache.ignite.internal.compute.configuration.ComputeConfiguration;
import org.apache.ignite.internal.configuration.AuthenticationConfiguration;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.ConfigurationModules;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.NodeConfigReadException;
import org.apache.ignite.internal.configuration.SecurityConfiguration;
import org.apache.ignite.internal.configuration.ServiceLoaderModulesProvider;
import org.apache.ignite.internal.configuration.storage.ConfigurationStorage;
import org.apache.ignite.internal.configuration.storage.DistributedConfigurationStorage;
import org.apache.ignite.internal.configuration.storage.LocalFileConfigurationStorage;
import org.apache.ignite.internal.deployunit.DeploymentManagerImpl;
import org.apache.ignite.internal.deployunit.IgniteDeployment;
import org.apache.ignite.internal.deployunit.configuration.DeploymentConfiguration;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.index.IndexManager;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.metastorage.server.persistence.RocksDbKeyValueStorage;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.metrics.configuration.MetricConfiguration;
import org.apache.ignite.internal.metrics.sources.JvmMetricSource;
import org.apache.ignite.internal.network.configuration.NetworkConfiguration;
import org.apache.ignite.internal.network.configuration.NetworkConfigurationSchema;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.storage.impl.VolatileLogStorageFactoryCreator;
import org.apache.ignite.internal.recovery.ConfigurationCatchUpListener;
import org.apache.ignite.internal.recovery.RecoveryCompletionFutureFactory;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.rest.RestComponent;
import org.apache.ignite.internal.rest.RestFactory;
import org.apache.ignite.internal.rest.authentication.AuthenticationProviderFactory;
import org.apache.ignite.internal.rest.cluster.ClusterManagementRestFactory;
import org.apache.ignite.internal.rest.configuration.PresentationsFactory;
import org.apache.ignite.internal.rest.configuration.RestConfiguration;
import org.apache.ignite.internal.rest.deployment.CodeDeploymentRestFactory;
import org.apache.ignite.internal.rest.metrics.MetricRestFactory;
import org.apache.ignite.internal.rest.node.NodeManagementRestFactory;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.security.authentication.AuthenticationManager;
import org.apache.ignite.internal.security.authentication.AuthenticationManagerImpl;
import org.apache.ignite.internal.sql.api.IgniteSqlImpl;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.SqlQueryProcessor;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.storage.DataStorageModule;
import org.apache.ignite.internal.storage.DataStorageModules;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.distributed.TableMessageGroup;
import org.apache.ignite.internal.table.distributed.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.IgniteTransactionsImpl;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.message.TxMessageGroup;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.VaultService;
import org.apache.ignite.internal.vault.persistence.PersistentVaultService;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.DefaultMessagingService;
import org.apache.ignite.network.MessageSerializationRegistryImpl;
import org.apache.ignite.network.NettyBootstrapFactory;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.NodeMetadata;
import org.apache.ignite.network.scalecube.ScaleCubeClusterServiceFactory;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.network.serialization.SerializationRegistryServiceLoader;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;
import org.apache.ignite.security.AuthenticationConfig;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.manager.IgniteTables;
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

    /** Vault manager. */
    private final VaultManager vaultMgr;

    /** Sql query engine. */
    private final SqlQueryProcessor qryEngine;

    /** Sql API facade. */
    private final IgniteSql sql;

    /** Configuration manager that handles node (local) configuration. */
    private final ConfigurationManager nodeCfgMgr;

    /** Cluster service (cluster network manager). */
    private final ClusterService clusterSvc;

    private final ComputeComponent computeComponent;

    /** Netty bootstrap factory. */
    private final NettyBootstrapFactory nettyBootstrapFactory;

    /** Raft manager. */
    private final Loza raftMgr;

    /** Meta storage manager. */
    private final MetaStorageManager metaStorageMgr;

    // TODO: IGNITE-16985 Design table management flow
    // /** Placement driver manager. */
    //private final PlacementDriverManager placementDriverMgr;

    /** Configuration manager that handles cluster (distributed) configuration. */
    private final ConfigurationManager clusterCfgMgr;

    /** Baseline manager. */
    private final BaselineManager baselineMgr;

    /** Replica manager. */
    private final ReplicaManager replicaMgr;

    /** Transactions manager. */
    private final TxManager txManager;

    /** Distributed table manager. */
    private final TableManager distributedTblMgr;

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
    private final IgniteCompute compute;

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

    /** Creator for volatile {@link org.apache.ignite.internal.raft.storage.LogStorageFactory} instances. */
    private final VolatileLogStorageFactoryCreator volatileLogStorageFactoryCreator;

    /** A hybrid logical clock. */
    private final HybridClock clock;

    private final OutgoingSnapshotsManager outgoingSnapshotsManager;

    private final RestAddressReporter restAddressReporter;

    private final DistributedConfigurationUpdater distributedConfigurationUpdater;
    private final CatalogManager catalogManager;

    private final AuthenticationManager authenticationManager;

    /**
     * The Constructor.
     *
     * @param name Ignite node name.
     * @param configPath Path to node configuration in the HOCON format.
     * @param workDir Work directory for the started node. Must not be {@code null}.
     * @param serviceProviderClassLoader The class loader to be used to load provider-configuration files and provider classes, or
     *         {@code null} if the system class loader (or, failing that the bootstrap class loader) is to be used.
     */
    IgniteImpl(String name, Path configPath, Path workDir, @Nullable ClassLoader serviceProviderClassLoader) {
        this.name = name;

        longJvmPauseDetector = new LongJvmPauseDetector(name);

        lifecycleManager = new LifecycleManager(name);

        vaultMgr = createVault(name, workDir);

        metricManager = new MetricManager();

        ConfigurationModules modules = loadConfigurationModules(serviceProviderClassLoader);

        nodeCfgMgr = new ConfigurationManager(
                modules.local().rootKeys(),
                modules.local().validators(),
                new LocalFileConfigurationStorage(configPath, modules.local().rootKeys(),
                        modules.local().internalSchemaExtensions(),
                        modules.local().polymorphicSchemaExtensions()),
                modules.local().internalSchemaExtensions(),
                modules.local().polymorphicSchemaExtensions()
        );

        ConfigurationRegistry nodeConfigRegistry = nodeCfgMgr.configurationRegistry();

        NetworkConfiguration networkConfiguration = nodeConfigRegistry.getConfiguration(NetworkConfiguration.KEY);

        MessageSerializationRegistry serializationRegistry = createSerializationRegistry(serviceProviderClassLoader);

        nettyBootstrapFactory = new NettyBootstrapFactory(networkConfiguration, name);

        clusterSvc = new ScaleCubeClusterServiceFactory().createClusterService(
                name,
                networkConfiguration,
                nettyBootstrapFactory,
                serializationRegistry
        );

        computeComponent = new ComputeComponentImpl(
                this,
                clusterSvc.messagingService(),
                nodeConfigRegistry.getConfiguration(ComputeConfiguration.KEY)
        );

        clock = new HybridClockImpl();

        RaftConfiguration raftConfiguration = nodeConfigRegistry.getConfiguration(RaftConfiguration.KEY);

        // TODO https://issues.apache.org/jira/browse/IGNITE-19051
        RaftGroupEventsClientListener raftGroupEventsClientListener = new RaftGroupEventsClientListener();

        raftMgr = new Loza(
                clusterSvc,
                raftConfiguration,
                workDir,
                clock,
                raftGroupEventsClientListener
        );

        LockManager lockMgr = new HeapLockManager();

        replicaMgr = new ReplicaManager(
                clusterSvc,
                clock,
                Set.of(TableMessageGroup.class, TxMessageGroup.class)
        );

        ReplicaService replicaSvc = new ReplicaService(clusterSvc.messagingService(), clock);

        txManager = new TxManagerImpl(replicaSvc, lockMgr, clock);

        // TODO: IGNITE-16841 - use common RocksDB instance to store cluster state as well.
        clusterStateStorage = new RocksDbClusterStateStorage(workDir.resolve(CMG_DB_PATH));

        var logicalTopology = new LogicalTopologyImpl(clusterStateStorage);

        distributedConfigurationUpdater = new DistributedConfigurationUpdater();

        cmgMgr = new ClusterManagementGroupManager(
                vaultMgr,
                clusterSvc,
                raftMgr,
                clusterStateStorage,
                logicalTopology,
                nodeConfigRegistry.getConfiguration(ClusterManagementConfiguration.KEY),
                distributedConfigurationUpdater,
                nodeConfigRegistry.getConfiguration(NodeAttributesConfiguration.KEY)
        );

        logicalTopologyService = new LogicalTopologyServiceImpl(logicalTopology, cmgMgr);

        metaStorageMgr = new MetaStorageManagerImpl(
                vaultMgr,
                clusterSvc,
                cmgMgr,
                logicalTopologyService,
                raftMgr,
                new RocksDbKeyValueStorage(name, workDir.resolve(METASTORAGE_DB_PATH))
        );

        this.cfgStorage = new DistributedConfigurationStorage(metaStorageMgr, vaultMgr);

        clusterCfgMgr = new ConfigurationManager(
                modules.distributed().rootKeys(),
                modules.distributed().validators(),
                cfgStorage,
                modules.distributed().internalSchemaExtensions(),
                modules.distributed().polymorphicSchemaExtensions()
        );

        ConfigurationRegistry clusterConfigRegistry = clusterCfgMgr.configurationRegistry();

        TablesConfiguration tablesConfiguration = clusterConfigRegistry.getConfiguration(TablesConfiguration.KEY);

        TopologyAwareRaftGroupServiceFactory topologyAwareRaftGroupServiceFactory = new TopologyAwareRaftGroupServiceFactory(
                clusterSvc,
                logicalTopologyService,
                Loza.FACTORY,
                raftGroupEventsClientListener
        );

        // TODO: IGNITE-16985 Design table management flow
        // placementDriverMgr = new PlacementDriverManager(
        //         metaStorageMgr,
        //         vaultMgr,
        //         MetastorageGroupId.INSTANCE,
        //         clusterSvc,
        //         raftConfiguration,
        //         cmgMgr::metaStorageNodes,
        //         logicalTopologyService,
        //         raftExecutorService,
        //         tablesConfiguration,
        //         clock
        // );

        metricManager.configure(clusterConfigRegistry.getConfiguration(MetricConfiguration.KEY));

        DistributionZonesConfiguration zonesConfiguration = clusterConfigRegistry
                .getConfiguration(DistributionZonesConfiguration.KEY);

        restAddressReporter = new RestAddressReporter(workDir);

        baselineMgr = new BaselineManager(
                clusterCfgMgr,
                metaStorageMgr,
                clusterSvc
        );

        Consumer<Function<Long, CompletableFuture<?>>> registry =
                c -> clusterConfigRegistry.listenUpdateStorageRevision(c::apply);

        DataStorageModules dataStorageModules = new DataStorageModules(
                ServiceLoader.load(DataStorageModule.class, serviceProviderClassLoader)
        );

        Path storagePath = getPartitionsStorePath(workDir);

        DistributionZonesConfiguration distributionZonesConfiguration =
                clusterConfigRegistry.getConfiguration(DistributionZonesConfiguration.KEY);

        dataStorageMgr = new DataStorageManager(
                tablesConfiguration,
                dataStorageModules.createStorageEngines(
                        name,
                        clusterConfigRegistry,
                        storagePath,
                        longJvmPauseDetector
                )
        );

        schemaManager = new SchemaManager(registry, tablesConfiguration, metaStorageMgr);

        distributionZoneManager = new DistributionZoneManager(
                zonesConfiguration,
                tablesConfiguration,
                metaStorageMgr,
                logicalTopologyService,
                vaultMgr,
                name
        );

        volatileLogStorageFactoryCreator = new VolatileLogStorageFactoryCreator(workDir.resolve("volatile-log-spillout"));

        outgoingSnapshotsManager = new OutgoingSnapshotsManager(clusterSvc.messagingService());

        distributedTblMgr = new TableManager(
                name,
                registry,
                tablesConfiguration,
                distributionZonesConfiguration,
                clusterSvc,
                raftMgr,
                replicaMgr,
                lockMgr,
                replicaSvc,
                baselineMgr,
                clusterSvc.topologyService(),
                txManager,
                dataStorageMgr,
                storagePath,
                metaStorageMgr,
                schemaManager,
                volatileLogStorageFactoryCreator,
                clock,
                outgoingSnapshotsManager,
                topologyAwareRaftGroupServiceFactory
        );

        indexManager = new IndexManager(name, tablesConfiguration, schemaManager, distributedTblMgr, clusterSvc);

        catalogManager = new CatalogServiceImpl(metaStorageMgr);

        qryEngine = new SqlQueryProcessor(
                registry,
                clusterSvc,
                distributedTblMgr,
                indexManager,
                schemaManager,
                dataStorageMgr,
                txManager,
                distributionZoneManager,
                () -> dataStorageModules.collectSchemasFields(modules.distributed().polymorphicSchemaExtensions()),
                replicaSvc,
                clock,
                catalogManager
        );

        sql = new IgniteSqlImpl(qryEngine);

        compute = new IgniteComputeImpl(clusterSvc.topologyService(), distributedTblMgr, computeComponent);

        clientHandlerModule = new ClientHandlerModule(
                qryEngine,
                distributedTblMgr,
                new IgniteTransactionsImpl(txManager),
                nodeConfigRegistry,
                compute,
                clusterSvc,
                nettyBootstrapFactory,
                sql,
                () -> cmgMgr.clusterState().thenApply(s -> s.clusterTag().clusterId()),
                metricManager,
                new ClientHandlerMetricSource()
        );

        deploymentManager = new DeploymentManagerImpl(clusterSvc,
                metaStorageMgr,
                workDir,
                nodeConfigRegistry.getConfiguration(DeploymentConfiguration.KEY),
                cmgMgr);

        authenticationManager = createAuthenticationManager();
        restComponent = createRestComponent(name);
    }

    private AuthenticationManager createAuthenticationManager() {
        AuthenticationConfiguration authConfiguration = clusterCfgMgr.configurationRegistry()
                .getConfiguration(SecurityConfiguration.KEY)
                .authentication();

        AuthenticationManager manager = new AuthenticationManagerImpl();
        authConfiguration.listen(manager);
        return manager;
    }

    private RestComponent createRestComponent(String name) {
        Supplier<RestFactory> presentationsFactory = () -> new PresentationsFactory(nodeCfgMgr, clusterCfgMgr);
        Supplier<RestFactory> clusterManagementRestFactory = () -> new ClusterManagementRestFactory(clusterSvc, cmgMgr);
        Supplier<RestFactory> nodeManagementRestFactory = () -> new NodeManagementRestFactory(lifecycleManager, () -> name);
        Supplier<RestFactory> nodeMetricRestFactory = () -> new MetricRestFactory(metricManager);
        Supplier<RestFactory> authProviderFactory = () -> new AuthenticationProviderFactory(authenticationManager);
        Supplier<RestFactory> deploymentCodeRestFactory = () -> new CodeDeploymentRestFactory(deploymentManager);
        RestConfiguration restConfiguration = nodeCfgMgr.configurationRegistry().getConfiguration(RestConfiguration.KEY);
        return new RestComponent(
                List.of(presentationsFactory,
                        clusterManagementRestFactory,
                        nodeManagementRestFactory,
                        nodeMetricRestFactory,
                        deploymentCodeRestFactory,
                        authProviderFactory),
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
     * @param configPath Node configuration based on
     *         {@link NetworkConfigurationSchema}. Following rules are used for applying the
     *         configuration properties:
     *
     *         <ol>
     *             <li>Specified property overrides existing one or just applies itself if it wasn't
     *             previously specified.</li>
     *             <li>All non-specified properties either use previous value or use default one from
     *             corresponding configuration schema.</li>
     *         </ol>
     *
     *         So that, in case of initial node start (first start ever) specified configuration, supplemented with defaults, is
     *         used. If no configuration was provided defaults are used for all configuration properties. In case of node
     *         restart, specified properties override existing ones, non specified properties that also weren't specified
     *         previously use default values. Please pay attention that previously specified properties are searched in the
     *         {@code workDir} specified by the user.
     */
    public CompletableFuture<Ignite> start(Path configPath) {
        ExecutorService startupExecutor = Executors.newSingleThreadExecutor(NamedThreadFactory.create(name, "start", LOG));

        try {
            metricManager.registerSource(new JvmMetricSource());

            lifecycleManager.startComponent(longJvmPauseDetector);

            lifecycleManager.startComponent(vaultMgr);

            vaultMgr.putName(name).get();

            // Node configuration manager startup.
            lifecycleManager.startComponent(nodeCfgMgr);

            // Node configuration manager bootstrap.

            try {
                nodeCfgMgr.bootstrap(configPath);
            } catch (Exception e) {
                throw new NodeConfigReadException("Unable to parse user-specific configuration", e);
            }

            // Start the components that are required to join the cluster.
            lifecycleManager.startComponents(
                    nettyBootstrapFactory,
                    clusterSvc,
                    restComponent,
                    raftMgr,
                    clusterStateStorage,
                    distributedConfigurationUpdater,
                    cmgMgr
            );

            clusterSvc.updateMetadata(new NodeMetadata(restComponent.host(), restComponent.httpPort(), restComponent.httpsPort()));

            restAddressReporter.writeReport(restHttpAddress(), restHttpsAddress());

            LOG.info("Components started, joining the cluster");

            return cmgMgr.joinFuture()
                    // using the default executor to avoid blocking the CMG Manager threads
                    .thenRunAsync(() -> {
                        LOG.info("Join complete, starting the remaining components");

                        // Start all other components after the join request has completed and the node has been validated.
                        try {
                            lifecycleManager.startComponents(
                                    metaStorageMgr,
                                    clusterCfgMgr,
                                    // TODO: IGNITE-16985 Design table management flow
                                    // placementDriverMgr,
                                    metricManager,
                                    distributionZoneManager,
                                    computeComponent,
                                    replicaMgr,
                                    txManager,
                                    baselineMgr,
                                    dataStorageMgr,
                                    schemaManager,
                                    volatileLogStorageFactoryCreator,
                                    outgoingSnapshotsManager,
                                    distributedTblMgr,
                                    catalogManager,
                                    indexManager,
                                    qryEngine,
                                    clientHandlerModule,
                                    deploymentManager
                            );
                        } catch (NodeStoppingException e) {
                            throw new CompletionException(e);
                        }
                    }, startupExecutor)
                    .thenComposeAsync(v -> {
                        LOG.info("Components started, performing recovery");

                        // Recovery future must be created before configuration listeners are triggered.
                        CompletableFuture<?> recoveryFuture = RecoveryCompletionFutureFactory.create(
                                clusterCfgMgr,
                                fut -> new ConfigurationCatchUpListener(cfgStorage, fut, LOG)
                        );

                        return notifyConfigurationListeners()
                                .thenComposeAsync(t -> {
                                    // Deploy all registered watches because all components are ready and have registered their listeners.
                                    try {
                                        metaStorageMgr.deployWatches();
                                    } catch (NodeStoppingException e) {
                                        throw new CompletionException(e);
                                    }

                                    return recoveryFuture;
                                }, startupExecutor);
                    }, startupExecutor)
                    .thenRunAsync(() -> {
                        SecurityConfiguration restConfiguration = clusterCfgMgr.configurationRegistry()
                                .getConfiguration(SecurityConfiguration.KEY);

                        distributedConfigurationUpdater.setClusterRestConfiguration(restConfiguration);
                    }, startupExecutor)
                    // Signal that local recovery is complete and the node is ready to join the cluster.
                    .thenComposeAsync(v -> {
                        LOG.info("Recovery complete, finishing join");

                        return cmgMgr.onJoinReady();
                    }, startupExecutor)
                    .thenRunAsync(() -> {
                        try {
                            // Transfer the node to the STARTED state.
                            lifecycleManager.onStartComplete();
                        } catch (NodeStoppingException e) {
                            throw new CompletionException(e);
                        }
                    }, startupExecutor)
                    .handleAsync((v, e) -> {
                        if (e != null) {
                            throw handleStartException(e);
                        }

                        return (Ignite) this;
                    }, startupExecutor)
                    // Moving to the common pool on purpose to close the startup pool and proceed user's code in the common pool.
                    .whenCompleteAsync((res, ex) -> {
                        startupExecutor.shutdownNow();
                    });
        } catch (Throwable e) {
            startupExecutor.shutdownNow();

            throw handleStartException(e);
        }
    }

    private RuntimeException handleStartException(Throwable e) {
        String errMsg = "Unable to start [node=" + name + "]";

        LOG.debug(errMsg, e);

        lifecycleManager.stopNode();

        return new IgniteException(errMsg, e);
    }

    /**
     * Stops ignite node.
     */
    public void stop() {
        lifecycleManager.stopNode();
        restAddressReporter.removeReport();
    }

    /** {@inheritDoc} */
    @Override
    public IgniteTables tables() {
        return distributedTblMgr;
    }

    public QueryProcessor queryEngine() {
        return qryEngine;
    }

    @TestOnly
    public IndexManager indexManager() {
        return indexManager;
    }

    /** {@inheritDoc} */
    @Override
    public IgniteTransactions transactions() {
        return new IgniteTransactionsImpl(txManager);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteSql sql() {
        return sql;
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        IgnitionManager.stop(name);
    }

    /** {@inheritDoc} */
    @Override
    public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override
    public IgniteCompute compute() {
        return compute;
    }

    /** {@inheritDoc} */
    @Override
    public Collection<ClusterNode> clusterNodes() {
        return clusterSvc.topologyService().allMembers();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Collection<ClusterNode>> clusterNodesAsync() {
        return CompletableFuture.completedFuture(clusterNodes());
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
        String host = restComponent.host();
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
    public NetworkAddress restHttpsAddress() {
        String host = restComponent.host();
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
     * @param cmgNodeNames         names of nodes that will host the CMG.
     * @param clusterName Human-readable name of a cluster.
     * @throws NodeStoppingException If node stopping intention was detected.
     */
    public void init(
            Collection<String> metaStorageNodeNames,
            Collection<String> cmgNodeNames,
            String clusterName,
            AuthenticationConfig authenticationConfig
    ) throws NodeStoppingException {
        cmgMgr.initCluster(metaStorageNodeNames, cmgNodeNames, clusterName, authenticationConfig);
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
    private static VaultManager createVault(String nodeName, Path workDir) {
        Path vaultPath = workDir.resolve(VAULT_DB_PATH);

        try {
            Files.createDirectories(vaultPath);
        } catch (IOException e) {
            throw new IgniteInternalException(e);
        }

        return new VaultManager(new PersistentVaultService(nodeName, vaultPath));
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
    public void dropMessages(BiPredicate<String, NetworkMessage> predicate) {
        ((DefaultMessagingService) clusterSvc.messagingService()).dropMessages(predicate);
    }

    // TODO: IGNITE-18493 - remove/move this
    @TestOnly
    @Nullable
    public BiPredicate<String, NetworkMessage> dropMessagesPredicate() {
        return ((DefaultMessagingService) clusterSvc.messagingService()).dropMessagesPredicate();
    }

    // TODO: IGNITE-18493 - remove/move this
    @TestOnly
    public void stopDroppingMessages() {
        ((DefaultMessagingService) clusterSvc.messagingService()).stopDroppingMessages();
    }

    /**
     * Returns the node's hybrid clock.
     *
     * @return Hybrid clock.
     */
    @TestOnly
    public HybridClock clock() {
        return clock;
    }
}
