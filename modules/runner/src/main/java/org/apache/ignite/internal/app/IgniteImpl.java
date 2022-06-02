/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.client.handler.ClientHandlerModule;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.configuration.schemas.compute.ComputeConfiguration;
import org.apache.ignite.configuration.schemas.network.NetworkConfiguration;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.baseline.BaselineManager;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessagesSerializationRegistryInitializer;
import org.apache.ignite.internal.cluster.management.raft.RocksDbClusterStateStorage;
import org.apache.ignite.internal.components.LongJvmPauseDetector;
import org.apache.ignite.internal.compute.ComputeComponent;
import org.apache.ignite.internal.compute.ComputeComponentImpl;
import org.apache.ignite.internal.compute.ComputeMessagesSerializationRegistryInitializer;
import org.apache.ignite.internal.compute.IgniteComputeImpl;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.ConfigurationModule;
import org.apache.ignite.internal.configuration.ConfigurationModules;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.ServiceLoaderModulesProvider;
import org.apache.ignite.internal.configuration.rest.ConfigurationHttpHandlers;
import org.apache.ignite.internal.configuration.storage.ConfigurationStorage;
import org.apache.ignite.internal.configuration.storage.DistributedConfigurationStorage;
import org.apache.ignite.internal.configuration.storage.LocalConfigurationStorage;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.server.persistence.RocksDbKeyValueStorage;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.recovery.ConfigurationCatchUpListener;
import org.apache.ignite.internal.recovery.RecoveryCompletionFutureFactory;
import org.apache.ignite.internal.rest.RestComponent;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.sql.api.IgniteSqlImpl;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.SqlQueryProcessor;
import org.apache.ignite.internal.sql.engine.message.SqlQueryMessagesSerializationRegistryInitializer;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.storage.DataStorageModule;
import org.apache.ignite.internal.storage.DataStorageModules;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.distributed.TableTxManagerImpl;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.IgniteTransactionsImpl;
import org.apache.ignite.internal.tx.message.TxMessagesSerializationRegistryInitializer;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.VaultService;
import org.apache.ignite.internal.vault.persistence.PersistentVaultService;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterLocalConfiguration;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.MessageSerializationRegistryImpl;
import org.apache.ignite.network.NettyBootstrapFactory;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.scalecube.ScaleCubeClusterServiceFactory;
import org.apache.ignite.raft.jraft.RaftMessagesSerializationRegistryInitializer;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.manager.IgniteTables;
import org.apache.ignite.tx.IgniteTransactions;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Ignite internal implementation.
 */
public class IgniteImpl implements Ignite {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(IgniteImpl.class);

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

    /** Configuration manager that handles cluster (distributed) configuration. */
    private final ConfigurationManager clusterCfgMgr;

    /** Baseline manager. */
    private final BaselineManager baselineMgr;

    /** Transactions manager. */
    private final TxManager txManager;

    /** Distributed table manager. */
    private final TableManager distributedTblMgr;

    /** Rest module. */
    private final RestComponent restComponent;

    private final ClusterManagementGroupManager cmgMgr;

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

    /**
     * The Constructor.
     *
     * @param name Ignite node name.
     * @param workDir Work directory for the started node. Must not be {@code null}.
     * @param serviceProviderClassLoader The class loader to be used to load provider-configuration files and provider classes, or
     *         {@code null} if the system class loader (or, failing that the bootstrap class loader) is to be used.
     */
    IgniteImpl(String name, Path workDir, @Nullable ClassLoader serviceProviderClassLoader) {
        this.name = name;

        lifecycleManager = new LifecycleManager(name);

        vaultMgr = createVault(workDir);

        ConfigurationModules modules = loadConfigurationModules(serviceProviderClassLoader);

        nodeCfgMgr = new ConfigurationManager(
                modules.local().rootKeys(),
                modules.local().validators(),
                new LocalConfigurationStorage(vaultMgr),
                modules.local().internalSchemaExtensions(),
                modules.local().polymorphicSchemaExtensions()
        );

        NetworkConfiguration networkConfiguration = nodeCfgMgr.configurationRegistry().getConfiguration(NetworkConfiguration.KEY);

        MessageSerializationRegistryImpl serializationRegistry = new MessageSerializationRegistryImpl();

        CmgMessagesSerializationRegistryInitializer.registerFactories(serializationRegistry);
        RaftMessagesSerializationRegistryInitializer.registerFactories(serializationRegistry);
        SqlQueryMessagesSerializationRegistryInitializer.registerFactories(serializationRegistry);
        TxMessagesSerializationRegistryInitializer.registerFactories(serializationRegistry);
        ComputeMessagesSerializationRegistryInitializer.registerFactories(serializationRegistry);

        var clusterLocalConfiguration = new ClusterLocalConfiguration(name, serializationRegistry);

        nettyBootstrapFactory = new NettyBootstrapFactory(networkConfiguration, clusterLocalConfiguration.getName());

        restComponent = new RestComponent(nodeCfgMgr, nettyBootstrapFactory);

        clusterSvc = new ScaleCubeClusterServiceFactory().createClusterService(
                clusterLocalConfiguration,
                networkConfiguration,
                nettyBootstrapFactory
        );

        computeComponent = new ComputeComponentImpl(
                this,
                clusterSvc.messagingService(),
                nodeCfgMgr.configurationRegistry().getConfiguration(ComputeConfiguration.KEY)
        );

        raftMgr = new Loza(clusterSvc, workDir);

        txManager = new TableTxManagerImpl(clusterSvc, new HeapLockManager());

        cmgMgr = new ClusterManagementGroupManager(
                vaultMgr,
                clusterSvc,
                raftMgr,
                restComponent,
                new RocksDbClusterStateStorage(workDir.resolve(CMG_DB_PATH))
        );

        metaStorageMgr = new MetaStorageManager(
                vaultMgr,
                clusterSvc,
                cmgMgr,
                raftMgr,
                new RocksDbKeyValueStorage(workDir.resolve(METASTORAGE_DB_PATH))
        );

        this.cfgStorage = new DistributedConfigurationStorage(metaStorageMgr, vaultMgr);

        clusterCfgMgr = new ConfigurationManager(
                modules.distributed().rootKeys(),
                modules.distributed().validators(),
                cfgStorage,
                modules.distributed().internalSchemaExtensions(),
                modules.distributed().polymorphicSchemaExtensions()
        );

        baselineMgr = new BaselineManager(
                clusterCfgMgr,
                metaStorageMgr,
                clusterSvc
        );

        Consumer<Function<Long, CompletableFuture<?>>> registry =
                c -> clusterCfgMgr.configurationRegistry().listenUpdateStorageRevision(c::apply);

        DataStorageModules dataStorageModules = new DataStorageModules(
                ServiceLoader.load(DataStorageModule.class, serviceProviderClassLoader)
        );

        dataStorageMgr = new DataStorageManager(
                clusterCfgMgr.configurationRegistry().getConfiguration(TablesConfiguration.KEY),
                dataStorageModules.createStorageEngines(
                        clusterCfgMgr.configurationRegistry(),
                        getPartitionsStorePath(workDir)
                )
        );

        schemaManager = new SchemaManager(
            registry,
            clusterCfgMgr.configurationRegistry().getConfiguration(TablesConfiguration.KEY)
        );

        distributedTblMgr = new TableManager(
                registry,
                clusterCfgMgr.configurationRegistry().getConfiguration(TablesConfiguration.KEY),
                raftMgr,
                baselineMgr,
                clusterSvc.topologyService(),
                txManager,
                dataStorageMgr,
                metaStorageMgr,
                schemaManager
        );

        qryEngine = new SqlQueryProcessor(
                registry,
                clusterSvc,
                distributedTblMgr,
                dataStorageMgr,
                () -> dataStorageModules.collectSchemasFields(modules.distributed().polymorphicSchemaExtensions())
        );

        sql = new IgniteSqlImpl(qryEngine);

        compute = new IgniteComputeImpl(clusterSvc.topologyService(), distributedTblMgr, computeComponent);

        clientHandlerModule = new ClientHandlerModule(
                qryEngine,
                distributedTblMgr,
                new IgniteTransactionsImpl(txManager),
                nodeCfgMgr.configurationRegistry(),
                compute,
                clusterSvc,
                nettyBootstrapFactory,
                sql
        );

        new ConfigurationHttpHandlers(nodeCfgMgr, clusterCfgMgr).registerHandlers(restComponent);

        longJvmPauseDetector = new LongJvmPauseDetector(name);
    }

    private static ConfigurationModules loadConfigurationModules(ClassLoader classLoader) {
        var modulesProvider = new ServiceLoaderModulesProvider();
        List<ConfigurationModule> modules = modulesProvider.modules(classLoader);

        if (LOG.isInfoEnabled()) {
            LOG.info("Configuration modules loaded: {}", modules);
        }

        if (modules.isEmpty()) {
            throw new IllegalStateException("No configuration modules were loaded, this means Ignite cannot start. "
                    + "Please make sure that the classloader for loading services is correct.");
        }

        var configModules = new ConfigurationModules(modules);

        if (LOG.isInfoEnabled()) {
            LOG.info("Local root keys: {}", configModules.local().rootKeys());
            LOG.info("Distributed root keys: {}", configModules.distributed().rootKeys());
        }

        return configModules;
    }

    /**
     * Starts ignite node.
     *
     * <p>When this method returns, the node is partially started and ready to accept the init command (that is, its
     * REST endpoint is functional).
     *
     * @param cfg Optional node configuration based on
     *         {@link org.apache.ignite.configuration.schemas.network.NetworkConfigurationSchema}. Following rules are used for applying the
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
    public CompletableFuture<Ignite> start(@Language("HOCON") @Nullable String cfg) {
        try {
            lifecycleManager.startComponent(longJvmPauseDetector);

            lifecycleManager.startComponent(vaultMgr);

            vaultMgr.putName(name).get();

            // Node configuration manager startup.
            lifecycleManager.startComponent(nodeCfgMgr);

            // Node configuration manager bootstrap.
            if (cfg != null) {
                try {
                    nodeCfgMgr.bootstrap(cfg);
                } catch (Exception e) {
                    throw new IgniteException("Unable to parse user-specific configuration", e);
                }
            } else {
                nodeCfgMgr.configurationRegistry().initializeDefaults();
            }

            // Start the components that are required to join the cluster.
            lifecycleManager.startComponents(
                    nettyBootstrapFactory,
                    clusterSvc,
                    restComponent,
                    raftMgr,
                    cmgMgr
            );

            return cmgMgr.joinFuture()
                    // using the default executor to avoid blocking the CMG Manager threads
                    .thenRunAsync(() -> {
                        // Start all other components after the join request has completed and the node has been validated.
                        try {
                            lifecycleManager.startComponents(
                                    metaStorageMgr,
                                    clusterCfgMgr,
                                    computeComponent,
                                    txManager,
                                    baselineMgr,
                                    dataStorageMgr,
                                    schemaManager,
                                    distributedTblMgr,
                                    qryEngine,
                                    clientHandlerModule
                            );
                        } catch (NodeStoppingException e) {
                            throw new CompletionException(e);
                        }
                    })
                    .thenCompose(v -> {
                        // Recovery future must be created before configuration listeners are triggered.
                        CompletableFuture<Void> recoveryFuture = RecoveryCompletionFutureFactory.create(
                                clusterCfgMgr,
                                fut -> new ConfigurationCatchUpListener(cfgStorage, fut, LOG)
                        );

                        return notifyConfigurationListeners()
                                .thenCompose(t -> {
                                    // Deploy all registered watches because all components are ready and have registered their listeners.
                                    try {
                                        metaStorageMgr.deployWatches();
                                    } catch (NodeStoppingException e) {
                                        throw new CompletionException(e);
                                    }

                                    return recoveryFuture;
                                });
                    })
                    // Signal that local recovery is complete and the node is ready to join the cluster.
                    .thenCompose(v -> cmgMgr.onJoinReady())
                    .thenRun(() -> {
                        try {
                            // Transfer the node to the STARTED state.
                            lifecycleManager.onStartComplete();
                        } catch (NodeStoppingException e) {
                            throw new CompletionException(e);
                        }
                    })
                    .handle((v, e) -> {
                        if (e != null) {
                            throw handleStartException(e);
                        }

                        return this;
                    });
        } catch (Throwable e) {
            throw handleStartException(e);
        }
    }

    private RuntimeException handleStartException(Throwable e) {
        String errMsg = "Unable to start node=[" + name + "].";

        LOG.error(errMsg, e);

        lifecycleManager.stopNode();

        return new IgniteException(errMsg, e);
    }

    /**
     * Stops ignite node.
     */
    public void stop() {
        lifecycleManager.stopNode();
    }

    /** {@inheritDoc} */
    @Override
    public IgniteTables tables() {
        return distributedTblMgr;
    }

    public QueryProcessor queryEngine() {
        return qryEngine;
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
     * Returns the local address of REST endpoints.
     *
     * @throws IgniteInternalException if the REST module is not started.
     */
    // TODO: should be encapsulated in local properties, see https://issues.apache.org/jira/browse/IGNITE-15131
    public NetworkAddress restAddress() {
        return NetworkAddress.from(restComponent.localAddress());
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
     * @throws NodeStoppingException If node stopping intention was detected.
     */
    public void init(
            Collection<String> metaStorageNodeNames,
            Collection<String> cmgNodeNames,
            String clusterName
    ) throws NodeStoppingException {
        cmgMgr.initCluster(metaStorageNodeNames, cmgNodeNames, clusterName);
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
    @NotNull
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
    public ClusterNode node() {
        return clusterSvc.topologyService().localMember();
    }
}
