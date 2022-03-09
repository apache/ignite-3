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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.client.handler.ClientHandlerModule;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.configuration.schemas.compute.ComputeConfiguration;
import org.apache.ignite.configuration.schemas.network.NetworkConfiguration;
import org.apache.ignite.configuration.schemas.store.DataStorageConfiguration;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.baseline.BaselineManager;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.messages.CmgMessagesSerializationRegistryInitializer;
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
import org.apache.ignite.internal.configuration.storage.DistributedConfigurationStorage;
import org.apache.ignite.internal.configuration.storage.LocalConfigurationStorage;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.server.persistence.RocksDbKeyValueStorage;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.rest.RestComponent;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.SqlQueryProcessor;
import org.apache.ignite.internal.sql.engine.message.SqlQueryMessagesSerializationRegistryInitializer;
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
     * Path for the partitions persistent storage.
     */
    private static final Path PARTITIONS_STORE_PATH = Paths.get("db");

    /** Ignite node name. */
    private final String name;

    /** Vault manager. */
    private final VaultManager vaultMgr;

    /** Sql query engine. */
    private final SqlQueryProcessor qryEngine;

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

    /** Node status. Adds ability to stop currently starting node. */
    private final AtomicReference<Status> status = new AtomicReference<>(Status.STARTING);

    @Nullable
    private transient IgniteCompute compute;

    /** JVM pause detector. */
    private final LongJvmPauseDetector longJvmPauseDetector;

    /**
     * The Constructor.
     *
     * @param name Ignite node name.
     * @param workDir Work directory for the started node. Must not be {@code null}.
     * @param serviceProviderClassLoader The class loader to be used to load provider-configuration files and provider classes, or
     *      {@code null} if the system class loader (or, failing that the bootstrap class loader) is to be used.
     */
    IgniteImpl(String name, Path workDir, ClassLoader serviceProviderClassLoader) {
        this.name = name;

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

        computeComponent = new ComputeComponentImpl(this, clusterSvc.messagingService(),
                nodeCfgMgr.configurationRegistry().getConfiguration(ComputeConfiguration.KEY));

        raftMgr = new Loza(clusterSvc, workDir);

        txManager = new TableTxManagerImpl(clusterSvc, new HeapLockManager());

        cmgMgr = new ClusterManagementGroupManager(clusterSvc, raftMgr, restComponent);

        metaStorageMgr = new MetaStorageManager(
                vaultMgr,
                clusterSvc,
                cmgMgr,
                raftMgr,
                new RocksDbKeyValueStorage(workDir.resolve(METASTORAGE_DB_PATH))
        );

        clusterCfgMgr = new ConfigurationManager(
                modules.distributed().rootKeys(),
                modules.distributed().validators(),
                new DistributedConfigurationStorage(metaStorageMgr, vaultMgr),
                modules.distributed().internalSchemaExtensions(),
                modules.distributed().polymorphicSchemaExtensions()
        );

        baselineMgr = new BaselineManager(
                clusterCfgMgr,
                metaStorageMgr,
                clusterSvc
        );

        Consumer<Consumer<Long>> registry = (c) -> {
            clusterCfgMgr.configurationRegistry().listenUpdateStorageRevision(newStorageRevision -> {
                c.accept(newStorageRevision);

                return CompletableFuture.completedFuture(null);
            });
        };

        distributedTblMgr = new TableManager(
                registry,
                clusterCfgMgr.configurationRegistry().getConfiguration(TablesConfiguration.KEY),
                clusterCfgMgr.configurationRegistry().getConfiguration(DataStorageConfiguration.KEY),
                raftMgr,
                baselineMgr,
                clusterSvc.topologyService(),
                getPartitionsStorePath(workDir),
                txManager
        );

        qryEngine = new SqlQueryProcessor(
                registry,
                clusterSvc,
                distributedTblMgr
        );

        clientHandlerModule = new ClientHandlerModule(
                qryEngine,
                distributedTblMgr,
                new IgniteTransactionsImpl(txManager),
                nodeCfgMgr.configurationRegistry(),
                nettyBootstrapFactory
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
     * @param cfg Optional node configuration based on {@link org.apache.ignite.configuration.schemas.network.NetworkConfigurationSchema}.
     *      Following rules are used for applying the configuration properties:
     *      <ol>
     *      <li>Specified property overrides existing one or just applies itself if it wasn't
     *      previously specified.</li>
     *      <li>All non-specified properties either use previous value or use default one from
     *      corresponding configuration schema.</li>
     *      </ol>
     *      So that, in case of initial node start (first start ever) specified configuration, supplemented with defaults, is
     *      used. If no configuration was provided defaults are used for all configuration properties. In case of node
     *      restart, specified properties override existing ones, non specified properties that also weren't specified
     *      previously use default values. Please pay attention that previously specified properties are searched in the
     *      {@code workDir} specified by the user.
     */
    public void start(@Language("HOCON") @Nullable String cfg) {
        List<IgniteComponent> startedComponents = new ArrayList<>();

        try {
            doStartComponent(
                    name,
                    startedComponents,
                    longJvmPauseDetector
            );

            // Vault startup.
            doStartComponent(
                    name,
                    startedComponents,
                    vaultMgr
            );

            vaultMgr.putName(name).join();

            // Node configuration manager startup.
            doStartComponent(name, startedComponents, nodeCfgMgr);

            // Node configuration manager bootstrap.
            if (cfg != null) {
                try {
                    nodeCfgMgr.bootstrap(cfg);
                } catch (Exception e) {
                    throw new IgniteException("Unable to parse user-specific configuration.", e);
                }
            } else {
                nodeCfgMgr.configurationRegistry().initializeDefaults();
            }

            // Start the remaining components.
            List<IgniteComponent> otherComponents = List.of(
                    nettyBootstrapFactory,
                    clusterSvc,
                    restComponent,
                    computeComponent,
                    raftMgr,
                    txManager,
                    clusterCfgMgr,
                    cmgMgr,
                    metaStorageMgr,
                    baselineMgr,
                    distributedTblMgr,
                    qryEngine,
                    clientHandlerModule
            );

            for (IgniteComponent component : otherComponents) {
                doStartComponent(name, startedComponents, component);
            }

            notifyConfigurationListeners();

            // Deploy all registered watches because all components are ready and have registered their listeners.
            metaStorageMgr.deployWatches();

            if (!status.compareAndSet(Status.STARTING, Status.STARTED)) {
                throw new NodeStoppingException();
            }
        } catch (Exception e) {
            String errMsg = "Unable to start node=[" + name + "].";

            LOG.error(errMsg, e);

            doStopNode(startedComponents);

            throw new IgniteException(errMsg, e);
        }
    }

    /**
     * Stops ignite node.
     */
    public void stop() {
        if (status.getAndSet(Status.STOPPING) == Status.STARTED) {
            doStopNode(List.of(longJvmPauseDetector, vaultMgr, nodeCfgMgr, clusterSvc, computeComponent, raftMgr, txManager, metaStorageMgr,
                    clusterCfgMgr, cmgMgr, baselineMgr, distributedTblMgr, qryEngine, restComponent, clientHandlerModule,
                    nettyBootstrapFactory));
        }
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
    public void setBaseline(Set<String> baselineNodes) {
        try {
            distributedTblMgr.setBaseline(baselineNodes);
        } catch (NodeStoppingException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public IgniteCompute compute() {
        if (compute == null) {
            compute = new IgniteComputeImpl(clusterSvc.topologyService(), computeComponent);
        }
        return compute;
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
     * @param metaStorageNodeNames names of nodes that will host the Meta Storage and the CMG.
     * @throws NodeStoppingException If node stopping intention was detected.
     */
    public void init(Collection<String> metaStorageNodeNames) throws NodeStoppingException {
        init(metaStorageNodeNames, List.of());
    }

    /**
     * Initializes the cluster that this node is present in.
     *
     * @param metaStorageNodeNames names of nodes that will host the Meta Storage.
     * @param cmgNodeNames names of nodes that will host the CMG.
     * @throws NodeStoppingException If node stopping intention was detected.
     */
    public void init(Collection<String> metaStorageNodeNames, Collection<String> cmgNodeNames) throws NodeStoppingException {
        cmgMgr.initCluster(metaStorageNodeNames, cmgNodeNames);
    }

    /**
     * Checks node status. If it's {@link Status#STOPPING} then prevents further starting and throws NodeStoppingException that will lead to
     * stopping already started components later on, otherwise starts component and add it to started components list.
     *
     * @param nodeName Node name.
     * @param startedComponents List of already started components for given node.
     * @param component Ignite component to start.
     * @param <T> Ignite component type.
     * @throws NodeStoppingException If node stopping intention was detected.
     */
    private <T extends IgniteComponent> void doStartComponent(
            @NotNull String nodeName,
            @NotNull List<IgniteComponent> startedComponents,
            @NotNull T component
    ) throws NodeStoppingException {
        if (status.get() == Status.STOPPING) {
            throw new NodeStoppingException("Node=[" + nodeName + "] was stopped.");
        } else {
            startedComponents.add(component);

            component.start();
        }
    }

    /**
     * Calls {@link IgniteComponent#beforeNodeStop()} and then {@link IgniteComponent#stop()} for all components in start-reverse-order.
     * Cleanups node started components map and node status map.
     *
     * @param startedComponents List of already started components for given node.
     */
    private void doStopNode(@NotNull List<IgniteComponent> startedComponents) {
        ListIterator<IgniteComponent> beforeStopIter =
                startedComponents.listIterator(startedComponents.size());

        while (beforeStopIter.hasPrevious()) {
            IgniteComponent componentToExecBeforeNodeStop = beforeStopIter.previous();

            try {
                componentToExecBeforeNodeStop.beforeNodeStop();
            } catch (Exception e) {
                LOG.error("Unable to execute before node stop on the component=["
                        + componentToExecBeforeNodeStop + "] within node=[" + name + ']', e);
            }
        }

        ListIterator<IgniteComponent> stopIter =
                startedComponents.listIterator(startedComponents.size());

        while (stopIter.hasPrevious()) {
            IgniteComponent componentToStop = stopIter.previous();

            try {
                componentToStop.stop();
            } catch (Exception e) {
                LOG.error("Unable to stop component=[" + componentToStop + "] within node=[" + name + ']', e);
            }
        }
    }

    /**
     * Notify all listeners of current configurations.
     *
     * @throws Exception If failed.
     */
    private void notifyConfigurationListeners() throws Exception {
        CompletableFuture.allOf(
                nodeConfiguration().notifyCurrentConfigurationListeners(),
                clusterConfiguration().notifyCurrentConfigurationListeners()
        ).get();
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

    /**
     * Node state.
     */
    private enum Status {
        STARTING,

        STARTED,

        STOPPING
    }
}
