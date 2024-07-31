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

package org.apache.ignite.internal;

import static java.util.Collections.reverse;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIMEM_PROFILE_NAME;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.apache.ignite.internal.util.IgniteUtils.stopAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteServer;
import org.apache.ignite.InitParameters;
import org.apache.ignite.configuration.ConfigurationModule;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopology;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.ConfigurationModules;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.ServiceLoaderModulesProvider;
import org.apache.ignite.internal.configuration.storage.DistributedConfigurationStorage;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.persistence.PersistentVaultService;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

/**
 * Base class for node's restart tests.
 */
public abstract class BaseIgniteRestartTest extends IgniteAbstractTest {
    /** Default node port. */
    protected static final int DEFAULT_NODE_PORT = 3344;

    protected static final int DEFAULT_CLIENT_PORT = 10800;

    protected static final int DEFAULT_HTTP_PORT = 10300;

    protected static final int DEFAULT_HTTPS_PORT = 10400;

    @Language("HOCON")
    protected static final String RAFT_CFG = "{\n"
            + "  fsync: false,\n"
            + "  retryDelay: 20\n"
            + "}";

    /** Nodes bootstrap configuration pattern. */
    @Language("HOCON")
    protected static final String NODE_BOOTSTRAP_CFG = "{\n"
            + "  network.port: {},\n"
            + "  network.nodeFinder.netClusterNodes: {}\n"
            + "  network.membership: {\n"
            + "    membershipSyncInterval: 1000,\n"
            + "    failurePingInterval: 500,\n"
            + "    scaleCube: {\n"
            + "      membershipSuspicionMultiplier: 1,\n"
            + "      failurePingRequestMembers: 1,\n"
            + "      gossipInterval: 10\n"
            + "    },\n"
            + "  },\n"
            + "  raft: " + RAFT_CFG + ",\n"
            + "  clientConnector.port: {},\n"
            + "  storage: {\n"
            + "    profiles: {" + DEFAULT_AIMEM_PROFILE_NAME + ": { engine: \"aimem\"}}\n"
            + "  },\n"
            + "  rest: {\n"
            + "    port: {}, \n"
            + "    ssl.port: {} \n"
            + "  },\n"
            + "  nodeAttributes: {"
            + "    nodeAttributes: {}"
            + "  }"
            + "}";

    public TestInfo testInfo;

    protected static final List<IgniteServer> IGNITE_SERVERS = new ArrayList<>();

    /** Cluster nodes. */
    protected List<PartialNode> partialNodes;

    protected static final long TIMEOUT_MILLIS = 10_000L;

    @BeforeEach
    void setUp(TestInfo testInfo) {
        this.testInfo = testInfo;
        this.partialNodes = new CopyOnWriteArrayList<>();
    }

    /**
     * Stops all started nodes.
     */
    @AfterEach
    public void afterEachTest() throws Exception {
        var closeables = new ArrayList<AutoCloseable>();

        for (IgniteServer node : IGNITE_SERVERS) {
            if (node != null) {
                closeables.add(node::shutdown);
            }
        }

        if (!partialNodes.isEmpty()) {
            for (PartialNode partialNode : partialNodes) {
                closeables.add(partialNode::stop);
            }
        }

        closeAll(closeables);

        IGNITE_SERVERS.clear();
    }

    /**
     * Load configuration modules.
     *
     * @param log Log.
     * @param classLoader Class loader.
     * @return Configuration modules.
     */
    public static ConfigurationModules loadConfigurationModules(IgniteLogger log, ClassLoader classLoader) {
        var modulesProvider = new ServiceLoaderModulesProvider();
        List<ConfigurationModule> modules = modulesProvider.modules(classLoader);

        if (log.isInfoEnabled()) {
            log.info("Configuration modules loaded: {}", modules);
        }

        if (modules.isEmpty()) {
            throw new IllegalStateException("No configuration modules were loaded, this means Ignite cannot start. "
                    + "Please make sure that the classloader for loading services is correct.");
        }

        var configModules = new ConfigurationModules(modules);

        if (log.isInfoEnabled()) {
            log.info("Local root keys: {}", configModules.local().rootKeys());
            log.info("Distributed root keys: {}", configModules.distributed().rootKeys());
        }

        return configModules;
    }

    /**
     * Starts the Vault component.
     */
    public static VaultManager createVault(Path workDir) {
        Path vaultPath = workDir.resolve(Paths.get("vault"));

        try {
            Files.createDirectories(vaultPath);
        } catch (IOException e) {
            throw new IgniteInternalException(e);
        }

        return new VaultManager(new PersistentVaultService(vaultPath));
    }

    /**
     * Find component of a given type in list.
     * Note that it could be possible that in a list of components are presented several instances of a one class.
     *
     * @param components Components list.
     * @param cls Class.
     * @param <T> Type parameter.
     * @return Ignite component.
     */
    @Nullable
    public static <T extends IgniteComponent> T findComponent(List<IgniteComponent> components, Class<T> cls) {
        for (IgniteComponent component : components) {
            if (cls.isAssignableFrom(component.getClass())) {
                return cls.cast(component);
            }
        }

        return null;
    }

    /**
     * Build a configuration string.
     *
     * @param idx Node index.
     * @return Configuration string.
     */
    protected String configurationString(int idx) {
        return configurationString(idx, "{}");
    }

    /**
     * Build a configuration string.
     *
     * @param idx Node index.
     * @param attributes Node attributes, should be empty string if not needed.
     * @return Configuration string.
     */
    protected static String configurationString(int idx, String attributes) {
        int port = DEFAULT_NODE_PORT + idx;
        int clientPort = DEFAULT_CLIENT_PORT + idx;
        int httpPort = DEFAULT_HTTP_PORT + idx;
        int httpsPort = DEFAULT_HTTPS_PORT + idx;

        // The address of the first node.
        @Language("HOCON") String connectAddr = "[localhost\":\"" + DEFAULT_NODE_PORT + "]";

        return IgniteStringFormatter.format(NODE_BOOTSTRAP_CFG, port, connectAddr, clientPort, httpPort, httpsPort, attributes);
    }

    /**
     * Returns partial node. Chains deploying watches to configuration notifications and waits for it,
     * so returned partial node is started and ready to work.
     *
     * @param name Node name.
     * @param nodeCfgMgr Node configuration manager.
     * @param clusterCfgMgr Cluster configuration manager.
     * @param components Started components of a node.
     * @param localConfigurationGenerator Local configuration generator.
     * @param logicalTopology Logical topology.
     * @param cfgStorage Distributed configuration storage.
     * @param distributedConfigurationGenerator Distributes configuration generator.
     * @param clock Hybrid clock.
     * @return Partial node.
     */
    public PartialNode partialNode(
            String name,
            ConfigurationManager nodeCfgMgr,
            ConfigurationManager clusterCfgMgr,
            MetaStorageManager metaStorageMgr,
            List<IgniteComponent> components,
            ConfigurationTreeGenerator localConfigurationGenerator,
            LogicalTopologyImpl logicalTopology,
            DistributedConfigurationStorage cfgStorage,
            ConfigurationTreeGenerator distributedConfigurationGenerator,
            ConfigurationRegistry clusterConfigRegistry,
            HybridClock clock
    ) {
        CompletableFuture<?> startFuture = CompletableFuture.allOf(
                nodeCfgMgr.configurationRegistry().notifyCurrentConfigurationListeners(),
                clusterConfigRegistry.notifyCurrentConfigurationListeners(),
                ((MetaStorageManagerImpl) metaStorageMgr).notifyRevisionUpdateListenerOnStart()
        ).thenCompose(unused ->
                // Deploy all registered watches because all components are ready and have registered their listeners.
                metaStorageMgr.deployWatches()
        );

        assertThat("Partial node was not started", startFuture, willCompleteSuccessfully());

        Long recoveryRevision = metaStorageMgr.recoveryFinishedFuture().getNow(null);

        assertNotNull(recoveryRevision);

        log.info("Completed recovery on partially started node, MetaStorage revision recovered to: " + recoveryRevision);

        return new PartialNode(
                name,
                components,
                List.of(localConfigurationGenerator, distributedConfigurationGenerator),
                logicalTopology,
                log,
                clock
        );
    }

    /**
     * Starts a node with the given parameters.
     *
     * @param idx Node index.
     * @return Created node instance.
     */
    protected IgniteImpl startNode(int idx) {
        return startNode(idx, null);
    }

    /**
     * Starts a node with the given parameters.
     *
     * @param idx Node index.
     * @param cfg Configuration string or {@code null} to use the default configuration.
     * @return Created node instance.
     */
    protected IgniteImpl startNode(int idx, @Nullable String cfg) {
        boolean initNeeded = IGNITE_SERVERS.isEmpty();

        IgniteServer node = startEmbeddedNode(idx, cfg);

        if (initNeeded) {
            InitParameters initParameters = InitParameters.builder()
                    .metaStorageNodes(node)
                    .clusterName("cluster")
                    .build();
            TestIgnitionManager.init(node, initParameters);
        }

        assertThat(node.waitForInitAsync(), willCompleteSuccessfully());

        return (IgniteImpl) node.api();
    }

    /**
     * Starts a node with the given parameters. Does not run the Init command.
     *
     * @param idx Node index.
     * @param cfg Configuration string or {@code null} to use the default configuration.
     * @return Future that completes with a created node instance.
     */
    protected IgniteServer startEmbeddedNode(int idx, @Nullable String cfg) {
        String nodeName = testNodeName(testInfo, idx);

        String cfgString = cfg == null ? configurationString(idx) : cfg;

        IgniteServer node = TestIgnitionManager.start(nodeName, cfgString, workDir.resolve(nodeName));

        if (IGNITE_SERVERS.size() == idx) {
            IGNITE_SERVERS.add(node);
        } else {
            assertNull(IGNITE_SERVERS.get(idx));

            IGNITE_SERVERS.set(idx, node);
        }

        return node;
    }

    /**
     * Starts an {@code amount} number of nodes (with sequential indices starting from 0).
     */
    protected List<IgniteImpl> startNodes(int amount) {
        boolean initNeeded = IGNITE_SERVERS.isEmpty();

        List<IgniteServer> nodes = IntStream.range(0, amount)
                .mapToObj(i -> startEmbeddedNode(i, null))
                .collect(toList());

        if (initNeeded) {
            IgniteServer node = nodes.get(0);

            InitParameters initParameters = InitParameters.builder()
                    .metaStorageNodes(node)
                    .clusterName("cluster")
                    .build();
            TestIgnitionManager.init(node, initParameters);
        }

        return nodes.stream()
                .map(node -> {
                    assertThat(node.waitForInitAsync(), willCompleteSuccessfully());

                    return (IgniteImpl) node.api();
                })
                .collect(toList());
    }

    /**
     * Stop the node with given index.
     *
     * @param idx Node index.
     */
    protected void stopNode(int idx) {
        IgniteServer node = IGNITE_SERVERS.set(idx, null);

        if (node != null) {
            node.shutdown();
        }
    }

    /**
     * Node with partially started components.
     */
    public static class PartialNode {
        private final String name;

        private final List<IgniteComponent> startedComponents;

        private final List<ManuallyCloseable> closeables;

        private final LogicalTopology logicalTopology;

        private final IgniteLogger log;

        private final HybridClock clock;

        PartialNode(
                String name,
                List<IgniteComponent> startedComponents,
                List<ManuallyCloseable> closeables,
                LogicalTopology logicalTopology,
                IgniteLogger log,
                HybridClock clock
        ) {
            this.name = name;
            this.startedComponents = startedComponents;
            this.closeables = closeables;
            this.logicalTopology = logicalTopology;
            this.log = log;
            this.clock = clock;
        }

        /**
         * Node name.
         *
         * @return Node name.
         */
        public String name() {
            return name;
        }

        /**
         * Stops node.
         */
        public void stop() {
            List<IgniteComponent> components = new ArrayList<>(startedComponents);
            reverse(components);

            for (IgniteComponent component : components) {
                try {
                    component.beforeNodeStop();
                } catch (Exception e) {
                    log.error("Error during calling `beforeNodeStop`", e);
                }
            }

            assertThat(stopAsync(new ComponentContext(), components), willCompleteSuccessfully());

            closeables.forEach(c -> {
                try {
                    c.close();
                } catch (Exception e) {
                    log.error("Error during close", e);
                }
            });
        }

        public List<IgniteComponent> startedComponents() {
            return startedComponents;
        }

        public LogicalTopology logicalTopology() {
            return logicalTopology;
        }

        public HybridClock clock() {
            return clock;
        }
    }
}
