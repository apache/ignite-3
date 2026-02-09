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

import static org.apache.ignite.internal.ConfigTemplates.NL;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.Constants.MiB;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import org.apache.ignite.IgniteServer;
import org.apache.ignite.InitParameters;
import org.apache.ignite.configuration.ConfigurationChangeException;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.configuration.NodeChange;
import org.apache.ignite.internal.configuration.NodeConfiguration;
import org.apache.ignite.internal.rest.configuration.RestExtensionChange;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.wrapper.Wrappers;
import org.apache.ignite.lang.ClusterNotInitializedException;
import org.apache.ignite.lang.NodeStartException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * IgniteServer interface tests.
 */
@SuppressWarnings("ThrowableNotThrown")
@ExtendWith(WorkDirectoryExtension.class)
class ItIgniteServerTest extends BaseIgniteAbstractTest {
    private static final String NODE_CONFIGURATION_TEMPLATE = "ignite {" + NL
            + "  network: {" + NL
            + "    port: {}," + NL
            + "    nodeFinder.netClusterNodes: [ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]" + NL
            + "  }," + NL
            + "  clientConnector.port: {}," + NL
            + "  rest.port: {}," + NL
            + "  failureHandler.handler.type: noop," + NL
            + "  failureHandler.dumpThreadsOnFailure: false," + NL
            + "  storage.profiles.default {engine: aipersist, sizeBytes: " + 256 * MiB + '}' + NL
            + "}";

    /** Nodes bootstrap configuration. */
    private final Map<String, String> nodesBootstrapCfg = new LinkedHashMap<>();

    private final List<IgniteServer> startedIgniteServers = new ArrayList<>();

    /** Path to the working directory. */
    @WorkDirectory
    private Path workDir;

    /**
     * Before each.
     */
    @BeforeEach
    void setUp(TestInfo testInfo) {
        for (int i = 0; i < 3; i++) {
            int port = 3344 + i;
            nodesBootstrapCfg.put(
                    testNodeName(testInfo, port),
                    format(NODE_CONFIGURATION_TEMPLATE, port, 10800 + i, 10300 + i)
            );
        }
    }

    /**
     * After each.
     */
    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.closeAll(startedIgniteServers.stream().map(node -> node::shutdown));
    }

    /**
     * Check that EmbeddedNode.start() with bootstrap configuration starts a node and its api() method throws an exception because the
     * cluster is not initialized.
     */
    @Test
    void testNodesStartWithBootstrapConfiguration() {
        for (Map.Entry<String, String> e : nodesBootstrapCfg.entrySet()) {
            startAndRegisterNode(e.getKey(), name -> startNode(name, e.getValue()));
        }

        assertThat(startedIgniteServers, hasSize(3));

        assertThrowsWithCause(
                () -> startedIgniteServers.get(0).api(),
                ClusterNotInitializedException.class,
                "Cluster is not initialized."
        );
    }

    /**
     * Check that IgniteServer.start() with bootstrap configuration returns a node and its api() method returns Ignite instance after init.
     */
    @Test
    void testNodesStartWithBootstrapConfigurationInitializedCluster() {
        for (Map.Entry<String, String> e : nodesBootstrapCfg.entrySet()) {
            startAndRegisterNode(e.getKey(), name -> startNode(name, e.getValue()));
        }

        assertThat(startedIgniteServers, hasSize(3));

        IgniteServer igniteServer = startedIgniteServers.get(0);
        InitParameters initParameters = InitParameters.builder()
                .metaStorageNodes(igniteServer)
                .clusterName("cluster")
                .build();
        assertThat(igniteServer.initClusterAsync(initParameters), willCompleteSuccessfully());

        // Check the api method on the first node, it should be available after the initClusterAsync is complete.
        assertThat(igniteServer.api(), notNullValue());

        startedIgniteServers.forEach(node -> {
            assertThat(node.waitForInitAsync(), willCompleteSuccessfully());
            assertThat(node.api(), notNullValue());
        });
    }

    /**
     * Check that cluster can be initialized even if nodes in the metastorage group doesn't call waitForInitAsync.
     */
    @Test
    void testClusterInitWithoutWait() {
        for (Map.Entry<String, String> e : nodesBootstrapCfg.entrySet()) {
            startAndRegisterNode(e.getKey(), name -> startNode(name, e.getValue()));
        }

        assertThat(startedIgniteServers, hasSize(3));

        IgniteServer igniteServer = startedIgniteServers.get(0);

        // Initialize the cluster with all nodes in the metastorage group
        InitParameters initParameters = InitParameters.builder()
                .metaStorageNodes(startedIgniteServers)
                .clusterName("cluster")
                .build();
        assertThat(igniteServer.initClusterAsync(initParameters), willCompleteSuccessfully());
    }

    /**
     * Tests scenario when we try to start node with invalid configuration.
     */
    @Test
    void testErrorWhenStartNodeWithInvalidConfiguration() {
        assertThrowsWithCause(
                () -> IgniteServer.start("invalid-config-name", Path.of("no-such-path"), workDir.resolve("invalid-config-name")),
                NodeStartException.class,
                "Config file doesn't exist"
        );
    }

    @ParameterizedTest
    @EnumSource
    void differentStartKindsWork(StartKind startKind) {
        for (Map.Entry<String, String> e : nodesBootstrapCfg.entrySet()) {
            startAndRegisterNode(e.getKey(), name -> startNode(name, e.getValue(), startKind));
        }

        assertThat(startedIgniteServers, hasSize(3));

        IgniteServer igniteServer = startedIgniteServers.get(0);
        InitParameters initParameters = InitParameters.builder()
                .metaStorageNodes(igniteServer)
                .clusterName("cluster")
                .build();
        assertThat(igniteServer.initClusterAsync(initParameters), willCompleteSuccessfully());
    }

    @Test
    void startWithConfigStringIsReadOnly() {
        // Start a single node
        Map.Entry<String, String> firstNode = nodesBootstrapCfg.entrySet().stream().findFirst().orElseThrow();
        startAndRegisterNode(firstNode.getKey(), name -> startNode(name, firstNode.getValue(), StartKind.START_STRING_CONFIG));

        // Initialize cluster
        IgniteServer igniteServer = startedIgniteServers.get(0);
        InitParameters initParameters = InitParameters.builder().clusterName("cluster").build();
        assertThat(igniteServer.initClusterAsync(initParameters), willCompleteSuccessfully());

        IgniteImpl igniteImpl = Wrappers.unwrap(igniteServer.api(), IgniteImpl.class);

        // Try to change node configuration
        CompletableFuture<Void> changeFuture = igniteImpl.nodeConfiguration().change(superRootChange -> {
            NodeChange nodeChange = superRootChange.changeRoot(NodeConfiguration.KEY);
            ((RestExtensionChange) nodeChange).changeRest().changePort(10400);
        });

        assertThat(changeFuture, willThrow(ConfigurationChangeException.class));
    }

    private void startAndRegisterNode(String nodeName, Function<String, IgniteServer> starter) {
        startedIgniteServers.add(starter.apply(nodeName));
    }

    private IgniteServer startNode(String name, String config) {
        return startNode(name, config, StartKind.START_FILE_CONFIG);
    }

    private IgniteServer startNode(String name, String config, Starter starter) {
        return starter.start(name, config, workDir.resolve(name));
    }

    private static Path writeConfig(Path nodeWorkDir, String config) {
        Path configPath = nodeWorkDir.resolve("ignite-config.conf");
        try {
            Files.createDirectories(nodeWorkDir);
            return Files.writeString(configPath, config);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @FunctionalInterface
    private interface Starter {
        IgniteServer start(String name, String config, Path workDir);
    }

    enum StartKind implements Starter {
        START_FILE_CONFIG {
            @Override
            public IgniteServer start(String name, String config, Path workDir) {
                return IgniteServer.start(name, writeConfig(workDir, config), workDir);
            }
        },
        START_ASYNC_JOIN_FILE_CONFIG {
            @Override
            public IgniteServer start(String name, String config, Path workDir) {
                return interruptibleJoin(IgniteServer.startAsync(name, writeConfig(workDir, config), workDir));
            }
        },
        BUILD_START_FILE_CONFIG {
            @Override
            public IgniteServer start(String name, String config, Path workDir) {
                IgniteServer server = IgniteServer.builder(name, writeConfig(workDir, config), workDir).build();
                server.start();
                return server;
            }
        },
        BUILD_START_ASYNC_JOIN_FILE_CONFIG {
            @Override
            public IgniteServer start(String name, String config, Path workDir) {
                IgniteServer server = IgniteServer.builder(name, writeConfig(workDir, config), workDir).build();
                interruptibleJoin(server.startAsync());
                return server;
            }
        },
        START_STRING_CONFIG {
            @Override
            public IgniteServer start(String name, String config, Path workDir) {
                return IgniteServer.start(name, config, workDir);
            }
        },
        START_ASYNC_JOIN_STRING_CONFIG {
            @Override
            public IgniteServer start(String name, String config, Path workDir) {
                return interruptibleJoin(IgniteServer.startAsync(name, config, workDir));
            }
        },
        BUILD_START_STRING_CONFIG {
            @Override
            public IgniteServer start(String name, String config, Path workDir) {
                IgniteServer server = IgniteServer.builder(name, config, workDir).build();
                server.start();
                return server;
            }
        },
        BUILD_START_ASYNC_JOIN_STRING_CONFIG {
            @Override
            public IgniteServer start(String name, String config, Path workDir) {
                IgniteServer server = IgniteServer.builder(name, config, workDir).build();
                interruptibleJoin(server.startAsync());
                return server;
            }
        };

        private static <T> T interruptibleJoin(CompletableFuture<T> future) {
            try {
                return future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
