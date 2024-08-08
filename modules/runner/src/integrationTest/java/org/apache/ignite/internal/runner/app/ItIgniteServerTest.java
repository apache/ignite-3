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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.ignite.IgniteServer;
import org.apache.ignite.InitParameters;
import org.apache.ignite.configuration.ClusterConfiguration;
import org.apache.ignite.configuration.NodeConfiguration;
import org.apache.ignite.failure.configuration.FailureProcessorBuilder;
import org.apache.ignite.failure.handlers.configuration.StopNodeOrHaltFailureHandlerBuilder;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.failure.configuration.FailureProcessorConfiguration;
import org.apache.ignite.internal.failure.handlers.configuration.FailureHandlerView;
import org.apache.ignite.internal.failure.handlers.configuration.StopNodeOrHaltFailureHandlerView;
import org.apache.ignite.internal.security.authentication.basic.BasicAuthenticationProviderView;
import org.apache.ignite.internal.security.configuration.SecurityConfiguration;
import org.apache.ignite.internal.security.configuration.SecurityView;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.ClusterNotInitializedException;
import org.apache.ignite.lang.NodeStartException;
import org.apache.ignite.security.authentication.basic.BasicAuthenticationProviderBuilder;
import org.apache.ignite.security.authentication.basic.BasicUserBuilder;
import org.apache.ignite.security.authentication.configuration.AuthenticationBuilder;
import org.apache.ignite.security.configuration.SecurityBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * IgniteServer interface tests.
 */
@ExtendWith(WorkDirectoryExtension.class)
class ItIgniteServerTest extends BaseIgniteAbstractTest {
    /** Network ports of the test nodes. */
    private static final int[] PORTS = {3344, 3345, 3346};

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
        String node0Name = testNodeName(testInfo, PORTS[0]);
        String node1Name = testNodeName(testInfo, PORTS[1]);
        String node2Name = testNodeName(testInfo, PORTS[2]);

        nodesBootstrapCfg.put(
                node0Name,
                "{\n"
                        + "  network: {\n"
                        + "    port: " + PORTS[0] + ",\n"
                        + "    nodeFinder: {\n"
                        + "      netClusterNodes: [ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n"
                        + "    }\n"
                        + "  },\n"
                        + "  clientConnector.port: 10800,\n"
                        + "  rest.port: 10300\n"
                        + "}"
        );

        nodesBootstrapCfg.put(
                node1Name,
                "{\n"
                        + "  network: {\n"
                        + "    port: " + PORTS[1] + ",\n"
                        + "    nodeFinder: {\n"
                        + "      netClusterNodes: [ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n"
                        + "    }\n"
                        + "  },\n"
                        + "  clientConnector.port: 10801,\n"
                        + "  rest.port: 10301\n"
                        + "}"
        );

        nodesBootstrapCfg.put(
                node2Name,
                "{\n"
                        + "  network: {\n"
                        + "    port: " + PORTS[2] + ",\n"
                        + "    nodeFinder: {\n"
                        + "      netClusterNodes: [ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n"
                        + "    }\n"
                        + "  },\n"
                        + "  clientConnector.port: 10802,\n"
                        + "  rest.port: 10302\n"
                        + "}"
        );
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
            startNode(e.getKey(), name -> startNode(name, e.getValue()));
        }

        assertThat(startedIgniteServers, hasSize(3));

        assertThrowsWithCause(
                () -> startedIgniteServers.get(0).api(),
                ClusterNotInitializedException.class,
                "Cluster is not initialized."
        );
    }

    /**
     * Check that EmbeddedNode.start() with bootstrap configuration returns a node and its api() method returns Ignite instance after init.
     */
    @Test
    void testNodesStartWithBootstrapConfigurationInitializedCluster() {
        for (Map.Entry<String, String> e : nodesBootstrapCfg.entrySet()) {
            startNode(e.getKey(), name -> startNode(name, e.getValue()));
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
            if (node != igniteServer) {
                assertThrowsWithCause(node::api, ClusterNotInitializedException.class, "Cluster is not initialized.");
            }
            assertThat(node.waitForInitAsync(), willCompleteSuccessfully());
            assertThat(node.api(), notNullValue());
        });
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

    @Test
    void configurationPojo() {
        NodeConfiguration nodeConfiguration = NodeConfiguration.create().failureHandler(
                FailureProcessorBuilder.create().handler(
                        StopNodeOrHaltFailureHandlerBuilder.create().timeoutMillis(1)
                ));

        String nodeName = nodesBootstrapCfg.keySet().stream().findFirst().orElseThrow();
        startNode(nodeName, name -> IgniteServer.start(name, nodeConfiguration, workDir.resolve(name), null));

        IgniteServer igniteServer = startedIgniteServers.get(0);

        ClusterConfiguration clusterConfiguration = ClusterConfiguration.create().security(
                SecurityBuilder.create()
                        .enabled(true)
                        .authentication(AuthenticationBuilder.create()
                                .addProvider("basic",
                                        BasicAuthenticationProviderBuilder.create()
                                                .addUser("username", BasicUserBuilder.create().password("password"))
                                ))
        );
        InitParameters initParameters = InitParameters.builder()
                .metaStorageNodes(igniteServer)
                .clusterName("cluster")
                .clusterConfiguration(clusterConfiguration)
                .build();
        assertThat(igniteServer.initClusterAsync(initParameters), willCompleteSuccessfully());

        FailureHandlerView handlerConfiguration = ((IgniteImpl) igniteServer.api()).nodeConfiguration()
                .getConfiguration(FailureProcessorConfiguration.KEY)
                .handler().value();
        assertThat(handlerConfiguration, instanceOf(StopNodeOrHaltFailureHandlerView.class));
        assertThat(((StopNodeOrHaltFailureHandlerView) handlerConfiguration).timeoutMillis(), is(1L));

        SecurityView securityConfiguration = ((IgniteImpl) igniteServer.api()).clusterConfiguration()
                .getConfiguration(SecurityConfiguration.KEY).value();
        assertThat(securityConfiguration.enabled(), is(true));
        assertThat(securityConfiguration.authentication().providers(), contains(instanceOf(BasicAuthenticationProviderView.class)));
        BasicAuthenticationProviderView basicProvider =
                (BasicAuthenticationProviderView) securityConfiguration.authentication().providers().get(0);
        assertThat(basicProvider.users().size(), is(1));
        assertThat(basicProvider.users().get(0).username(), is("username"));
        assertThat(basicProvider.users().get(0).password(), is("password"));
    }

    private void startNode(String nodeName, Function<String, IgniteServer> starter) {
        startedIgniteServers.add(starter.apply(nodeName));
    }

    private IgniteServer startNode(String name, String config) {
        Path nodeWorkDir = workDir.resolve(name);
        Path configPath = nodeWorkDir.resolve("ignite-config.conf");
        try {
            Files.createDirectories(nodeWorkDir);
            Files.writeString(configPath, config);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        return IgniteServer.start(name, configPath, nodeWorkDir);
    }

}
