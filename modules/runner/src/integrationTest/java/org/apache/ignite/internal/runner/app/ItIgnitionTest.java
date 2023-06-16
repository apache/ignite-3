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
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.InitParameters;
import org.apache.ignite.internal.IgniteIntegrationTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Ignition interface tests.
 */
class ItIgnitionTest extends IgniteIntegrationTest {
    /** Network ports of the test nodes. */
    private static final int[] PORTS = {3344, 3345, 3346};

    /** Nodes bootstrap configuration. */
    private final Map<String, String> nodesBootstrapCfg = new LinkedHashMap<>();

    /** Collection of started nodes. */
    private final List<Ignite> startedNodes = new ArrayList<>();

    private final List<String> startedNodeNames = new ArrayList<>();

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
                        + "  clientConnector.port: 10800\n"
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
                        + "  clientConnector.port: 10801\n"
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
                        + "  clientConnector.port: 10802\n"
                        + "}"
        );
    }

    /**
     * After each.
     */
    @AfterEach
    void tearDown() throws Exception {
        List<AutoCloseable> closeables = startedNodeNames.stream()
                .map(name -> (AutoCloseable) () -> IgnitionManager.stop(name))
                .collect(Collectors.toList());

        IgniteUtils.closeAll(closeables);
    }

    /**
     * Check that Ignition.start() with bootstrap configuration returns Ignite instance.
     */
    @Test
    void testNodesStartWithBootstrapConfiguration() {
        for (Map.Entry<String, String> e : nodesBootstrapCfg.entrySet()) {
            startNode(e.getKey(), name -> {
                Path nodeWorkDir = workDir.resolve(name);
                Path configPath = nodeWorkDir.resolve("ignite-config.conf");
                try {
                    Files.createDirectories(nodeWorkDir);
                    Files.writeString(configPath, e.getValue());
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
                return IgnitionManager.start(name, configPath, nodeWorkDir);
            });
        }

        assertEquals(3, startedNodes.size());

        startedNodes.forEach(Assertions::assertNotNull);
    }

    /**
     * Tests scenario when we try to start node with invalid configuration.
     */
    @Test
    void testErrorWhenStartNodeWithInvalidConfiguration() {
        assertThrowsWithCause(
                () -> startNode(
                        "invalid-config-name",
                        name -> IgnitionManager.start(name, Path.of("no-such-path"), workDir.resolve(name))
                ),
                IgniteException.class,
                "Config file doesn't exist"
        );
    }

    private void startNode(String nodeName, Function<String, CompletableFuture<Ignite>> starter) {
        startedNodeNames.add(nodeName);

        CompletableFuture<Ignite> future = starter.apply(nodeName);

        if (startedNodes.isEmpty()) {
            InitParameters initParameters = InitParameters.builder()
                    .destinationNodeName(nodeName)
                    .metaStorageNodeNames(List.of(nodeName))
                    .clusterName("cluster")
                    .build();
            IgnitionManager.init(initParameters);
        }

        assertThat(future, willCompleteSuccessfully());

        startedNodes.add(future.join());
    }

}
