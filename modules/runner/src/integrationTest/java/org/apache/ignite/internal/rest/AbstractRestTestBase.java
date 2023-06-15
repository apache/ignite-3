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

package org.apache.ignite.internal.rest;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
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
import org.apache.ignite.internal.IgniteIntegrationTest;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

/**
 * Base class for integration REST test.
 */
abstract class AbstractRestTestBase extends IgniteIntegrationTest {
    /** Network ports of the test nodes. */
    static final int[] PORTS = {3344, 3345, 3346};

    /** HTTP host and port url part. */
    static final String HTTP_HOST_PORT = "http://localhost:10300";

    /** Nodes bootstrap configuration. */
    final Map<String, String> nodesBootstrapCfg = new LinkedHashMap<>();

    /** Node names that is starting (before init) or started (after init). */
    final List<String> nodeNames = new ArrayList<>();

    /** Collection of started nodes. */
    final List<Ignite> startedNodes = new ArrayList<>();

    /** Collection of starting nodes. */
    final List<CompletableFuture<Ignite>> startingNodes = new ArrayList<CompletableFuture<Ignite>>();

    /** HTTP client that is expected to be defined in subclasses. */
    HttpClient client;

    /** Path to the working directory. */
    @WorkDirectory
    private Path workDir;

    static HttpRequest get(String path) {
        return HttpRequest.newBuilder(URI.create(HTTP_HOST_PORT + path)).build();
    }

    static HttpRequest patch(String path, String body) {
        return HttpRequest.newBuilder(URI.create(HTTP_HOST_PORT + path))
                .method("PATCH", BodyPublishers.ofString(body))
                .build();
    }

    static HttpRequest post(String path, String body) {
        return HttpRequest.newBuilder(URI.create(HTTP_HOST_PORT + path))
                .header("content-type", "application/json")
                .POST(BodyPublishers.ofString(body))
                .build();
    }

    /**
     * Before each.
     */
    @BeforeEach
    void setUp(TestInfo testInfo) throws IOException, InterruptedException {
        client = HttpClient.newBuilder().build();

        startAllNodesWithoutInit(testInfo);
    }

    private void startAllNodesWithoutInit(TestInfo testInfo) {
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
                        + "  clientConnector.port: 10800"
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
                        + "  clientConnector.port: 10801"
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
                        + "  clientConnector.port: 10802"
                        + "}"
        );

        for (Map.Entry<String, String> e : nodesBootstrapCfg.entrySet()) {
            startNodeWithoutInit(e.getKey(), name -> TestIgnitionManager.start(name, e.getValue(), workDir.resolve(name)));
        }
    }

    /**
     * After each.
     */
    @AfterEach
    void tearDown() throws Exception {
        List<AutoCloseable> closeables = nodeNames.stream()
                .map(name -> (AutoCloseable) () -> IgnitionManager.stop(name))
                .collect(Collectors.toList());

        IgniteUtils.closeAll(closeables);
    }

    void startNodeWithoutInit(String nodeName, Function<String, CompletableFuture<Ignite>> starter) {
        nodeNames.add(nodeName);

        CompletableFuture<Ignite> future = starter.apply(nodeName);

        startingNodes.add(future);
    }

    void checkAllNodesStarted() {
        startingNodes.forEach(future -> assertThat(future, willCompleteSuccessfully()));
    }
}
