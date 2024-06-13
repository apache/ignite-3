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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.ignite.EmbeddedNode;
import org.apache.ignite.internal.rest.api.Problem;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Base class for integration REST test.
 */
@ExtendWith(WorkDirectoryExtension.class)
public abstract class AbstractRestTestBase extends BaseIgniteAbstractTest {
    /** Network ports of the test nodes. */
    static final int[] NETWORK_PORTS = {3344, 3345, 3346};

    /** Client ports of the test nodes. */
    static final int[] CLIENT_PORTS = {10800, 10801, 10802};

    /** HTTP ports of the test nodes. */
    static final int[] HTTP_PORTS = {10300, 10301, 10302};

    /** HTTP host and port url part. */
    static final String HTTP_HOST_PORT = "http://localhost:" + HTTP_PORTS[0];

    /** Nodes bootstrap configuration. */
    final Map<String, String> nodesBootstrapCfg = new LinkedHashMap<>();

    /** Node names that is starting (before init) or started (after init). */
    protected final List<String> nodeNames = new ArrayList<>();

    /** Collection of starting nodes. */
    protected final List<EmbeddedNode> nodes = new ArrayList<>();

    protected ObjectMapper objectMapper;

    /** HTTP client that is expected to be defined in subclasses. */
    private HttpClient client;

    /** Path to the working directory. */
    @WorkDirectory
    private Path workDir;

    protected static HttpRequest get(String path) {
        return HttpRequest.newBuilder(URI.create(HTTP_HOST_PORT + path)).build();
    }

    protected static HttpRequest patch(String path, String body) {
        return HttpRequest.newBuilder(URI.create(HTTP_HOST_PORT + path))
                .method("PATCH", BodyPublishers.ofString(body))
                .build();
    }

    protected static HttpRequest post(String path, String body) {
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
        objectMapper = new ObjectMapper();
        client = HttpClient.newBuilder().build();

        startAllNodesWithoutInit(testInfo);
    }

    private void startAllNodesWithoutInit(TestInfo testInfo) {
        String node0Name = testNodeName(testInfo, NETWORK_PORTS[0]);
        String node1Name = testNodeName(testInfo, NETWORK_PORTS[1]);
        String node2Name = testNodeName(testInfo, NETWORK_PORTS[2]);

        nodesBootstrapCfg.put(
                node0Name,
                "{\n"
                        + "  network: {\n"
                        + "    port: " + NETWORK_PORTS[0] + ",\n"
                        + "    nodeFinder: {\n"
                        + "      netClusterNodes: [ \"localhost:" + NETWORK_PORTS[0] + "\", \"localhost:" + NETWORK_PORTS[1]
                        + "\", \"localhost:" + NETWORK_PORTS[2] + "\" ]\n"
                        + "    }\n"
                        + "  },\n"
                        + "  clientConnector.port: " + CLIENT_PORTS[0] + ", \n"
                        + "  rest.port: " + HTTP_PORTS[0]
                        + "}"
        );

        nodesBootstrapCfg.put(
                node1Name,
                "{\n"
                        + "  network: {\n"
                        + "    port: " + NETWORK_PORTS[1] + ",\n"
                        + "    nodeFinder: {\n"
                        + "      netClusterNodes: [ \"localhost:" + NETWORK_PORTS[0] + "\", \"localhost:" + NETWORK_PORTS[1]
                        + "\", \"localhost:" + NETWORK_PORTS[2] + "\" ]\n"
                        + "    }\n"
                        + "  },\n"
                        + "  clientConnector.port: "  + CLIENT_PORTS[1] + ", \n"
                        + "  rest.port: " + HTTP_PORTS[1]
                        + "}"
        );

        nodesBootstrapCfg.put(
                node2Name,
                "{\n"
                        + "  network: {\n"
                        + "    port: " + NETWORK_PORTS[2] + ",\n"
                        + "    nodeFinder: {\n"
                        + "      netClusterNodes: [ \"localhost:" + NETWORK_PORTS[0] + "\", \"localhost:" + NETWORK_PORTS[1]
                        + "\", \"localhost:" + NETWORK_PORTS[2] + "\" ]\n"
                        + "    }\n"
                        + "  },\n"
                        + "  clientConnector.port: "  + CLIENT_PORTS[2] + ", \n"
                        + "  rest.port: " + HTTP_PORTS[2]
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
        IgniteUtils.closeAll(nodes.stream().map(node -> node::stop));
    }

    void startNodeWithoutInit(String nodeName, Function<String, EmbeddedNode> starter) {
        nodeNames.add(nodeName);

        nodes.add(starter.apply(nodeName));
    }

    void checkAllNodesStarted() {
        nodes.forEach(node -> assertThat(node.joinClusterAsync(), willCompleteSuccessfully()));
    }

    protected HttpResponse<String> send(HttpRequest request) throws IOException, InterruptedException {
        return client.send(request, BodyHandlers.ofString());
    }

    protected Problem getProblem(HttpResponse<String> initResponse) throws JsonProcessingException {
        return objectMapper.readValue(initResponse.body(), Problem.class);
    }
}
