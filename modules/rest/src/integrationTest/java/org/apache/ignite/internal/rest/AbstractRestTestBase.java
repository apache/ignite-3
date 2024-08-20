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

import static java.util.stream.Collectors.joining;
import static org.apache.ignite.internal.Cluster.BASE_CLIENT_PORT;
import static org.apache.ignite.internal.Cluster.BASE_HTTP_PORT;
import static org.apache.ignite.internal.Cluster.BASE_PORT;
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
import java.util.stream.IntStream;
import org.apache.ignite.IgniteServer;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
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
    private static final String NODE_CONFIG_TEMPLATE = "ignite {\n"
            + "  network {\n"
            + "    port: {}\n"
            + "    nodeFinder.netClusterNodes: [ {} ]\n"
            + "  }\n"
            + "  clientConnector.port: {}\n"
            + "  rest.port: {}\n"
            + "}";

    /** HTTP host and port url part. */
    private static final String HTTP_HOST_PORT = "http://localhost:" + BASE_HTTP_PORT;

    /** Nodes bootstrap configuration. */
    final Map<String, String> nodesBootstrapCfg = new LinkedHashMap<>();

    /** Node names that is starting (before init) or started (after init). */
    protected final List<String> nodeNames = new ArrayList<>();

    /** Collection of starting nodes. */
    protected final List<IgniteServer> nodes = new ArrayList<>();

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
        int nodesCount = 3;

        String seedAddressesString = IntStream.range(0, nodesCount)
                .map(index -> BASE_PORT + index)
                .mapToObj(port -> "\"localhost:" + port + '\"')
                .collect(joining(", "));

        for (int nodeIndex = 0; nodeIndex < nodesCount; nodeIndex++) {
            String nodeName = testNodeName(testInfo, nodeIndex);

            String config = IgniteStringFormatter.format(
                    NODE_CONFIG_TEMPLATE,
                    BASE_PORT + nodeIndex,
                    seedAddressesString,
                    BASE_CLIENT_PORT + nodeIndex,
                    BASE_HTTP_PORT + nodeIndex
            );

            nodeNames.add(nodeName);

            nodes.add(TestIgnitionManager.start(nodeName, config, workDir.resolve(nodeName)));
        }
    }

    /**
     * After each.
     */
    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.closeAll(nodes.stream().map(node -> node::shutdown));
    }

    void checkAllNodesStarted() {
        nodes.forEach(node -> assertThat(node.waitForInitAsync(), willCompleteSuccessfully()));
    }

    protected HttpResponse<String> send(HttpRequest request) throws IOException, InterruptedException {
        return client.send(request, BodyHandlers.ofString());
    }

    protected Problem getProblem(HttpResponse<String> initResponse) throws JsonProcessingException {
        return objectMapper.readValue(initResponse.body(), Problem.class);
    }
}
