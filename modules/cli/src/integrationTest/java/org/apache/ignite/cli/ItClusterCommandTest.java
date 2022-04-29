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

package org.apache.ignite.cli;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.joining;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.Environment;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.cli.spec.IgniteCliSpec;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import picocli.CommandLine;

/**
 * Integration test for {@code ignite cluster} commands.
 */
@ExtendWith(WorkDirectoryExtension.class)
class ItClusterCommandTest extends AbstractCliTest {
    private static final Node FIRST_NODE = new Node(0, 10100, 10300);
    private static final Node SECOND_NODE = new Node(1, 11100, 11300);
    private static final Node THIRD_NODE = new Node(2, 12100, 12300);
    private static final Node FOURTH_NODE = new Node(3, 13100, 13300);

    private static final List<Node> NODES = List.of(FIRST_NODE, SECOND_NODE, THIRD_NODE, FOURTH_NODE);

    private static final String NL = System.lineSeparator();

    private static final Logger topologyLogger = Logger.getLogger("org.apache.ignite.network.scalecube.ScaleCubeTopologyService");

    /** DI context. */
    private ApplicationContext ctx;

    /** stderr. */
    private final ByteArrayOutputStream err = new ByteArrayOutputStream();

    /** stdout. */
    private final ByteArrayOutputStream out = new ByteArrayOutputStream();

    @BeforeEach
    void setup(@WorkDirectory Path workDir, TestInfo testInfo) throws Exception {
        AtomicBoolean allNodesAreInPhysicalTopology = new AtomicBoolean(false);

        Handler physicalTopologyWaiter = physicalTopologyWaiter(allNodesAreInPhysicalTopology);
        topologyLogger.addHandler(physicalTopologyWaiter);

        try {
            startClusterWithoutInit(workDir, testInfo);

            waitTillAllNodesJoinPhysicalTopology(allNodesAreInPhysicalTopology);
        } finally {
            topologyLogger.removeHandler(physicalTopologyWaiter);
        }

        ctx = ApplicationContext.run(Environment.TEST);
    }

    private Handler physicalTopologyWaiter(AtomicBoolean physicalTopologyIsFull) {
        return new HandlerAdapter() {
            @Override
            public void publish(LogRecord record) {
                if (record.getMessage().contains("Topology snapshot [nodes=" + NODES.size() + "]")) {
                    physicalTopologyIsFull.set(true);
                }
            }
        };
    }

    private void startClusterWithoutInit(Path workDir, TestInfo testInfo) {
        CompletableFuture<?>[] futures = NODES.stream()
                .map(node -> runAsync(() -> startNodeWithoutInit(node, workDir, testInfo)))
                .toArray(CompletableFuture[]::new);

        CompletableFuture.allOf(futures).join();
    }

    private void waitTillAllNodesJoinPhysicalTopology(AtomicBoolean allNodesAreInPhysicalTopology) throws InterruptedException {
        assertTrue(IgniteTestUtils.waitForCondition(allNodesAreInPhysicalTopology::get, 10_000));
    }

    /**
     * Initiates node start and waits till it makes its REST endpoints available, but does NOT invoke init.
     *
     * @param node      node
     * @param workDir   working directory
     * @param testInfo  test info
     * @throws InterruptedException if we get interrupted
     */
    private void startNodeWithoutInit(Node node, Path workDir, TestInfo testInfo) throws InterruptedException, IOException {
        String nodeName = testNodeName(testInfo, node.nodeIndex);

        String config = configJsonFor(node);

        IgnitionManager.start(nodeName, config, workDir.resolve(nodeName));

        assertTrue(IgniteTestUtils.waitForCondition(() -> isHttpServedAt(node.restHostPort()), 10_000));
    }

    private String configJsonFor(Node node) throws IOException {
        String config = Files.readString(Path.of("src/integrationTest/resources/hardcoded-ports-config.json"));
        config = config.replaceAll("<NETWORK_PORT>", String.valueOf(node.networkPort));
        config = config.replaceAll("<REST_PORT>", String.valueOf(node.restPort));
        config = config.replaceAll("<NET_CLUSTER_NODES>", netClusterNodes());

        return config;
    }

    private String netClusterNodes() {
        return NODES.stream()
                .map(Node::networkHostPort)
                .map(s -> "\"" + s + "\"")
                .collect(joining(", ", "[", "]"));
    }

    private boolean isHttpServedAt(String hostPort) {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://" + hostPort + "/"))
                .GET()
                .build();

        try {
            HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.discarding());

            return true;
        } catch (IOException e) {
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    @AfterEach
    void tearDown(TestInfo testInfo) {
        for (int i = 0; i < 3; i++) {
            IgnitionManager.stop(testNodeName(testInfo, i));
        }

        if (ctx != null) {
            ctx.stop();
        }
    }

    /**
     * Starts a cluster of 4 nodes and executes init command on it. First node is used to issue the command via REST endpoint,
     * second will host the meta-storage RAFT group, third will host the Cluster Management RAFT Group (CMG), fourth
     * will be just a node.
     *
     * @param testInfo test info (used to derive node names)
     */
    @Test
    void initClusterWithNodesOfDifferentRoles(TestInfo testInfo) {
        int exitCode = cmd(ctx).execute(
                "cluster", "init",
                "--node-endpoint", FIRST_NODE.restHostPort(),
                "--meta-storage-node", SECOND_NODE.nodeName(testInfo),
                "--cmg-node", THIRD_NODE.nodeName(testInfo)
        );

        assertThat(
                String.format("Wrong exit code; std is '%s', stderr is '%s'", out.toString(UTF_8), err.toString(UTF_8)),
                exitCode, is(0)
        );
        assertThat(out.toString(UTF_8), is("Cluster was initialized successfully." + NL));

        // TODO: when IGNITE-16526 is implemented, also check that the logical topology contains all 4 nodes
    }

    /**
     * Creates a new command line interpreter.
     *
     * @param applicationCtx DI context.
     * @return New command line instance.
     */
    private CommandLine cmd(ApplicationContext applicationCtx) {
        CommandLine.IFactory factory = new CommandFactory(applicationCtx);

        return new CommandLine(IgniteCliSpec.class, factory)
                .setErr(new PrintWriter(err, true))
                .setOut(new PrintWriter(out, true));
    }

    private static class Node {
        private final int nodeIndex;
        private final int networkPort;
        private final int restPort;

        private Node(int nodeIndex, int networkPort, int restPort) {
            this.nodeIndex = nodeIndex;
            this.networkPort = networkPort;
            this.restPort = restPort;
        }

        String nodeName(TestInfo testInfo) {
            return testNodeName(testInfo, nodeIndex);
        }

        String networkHostPort() {
            return "localhost:" + networkPort;
        }

        String restHostPort() {
            return "localhost:" + restPort;
        }
    }
}
