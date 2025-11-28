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

package org.apache.ignite.internal.runner.app.client;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.client.IgniteClientConfiguration.DFLT_BACKGROUND_RECONNECT_INTERVAL;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.lang.ErrorGroups.Client.CONNECTION_ERR;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteServer;
import org.apache.ignite.InitParameters;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.IgniteClientConfiguration;
import org.apache.ignite.client.IgniteClientConnectionException;
import org.apache.ignite.client.RetryLimitPolicy;
import org.apache.ignite.client.RetryReadPolicy;
import org.apache.ignite.internal.client.ChannelValidator;
import org.apache.ignite.internal.client.IgniteClientConfigurationImpl;
import org.apache.ignite.internal.client.ProtocolContext;
import org.apache.ignite.internal.client.TcpIgniteClient;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.network.ClusterNode;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * End-to-end tests for the thin client network channel validator.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(WorkDirectoryExtension.class)
public class ItThinClientChannelValidatorTest extends BaseIgniteAbstractTest {
    private static final String ERROR_MESSAGE = "Connection to node aborted [name={}, address={}, version={}]";

    private static final int NODES_COUNT = 5;

    private final List<Ignite> startedNodes = new ArrayList<>();

    private List<IgniteServer> nodes;

    @BeforeAll
    void beforeAll(TestInfo testInfo, @WorkDirectory Path workDir) {
        setupCluster(testInfo, workDir);
    }

    @AfterAll
    void afterAll() throws Exception {
        var closeables = new ArrayList<AutoCloseable>();

        nodes.stream()
                .map(node -> (AutoCloseable) node::shutdown)
                .forEach(closeables::add);

        IgniteUtils.closeAll(closeables);
    }

    /**
     * The test verifies that the connection is established successfully if there is
     * at least one compatible node in the cluster to connect to.
     */
    @ParameterizedTest(name = "node = {0}")
    @ValueSource(ints = {0, NODES_COUNT - 1})
    void singleNodeCompatible(int compatibleNodeIdx) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(startedNodes.size());
        String compatibleNode = startedNodes.get(compatibleNodeIdx).name();

        ChannelValidator validator = ctx -> {
            latch.countDown();

            if (!compatibleNode.equals(ctx.clusterNode().name())) {
                raiseConnectException(ctx);
            }
        };

        long reconnectInterval = 1_000;

        try (IgniteClient client = startClient(reconnectInterval, validator)) {
            boolean validatorChecksMatch = latch.await(5, TimeUnit.SECONDS);

            assertThat(validatorChecksMatch, is(true));

            Awaitility.await().timeout(3, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(client.connections().size(), is(1)));

            client.sql().execute(null, "SELECT 1").close();

            // Make sure there are no new connections.
            Thread.sleep(reconnectInterval * 2);
            assertThat(client.connections().size(), is(1));
        }
    }

    /**
     * Test checks the channel validation exception if there are no compatible nodes.
     */
    @Test
    void noCompatibleNodes() {
        AtomicInteger attemptCounter = new AtomicInteger();

        ChannelValidator validator = ctx -> {
            attemptCounter.getAndIncrement();

            raiseConnectException(ctx);
        };

        IgniteTestUtils.assertThrows(
                IgniteClientConnectionException.class,
                () -> startClient(DFLT_BACKGROUND_RECONNECT_INTERVAL, validator),
                "Connection to node aborted"
        );

        assertThat(attemptCounter.get(), is(RetryLimitPolicy.DFLT_RETRY_LIMIT + /* initial attempt */ 1));
    }

    /**
     * Test verifies that the thin client successfully performs background reconnection to nodes.
     */
    @ParameterizedTest(name = "node = {0}")
    @ValueSource(ints = {0, NODES_COUNT - 1})
    void backgroundReconnect(int compatibleNodeIdx) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(startedNodes.size());
        String compatibleNode = startedNodes.get(compatibleNodeIdx).name();
        AtomicBoolean allowAll = new AtomicBoolean(false);

        ChannelValidator validator = ctx -> {
            latch.countDown();

            if (allowAll.get() || compatibleNode.equals(ctx.clusterNode().name())) {
                return;
            }

            raiseConnectException(ctx);
        };

        long reconnectInterval = 1_000;

        try (IgniteClient client = startClient(reconnectInterval, validator)) {
            boolean validatorChecksMatch = latch.await(5, TimeUnit.SECONDS);
            assertThat(validatorChecksMatch, is(true));

            Awaitility.await().timeout(3, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(client.connections().size(), is(1)));

            assertThat(client.connections().size(), is(1));

            allowAll.set(true);

            // Connections to all nodes should be established.
            Awaitility.await().timeout(10, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(client.connections().size(), is(nodes.size())));
        }
    }

    private IgniteClient startClient(long reconnectInterval, ChannelValidator channelValidator) {
        String[] addresses = startedNodes.stream()
                .map(ignite -> unwrapIgniteImpl(ignite).clientAddress().port())
                .map(port -> "127.0.0.1" + ":" + port)
                .toArray(String[]::new);

        var cfg = new IgniteClientConfigurationImpl(
                null,
                addresses,
                0,
                reconnectInterval,
                null,
                IgniteClientConfigurationImpl.DFLT_HEARTBEAT_INTERVAL,
                IgniteClientConfigurationImpl.DFLT_HEARTBEAT_TIMEOUT,
                new RetryReadPolicy(),
                null,
                null,
                false,
                null,
                IgniteClientConfiguration.DFLT_OPERATION_TIMEOUT,
                IgniteClientConfiguration.DFLT_SQL_PARTITION_AWARENESS_METADATA_CACHE_SIZE,
                null
        );

        return await(TcpIgniteClient.startAsync(
                cfg,
                HybridTimestampTracker.atomicTracker(null),
                channelValidator
        ));
    }

    private static void raiseConnectException(ProtocolContext ctx) {
        ClusterNode node = ctx.clusterNode();

        throw new IgniteClientConnectionException(
                CONNECTION_ERR,
                IgniteStringFormatter.format(ERROR_MESSAGE, node.name(), node.address(), ctx.productVersion()),
                null
        );
    }

    private void setupCluster(TestInfo testInfo, Path workDir) {
        Map<String, String> nodesBootstrapCfg = new LinkedHashMap<>();

        for (int i = 0; i < NODES_COUNT; i++) {
            String nodeName = testNodeName(testInfo, 3344 + i);

            nodesBootstrapCfg.put(
                    nodeName,
                    "ignite {\n"
                            + "  network.port: " + (3344 + i) + ",\n"
                            + "  network.nodeFinder.netClusterNodes: [ \"localhost:3344\" ]\n"
                            + (i == 1 ? ("  clientConnector.sendServerExceptionStackTraceToClient: true\n"
                            + "  clientConnector.metricsEnabled: true\n") : "")
                            + "  clientConnector.port: " + (10800 + i) + ",\n"
                            + "  rest.port: " + (10300 + i) + "\n"
                            + "  compute.threadPoolSize: 1\n"
                            + "}"
            );
        }

        nodes = nodesBootstrapCfg.entrySet().stream()
                .map(e -> TestIgnitionManager.start(e.getKey(), e.getValue(), workDir.resolve(e.getKey())))
                .collect(toList());

        IgniteServer metaStorageNode = nodes.get(0);

        InitParameters initParameters = InitParameters.builder()
                .metaStorageNodes(metaStorageNode)
                .clusterName("cluster")
                .build();
        TestIgnitionManager.init(metaStorageNode, initParameters);

        for (IgniteServer node : nodes) {
            assertThat(node.waitForInitAsync(), willCompleteSuccessfully());

            startedNodes.add(node.api());
        }
    }
}
