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

package org.apache.ignite.internal.testframework;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.InitParameters;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Integration test base. Setups ignite cluster per test class and provides useful fixtures and assertions.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@MicronautTest(rebuildContext = true)
@ExtendWith(WorkDirectoryExtension.class)
public class IntegrationTestBase extends BaseIgniteAbstractTest {
    /** Base port network number. */
    private static final int BASE_PORT = 3344;

    /** Base client port. */
    private static final int BASE_CLIENT_PORT = 10800;

    /** Base HTTP port. */
    private static final int BASE_HTTP_PORT = 10300;

    /** Base HTTPS port. */
    private static final int BASE_HTTPS_PORT = 10400;

    /** Correct ignite cluster url. */
    protected static final String NODE_URL = "http://localhost:" + BASE_HTTP_PORT;

    /** Cluster nodes. */
    protected static final List<Ignite> CLUSTER_NODES = new ArrayList<>();

    /** Cluster node names. */
    protected static final List<String> CLUSTER_NODE_NAMES = new ArrayList<>();

    /** Node name to its configuration map.*/
    protected static final Map<String, String> NODE_CONFIGS = new HashMap<>();

    private static final int DEFAULT_NODES_COUNT = 3;

    private static final IgniteLogger LOG = Loggers.forClass(IntegrationTestBase.class);

    /** Nodes bootstrap configuration pattern. */
    private static final String NODE_BOOTSTRAP_CFG = "{\n"
            + "  network: {\n"
            + "    port:{},\n"
            + "    portRange: 5,\n"
            + "    nodeFinder:{\n"
            + "      netClusterNodes: [ {} ]\n"
            + "    }\n"
            + "  },\n"
            + "  clientConnector: { port: {} }\n"
            + "  rest: {\n"
            + "    port:{},\n"
            + "    ssl: { port:{} }\n"
            + "  }\n"
            + "}";

    /** Futures that are going to be completed when all nodes are started and the cluster is initialized. */
    private static List<CompletableFuture<Ignite>> futures = new ArrayList<>();
    /** Work directory. */

    @WorkDirectory
    protected static Path WORK_DIR;

    /**
     * Before all.
     *
     * @param testInfo Test information object.
     */
    protected void startNodes(TestInfo testInfo) {
        String connectNodeAddr = "\"localhost:" + BASE_PORT + '\"';

        futures = IntStream.range(0, nodes())
                .mapToObj(i -> {
                    String nodeName = testNodeName(testInfo, i);
                    CLUSTER_NODE_NAMES.add(nodeName);

                    String config = IgniteStringFormatter.format(
                            nodeBootstrapConfigTemplate(),
                            BASE_PORT + i,
                            connectNodeAddr,
                            BASE_CLIENT_PORT + i,
                            BASE_HTTP_PORT + i,
                            BASE_HTTPS_PORT + i);

                    NODE_CONFIGS.put(nodeName, config);

                    return TestIgnitionManager.start(nodeName, config, WORK_DIR.resolve(nodeName));
                })
                .collect(toList());
    }

    protected String nodeBootstrapConfigTemplate() {
        return NODE_BOOTSTRAP_CFG;
    }

    protected void initializeCluster(String metaStorageNodeName) {
        InitParametersBuilder builder = InitParameters.builder()
                .destinationNodeName(metaStorageNodeName)
                .metaStorageNodeNames(List.of(metaStorageNodeName))
                .clusterName("cluster");

        configureInitParameters(builder);

        TestIgnitionManager.init(builder.build());

        awaitClusterInitialized();
    }

    protected void configureInitParameters(InitParametersBuilder builder) {
    }

    protected void awaitClusterInitialized() {
        for (CompletableFuture<Ignite> future : futures) {
            assertThat(future, willCompleteSuccessfully());

            CLUSTER_NODES.add(future.join());
        }
    }

    /**
     * Get a count of nodes in the Ignite cluster.
     *
     * @return Count of nodes.
     */
    protected static int nodes() {
        return DEFAULT_NODES_COUNT;
    }

    /**
     * After all.
     */
    protected void stopNodes(TestInfo testInfo) throws Exception {
        LOG.info("Start tearDown()");

        CLUSTER_NODES.clear();
        CLUSTER_NODE_NAMES.clear();

        List<AutoCloseable> closeables = IntStream.range(0, nodes())
                .mapToObj(i -> testNodeName(testInfo, i))
                .map(nodeName -> (AutoCloseable) () -> IgnitionManager.stop(nodeName))
                .collect(toList());

        IgniteUtils.closeAll(closeables);

        LOG.info("End tearDown()");
    }

    protected void stopNode(String nodeName) {
        IgnitionManager.stop(nodeName);
        CLUSTER_NODE_NAMES.remove(nodeName);
    }

    protected void startNode(String nodeName) {
        TestIgnitionManager.start(nodeName, NODE_CONFIGS.get(nodeName), WORK_DIR.resolve(nodeName));
        CLUSTER_NODE_NAMES.add(nodeName);
    }
}
