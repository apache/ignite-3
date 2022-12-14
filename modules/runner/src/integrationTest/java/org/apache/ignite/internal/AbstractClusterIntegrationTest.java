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

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.sql.engine.util.CursorUtils.getAllFromCursor;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedIn;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Abstract integration test that starts and stops a cluster.
 */
@SuppressWarnings("ALL")
@ExtendWith(WorkDirectoryExtension.class)
public abstract class AbstractClusterIntegrationTest extends BaseIgniteAbstractTest {
    private static final IgniteLogger LOG = Loggers.forClass(AbstractClusterIntegrationTest.class);

    /** Base port number. */
    private static final int BASE_PORT = 3344;

    /** Nodes bootstrap configuration pattern. */
    private static final String NODE_BOOTSTRAP_CFG = "{\n"
            + "  \"network\": {\n"
            + "    \"port\":{},\n"
            + "    \"nodeFinder\":{\n"
            + "      \"netClusterNodes\": [ {} ]\n"
            + "    }\n"
            + "  }\n"
            + "}";

    /** Cluster nodes. */
    private final List<Ignite> clusterNodes = new ArrayList<>();

    /** Work directory. */
    @WorkDirectory
    protected static Path WORK_DIR;

    /**
     * Starts nodes before each test.
     *
     * @param testInfo Test information object.
     */
    @BeforeEach
    void startNodes(TestInfo testInfo) {
        List<CompletableFuture<Ignite>> futures = IntStream.range(0, initialNodes())
                .mapToObj(i -> startNode0(i, testInfo))
                .collect(toList());

        String metaStorageNodeName = testNodeName(testInfo, initialNodes() - 1);

        IgnitionManager.init(metaStorageNodeName, List.of(metaStorageNodeName), "cluster");

        for (CompletableFuture<Ignite> future : futures) {
            assertThat(future, willSucceedIn(10, TimeUnit.SECONDS));

            clusterNodes.add(future.join());
        }
    }

    private static CompletableFuture<Ignite> startNode0(int nodeIndex, TestInfo testInfo) {
        String connectNodeAddr = "\"localhost:" + BASE_PORT + '\"';

        String nodeName = testNodeName(testInfo, nodeIndex);

        String config = IgniteStringFormatter.format(NODE_BOOTSTRAP_CFG, BASE_PORT + nodeIndex, connectNodeAddr);

        return IgnitionManager.start(nodeName, config, WORK_DIR.resolve(nodeName));
    }

    /**
     * Starts an Ignite node with the given index.
     *
     * @param nodeIndex Zero-based index (used to build node name).
     * @param testInfo Test info (used to build node name).
     * @return Started Ignite node.
     */
    protected IgniteImpl startNode(int nodeIndex, TestInfo testInfo) {
        CompletableFuture<Ignite> future = startNode0(nodeIndex, testInfo);

        assertThat(future, willSucceedIn(10, TimeUnit.SECONDS));

        Ignite ignite = future.join();

        if (nodeIndex < clusterNodes.size()) {
            clusterNodes.set(nodeIndex, ignite);
        } else if (nodeIndex == clusterNodes.size()) {
            clusterNodes.add(ignite);
        } else {
            throw new IllegalArgumentException("Cannot start node with index " + nodeIndex + " because we only have "
                    + clusterNodes.size() + " nodes");
        }

        return (IgniteImpl) ignite;
    }

    /**
     * Returns count of nodes in the Ignite cluster started before each test.
     *
     * @return Count of nodes in initial cluster.
     */
    protected int initialNodes() {
        return 3;
    }

    /**
     * Stops all nodes that are not stopped yet.
     */
    @AfterEach
    void stopNodes(TestInfo testInfo) throws Exception {
        LOG.info("Start tearDown()");

        List<AutoCloseable> closeables = clusterNodes.stream()
                .filter(Objects::nonNull)
                .map(node -> (AutoCloseable) () -> IgnitionManager.stop(node.name()))
                .collect(toList());

        IgniteUtils.closeAll(closeables);

        clusterNodes.clear();

        LOG.info("End tearDown()");
    }

    /**
     * Stops a node by index.
     *
     * @param nodeIndex Node index.
     * @param testInfo Test info (used to construct node name).
     */
    protected final void stopNode(int nodeIndex, TestInfo testInfo) {
        assertThat(clusterNodes.size(), is(greaterThan(nodeIndex)));
        assertThat(clusterNodes.get(nodeIndex), is(notNullValue()));

        IgnitionManager.stop(testNodeName(testInfo, nodeIndex));

        clusterNodes.set(nodeIndex, null);
    }

    /**
     * Restarts a node by index.
     *
     * @param nodeIndex Node index.
     * @param testInfo Test info (used to construct node name).
     * @return New node instance.
     */
    protected final Ignite restartNode(int nodeIndex, TestInfo testInfo) {
        stopNode(nodeIndex, testInfo);

        return startNode(nodeIndex, testInfo);
    }

    /**
     * Invokes before the test will start.
     *
     * @param testInfo Test information oject.
     * @throws Exception If failed.
     */
    @BeforeEach
    public void setup(TestInfo testInfo) throws Exception {
        setupBase(testInfo, WORK_DIR);
    }

    /**
     * Invokes after the test has finished.
     *
     * @param testInfo Test information oject.
     * @throws Exception If failed.
     */
    @AfterEach
    public void tearDown(TestInfo testInfo) throws Exception {
        tearDownBase(testInfo);
    }

    protected final IgniteImpl node(int index) {
        return (IgniteImpl) clusterNodes.get(index);
    }

    protected List<List<Object>> executeSql(String sql, Object... args) {
        return getAllFromCursor(
                node(0).queryEngine().queryAsync("PUBLIC", sql, args).get(0).join()
        );
    }
}
