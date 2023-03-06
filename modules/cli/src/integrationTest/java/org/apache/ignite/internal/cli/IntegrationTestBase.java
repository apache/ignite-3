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

package org.apache.ignite.internal.cli;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.InitParameters;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.sql.engine.AsyncCursor;
import org.apache.ignite.internal.sql.engine.AsyncCursor.BatchedResult;
import org.apache.ignite.internal.sql.engine.QueryContext;
import org.apache.ignite.internal.sql.engine.QueryProperty;
import org.apache.ignite.internal.sql.engine.property.PropertiesHolder;
import org.apache.ignite.internal.sql.engine.session.SessionId;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.table.Table;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Integration test base. Setups ignite cluster per test class and provides useful fixtures and assertions.
 */
@ExtendWith(WorkDirectoryExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@MicronautTest(rebuildContext = true)
public class IntegrationTestBase extends BaseIgniteAbstractTest {
    /** Correct ignite cluster url. */
    protected static final String NODE_URL = "http://localhost:10300";

    /** Cluster nodes. */
    protected static final List<Ignite> CLUSTER_NODES = new ArrayList<>();

    /** Cluster node names. */
    protected static final List<String> CLUSTER_NODE_NAMES = new ArrayList<>();

    /** Node name to its configuration map.*/
    protected static final Map<String, String> NODE_CONFIGS = new HashMap<>();

    /** Timeout should be big enough to prevent premature session expiration. */

    private static final long SESSION_IDLE_TIMEOUT = TimeUnit.SECONDS.toMillis(60);

    private static final int DEFAULT_NODES_COUNT = 3;

    private static final IgniteLogger LOG = Loggers.forClass(IntegrationTestBase.class);

    /** Base port number. */

    private static final int BASE_PORT = 3344;

    /** Nodes bootstrap configuration pattern. */
    private static final String NODE_BOOTSTRAP_CFG = "{\n"
            + "  network: {\n"
            + "    port:{},\n"
            + "    portRange: 5,\n"
            + "    nodeFinder:{\n"
            + "      netClusterNodes: [ {} ]\n"
            + "    }\n"
            + "  }\n"
            + "}";

    /** Template for node bootstrap config with Scalecube and Logical Topology settings for fast failure detection. */
    protected static final String FAST_FAILURE_DETECTION_NODE_BOOTSTRAP_CFG_TEMPLATE = "{\n"
            + "  network: {\n"
            + "    port: {},\n"
            + "    nodeFinder: {\n"
            + "      netClusterNodes: [ {} ]\n"
            + "    },\n"
            + "    membership: {\n"
            + "      membershipSyncInterval: 1000,\n"
            + "      failurePingInterval: 500,\n"
            + "      scaleCube: {\n"
            + "        membershipSuspicionMultiplier: 1,\n"
            + "        failurePingRequestMembers: 1,\n"
            + "        gossipInterval: 10\n"
            + "      },\n"
            + "    }\n"
            + "  },"
            + "  cluster.failoverTimeout: 100\n"
            + "}";

    /** Futures that are going to be completed when all nodes are started and the cluster is initialized. */
    private static List<CompletableFuture<Ignite>> futures = new ArrayList<>();
    /** Work directory. */

    @WorkDirectory
    private static Path WORK_DIR;

    protected static void createAndPopulateTable() {
        sql("CREATE TABLE person ( id INT PRIMARY KEY, name VARCHAR, salary DOUBLE)");

        int idx = 0;

        for (Object[] args : new Object[][]{
                {idx++, "Igor", 10d},
                {idx++, null, 15d},
                {idx++, "Ilya", 15d},
                {idx++, "Roma", 10d},
                {idx, "Roma", 10d}
        }) {
            sql("INSERT INTO person(id, name, salary) VALUES (?, ?, ?)", args);
        }
    }

    protected static List<List<Object>> sql(String sql, Object... args) {
        return sql(null, sql, args);
    }

    protected static List<List<Object>> sql(@Nullable Transaction tx, String sql, Object... args) {
        var queryEngine = ((IgniteImpl) CLUSTER_NODES.get(0)).queryEngine();

        SessionId sessionId = queryEngine.createSession(SESSION_IDLE_TIMEOUT, PropertiesHolder.fromMap(
                Map.of(QueryProperty.DEFAULT_SCHEMA, "PUBLIC")
        ));

        try {
            var context = tx != null ? QueryContext.of(tx) : QueryContext.of();

            return getAllFromCursor(
                    await(queryEngine.querySingleAsync(sessionId, context, sql, args))
            );
        } finally {
            queryEngine.closeSession(sessionId);
        }
    }

    private static <T> List<T> getAllFromCursor(AsyncCursor<T> cur) {
        List<T> res = new ArrayList<>();
        int batchSize = 256;

        var consumer = new Consumer<BatchedResult<T>>() {
            @Override
            public void accept(BatchedResult<T> br) {
                res.addAll(br.items());

                if (br.hasMore()) {
                    cur.requestNextAsync(batchSize).thenAccept(this);
                }
            }
        };

        await(cur.requestNextAsync(batchSize).thenAccept(consumer));
        await(cur.closeAsync());

        return res;
    }

    protected static PrintWriter output(List<Character> buffer) {
        return new PrintWriter(new Writer() {
            @Override
            public void write(char[] cbuf, int off, int len) {
                for (int i = off; i < off + len; i++) {
                    buffer.add(cbuf[i]);
                }
            }

            @Override
            public void flush() {

            }

            @Override
            public void close() {

            }
        });
    }

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

                    String config = IgniteStringFormatter.format(nodeBootstrapConfigTemplate(), BASE_PORT + i, connectNodeAddr);

                    NODE_CONFIGS.put(nodeName, config);

                    return IgnitionManager.start(nodeName, config, WORK_DIR.resolve(nodeName));
                })
                .collect(toList());
    }

    protected String nodeBootstrapConfigTemplate() {
        return NODE_BOOTSTRAP_CFG;
    }

    protected void initializeCluster(String metaStorageNodeName) {
        InitParameters initParameters = InitParameters.builder()
                .destinationNodeName(metaStorageNodeName)
                .metaStorageNodeNames(List.of(metaStorageNodeName))
                .clusterName("cluster")
                .build();

        IgnitionManager.init(initParameters);

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
    protected int nodes() {
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
        IgnitionManager.start(nodeName, NODE_CONFIGS.get(nodeName), WORK_DIR.resolve(nodeName));
        CLUSTER_NODE_NAMES.add(nodeName);
    }

    /** Drops all visible tables. */
    protected void dropAllTables() {
        for (Table t : CLUSTER_NODES.get(0).tables().tables()) {
            sql("DROP TABLE " + t.name());
        }
    }

    /**
     * Invokes before the test will start.
     *
     * @param testInfo Test information object.
     * @throws Exception If failed.
     */
    public void setUp(TestInfo testInfo) throws Exception {
        setupBase(testInfo, WORK_DIR);
    }

    /**
     * Invokes after the test has finished.
     *
     * @param testInfo Test information object.
     */
    @AfterEach
    public void tearDown(TestInfo testInfo) {
        tearDownBase(testInfo);
    }
}

