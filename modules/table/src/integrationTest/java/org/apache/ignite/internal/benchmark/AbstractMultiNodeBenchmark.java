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

package org.apache.ignite.internal.benchmark;

import static org.apache.ignite.internal.sql.engine.property.SqlPropertiesHelper.emptyProperties;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.capacity;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.InitParameters;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.util.AsyncCursor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.intellij.lang.annotations.Language;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

/**
 * Base benchmark class for {@link CriteriaThinClientBenchmark} and {@link CriteriaEmbeddedNodeBenchmark}.
 *
 * <p>Starts an Ignite cluster with a single table {@link #TABLE_NAME}, that has single PK column and 10 value columns.
 */
@State(Scope.Benchmark)
public class AbstractMultiNodeBenchmark {
    private static final int BASE_PORT = 3344;
    private static final int BASE_CLIENT_PORT = 10800;
    private static final int BASE_REST_PORT = 10300;

    protected static final String FIELD_VAL = "a".repeat(100);

    protected static final String TABLE_NAME = "USERTABLE";

    protected static final String ZONE_NAME = TABLE_NAME + "_ZONE";

    protected static final int DFLT_TABLE_SIZE = 10_000;

    /** Cluster nodes. */
    protected static final List<IgniteImpl> CLUSTER_NODES = new ArrayList<>();

    /**
     * Starts Ignite cluster and creates table {@link #TABLE_NAME}.
     */
    @Setup
    public final void clusterSetUp() throws Exception {
        IgniteImpl clusterNode = startCluster();

        try {
            var queryEngine = clusterNode.queryEngine();

            var createZoneStatement = IgniteStringFormatter.format("CREATE ZONE IF NOT EXISTS {}", ZONE_NAME);

            await(queryEngine.querySingleAsync(emptyProperties(), clusterNode.transactions(), null, createZoneStatement)
                    .thenCompose(AsyncCursor::closeAsync)
            );

            Table table = createTable(clusterNode, TABLE_NAME);
            populateTable(table);
        } catch (Throwable th) {
            clusterTearDown();

            throw th;
        }
    }

    /**
     * Create table that has single PK column and 10 value columns.
     */
    protected Table createTable(IgniteImpl clusterNode, String tableName) {
        var createTableStatement = "CREATE TABLE " + tableName + "(\n"
                + "    ycsb_key int PRIMARY KEY,\n"
                + "    field1   varchar(100),\n"
                + "    field2   varchar(100),\n"
                + "    field3   varchar(100),\n"
                + "    field4   varchar(100),\n"
                + "    field5   varchar(100),\n"
                + "    field6   varchar(100),\n"
                + "    field7   varchar(100),\n"
                + "    field8   varchar(100),\n"
                + "    field9   varchar(100),\n"
                + "    field10  varchar(100)\n"
                + ") WITH primary_zone='" + ZONE_NAME + "'";

        await(clusterNode.queryEngine().querySingleAsync(
                emptyProperties(), clusterNode.transactions(), null, createTableStatement
        ).thenCompose(AsyncCursor::closeAsync));

        return clusterNode.tables().table(tableName);
    }

    /**
     * Fills the table with data.
     */
    protected void populateTable(Table table) {
        var view = table.recordView();

        Tuple payload = Tuple.create();
        for (int j = 1; j <= 10; j++) {
            payload.set("field" + j, FIELD_VAL);
        }

        int batchSize = Math.min(tableSize(), 10_000);
        List<Tuple> batch = new ArrayList<>(capacity(batchSize));
        for (int i = 0; i < tableSize(); i++) {
            batch.add(Tuple.create(payload).set("ycsb_key", i));

            if (batch.size() == batchSize) {
                view.insertAll(null, batch);

                batch.clear();
            }
        }

        if (!batch.isEmpty()) {
            view.insertAll(null, batch);

            batch.clear();
        }
    }

    /**
     * Stops the cluster.
     *
     * @throws Exception In case of any error.
     */
    @TearDown
    public final void clusterTearDown() throws Exception {
        IgniteUtils.closeAll(CLUSTER_NODES);
    }

    private IgniteImpl startCluster() throws IOException {
        Path workDir = Files.createTempDirectory("tmpDirPrefix").toFile().toPath();

        String connectNodeAddr = "\"localhost:" + BASE_PORT + '\"';

        List<CompletableFuture<Ignite>> futures = new ArrayList<>();

        @Language("HOCON")
        String configTemplate = "{\n"
                + "  \"network\": {\n"
                + "    \"port\":{},\n"
                + "    \"nodeFinder\":{\n"
                + "      \"netClusterNodes\": [ {} ]\n"
                + "    }\n"
                + "  },\n"
                + "  clientConnector: { port:{} },\n"
                + "  rest.port: {},\n"
                + "  raft.fsync = false"
                + "}";

        for (int i = 0; i < initialNodes(); i++) {
            int port = BASE_PORT + i;
            String nodeName = nodeName(port);

            String config = IgniteStringFormatter.format(configTemplate, port, connectNodeAddr,
                    BASE_CLIENT_PORT + i, BASE_REST_PORT + i);

            futures.add(TestIgnitionManager.start(nodeName, config, workDir.resolve(nodeName)));
        }

        String metaStorageNodeName = nodeName(BASE_PORT);

        InitParameters initParameters = InitParameters.builder()
                .destinationNodeName(metaStorageNodeName)
                .metaStorageNodeNames(List.of(metaStorageNodeName))
                .clusterName("cluster")
                .build();

        TestIgnitionManager.init(initParameters);

        for (CompletableFuture<Ignite> future : futures) {
            assertThat(future, willCompleteSuccessfully());

            CLUSTER_NODES.add((IgniteImpl) await(future));
        }

        return CLUSTER_NODES.get(0);
    }

    private static String nodeName(int port) {
        return "node_" + port;
    }

    protected int initialNodes() {
        return 2;
    }

    protected int tableSize() {
        return DFLT_TABLE_SIZE;
    }
}
