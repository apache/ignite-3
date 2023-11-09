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

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.sql.engine.property.PropertiesHelper.newBuilder;
import static org.apache.ignite.internal.sql.engine.util.CursorUtils.getAllFromCursor;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.InitParameters;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.sql.engine.QueryContext;
import org.apache.ignite.internal.sql.engine.QueryProperty;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.session.SessionId;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.util.IgniteUtils;
import org.intellij.lang.annotations.Language;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

/**
 * Base benchmark class for {@link SelectBenchmark} and {@link InsertBenchmark}.
 *
 * <p>Starts an Ignite cluster with a single table {@link #TABLE_NAME}, that has
 * single PK column and 10 value columns.
 */
@State(Scope.Benchmark)
public class AbstractMultiNodeBenchmark {
    private static final int BASE_PORT = 3344;
    private static final int BASE_CLIENT_PORT = 10800;
    private static final int BASE_REST_PORT = 10300;

    protected static final String FIELD_VAL = "a".repeat(100);

    protected static final String TABLE_NAME = "usertable";

    protected static IgniteImpl clusterNode;

    @Param({"true", "false"})
    private boolean fsync;

    /**
     * Starts ignite node and creates table {@link #TABLE_NAME}.
     */
    @Setup
    public final void nodeSetUp() throws IOException {
        startCluster();

        var queryEngine = clusterNode.queryEngine();

        SessionId sessionId = queryEngine.createSession(newBuilder()
                .set(QueryProperty.DEFAULT_SCHEMA, "PUBLIC")
                .set(QueryProperty.QUERY_TIMEOUT, TimeUnit.SECONDS.toMillis(60))
                .build()
        );

        var sql = "CREATE TABLE " + TABLE_NAME + "(\n"
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
                + ");";

        try {
            var context = QueryContext.create(SqlQueryType.ALL);

            getAllFromCursor(
                    await(queryEngine.querySingleAsync(sessionId, context, clusterNode.transactions(), sql))
            );
        } finally {
            queryEngine.closeSession(sessionId);
        }
    }

    /**
     * Stops the cluster.
     *
     * @throws Exception In case of any error.
     */
    @TearDown
    public final void nodeTearDown() throws Exception {
        List<AutoCloseable> closeables = IntStream.range(0, nodes())
                .mapToObj(i -> nodeName(BASE_PORT + i))
                .map(nodeName -> (AutoCloseable) () -> IgnitionManager.stop(nodeName))
                .collect(toList());

        IgniteUtils.closeAll(closeables);
    }

    private void startCluster() throws IOException {
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
                + "  raft.fsync = " + fsync
                + "}";

        for (int i = 0; i < nodes(); i++) {
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

            if (clusterNode == null) {
                clusterNode = (IgniteImpl) await(future);
            } else {
                await(future);
            }
        }
    }

    private static String nodeName(int port) {
        return "node_" + port;
    }

    protected int nodes() {
        return 3;
    }
}
