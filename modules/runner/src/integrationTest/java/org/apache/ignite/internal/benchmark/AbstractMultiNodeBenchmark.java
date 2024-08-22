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

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.sql.engine.util.CursorUtils.getAllFromCursor;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteServer;
import org.apache.ignite.InitParameters;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.sql.engine.property.SqlPropertiesHelper;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
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
    protected static final int BASE_CLIENT_PORT = 10800;
    private static final int BASE_REST_PORT = 10300;

    protected static final String FIELD_VAL = "a".repeat(100);

    protected static final String FIELD_VAL_WITH_SPACES = FIELD_VAL + "   ";

    protected static final String TABLE_NAME = "USERTABLE";

    protected static final String ZONE_NAME = TABLE_NAME + "_ZONE";

    private final List<IgniteServer> igniteServers = new ArrayList<>();

    protected static Ignite publicIgnite;
    protected static IgniteImpl igniteImpl;

    @Param({"false", "true"})
    private boolean fsync;

    /**
     * Starts ignite node and creates table {@link #TABLE_NAME}.
     */
    @Setup
    public void nodeSetUp() throws Exception {
        System.setProperty("jraft.available_processors", "2");
        startCluster();

        try {
            var queryEngine = igniteImpl.queryEngine();

            var createZoneStatement = "CREATE ZONE IF NOT EXISTS " + ZONE_NAME + " WITH partitions=" + partitionCount()
                    + ", storage_profiles ='" + DEFAULT_STORAGE_PROFILE + "'";

            getAllFromCursor(
                    await(queryEngine.queryAsync(
                            SqlPropertiesHelper.emptyProperties(), igniteImpl.observableTimeTracker(), null, createZoneStatement
                    ))
            );

            createTable(TABLE_NAME);
        } catch (Throwable th) {
            nodeTearDown();

            throw th;
        }
    }

    protected void createTable(String tableName) {
        createTable(tableName,
                List.of(
                        "ycsb_key int",
                        "field1   varchar(100)",
                        "field2   varchar(100)",
                        "field3   varchar(100)",
                        "field4   varchar(100)",
                        "field5   varchar(100)",
                        "field6   varchar(100)",
                        "field7   varchar(100)",
                        "field8   varchar(100)",
                        "field9   varchar(100)",
                        "field10  varchar(100)"
                ),
                List.of("ycsb_key"),
                List.of()
        );
    }

    protected static void createTable(String tableName, List<String> columns, List<String> primaryKeys, List<String> colocationKeys) {
        var createTableStatement = "CREATE TABLE " + tableName + "(\n";

        createTableStatement += String.join(",\n", columns);
        createTableStatement += "\n, PRIMARY KEY (" + String.join(", ", primaryKeys) + ")\n)";

        if (!colocationKeys.isEmpty()) {
            createTableStatement += "\nCOLOCATE BY (" + String.join(", ", colocationKeys) + ")";
        }

        createTableStatement += "\nWITH primary_zone='" + ZONE_NAME + "'";

        getAllFromCursor(
                await(igniteImpl.queryEngine().queryAsync(
                        SqlPropertiesHelper.emptyProperties(), igniteImpl.observableTimeTracker(), null, createTableStatement
                ))
        );
    }

    static void populateTable(String tableName, int size, int batchSize) {
        RecordView<Tuple> view = publicIgnite.tables().table(tableName).recordView();

        Tuple payload = Tuple.create();
        for (int j = 1; j <= 10; j++) {
            payload.set("field" + j, FIELD_VAL);
        }

        batchSize = Math.min(size, batchSize);
        List<Tuple> batch = new ArrayList<>(batchSize);
        for (int i = 0; i < size; i++) {
            batch.add(Tuple.copy(payload).set("ycsb_key", i));

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
    public final void nodeTearDown() throws Exception {
        IgniteUtils.closeAll(igniteServers.stream().map(node -> node::shutdown));
    }

    private void startCluster() throws Exception {
        Path workDir = workDir();

        String connectNodeAddr = "\"localhost:" + BASE_PORT + '\"';

        @Language("HOCON")
        String configTemplate = "ignite {\n"
                + "  \"network\": {\n"
                + "    \"port\":{},\n"
                + "    \"nodeFinder\":{\n"
                + "      \"netClusterNodes\": [ {} ]\n"
                + "    }\n"
                + "  },\n"
                + "  storage.profiles: {"
                + "        " + DEFAULT_STORAGE_PROFILE + ".engine: aipersist, "
                + "        " + DEFAULT_STORAGE_PROFILE + ".size: 2073741824 " // Avoid page replacement.
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

            igniteServers.add(TestIgnitionManager.start(nodeName, config, workDir.resolve(nodeName)));
        }

        String metaStorageNodeName = nodeName(BASE_PORT);

        InitParameters initParameters = InitParameters.builder()
                .metaStorageNodeNames(metaStorageNodeName)
                .clusterName("cluster")
                .build();

        TestIgnitionManager.init(igniteServers.get(0), initParameters);

        for (IgniteServer node : igniteServers) {
            assertThat(node.waitForInitAsync(), willCompleteSuccessfully());

            if (publicIgnite == null) {
                publicIgnite = node.api();
                igniteImpl = unwrapIgniteImpl(publicIgnite);
            }
        }
    }

    private static String nodeName(int port) {
        return "node_" + port;
    }

    protected Path workDir() throws Exception {
        return Files.createTempDirectory("tmpDirPrefix").toFile().toPath();
    }

    protected int nodes() {
        return 3;
    }

    protected int partitionCount() {
        return CatalogUtils.DEFAULT_PARTITION_COUNT;
    }
}
