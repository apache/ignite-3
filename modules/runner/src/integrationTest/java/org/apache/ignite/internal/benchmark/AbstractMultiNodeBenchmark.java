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
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteServer;
import org.apache.ignite.InitParameters;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.failure.handlers.configuration.StopNodeOrHaltFailureHandlerConfigurationSchema;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.Nullable;
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

    @Param({"false"})
    protected boolean remote;

    @Param({"false"})
    private boolean fsync;

    @Nullable
    protected String clusterConfiguration() {
        return "ignite {}";
    }

    /**
     * Starts ignite node and creates table {@link #TABLE_NAME}.
     */
    @Setup
    public void nodeSetUp() throws Exception {
        System.setProperty("jraft.available_processors", "2");
        if (!remote) {
            startCluster();
        }

        try {
            // Create a new zone on the cluster's start-up.
            createDistributionZoneOnStartup();

            // Create tables on the cluster's start-up.
            createTablesOnStartup();
        } catch (Throwable th) {
            nodeTearDown();

            throw th;
        }
    }

    protected void createDistributionZoneOnStartup() {
        var createZoneStatement = "CREATE ZONE IF NOT EXISTS " + ZONE_NAME + " (partitions " + partitionCount()
                + ", replicas " + replicaCount() + ") storage profiles ['" + DEFAULT_STORAGE_PROFILE + "']";

        try (ResultSet<SqlRow> rs = publicIgnite.sql().execute(createZoneStatement)) {
            // No-op.
        }
    }

    protected void createTablesOnStartup() {
        createTable(TABLE_NAME);
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
        var createTableStatement = "CREATE TABLE IF NOT EXISTS " + tableName + "(\n";

        createTableStatement += String.join(",\n", columns);
        createTableStatement += "\n, PRIMARY KEY (" + String.join(", ", primaryKeys) + ")\n)";

        if (!colocationKeys.isEmpty()) {
            createTableStatement += "\nCOLOCATE BY (" + String.join(", ", colocationKeys) + ")";
        }

        createTableStatement += "\nZONE " + ZONE_NAME;

        try (ResultSet<SqlRow> rs = publicIgnite.sql().execute(createTableStatement)) {
            // No-op.
        }
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
    public void nodeTearDown() throws Exception {
        IgniteUtils.closeAll(igniteServers.stream().map(node -> node::shutdown));
    }

    public IgniteImpl node(int idx) {
        return unwrapIgniteImpl(igniteServers.get(idx).api());
    }

    private void startCluster() throws Exception {
        if (remote) {
            throw new AssertionError("Can't start the cluster in remote mode");
        }

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
                + "        " + DEFAULT_STORAGE_PROFILE + ".sizeBytes: 2073741824 " // Avoid page replacement.
                + "  },\n"
                + "  clientConnector: { port:{} },\n"
                + "  clientConnector.sendServerExceptionStackTraceToClient: true\n"
                + "  rest.port: {},\n"
                + "  raft.fsync = " + fsync() + ",\n"
                + "  system.partitionsLogPath = \"" + logPath() + "\",\n"
                + "  failureHandler.handler: {\n"
                + "      type: \"" + StopNodeOrHaltFailureHandlerConfigurationSchema.TYPE + "\",\n"
                + "      tryStop: true,\n"
                + "      timeoutMillis: 60000,\n" // 1 minute for graceful shutdown
                + "  },\n"
                + "}";

        for (int i = 0; i < nodes(); i++) {
            int port = BASE_PORT + i;
            String nodeName = nodeName(port);

            String config = IgniteStringFormatter.format(configTemplate, port, connectNodeAddr,
                    BASE_CLIENT_PORT + i, BASE_REST_PORT + i);

            igniteServers.add(TestIgnitionManager.startWithProductionDefaults(nodeName, config, workDir.resolve(nodeName)));
        }

        String metaStorageNodeName = nodeName(BASE_PORT);

        InitParameters initParameters = InitParameters.builder()
                .metaStorageNodeNames(metaStorageNodeName)
                .clusterName("cluster")
                .clusterConfiguration(clusterConfiguration())
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

    /**
     * Gets client connector addresses for the specified nodes.
     *
     * @return Array of client addresses.
     */
    static String[] getServerEndpoints(int clusterNodes) {
        return IntStream.range(0, clusterNodes)
                .mapToObj(i -> "127.0.0.1:" + (BASE_CLIENT_PORT + i))
                .toArray(String[]::new);
    }

    private static String nodeName(int port) {
        return "node_" + port;
    }

    protected Path workDir() throws Exception {
        return Files.createTempDirectory("tmpDirPrefix").toFile().toPath();
    }

    protected String logPath() {
        return "";
    }

    protected boolean fsync() {
        return fsync;
    }

    protected int nodes() {
        return 3;
    }

    protected int partitionCount() {
        return CatalogUtils.DEFAULT_PARTITION_COUNT;
    }

    protected int replicaCount() {
        return CatalogUtils.DEFAULT_REPLICA_COUNT;
    }
}
