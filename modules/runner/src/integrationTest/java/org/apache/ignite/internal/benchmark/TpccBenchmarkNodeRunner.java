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
import static org.apache.ignite.internal.testframework.IgniteTestUtils.getAllResultSet;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.File;
import java.nio.file.Path;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteServer;
import org.apache.ignite.InitParameters;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.failure.handlers.configuration.StopNodeOrHaltFailureHandlerConfigurationSchema;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.sql.Statement.StatementBuilder;
import org.apache.ignite.tx.Transaction;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.Nullable;

/**
 * Extendable class to start a dedicated cluster node for TPC-C benchmark.
 */
public class TpccBenchmarkNodeRunner {
    private static final int BASE_PORT = 3344;
    private static final int BASE_CLIENT_PORT = 10942;
    private static final int BASE_REST_PORT = 10300;

    private static final List<IgniteServer> igniteServers = new ArrayList<>();

    protected static Ignite publicIgnite;
    protected static IgniteImpl igniteImpl;

    public static void main(String[] args) throws Exception {
        TpccBenchmarkNodeRunner runner = new TpccBenchmarkNodeRunner();
        runner.startCluster();
    }

    public IgniteImpl node(int idx) {
        return unwrapIgniteImpl(igniteServers.get(idx).api());
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
                + "        " + DEFAULT_STORAGE_PROFILE + ".sizeBytes: " + pageMemorySize() + " "
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

    @Nullable
    protected String clusterConfiguration() {
        return "ignite {}";
    }

    protected static String nodeName(int port) {
        return "node_" + port;
    }

    protected Path workDir() throws Exception {
        return new File("c:/work/tpcc").toPath();
    }

    protected int pageMemorySize() {
        return 2073741824;
    }

    protected String logPath() {
        return "";
    }

    protected boolean fsync() {
        return false;
    }

    protected int nodes() {
        return 1;
    }

    protected void dumpWarehouse() {
        final String query = "select * from warehouse";
        System.out.println("Executing the query: ");
        List<List<Object>> rows = sql(publicIgnite, null, null, null, query);
        for (List<Object> row : rows) {
            System.out.println("Row: " + row);
        }
    }

    protected static List<List<Object>> sql(Ignite node, @Nullable Transaction tx, @Nullable String schema, @Nullable ZoneId zoneId,
            String query, Object... args) {
        IgniteSql sql = node.sql();
        StatementBuilder builder = sql.statementBuilder()
                .query(query);

        if (zoneId != null) {
            builder.timeZoneId(zoneId);
        }

        if (schema != null) {
            builder.defaultSchema(schema);
        }

        Statement statement = builder.build();
        try (ResultSet<SqlRow> rs = sql.execute(tx, statement, args)) {
            return getAllResultSet(rs);
        }
    }
}
