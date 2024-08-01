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

package org.apache.ignite.internal.runner.app;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.MAX_TIME_PRECISION;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.createZone;
import static org.apache.ignite.internal.table.TableTestUtils.createTable;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.escapeWindowsPath;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.getResourcePath;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.sql.ColumnType.BOOLEAN;
import static org.apache.ignite.sql.ColumnType.BYTE_ARRAY;
import static org.apache.ignite.sql.ColumnType.DATE;
import static org.apache.ignite.sql.ColumnType.DATETIME;
import static org.apache.ignite.sql.ColumnType.DECIMAL;
import static org.apache.ignite.sql.ColumnType.DOUBLE;
import static org.apache.ignite.sql.ColumnType.FLOAT;
import static org.apache.ignite.sql.ColumnType.INT16;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.apache.ignite.sql.ColumnType.INT64;
import static org.apache.ignite.sql.ColumnType.INT8;
import static org.apache.ignite.sql.ColumnType.STRING;
import static org.apache.ignite.sql.ColumnType.TIME;
import static org.apache.ignite.sql.ColumnType.TIMESTAMP;
import static org.apache.ignite.sql.ColumnType.UUID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.netty.util.ResourceLeakDetector;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteServer;
import org.apache.ignite.InitParameters;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.compute.task.MapReduceJob;
import org.apache.ignite.compute.task.MapReduceTask;
import org.apache.ignite.compute.task.TaskExecutionContext;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.client.proto.ColumnTypeConverter;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.TupleMarshaller;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerImpl;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.security.authentication.basic.BasicAuthenticationProviderChange;
import org.apache.ignite.internal.security.configuration.SecurityChange;
import org.apache.ignite.internal.security.configuration.SecurityConfiguration;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.table.RecordBinaryViewImpl;
import org.apache.ignite.internal.table.partition.HashPartition;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.wrapper.Wrappers;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.IgniteCheckedException;
import org.apache.ignite.table.DataStreamerReceiver;
import org.apache.ignite.table.DataStreamerReceiverContext;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.partition.Partition;
import org.jetbrains.annotations.Nullable;

/**
 * Helper class for non-Java platform tests (.NET, C++, Python, ...). Starts nodes, populates tables and data for tests.
 */
@SuppressWarnings("CallToSystemGetenv")
public class PlatformTestNodeRunner {
    /** Test node name. */
    private static final String NODE_NAME = PlatformTestNodeRunner.class.getCanonicalName();

    /** Test node name 2. */
    private static final String NODE_NAME2 = PlatformTestNodeRunner.class.getCanonicalName() + "_2";

    /** Test node name 3. */
    private static final String NODE_NAME3 = PlatformTestNodeRunner.class.getCanonicalName() + "_3";

    /** Test node name 4. */
    private static final String NODE_NAME4 = PlatformTestNodeRunner.class.getCanonicalName() + "_4";

    private static final String TABLE_NAME = "TBL1";

    private static final String TABLE_NAME_ALL_COLUMNS = "TBL_ALL_COLUMNS";

    private static final String TABLE_NAME_ALL_COLUMNS_SQL = "TBL_ALL_COLUMNS_SQL"; // All column types supported by SQL.

    private static final String TABLE_NAME_ALL_COLUMNS_NOT_NULL = "TBL_ALL_COLUMNS_NOT_NULL";

    private static final String ZONE_NAME = "zone1";

    /** Time to keep the node alive. */
    private static final int RUN_TIME_MINUTES = 30;

    /** Time to keep the node alive - env var. */
    private static final String RUN_TIME_MINUTES_ENV = "IGNITE_PLATFORM_TEST_NODE_RUNNER_RUN_TIME_MINUTES";

    /** Nodes bootstrap configuration. */
    private static final Map<String, String> nodesBootstrapCfg = Map.of(
            NODE_NAME, "{\n"
                    + "  \"clientConnector\":{\"port\": 10942,\"idleTimeout\":6000,\""
                    + "sendServerExceptionStackTraceToClient\":true},"
                    + "  \"network\": {\n"
                    + "    \"port\":3344,\n"
                    + "    \"nodeFinder\": {\n"
                    + "      \"netClusterNodes\":[ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\", \"localhost:3347\" ]\n"
                    + "    }\n"
                    + "  },\n"
                    + "  rest.port: 10300\n"
                    + "}",

            NODE_NAME2, "{\n"
                    + "  \"clientConnector\":{\"port\": 10943,\"idleTimeout\":6000,"
                    + "\"sendServerExceptionStackTraceToClient\":true},"
                    + "  \"network\": {\n"
                    + "    \"port\":3345,\n"
                    + "    \"nodeFinder\": {\n"
                    + "      \"netClusterNodes\":[ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\", \"localhost:3347\" ]\n"
                    + "    }\n"
                    + "  },\n"
                    + "  rest.port: 10301\n"
                    + "}",

            NODE_NAME3, "{\n"
                    + "  \"clientConnector\":{"
                    + "    \"port\": 10944,"
                    + "    \"idleTimeout\":6000,"
                    + "    \"sendServerExceptionStackTraceToClient\":true, "
                    + "    \"ssl\": {\n"
                    + "      enabled: true,\n"
                    + "      keyStore: {\n"
                    + "        path: \"KEYSTORE_PATH\",\n"
                    + "        password: \"SSL_STORE_PASS\"\n"
                    + "      }\n"
                    + "    }\n"
                    + "  },\n"
                    + "  \"network\": {\n"
                    + "    \"port\":3346,\n"
                    + "    \"nodeFinder\": {\n"
                    + "      \"netClusterNodes\":[ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\", \"localhost:3347\" ]\n"
                    + "    }\n"
                    + "  },\n"
                    + "  rest.port: 10303\n"
                    + "}",

            NODE_NAME4, "{\n"
                    + "  \"clientConnector\":{"
                    + "    \"port\": 10945,"
                    + "    \"idleTimeout\":6000,"
                    + "    \"sendServerExceptionStackTraceToClient\":true, "
                    + "    \"ssl\": {\n"
                    + "      enabled: true,\n"
                    + "      clientAuth: \"require\",\n"
                    + "      keyStore: {\n"
                    + "        path: \"KEYSTORE_PATH\",\n"
                    + "        password: \"SSL_STORE_PASS\"\n"
                    + "      },\n"
                    + "      trustStore: {\n"
                    + "        path: \"TRUSTSTORE_PATH\",\n"
                    + "        password: \"SSL_STORE_PASS\"\n"
                    + "      }\n"
                    + "    }\n"
                    + "  },\n"
                    + "  \"network\": {\n"
                    + "    \"port\":3347,\n"
                    + "    \"nodeFinder\": {\n"
                    + "      \"netClusterNodes\":[ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\", \"localhost:3347\" ]\n"
                    + "    }\n"
                    + "  },\n"
                    + "  rest.port: 10304\n"
                    + "}"
    );

    /** Base path for all temporary folders. */
    private static final Path BASE_PATH = Path.of("target", "work", "PlatformTestNodeRunner");

    /**
     * Entry point.
     *
     * @param args Args.
     */
    public static void main(String[] args) throws Exception {
        System.out.println("Starting test node runner...");
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);

        for (int i = 0; i < args.length; i++) {
            System.out.println("Arg " + i + ": " + args[i]);
        }

        if (args.length > 0 && "dry-run".equals(args[0])) {
            System.out.println("Dry run succeeded.");
            return;
        }

        List<IgniteServer> startedIgniteServers = startNodes(BASE_PATH, nodesBootstrapCfg);

        List<Ignite> startedNodes = startedIgniteServers.stream()
                .map(IgniteServer::api)
                .collect(toList());

        createTables(startedNodes.get(0));

        String ports = startedNodes.stream()
                .map(n -> String.valueOf(getPort((IgniteImpl) n)))
                .collect(Collectors.joining(","));

        System.out.println("THIN_CLIENT_PORTS=" + ports);

        long runTimeMinutes = getRunTimeMinutes();
        System.out.println("Nodes will be active for " + runTimeMinutes + " minutes.");

        Thread.sleep(runTimeMinutes * 60_000);
        System.out.println("Exiting after " + runTimeMinutes + " minutes.");

        for (IgniteServer node : startedIgniteServers) {
            node.shutdown();
        }
    }

    /**
     * Start nodes.
     *
     * @param basePath Base path.
     * @param nodeCfg Node configuration.
     * @return Started nodes.
     */
    static List<IgniteServer> startNodes(Path basePath, Map<String, String> nodeCfg) throws IOException {
        IgniteUtils.deleteIfExists(basePath);
        Files.createDirectories(basePath);

        var sslPassword = "123456";
        var trustStorePath = escapeWindowsPath(getResourcePath(PlatformTestNodeRunner.class, "ssl/trust.jks"));
        var keyStorePath = escapeWindowsPath(getResourcePath(PlatformTestNodeRunner.class, "ssl/server.jks"));

        List<IgniteServer> nodes = nodeCfg.entrySet().stream()
                .map(e -> {
                    String nodeName = e.getKey();
                    String config = e.getValue()
                            .replace("KEYSTORE_PATH", keyStorePath)
                            .replace("TRUSTSTORE_PATH", trustStorePath)
                            .replace("SSL_STORE_PASS", sslPassword);

                    return TestIgnitionManager.start(nodeName, config, basePath.resolve(nodeName));
                })
                .collect(toList());

        IgniteServer metaStorageNode = nodes.get(0);

        InitParameters initParameters = InitParameters.builder()
                .metaStorageNodes(metaStorageNode)
                .clusterName("cluster")
                .build();
        TestIgnitionManager.init(metaStorageNode, initParameters);

        System.out.println("Initialization complete");

        allOf(nodes.stream().map(IgniteServer::waitForInitAsync).toArray(CompletableFuture[]::new)).join();

        System.out.println("Ignite nodes started");

        return nodes;
    }

    private static void createTables(Ignite node) {
        var keyCol = "KEY";

        IgniteImpl ignite = ((IgniteImpl) node);

        createZone(ignite.catalogManager(), ZONE_NAME, 10, 1);

        createTable(
                ignite.catalogManager(),
                SqlCommon.DEFAULT_SCHEMA_NAME,
                ZONE_NAME,
                TABLE_NAME,
                List.of(
                        ColumnParams.builder().name(keyCol).type(INT64).build(),
                        ColumnParams.builder().name("VAL").type(STRING).length(1000).nullable(true).build()
                ),
                List.of(keyCol)
        );

        int maxTimePrecision = MAX_TIME_PRECISION;

        createTable(
                ignite.catalogManager(),
                SqlCommon.DEFAULT_SCHEMA_NAME,
                ZONE_NAME,
                TABLE_NAME_ALL_COLUMNS,
                List.of(
                        ColumnParams.builder().name("STR").type(STRING).nullable(true).length(1000).build(),
                        ColumnParams.builder().name(keyCol).type(INT64).build(),
                        ColumnParams.builder().name("INT8").type(INT8).nullable(true).build(),
                        ColumnParams.builder().name("INT16").type(INT16).nullable(true).build(),
                        ColumnParams.builder().name("INT32").type(INT32).nullable(true).build(),
                        ColumnParams.builder().name("INT64").type(INT64).nullable(true).build(),
                        ColumnParams.builder().name("FLOAT").type(FLOAT).nullable(true).build(),
                        ColumnParams.builder().name("DOUBLE").type(DOUBLE).nullable(true).build(),
                        ColumnParams.builder().name("UUID").type(UUID).nullable(true).build(),
                        ColumnParams.builder().name("DATE").type(DATE).nullable(true).build(),
                        ColumnParams.builder().name("TIME").type(TIME).precision(maxTimePrecision).nullable(true).build(),
                        ColumnParams.builder().name("TIME2").type(TIME).precision(2).nullable(true).build(),
                        ColumnParams.builder().name("DATETIME").type(DATETIME).precision(maxTimePrecision).nullable(true).build(),
                        ColumnParams.builder().name("DATETIME2").type(DATETIME).precision(3).nullable(true).build(),
                        ColumnParams.builder().name("TIMESTAMP").type(TIMESTAMP).precision(maxTimePrecision).nullable(true).build(),
                        ColumnParams.builder().name("TIMESTAMP2").type(TIMESTAMP).precision(4).nullable(true).build(),
                        ColumnParams.builder().name("BLOB").type(BYTE_ARRAY).length(1000).nullable(true).build(),
                        ColumnParams.builder().name("DECIMAL").type(DECIMAL).precision(19).scale(3).nullable(true).build(),
                        ColumnParams.builder().name("BOOLEAN").type(BOOLEAN).nullable(true).build()
                ),
                List.of(keyCol)
        );

        createTable(
                ignite.catalogManager(),
                SqlCommon.DEFAULT_SCHEMA_NAME,
                ZONE_NAME,
                TABLE_NAME_ALL_COLUMNS_NOT_NULL,
                List.of(
                        ColumnParams.builder().name("STR").type(STRING).nullable(false)
                                .defaultValue(DefaultValue.constant("")).length(1000).build(),
                        ColumnParams.builder().name("INT8").type(INT8).nullable(false)
                                .defaultValue(DefaultValue.constant((byte) 0)).build(),
                        ColumnParams.builder().name("INT16").type(INT16).nullable(false)
                                .defaultValue(DefaultValue.constant((short) 0)).build(),
                        ColumnParams.builder().name(keyCol).type(INT64).build(),
                        ColumnParams.builder().name("INT32").type(INT32).nullable(false)
                                .defaultValue(DefaultValue.constant(0)).build(),
                        ColumnParams.builder().name("INT64").type(INT64).nullable(false)
                                .defaultValue(DefaultValue.constant((long) 0)).build(),
                        ColumnParams.builder().name("FLOAT").type(FLOAT).nullable(false)
                                .defaultValue(DefaultValue.constant((float) 0)).build(),
                        ColumnParams.builder().name("DOUBLE").type(DOUBLE).nullable(false)
                                .defaultValue(DefaultValue.constant((double) 0)).build(),
                        ColumnParams.builder().name("UUID").type(UUID).nullable(false)
                                .defaultValue(DefaultValue.constant(new java.util.UUID(0, 0))).build(),
                        ColumnParams.builder().name("DECIMAL").type(DECIMAL).precision(19).scale(3).nullable(false)
                                .defaultValue(DefaultValue.constant(BigDecimal.ZERO)).build()
                ),
                List.of(keyCol)
        );

        createTable(
                ignite.catalogManager(),
                SqlCommon.DEFAULT_SCHEMA_NAME,
                ZONE_NAME,
                TABLE_NAME_ALL_COLUMNS_SQL,
                List.of(
                        ColumnParams.builder().name("STR").type(STRING).length(1000).nullable(true).build(),
                        ColumnParams.builder().name("INT8").type(INT8).nullable(true).build(),
                        ColumnParams.builder().name(keyCol).type(INT64).build(),
                        ColumnParams.builder().name("INT16").type(INT16).nullable(true).build(),
                        ColumnParams.builder().name("INT32").type(INT32).nullable(true).build(),
                        ColumnParams.builder().name("INT64").type(INT64).nullable(true).build(),
                        ColumnParams.builder().name("FLOAT").type(FLOAT).nullable(true).build(),
                        ColumnParams.builder().name("DOUBLE").type(DOUBLE).nullable(true).build(),
                        ColumnParams.builder().name("UUID").type(UUID).nullable(true).build(),
                        ColumnParams.builder().name("DATE").type(DATE).nullable(true).build(),
                        ColumnParams.builder().name("TIME").type(TIME).precision(maxTimePrecision).nullable(true).build(),
                        ColumnParams.builder().name("TIME2").type(TIME).precision(maxTimePrecision).nullable(true).build(),
                        ColumnParams.builder().name("DATETIME").type(DATETIME).precision(maxTimePrecision).nullable(true).build(),
                        ColumnParams.builder().name("DATETIME2").type(DATETIME).precision(maxTimePrecision).nullable(true).build(),
                        ColumnParams.builder().name("TIMESTAMP").type(TIMESTAMP).precision(maxTimePrecision).nullable(true).build(),
                        ColumnParams.builder().name("TIMESTAMP2").type(TIMESTAMP).precision(maxTimePrecision).nullable(true).build(),
                        ColumnParams.builder().name("BLOB").type(BYTE_ARRAY).length(1000).nullable(true).build(),
                        ColumnParams.builder().name("DECIMAL").type(DECIMAL).precision(19).scale(3).nullable(true).build(),
                        ColumnParams.builder().name("BOOLEAN").type(BOOLEAN).nullable(true).build()
                ),
                List.of(keyCol)
        );

        createTwoColumnTable(
                ignite,
                ColumnParams.builder().name("KEY").type(INT8).build(),
                ColumnParams.builder().name("VAL").type(INT8).nullable(true).build()
        );

        createTwoColumnTable(
                ignite,
                ColumnParams.builder().name("KEY").type(BOOLEAN).build(),
                ColumnParams.builder().name("VAL").type(BOOLEAN).nullable(true).build()
        );

        createTwoColumnTable(
                ignite,
                ColumnParams.builder().name("KEY").type(INT16).build(),
                ColumnParams.builder().name("VAL").type(INT16).nullable(true).build()
        );

        createTwoColumnTable(
                ignite,
                ColumnParams.builder().name("KEY").type(INT32).build(),
                ColumnParams.builder().name("VAL").type(INT32).nullable(true).build()
        );

        createTwoColumnTable(
                ignite,
                ColumnParams.builder().name("KEY").type(INT64).build(),
                ColumnParams.builder().name("VAL").type(INT64).nullable(true).build()
        );

        createTwoColumnTable(
                ignite,
                ColumnParams.builder().name("KEY").type(FLOAT).build(),
                ColumnParams.builder().name("VAL").type(FLOAT).nullable(true).build()
        );

        createTwoColumnTable(
                ignite,
                ColumnParams.builder().name("KEY").type(DOUBLE).build(),
                ColumnParams.builder().name("VAL").type(DOUBLE).nullable(true).build()
        );

        createTwoColumnTable(
                ignite,
                ColumnParams.builder().name("KEY").type(UUID).build(),
                ColumnParams.builder().name("VAL").type(UUID).nullable(true).build()
        );

        createTwoColumnTable(
                ignite,
                ColumnParams.builder().name("KEY").type(DECIMAL).precision(19).scale(3).build(),
                ColumnParams.builder().name("VAL").type(DECIMAL).precision(19).scale(3).nullable(true).build()
        );

        createTwoColumnTable(
                ignite,
                ColumnParams.builder().name("KEY").type(STRING).length(1000).build(),
                ColumnParams.builder().name("VAL").type(STRING).length(1000).nullable(true).build()
        );

        createTwoColumnTable(
                ignite,
                ColumnParams.builder().name("KEY").type(DATE).build(),
                ColumnParams.builder().name("VAL").type(DATE).nullable(true).build()
        );

        createTwoColumnTable(
                ignite,
                ColumnParams.builder().name("KEY").type(DATETIME).precision(6).build(),
                ColumnParams.builder().name("VAL").type(DATETIME).precision(6).nullable(true).build()
        );

        createTwoColumnTable(
                ignite,
                ColumnParams.builder().name("KEY").type(TIME).precision(6).build(),
                ColumnParams.builder().name("VAL").type(TIME).precision(6).nullable(true).build()
        );

        createTwoColumnTable(
                ignite,
                ColumnParams.builder().name("KEY").type(TIMESTAMP).precision(6).build(),
                ColumnParams.builder().name("VAL").type(TIMESTAMP).precision(6).nullable(true).build()
        );

        createTwoColumnTable(
                ignite,
                ColumnParams.builder().name("KEY").type(BYTE_ARRAY).length(1000).build(),
                ColumnParams.builder().name("VAL").type(BYTE_ARRAY).length(1000).nullable(true).build()
        );
    }

    private static void createTwoColumnTable(IgniteImpl ignite, ColumnParams keyColumnParams, ColumnParams valueColumnParams) {
        assertEquals(keyColumnParams.type(), valueColumnParams.type());

        createTable(
                ignite.catalogManager(),
                SqlCommon.DEFAULT_SCHEMA_NAME,
                ZONE_NAME,
                ("tbl_" + keyColumnParams.type().name()).toUpperCase(),
                List.of(keyColumnParams, valueColumnParams),
                List.of(keyColumnParams.name())
        );
    }

    /**
     * Gets the thin client port.
     *
     * @param node Node.
     * @return Port number.
     */
    private static int getPort(IgniteImpl node) {
        return node.clientAddress().port();
    }

    /**
     * Gets run time limit, in minutes.
     *
     * @return Node run time limit, in minutes.
     */
    private static long getRunTimeMinutes() {
        String runTimeMinutesFromEnv = System.getenv(RUN_TIME_MINUTES_ENV);

        if (runTimeMinutesFromEnv == null) {
            return RUN_TIME_MINUTES;
        }

        try {
            return Long.parseLong(runTimeMinutesFromEnv);
        } catch (Exception ignored) {
            // No-op.
        }

        return RUN_TIME_MINUTES;
    }

    /**
     * Compute job that creates a table.
     */
    @SuppressWarnings("unused") // Used by platform tests.
    private static class CreateTableJob implements ComputeJob<String, String> {
        @Override
        public CompletableFuture<String> executeAsync(JobExecutionContext context, String tableName) {
            context.ignite().sql().execute(null, "CREATE TABLE " + tableName + "(key BIGINT PRIMARY KEY, val INT)");

            return completedFuture(tableName);
        }
    }

    /**
     * Compute job that drops a table.
     */
    @SuppressWarnings("unused") // Used by platform tests.
    private static class DropTableJob implements ComputeJob<String, String> {
        @Override
        public CompletableFuture<String> executeAsync(JobExecutionContext context, String tableName) {
            context.ignite().sql().execute(null, "DROP TABLE " + tableName + "");

            return completedFuture(tableName);
        }
    }

    /**
     * Compute job that throws an exception.
     */
    @SuppressWarnings("unused") // Used by platform tests.
    private static class ExceptionJob implements ComputeJob<String, String> {
        @Override
        public CompletableFuture<String> executeAsync(JobExecutionContext context, String msg) {
            throw new RuntimeException("Test exception: " + msg);
        }
    }

    /**
     * Compute job that throws an exception.
     */
    @SuppressWarnings("unused") // Used by platform tests.
    private static class CheckedExceptionJob implements ComputeJob<String, String> {
        @Override
        public CompletableFuture<String> executeAsync(JobExecutionContext context, String msg) {
            throw new CompletionException(new IgniteCheckedException(Common.NODE_LEFT_ERR, "TestCheckedEx: " + msg));
        }
    }

    /**
     * Compute job that computes row colocation hash.
     */
    @SuppressWarnings("unused") // Used by platform tests.
    private static class ColocationHashJob implements ComputeJob<byte[], Integer> {
        @Override
        public CompletableFuture<Integer> executeAsync(JobExecutionContext context, byte[] args) {
            BinaryTupleReader argsReader = new BinaryTupleReader(4, args);
            var columnCount = argsReader.intValue(0);
            var timePrecision = argsReader.intValue(1);
            var timestampPrecision = argsReader.intValue(2);
            var buf = argsReader.bytesValue(3);

            List<Column> columns = new ArrayList<>(columnCount);
            var tuple = Tuple.create(columnCount);
            var reader = new BinaryTupleReader(columnCount * 3, buf);

            for (int i = 0; i < columnCount; i++) {
                var type = ColumnTypeConverter.fromIdOrThrow(reader.intValue(i * 3));
                var scale = reader.intValue(i * 3 + 1);
                var valIdx = i * 3 + 2;

                String colName = "COL" + i;

                switch (type) {
                    case BOOLEAN:
                        columns.add(new Column(colName, NativeTypes.BOOLEAN, false));
                        tuple.set(colName, reader.booleanValue(valIdx));
                        break;

                    case INT8:
                        columns.add(new Column(colName, NativeTypes.INT8, false));
                        tuple.set(colName, reader.byteValue(valIdx));
                        break;

                    case INT16:
                        columns.add(new Column(colName, NativeTypes.INT16, false));
                        tuple.set(colName, reader.shortValue(valIdx));
                        break;

                    case INT32:
                        columns.add(new Column(colName, NativeTypes.INT32, false));
                        tuple.set(colName, reader.intValue(valIdx));
                        break;

                    case INT64:
                        columns.add(new Column(colName, NativeTypes.INT64, false));
                        tuple.set(colName, reader.longValue(valIdx));
                        break;

                    case FLOAT:
                        columns.add(new Column(colName, NativeTypes.FLOAT, false));
                        tuple.set(colName, reader.floatValue(valIdx));
                        break;

                    case DOUBLE:
                        columns.add(new Column(colName, NativeTypes.DOUBLE, false));
                        tuple.set(colName, reader.doubleValue(valIdx));
                        break;

                    case DECIMAL:
                        columns.add(new Column(colName, NativeTypes.decimalOf(100, scale), false));
                        tuple.set(colName, reader.decimalValue(valIdx, scale));
                        break;

                    case STRING:
                        columns.add(new Column(colName, NativeTypes.STRING, false));
                        tuple.set(colName, reader.stringValue(valIdx));
                        break;

                    case UUID:
                        columns.add(new Column(colName, NativeTypes.UUID, false));
                        tuple.set(colName, reader.uuidValue(valIdx));
                        break;

                    case DATE:
                        columns.add(new Column(colName, NativeTypes.DATE, false));
                        tuple.set(colName, reader.dateValue(valIdx));
                        break;

                    case TIME:
                        columns.add(new Column(colName, NativeTypes.time(timePrecision), false));
                        tuple.set(colName, reader.timeValue(valIdx));
                        break;

                    case DATETIME:
                        columns.add(new Column(colName, NativeTypes.datetime(timePrecision), false));
                        tuple.set(colName, reader.dateTimeValue(valIdx));
                        break;

                    case TIMESTAMP:
                        columns.add(new Column(colName, NativeTypes.timestamp(timestampPrecision), false));
                        tuple.set(colName, reader.timestampValue(valIdx));
                        break;

                    default:
                        throw new IllegalArgumentException("Unsupported type: " + type);
                }
            }

            List<String> colocationColumns = columns.stream().map(Column::name).collect(toList());
            var schema = new SchemaDescriptor(1, columns, colocationColumns, null);

            var marsh = new TupleMarshallerImpl(schema);

            Row row = marsh.marshal(tuple);

            return completedFuture(row.colocationHash());
        }
    }

    /**
     * Compute job that computes row colocation hash according to the current table schema.
     */
    @SuppressWarnings("unused") // Used by platform tests.
    private static class TableRowColocationHashJob implements ComputeJob<byte[], Integer> {
        @Override
        public CompletableFuture<Integer> executeAsync(JobExecutionContext context, byte[] args) {
            BinaryTupleReader reader = new BinaryTupleReader(2, args);
            String tableName = reader.stringValue(0);
            int i = reader.intValue(1);
            Tuple key = Tuple.create().set("id", 1 + i).set("id0", 2L + i).set("id1", "3" + i);

            @SuppressWarnings("resource")
            Table table = context.ignite().tables().table(tableName);
            RecordBinaryViewImpl view = Wrappers.unwrap(table.recordView(), RecordBinaryViewImpl.class);
            TupleMarshaller marsh = view.marshaller(1);

            return completedFuture(marsh.marshal(key).colocationHash());
        }
    }

    /**
     * Compute job that enables or disables client authentication.
     */
    @SuppressWarnings("unused") // Used by platform tests.
    private static class EnableAuthenticationJob implements ComputeJob<Integer, Void> {
        @Override
        public CompletableFuture<Void> executeAsync(JobExecutionContext context, Integer flag) {
            boolean enable = flag != 0;
            @SuppressWarnings("resource") IgniteImpl ignite = (IgniteImpl) context.ignite();

            CompletableFuture<Void> changeFuture = ignite.clusterConfiguration().change(
                    root -> {
                        SecurityChange securityChange = root.changeRoot(SecurityConfiguration.KEY);
                        securityChange.changeEnabled(enable);
                        securityChange.changeAuthentication().changeProviders().update("default", defaultProviderChange -> {
                            defaultProviderChange.convert(BasicAuthenticationProviderChange.class).changeUsers(users -> {
                                        if (enable) {
                                            users.create("user-1", user -> user.changePassword("password-1"));
                                        } else {
                                            users.delete("user-1");
                                        }
                                    }
                            );
                        });
                    });

            assertThat(changeFuture, willCompleteSuccessfully());

            return null;
        }
    }

    @SuppressWarnings("unused") // Used by platform tests.
    private static class TestReceiver implements DataStreamerReceiver<String, String, String> {
        @SuppressWarnings("resource")
        @Override
        public @Nullable CompletableFuture<List<String>> receive(List<String> page, DataStreamerReceiverContext ctx, String arg) {
            String[] args = arg.split(":", 3);
            String tableName = args[0];
            String arg1 = args[1];
            int arg2 = Integer.parseInt(args[2]);

            if (Objects.equals(arg1, "throw")) {
                throw new ArithmeticException("Test exception: " + arg2);
            }

            Table table = ctx.ignite().tables().table(tableName);
            RecordView<Tuple> recordView = table.recordView();
            List<String> res = new ArrayList<>();

            for (String s : page) {
                String[] parts = s.split("-", 2);
                String val = parts[1] + "_" + arg1 + "_" + arg2;

                Tuple rec = Tuple.create()
                        .set("key", Long.parseLong(parts[0]))
                        .set("val", val);

                res.add(val);

                recordView.upsert(null, rec);
            }

            return completedFuture(res);
        }
    }

    @SuppressWarnings("unused") // Used by platform tests.
    private static class UpsertElementTypeNameReceiver implements DataStreamerReceiver<Object, String, Object> {
        @SuppressWarnings("resource")
        @Override
        public @Nullable CompletableFuture<List<Object>> receive(List<Object> page, DataStreamerReceiverContext ctx, String arg) {
            String[] args = arg.split(":", 3);

            String tableName = args[0];
            long id1 = Long.parseLong(args[1]);
            long id2 = Long.parseLong(args[2]);

            Table table = ctx.ignite().tables().table(tableName);
            RecordView<Tuple> recordView = table.recordView();

            for (Object item : page) {
                Tuple classNameRec = Tuple.create()
                        .set("key", id1)
                        .set("val", item.getClass().getName());

                Tuple valStrRec = Tuple.create()
                        .set("key", id2)
                        .set("val", item instanceof byte[]
                                ? Arrays.toString((byte[]) item)
                                : item.toString());

                recordView.upsertAll(null, List.of(classNameRec, valStrRec));
            }

            return null;
        }
    }

    @SuppressWarnings("unused") // Used by platform tests.
    private static class EchoArgsReceiver implements DataStreamerReceiver<Object, Object, Object> {
        @Override
        public CompletableFuture<List<Object>> receive(List<Object> page, DataStreamerReceiverContext ctx, Object arg) {
            return CompletableFuture.completedFuture(List.of(arg));
        }
    }

    @SuppressWarnings("unused") // Used by platform tests.
    private static class EchoReceiver implements DataStreamerReceiver<Object, Object, Object> {
        @Override
        public CompletableFuture<List<Object>> receive(List<Object> page, DataStreamerReceiverContext ctx, Object arg) {
            return CompletableFuture.completedFuture(page);
        }
    }

    @SuppressWarnings("unused") // Used by platform tests.
    private static class PartitionJob implements ComputeJob<Long, Integer> {
        @Override
        public CompletableFuture<Integer> executeAsync(JobExecutionContext context, Long id) {
            Table table = context.ignite().tables().table(TABLE_NAME);
            Tuple key = Tuple.create().set("key", id);
            Partition partition = table.partitionManager().partitionAsync(key).join();

            return completedFuture(((HashPartition) partition).partitionId());
        }
    }

    @SuppressWarnings("unused") // Used by platform tests.
    private static class SleepTask implements MapReduceTask<Integer, Integer, Void, Void> {
        @Override
        public CompletableFuture<List<MapReduceJob<Integer, Void>>> splitAsync(TaskExecutionContext context, Integer input) {
            return completedFuture(context.ignite().clusterNodes().stream()
                    .map(node -> MapReduceJob.<Integer, Void>builder()
                            .jobDescriptor(JobDescriptor.builder(SleepJob.class).build())
                            .nodes(Set.of(node))
                            .args(input)
                            .build())
                    .collect(toList()));
        }

        @Override
        public CompletableFuture<Void> reduceAsync(TaskExecutionContext taskContext, Map<java.util.UUID, Void> results) {
            return completedFuture(null);
        }
    }

    private static class SleepJob implements ComputeJob<Integer, Void> {
        @Override
        public @Nullable CompletableFuture<Void> executeAsync(JobExecutionContext context, Integer args) {
            try {
                Thread.sleep(args);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            return null;
        }
    }
}
