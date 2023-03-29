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

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.escapeWindowsPath;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.getResourcePath;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.InitParameters;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.client.proto.ClientDataType;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerException;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerImpl;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.testutils.SchemaConfigurationConverter;
import org.apache.ignite.internal.schema.testutils.builder.SchemaBuilders;
import org.apache.ignite.internal.schema.testutils.definition.ColumnType;
import org.apache.ignite.internal.schema.testutils.definition.ColumnType.TemporalColumnType;
import org.apache.ignite.internal.schema.testutils.definition.TableDefinition;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.impl.DummySchemaManagerImpl;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.sql.Session;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.NotNull;

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

    private static final String SCHEMA_NAME = "PUBLIC";

    private static final String TABLE_NAME = "TBL1";

    private static final String TABLE_NAME_ALL_COLUMNS = "tbl_all_columns";

    private static final String TABLE_NAME_ALL_COLUMNS_SQL = "tbl_all_columns_sql"; // All column types supported by SQL.

    /** Time to keep the node alive. */
    private static final int RUN_TIME_MINUTES = 30;

    /** Time to keep the node alive - env var. */
    private static final String RUN_TIME_MINUTES_ENV = "IGNITE_PLATFORM_TEST_NODE_RUNNER_RUN_TIME_MINUTES";

    /** Nodes bootstrap configuration. */
    private static final Map<String, String> nodesBootstrapCfg = Map.of(
            NODE_NAME, "{\n"
                    + "  \"clientConnector\":{\"port\": 10942,\"portRange\":1,\"idleTimeout\":3000,\""
                    + "sendServerExceptionStackTraceToClient\":true},"
                    + "  \"network\": {\n"
                    + "    \"port\":3344,\n"
                    + "    \"nodeFinder\": {\n"
                    + "      \"netClusterNodes\":[ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\", \"localhost:3347\" ]\n"
                    + "    }\n"
                    + "  }\n"
                    + "}",

            NODE_NAME2, "{\n"
                    + "  \"clientConnector\":{\"port\": 10943,\"portRange\":1,\"idleTimeout\":3000,"
                    + "\"sendServerExceptionStackTraceToClient\":true},"
                    + "  \"network\": {\n"
                    + "    \"port\":3345,\n"
                    + "    \"nodeFinder\": {\n"
                    + "      \"netClusterNodes\":[ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\", \"localhost:3347\" ]\n"
                    + "    }\n"
                    + "  }\n"
                    + "}",

            NODE_NAME3, "{\n"
                    + "  \"clientConnector\":{"
                    + "    \"port\": 10944,"
                    + "    \"portRange\":1,"
                    + "    \"idleTimeout\":3000,"
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
                    + "  }\n"
                    + "}",

            NODE_NAME4, "{\n"
                    + "  \"clientConnector\":{"
                    + "    \"port\": 10945,"
                    + "    \"portRange\":1,"
                    + "    \"idleTimeout\":3000,"
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
                    + "  }\n"
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

        for (int i = 0; i < args.length; i++) {
            System.out.println("Arg " + i + ": " + args[i]);
        }

        if (args.length > 0 && "dry-run".equals(args[0])) {
            System.out.println("Dry run succeeded.");
            return;
        }

        List<Ignite> startedNodes = startNodes(BASE_PATH, nodesBootstrapCfg);

        createTables(startedNodes.get(0));

        String ports = startedNodes.stream()
                .map(n -> String.valueOf(getPort((IgniteImpl) n)))
                .collect(Collectors.joining(","));

        System.out.println("THIN_CLIENT_PORTS=" + ports);

        long runTimeMinutes = getRunTimeMinutes();
        System.out.println("Nodes will be active for " + runTimeMinutes + " minutes.");

        Thread.sleep(runTimeMinutes * 60_000);
        System.out.println("Exiting after " + runTimeMinutes + " minutes.");

        for (Ignite node : startedNodes) {
            IgnitionManager.stop(node.name());
        }
    }

    /**
     * Start nodes.
     *
     * @param basePath Base path.
     * @param nodeCfg Node configuration.
     * @return Started nodes.
     */
    @NotNull
    static List<Ignite> startNodes(Path basePath, Map<String, String> nodeCfg) throws IOException {
        IgniteUtils.deleteIfExists(basePath);
        Files.createDirectories(basePath);

        var sslPassword = "123456";
        var trustStorePath = escapeWindowsPath(getResourcePath(PlatformTestNodeRunner.class, "ssl/trust.jks"));
        var keyStorePath = escapeWindowsPath(getResourcePath(PlatformTestNodeRunner.class, "ssl/server.jks"));

        List<CompletableFuture<Ignite>> igniteFutures = nodeCfg.entrySet().stream()
                .map(e -> {
                    String nodeName = e.getKey();
                    String config = e.getValue()
                            .replace("KEYSTORE_PATH", keyStorePath)
                            .replace("TRUSTSTORE_PATH", trustStorePath)
                            .replace("SSL_STORE_PASS", sslPassword);

                    return IgnitionManager.start(nodeName, config, basePath.resolve(nodeName));
                })
                .collect(toList());

        String metaStorageNodeName = nodeCfg.keySet().iterator().next();

        InitParameters initParameters = InitParameters.builder()
                .destinationNodeName(metaStorageNodeName)
                .metaStorageNodeNames(List.of(metaStorageNodeName))
                .clusterName("cluster")
                .build();
        IgnitionManager.init(initParameters);

        System.out.println("Initialization complete");

        List<Ignite> startedNodes = igniteFutures.stream().map(CompletableFuture::join).collect(toList());

        System.out.println("Ignite nodes started");

        return startedNodes;
    }

    private static void createTables(Ignite node) {
        var keyCol = "key";

        TableDefinition schTbl = SchemaBuilders.tableBuilder(SCHEMA_NAME, TABLE_NAME).columns(
                SchemaBuilders.column(keyCol, ColumnType.INT64).build(),
                SchemaBuilders.column("val", ColumnType.string()).asNullable(true).build()
        ).withPrimaryKey(keyCol).build();

        await(((TableManager) node.tables()).createTableAsync(schTbl.name(), tblCh ->
                SchemaConfigurationConverter.convert(schTbl, tblCh)
                        .changeReplicas(1)
                        .changePartitions(10)
        ));

        int maxTimePrecision = TemporalColumnType.MAX_TIME_PRECISION;

        TableDefinition schTblAll = SchemaBuilders.tableBuilder(SCHEMA_NAME, TABLE_NAME_ALL_COLUMNS).columns(
                SchemaBuilders.column(keyCol, ColumnType.INT64).build(),
                SchemaBuilders.column("str", ColumnType.string()).asNullable(true).build(),
                SchemaBuilders.column("int8", ColumnType.INT8).asNullable(true).build(),
                SchemaBuilders.column("int16", ColumnType.INT16).asNullable(true).build(),
                SchemaBuilders.column("int32", ColumnType.INT32).asNullable(true).build(),
                SchemaBuilders.column("int64", ColumnType.INT64).asNullable(true).build(),
                SchemaBuilders.column("float", ColumnType.FLOAT).asNullable(true).build(),
                SchemaBuilders.column("double", ColumnType.DOUBLE).asNullable(true).build(),
                SchemaBuilders.column("uuid", ColumnType.UUID).asNullable(true).build(),
                SchemaBuilders.column("date", ColumnType.DATE).asNullable(true).build(),
                SchemaBuilders.column("bitmask", ColumnType.bitmaskOf(64)).asNullable(true).build(),
                SchemaBuilders.column("time", ColumnType.time(maxTimePrecision)).asNullable(true).build(),
                SchemaBuilders.column("time2", ColumnType.time(2)).asNullable(true).build(),
                SchemaBuilders.column("datetime", ColumnType.datetime(maxTimePrecision)).asNullable(true).build(),
                SchemaBuilders.column("datetime2", ColumnType.datetime(3)).asNullable(true).build(),
                SchemaBuilders.column("timestamp", ColumnType.timestamp(maxTimePrecision)).asNullable(true).build(),
                SchemaBuilders.column("timestamp2", ColumnType.timestamp(4)).asNullable(true).build(),
                SchemaBuilders.column("blob", ColumnType.blob()).asNullable(true).build(),
                SchemaBuilders.column("decimal", ColumnType.decimal()).asNullable(true).build()
        ).withPrimaryKey(keyCol).build();

        await(((TableManager) node.tables()).createTableAsync(schTblAll.name(), tblCh ->
                SchemaConfigurationConverter.convert(schTblAll, tblCh)
                        .changeReplicas(1)
                        .changePartitions(10)
        ));

        // TODO IGNITE-18431 remove extra table, use TABLE_NAME_ALL_COLUMNS for SQL tests.
        TableDefinition schTblAllSql = SchemaBuilders.tableBuilder(SCHEMA_NAME, TABLE_NAME_ALL_COLUMNS_SQL).columns(
                SchemaBuilders.column(keyCol, ColumnType.INT64).build(),
                SchemaBuilders.column("str", ColumnType.string()).asNullable(true).build(),
                SchemaBuilders.column("int8", ColumnType.INT8).asNullable(true).build(),
                SchemaBuilders.column("int16", ColumnType.INT16).asNullable(true).build(),
                SchemaBuilders.column("int32", ColumnType.INT32).asNullable(true).build(),
                SchemaBuilders.column("int64", ColumnType.INT64).asNullable(true).build(),
                SchemaBuilders.column("float", ColumnType.FLOAT).asNullable(true).build(),
                SchemaBuilders.column("double", ColumnType.DOUBLE).asNullable(true).build(),
                SchemaBuilders.column("date", ColumnType.DATE).asNullable(true).build(),
                SchemaBuilders.column("time", ColumnType.time(maxTimePrecision)).asNullable(true).build(),
                SchemaBuilders.column("time2", ColumnType.time(maxTimePrecision)).asNullable(true).build(),
                SchemaBuilders.column("datetime", ColumnType.datetime(maxTimePrecision)).asNullable(true).build(),
                SchemaBuilders.column("datetime2", ColumnType.datetime(maxTimePrecision)).asNullable(true).build(),
                SchemaBuilders.column("timestamp", ColumnType.timestamp(maxTimePrecision)).asNullable(true).build(),
                SchemaBuilders.column("timestamp2", ColumnType.timestamp(maxTimePrecision)).asNullable(true).build(),
                SchemaBuilders.column("blob", ColumnType.blob()).asNullable(true).build(),
                SchemaBuilders.column("decimal", ColumnType.decimal()).asNullable(true).build()
        ).withPrimaryKey(keyCol).build();

        await(((TableManager) node.tables()).createTableAsync(schTblAllSql.name(), tblCh ->
                SchemaConfigurationConverter.convert(schTblAllSql, tblCh)
                        .changeReplicas(1)
                        .changePartitions(10)
        ));

        createTwoColumnTable(node, ColumnType.INT8);
        createTwoColumnTable(node, ColumnType.INT16);
        createTwoColumnTable(node, ColumnType.INT32);
        createTwoColumnTable(node, ColumnType.INT64);
        createTwoColumnTable(node, ColumnType.FLOAT);
        createTwoColumnTable(node, ColumnType.DOUBLE);
        createTwoColumnTable(node, ColumnType.decimal());
        createTwoColumnTable(node, ColumnType.string());
        createTwoColumnTable(node, ColumnType.DATE);
        createTwoColumnTable(node, ColumnType.datetime());
        createTwoColumnTable(node, ColumnType.time());
        createTwoColumnTable(node, ColumnType.timestamp());
        createTwoColumnTable(node, ColumnType.number());
        createTwoColumnTable(node, ColumnType.blob());
        createTwoColumnTable(node, ColumnType.bitmaskOf(32));
    }

    private static void createTwoColumnTable(Ignite node, ColumnType type) {
        var keyCol = "key";

        TableDefinition schTbl = SchemaBuilders.tableBuilder(SCHEMA_NAME, "tbl_" + type.typeSpec().name()).columns(
                SchemaBuilders.column(keyCol, type).build(),
                SchemaBuilders.column("val", type).asNullable(true).build()
        ).withPrimaryKey(keyCol).build();

        await(((TableManager) node.tables()).createTableAsync(schTbl.name(), tblCh ->
                SchemaConfigurationConverter.convert(schTbl, tblCh)
                        .changeReplicas(1)
                        .changePartitions(10)
        ));
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
    @SuppressWarnings({"unused"}) // Used by platform tests.
    private static class CreateTableJob implements ComputeJob<String> {
        @Override
        public String execute(JobExecutionContext context, Object... args) {
            String tableName = (String) args[0];

            try (Session session = context.ignite().sql().createSession()) {
                session.execute(null, "CREATE TABLE " + tableName + "(key BIGINT PRIMARY KEY, val INT)");
            }

            return tableName;
        }
    }

    /**
     * Compute job that drops a table.
     */
    @SuppressWarnings({"unused"}) // Used by platform tests.
    private static class DropTableJob implements ComputeJob<String> {
        @Override
        public String execute(JobExecutionContext context, Object... args) {
            String tableName = (String) args[0];
            try (Session session = context.ignite().sql().createSession()) {
                session.execute(null, "DROP TABLE " + tableName + "");
            }

            return tableName;
        }
    }

    /**
     * Compute job that throws an exception.
     */
    @SuppressWarnings({"unused"}) // Used by platform tests.
    private static class ExceptionJob implements ComputeJob<String> {
        @Override
        public String execute(JobExecutionContext context, Object... args) {
            throw new RuntimeException("Test exception: " + args[0]);
        }
    }

    /**
     * Compute job that computes row colocation hash.
     */
    @SuppressWarnings({"unused"}) // Used by platform tests.
    private static class ColocationHashJob implements ComputeJob<Integer> {
        @Override
        public Integer execute(JobExecutionContext context, Object... args) {
            var columnCount = (int) args[0];
            var buf = (byte[]) args[1];
            var timePrecision = (int) args[2];
            var timestampPrecision = (int) args[3];

            var columns = new Column[columnCount];
            var tuple = Tuple.create(columnCount);
            var reader = new BinaryTupleReader(columnCount * 3, buf);

            for (int i = 0; i < columnCount; i++) {
                var type = reader.intValue(i * 3);
                var scale = reader.intValue(i * 3 + 1);
                var valIdx = i * 3 + 2;

                String colName = "col" + i;

                switch (type) {
                    case ClientDataType.INT8:
                        columns[i] = new Column(i, colName, NativeTypes.INT8, false);
                        tuple.set(colName, reader.byteValue(valIdx));
                        break;

                    case ClientDataType.INT16:
                        columns[i] = new Column(i, colName, NativeTypes.INT16, false);
                        tuple.set(colName, reader.shortValue(valIdx));
                        break;

                    case ClientDataType.INT32:
                        columns[i] = new Column(i, colName, NativeTypes.INT32, false);
                        tuple.set(colName, reader.intValue(valIdx));
                        break;

                    case ClientDataType.INT64:
                        columns[i] = new Column(i, colName, NativeTypes.INT64, false);
                        tuple.set(colName, reader.longValue(valIdx));
                        break;

                    case ClientDataType.FLOAT:
                        columns[i] = new Column(i, colName, NativeTypes.FLOAT, false);
                        tuple.set(colName, reader.floatValue(valIdx));
                        break;

                    case ClientDataType.DOUBLE:
                        columns[i] = new Column(i, colName, NativeTypes.DOUBLE, false);
                        tuple.set(colName, reader.doubleValue(valIdx));
                        break;

                    case ClientDataType.DECIMAL:
                        columns[i] = new Column(i, colName, NativeTypes.decimalOf(100, scale), false);
                        tuple.set(colName, reader.decimalValue(valIdx, scale));
                        break;

                    case ClientDataType.STRING:
                        columns[i] = new Column(i, colName, NativeTypes.STRING, false);
                        tuple.set(colName, reader.stringValue(valIdx));
                        break;

                    case ClientDataType.UUID:
                        columns[i] = new Column(i, colName, NativeTypes.UUID, false);
                        tuple.set(colName, reader.uuidValue(valIdx));
                        break;

                    case ClientDataType.NUMBER:
                        columns[i] = new Column(i, colName, NativeTypes.numberOf(255), false);
                        tuple.set(colName, reader.numberValue(valIdx));
                        break;

                    case ClientDataType.BITMASK:
                        columns[i] = new Column(i, colName, NativeTypes.bitmaskOf(32), false);
                        tuple.set(colName, reader.bitmaskValue(valIdx));
                        break;

                    case ClientDataType.DATE:
                        columns[i] = new Column(i, colName, NativeTypes.DATE, false);
                        tuple.set(colName, reader.dateValue(valIdx));
                        break;

                    case ClientDataType.TIME:
                        columns[i] = new Column(i, colName, NativeTypes.time(timePrecision), false);
                        tuple.set(colName, reader.timeValue(valIdx));
                        break;

                    case ClientDataType.DATETIME:
                        columns[i] = new Column(i, colName, NativeTypes.datetime(timePrecision), false);
                        tuple.set(colName, reader.dateTimeValue(valIdx));
                        break;

                    case ClientDataType.TIMESTAMP:
                        columns[i] = new Column(i, colName, NativeTypes.timestamp(timestampPrecision), false);
                        tuple.set(colName, reader.timestampValue(valIdx));
                        break;

                    default:
                        throw new IllegalArgumentException("Unsupported type: " + type);
                }
            }

            var colocationColumns = Arrays.stream(columns).map(Column::name).toArray(String[]::new);
            var schema = new SchemaDescriptor(1, columns, colocationColumns, new Column[0]);

            var marsh = new TupleMarshallerImpl(new DummySchemaManagerImpl(schema));

            try {
                Row row = marsh.marshal(tuple);

                return row.colocationHash();
            } catch (TupleMarshallerException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
