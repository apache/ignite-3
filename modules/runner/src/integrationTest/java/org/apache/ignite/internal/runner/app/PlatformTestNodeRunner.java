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
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_SCHEMA_NAME;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.createZone;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.escapeWindowsPath;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.getResourcePath;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.netty.util.ResourceLeakDetector;
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
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.CreateTableParams;
import org.apache.ignite.internal.catalog.commands.CreateZoneParams;
import org.apache.ignite.internal.client.proto.ColumnTypeConverter;
import org.apache.ignite.internal.configuration.BasicAuthenticationProviderChange;
import org.apache.ignite.internal.configuration.SecurityConfiguration;
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
import org.apache.ignite.internal.table.RecordBinaryViewImpl;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.impl.DummySchemaManagerImpl;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.sql.Session;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;

/**
 * Helper class for non-Java platform tests (.NET, C++, Python, ...). Starts nodes, populates tables and data for tests.
 */
@SuppressWarnings("CallToSystemGetenv")
// TODO: IGNITE-19499 Use catalog only
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

    private static final String ZONE_NAME = "zone1";

    /** Time to keep the node alive. */
    private static final int RUN_TIME_MINUTES = 30;

    /** Time to keep the node alive - env var. */
    private static final String RUN_TIME_MINUTES_ENV = "IGNITE_PLATFORM_TEST_NODE_RUNNER_RUN_TIME_MINUTES";

    /** Nodes bootstrap configuration. */
    private static final Map<String, String> nodesBootstrapCfg = Map.of(
            NODE_NAME, "{\n"
                    + "  \"clientConnector\":{\"port\": 10942,\"idleTimeout\":3000,\""
                    + "sendServerExceptionStackTraceToClient\":true},"
                    + "  \"network\": {\n"
                    + "    \"port\":3344,\n"
                    + "    \"nodeFinder\": {\n"
                    + "      \"netClusterNodes\":[ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\", \"localhost:3347\" ]\n"
                    + "    }\n"
                    + "  }\n"
                    + "}",

            NODE_NAME2, "{\n"
                    + "  \"clientConnector\":{\"port\": 10943,\"idleTimeout\":3000,"
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
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);

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

                    return TestIgnitionManager.start(nodeName, config, basePath.resolve(nodeName));
                })
                .collect(toList());

        String metaStorageNodeName = nodeCfg.keySet().iterator().next();

        InitParameters initParameters = InitParameters.builder()
                .destinationNodeName(metaStorageNodeName)
                .metaStorageNodeNames(List.of(metaStorageNodeName))
                .clusterName("cluster")
                .build();
        TestIgnitionManager.init(initParameters);

        System.out.println("Initialization complete");

        List<Ignite> startedNodes = igniteFutures.stream().map(CompletableFuture::join).collect(toList());

        System.out.println("Ignite nodes started");

        return startedNodes;
    }

    private static void createTables(Ignite node) {
        var keyCol = "KEY";

        createZone(((IgniteImpl) node).distributionZoneManager(), ZONE_NAME, 10, 1);

        IgniteImpl ignite = ((IgniteImpl) node);

        CreateZoneParams createZoneParams = CreateZoneParams.builder()
                .zoneName(ZONE_NAME)
                .partitions(10)
                .replicas(1)
                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                .build();

        assertThat(ignite.catalogManager().createZone(createZoneParams), willBe(nullValue()));

        TableDefinition schTbl = SchemaBuilders.tableBuilder(DEFAULT_SCHEMA_NAME, TABLE_NAME).columns(
                SchemaBuilders.column(keyCol, ColumnType.INT64).build(),
                SchemaBuilders.column("val", ColumnType.string()).asNullable(true).build()
        ).withPrimaryKey(keyCol).build();

        await(((TableManager) node.tables()).createTableAsync(schTbl.name(), ZONE_NAME, tblCh ->
                SchemaConfigurationConverter.convert(schTbl, tblCh)
        ));

        CreateTableParams createTableParams = CreateTableParams.builder()
                .schemaName(DEFAULT_SCHEMA_NAME)
                .zone(ZONE_NAME)
                .tableName(TABLE_NAME)
                .columns(List.of(
                        ColumnParams.builder().name(keyCol).type(org.apache.ignite.sql.ColumnType.INT64).build(),
                        ColumnParams.builder().name("VAL").type(org.apache.ignite.sql.ColumnType.STRING).length(0).nullable(true).build()
                ))
                .primaryKeyColumns(List.of(keyCol))
                .build();

        assertThat(ignite.catalogManager().createTable(createTableParams), willBe(nullValue()));

        int maxTimePrecision = TemporalColumnType.MAX_TIME_PRECISION;

        TableDefinition schTblAll = SchemaBuilders.tableBuilder(DEFAULT_SCHEMA_NAME, TABLE_NAME_ALL_COLUMNS).columns(
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

        await(((TableManager) node.tables()).createTableAsync(schTblAll.name(), ZONE_NAME, tblCh ->
                SchemaConfigurationConverter.convert(schTblAll, tblCh)
        ));

        CreateTableParams createTableParamsAll = CreateTableParams.builder()
                .schemaName(DEFAULT_SCHEMA_NAME)
                .zone(ZONE_NAME)
                .tableName(TABLE_NAME_ALL_COLUMNS)
                .columns(List.of(
                        ColumnParams.builder().name(keyCol).type(org.apache.ignite.sql.ColumnType.INT64).build(),
                        ColumnParams.builder().name("STR").type(org.apache.ignite.sql.ColumnType.STRING).length(0).nullable(true).build(),
                        ColumnParams.builder().name("INT8").type(org.apache.ignite.sql.ColumnType.INT8).nullable(true).build(),
                        ColumnParams.builder().name("INT16").type(org.apache.ignite.sql.ColumnType.INT16).nullable(true).build(),
                        ColumnParams.builder().name("INT32").type(org.apache.ignite.sql.ColumnType.INT32).nullable(true).build(),
                        ColumnParams.builder().name("INT64").type(org.apache.ignite.sql.ColumnType.INT64).nullable(true).build(),
                        ColumnParams.builder().name("FLOAT").type(org.apache.ignite.sql.ColumnType.FLOAT).nullable(true).build(),
                        ColumnParams.builder().name("DOUBLE").type(org.apache.ignite.sql.ColumnType.DOUBLE).nullable(true).build(),
                        ColumnParams.builder().name("UUID").type(org.apache.ignite.sql.ColumnType.UUID).nullable(true).build(),
                        ColumnParams.builder().name("DATE").type(org.apache.ignite.sql.ColumnType.DATE).nullable(true).build(),
                        ColumnParams.builder().name("BITMASK").type(org.apache.ignite.sql.ColumnType.BITMASK).length(64).nullable(true)
                                .build(),
                        ColumnParams.builder().name("TIME").type(org.apache.ignite.sql.ColumnType.TIME).precision(maxTimePrecision)
                                .nullable(true).build(),
                        ColumnParams.builder().name("TIME2").type(org.apache.ignite.sql.ColumnType.TIME).precision(2).nullable(true)
                                .build(),
                        ColumnParams.builder().name("DATETIME").type(org.apache.ignite.sql.ColumnType.DATETIME).precision(maxTimePrecision)
                                .nullable(true).build(),
                        ColumnParams.builder().name("DATETIME2").type(org.apache.ignite.sql.ColumnType.DATETIME).precision(3).nullable(true)
                                .build(),
                        ColumnParams.builder().name("TIMESTAMP").type(org.apache.ignite.sql.ColumnType.DATETIME).precision(maxTimePrecision)
                                .nullable(true).build(),
                        ColumnParams.builder().name("TIMESTAMP2").type(org.apache.ignite.sql.ColumnType.DATETIME).precision(4)
                                .nullable(true).build(),
                        ColumnParams.builder().name("BLOB").type(org.apache.ignite.sql.ColumnType.BYTE_ARRAY).length(0).nullable(true)
                                .build(),
                        ColumnParams.builder().name("DECIMAL").type(org.apache.ignite.sql.ColumnType.DECIMAL).precision(19).scale(3)
                                .nullable(true).build()
                ))
                .primaryKeyColumns(List.of(keyCol))
                .build();

        assertThat(ignite.catalogManager().createTable(createTableParamsAll), willBe(nullValue()));

        // TODO IGNITE-18431 remove extra table, use TABLE_NAME_ALL_COLUMNS for SQL tests.
        TableDefinition schTblAllSql = SchemaBuilders.tableBuilder(DEFAULT_SCHEMA_NAME, TABLE_NAME_ALL_COLUMNS_SQL).columns(
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
                SchemaBuilders.column("time", ColumnType.time(maxTimePrecision)).asNullable(true).build(),
                SchemaBuilders.column("time2", ColumnType.time(maxTimePrecision)).asNullable(true).build(),
                SchemaBuilders.column("datetime", ColumnType.datetime(maxTimePrecision)).asNullable(true).build(),
                SchemaBuilders.column("datetime2", ColumnType.datetime(maxTimePrecision)).asNullable(true).build(),
                SchemaBuilders.column("timestamp", ColumnType.timestamp(maxTimePrecision)).asNullable(true).build(),
                SchemaBuilders.column("timestamp2", ColumnType.timestamp(maxTimePrecision)).asNullable(true).build(),
                SchemaBuilders.column("blob", ColumnType.blob()).asNullable(true).build(),
                SchemaBuilders.column("decimal", ColumnType.decimal()).asNullable(true).build()
        ).withPrimaryKey(keyCol).build();

        await(((TableManager) node.tables()).createTableAsync(schTblAllSql.name(), ZONE_NAME, tblCh ->
                SchemaConfigurationConverter.convert(schTblAllSql, tblCh)
        ));

        CreateTableParams createTableParamsAllSql = CreateTableParams.builder()
                .schemaName(DEFAULT_SCHEMA_NAME)
                .zone(ZONE_NAME)
                .tableName(TABLE_NAME_ALL_COLUMNS_SQL)
                .columns(List.of(
                        ColumnParams.builder().name(keyCol).type(org.apache.ignite.sql.ColumnType.INT64).build(),
                        ColumnParams.builder().name("STR").type(org.apache.ignite.sql.ColumnType.STRING).length(0).nullable(true).build(),
                        ColumnParams.builder().name("INT8").type(org.apache.ignite.sql.ColumnType.INT8).nullable(true).build(),
                        ColumnParams.builder().name("INT16").type(org.apache.ignite.sql.ColumnType.INT16).nullable(true).build(),
                        ColumnParams.builder().name("INT32").type(org.apache.ignite.sql.ColumnType.INT32).nullable(true).build(),
                        ColumnParams.builder().name("INT64").type(org.apache.ignite.sql.ColumnType.INT64).nullable(true).build(),
                        ColumnParams.builder().name("FLOAT").type(org.apache.ignite.sql.ColumnType.FLOAT).nullable(true).build(),
                        ColumnParams.builder().name("DOUBLE").type(org.apache.ignite.sql.ColumnType.DOUBLE).nullable(true).build(),
                        ColumnParams.builder().name("UUID").type(org.apache.ignite.sql.ColumnType.UUID).nullable(true).build(),
                        ColumnParams.builder().name("DATE").type(org.apache.ignite.sql.ColumnType.DATE).nullable(true).build(),
                        ColumnParams.builder().name("TIME").type(org.apache.ignite.sql.ColumnType.TIME).precision(maxTimePrecision)
                                .nullable(true).build(),
                        ColumnParams.builder().name("TIME2").type(org.apache.ignite.sql.ColumnType.TIME).precision(maxTimePrecision)
                                .nullable(true).build(),
                        ColumnParams.builder().name("DATETIME").type(org.apache.ignite.sql.ColumnType.DATETIME).precision(maxTimePrecision)
                                .nullable(true).build(),
                        ColumnParams.builder().name("DATETIME2").type(org.apache.ignite.sql.ColumnType.DATETIME).precision(maxTimePrecision)
                                .nullable(true).build(),
                        ColumnParams.builder().name("TIMESTAMP").type(org.apache.ignite.sql.ColumnType.TIMESTAMP)
                                .precision(maxTimePrecision).nullable(true).build(),
                        ColumnParams.builder().name("TIMESTAMP2").type(org.apache.ignite.sql.ColumnType.TIMESTAMP)
                                .precision(maxTimePrecision).nullable(true).build(),
                        ColumnParams.builder().name("BLOB").type(org.apache.ignite.sql.ColumnType.BYTE_ARRAY).length(0).nullable(true)
                                .build(),
                        ColumnParams.builder().name("DECIMAL").type(org.apache.ignite.sql.ColumnType.DECIMAL).precision(19).scale(3)
                                .nullable(true).build()
                ))
                .primaryKeyColumns(List.of(keyCol))
                .build();

        assertThat(ignite.catalogManager().createTable(createTableParamsAllSql), willBe(nullValue()));

        createTwoColumnTable(node, ColumnType.INT8);
        createTwoColumnTable(node, ColumnType.INT16);
        createTwoColumnTable(node, ColumnType.INT32);
        createTwoColumnTable(node, ColumnType.INT64);
        createTwoColumnTable(node, ColumnType.FLOAT);
        createTwoColumnTable(node, ColumnType.DOUBLE);
        createTwoColumnTable(node, ColumnType.UUID);
        createTwoColumnTable(node, ColumnType.decimal());
        createTwoColumnTable(node, ColumnType.string());
        createTwoColumnTable(node, ColumnType.DATE);
        createTwoColumnTable(node, ColumnType.datetime());
        createTwoColumnTable(node, ColumnType.time());
        createTwoColumnTable(node, ColumnType.timestamp());
        createTwoColumnTable(node, ColumnType.number());
        createTwoColumnTable(node, ColumnType.blob());
        createTwoColumnTable(node, ColumnType.bitmaskOf(32));

        createTwoColumnTable(
                ignite,
                ColumnParams.builder().name("KEY").type(org.apache.ignite.sql.ColumnType.INT8).build(),
                ColumnParams.builder().name("VAL").type(org.apache.ignite.sql.ColumnType.INT8).nullable(true).build()
        );

        createTwoColumnTable(
                ignite,
                ColumnParams.builder().name("KEY").type(org.apache.ignite.sql.ColumnType.INT16).build(),
                ColumnParams.builder().name("VAL").type(org.apache.ignite.sql.ColumnType.INT16).nullable(true).build()
        );

        createTwoColumnTable(
                ignite,
                ColumnParams.builder().name("KEY").type(org.apache.ignite.sql.ColumnType.INT32).build(),
                ColumnParams.builder().name("VAL").type(org.apache.ignite.sql.ColumnType.INT32).nullable(true).build()
        );

        createTwoColumnTable(
                ignite,
                ColumnParams.builder().name("KEY").type(org.apache.ignite.sql.ColumnType.INT64).build(),
                ColumnParams.builder().name("VAL").type(org.apache.ignite.sql.ColumnType.INT64).nullable(true).build()
        );

        createTwoColumnTable(
                ignite,
                ColumnParams.builder().name("KEY").type(org.apache.ignite.sql.ColumnType.FLOAT).build(),
                ColumnParams.builder().name("VAL").type(org.apache.ignite.sql.ColumnType.FLOAT).nullable(true).build()
        );

        createTwoColumnTable(
                ignite,
                ColumnParams.builder().name("KEY").type(org.apache.ignite.sql.ColumnType.DOUBLE).build(),
                ColumnParams.builder().name("VAL").type(org.apache.ignite.sql.ColumnType.DOUBLE).nullable(true).build()
        );

        createTwoColumnTable(
                ignite,
                ColumnParams.builder().name("KEY").type(org.apache.ignite.sql.ColumnType.UUID).build(),
                ColumnParams.builder().name("VAL").type(org.apache.ignite.sql.ColumnType.UUID).nullable(true).build()
        );

        createTwoColumnTable(
                ignite,
                ColumnParams.builder().name("KEY").type(org.apache.ignite.sql.ColumnType.DECIMAL).precision(19).scale(3).build(),
                ColumnParams.builder().name("VAL").type(org.apache.ignite.sql.ColumnType.DECIMAL).precision(19).scale(3).nullable(true)
                        .build()
        );

        createTwoColumnTable(
                ignite,
                ColumnParams.builder().name("KEY").type(org.apache.ignite.sql.ColumnType.STRING).length(0).build(),
                ColumnParams.builder().name("VAL").type(org.apache.ignite.sql.ColumnType.STRING).length(0).nullable(true).build()
        );

        createTwoColumnTable(
                ignite,
                ColumnParams.builder().name("KEY").type(org.apache.ignite.sql.ColumnType.DATE).build(),
                ColumnParams.builder().name("VAL").type(org.apache.ignite.sql.ColumnType.DATE).nullable(true).build()
        );

        createTwoColumnTable(
                ignite,
                ColumnParams.builder().name("KEY").type(org.apache.ignite.sql.ColumnType.DATETIME).precision(6).build(),
                ColumnParams.builder().name("VAL").type(org.apache.ignite.sql.ColumnType.DATETIME).precision(6).nullable(true).build()
        );

        createTwoColumnTable(
                ignite,
                ColumnParams.builder().name("KEY").type(org.apache.ignite.sql.ColumnType.TIME).precision(6).build(),
                ColumnParams.builder().name("VAL").type(org.apache.ignite.sql.ColumnType.TIME).precision(6).nullable(true).build()
        );

        createTwoColumnTable(
                ignite,
                ColumnParams.builder().name("KEY").type(org.apache.ignite.sql.ColumnType.TIMESTAMP).precision(6).build(),
                ColumnParams.builder().name("VAL").type(org.apache.ignite.sql.ColumnType.TIMESTAMP).precision(6).nullable(true).build()
        );

        createTwoColumnTable(
                ignite,
                ColumnParams.builder().name("KEY").type(org.apache.ignite.sql.ColumnType.NUMBER).precision(Integer.MAX_VALUE).build(),
                ColumnParams.builder().name("VAL").type(org.apache.ignite.sql.ColumnType.NUMBER).precision(Integer.MAX_VALUE).nullable(true)
                        .build()
        );

        createTwoColumnTable(
                ignite,
                ColumnParams.builder().name("KEY").type(org.apache.ignite.sql.ColumnType.BYTE_ARRAY).length(0).build(),
                ColumnParams.builder().name("VAL").type(org.apache.ignite.sql.ColumnType.BYTE_ARRAY).length(0).nullable(true)
                        .build()
        );

        createTwoColumnTable(
                ignite,
                ColumnParams.builder().name("KEY").type(org.apache.ignite.sql.ColumnType.BITMASK).length(32).build(),
                ColumnParams.builder().name("VAL").type(org.apache.ignite.sql.ColumnType.BITMASK).length(32).nullable(true)
                        .build()
        );
    }

    private static void createTwoColumnTable(Ignite node, ColumnType type) {
        var keyCol = "key";

        TableDefinition schTbl = SchemaBuilders.tableBuilder(DEFAULT_SCHEMA_NAME, "tbl_" + type.typeSpec().name()).columns(
                SchemaBuilders.column(keyCol, type).build(),
                SchemaBuilders.column("val", type).asNullable(true).build()
        ).withPrimaryKey(keyCol).build();

        await(((TableManager) node.tables()).createTableAsync(schTbl.name(), ZONE_NAME, tblCh ->
                SchemaConfigurationConverter.convert(schTbl, tblCh)
        ));
    }

    private static void createTwoColumnTable(IgniteImpl ignite, ColumnParams keyColumnParams, ColumnParams valueColumnParams) {
        assertEquals(keyColumnParams.type(), valueColumnParams.type());

        // TODO: IGNITE-19499 We need to use only the column type used in the catalog
        String tableNamePostfix = keyColumnParams.type() == org.apache.ignite.sql.ColumnType.BYTE_ARRAY
                ? ColumnType.blob().typeSpec().name()
                : keyColumnParams.type().name();

        CreateTableParams createTableParams = CreateTableParams.builder()
                .schemaName(DEFAULT_SCHEMA_NAME)
                .zone(ZONE_NAME)
                .tableName(("tbl_" + tableNamePostfix).toUpperCase())
                .columns(List.of(keyColumnParams, valueColumnParams))
                .primaryKeyColumns(List.of(keyColumnParams.name()))
                .build();

        assertThat(ignite.catalogManager().createTable(createTableParams), willBe(nullValue()));
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
    @SuppressWarnings("unused") // Used by platform tests.
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
    @SuppressWarnings("unused") // Used by platform tests.
    private static class ExceptionJob implements ComputeJob<String> {
        @Override
        public String execute(JobExecutionContext context, Object... args) {
            throw new RuntimeException("Test exception: " + args[0]);
        }
    }

    /**
     * Compute job that computes row colocation hash.
     */
    @SuppressWarnings("unused") // Used by platform tests.
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
                var type = ColumnTypeConverter.fromOrdinalOrThrow(reader.intValue(i * 3));
                var scale = reader.intValue(i * 3 + 1);
                var valIdx = i * 3 + 2;

                String colName = "col" + i;

                switch (type) {
                    case BOOLEAN:
                        columns[i] = new Column(i, colName, NativeTypes.BOOLEAN, false);
                        tuple.set(colName, reader.booleanValue(valIdx));
                        break;

                    case INT8:
                        columns[i] = new Column(i, colName, NativeTypes.INT8, false);
                        tuple.set(colName, reader.byteValue(valIdx));
                        break;

                    case INT16:
                        columns[i] = new Column(i, colName, NativeTypes.INT16, false);
                        tuple.set(colName, reader.shortValue(valIdx));
                        break;

                    case INT32:
                        columns[i] = new Column(i, colName, NativeTypes.INT32, false);
                        tuple.set(colName, reader.intValue(valIdx));
                        break;

                    case INT64:
                        columns[i] = new Column(i, colName, NativeTypes.INT64, false);
                        tuple.set(colName, reader.longValue(valIdx));
                        break;

                    case FLOAT:
                        columns[i] = new Column(i, colName, NativeTypes.FLOAT, false);
                        tuple.set(colName, reader.floatValue(valIdx));
                        break;

                    case DOUBLE:
                        columns[i] = new Column(i, colName, NativeTypes.DOUBLE, false);
                        tuple.set(colName, reader.doubleValue(valIdx));
                        break;

                    case DECIMAL:
                        columns[i] = new Column(i, colName, NativeTypes.decimalOf(100, scale), false);
                        tuple.set(colName, reader.decimalValue(valIdx, scale));
                        break;

                    case STRING:
                        columns[i] = new Column(i, colName, NativeTypes.STRING, false);
                        tuple.set(colName, reader.stringValue(valIdx));
                        break;

                    case UUID:
                        columns[i] = new Column(i, colName, NativeTypes.UUID, false);
                        tuple.set(colName, reader.uuidValue(valIdx));
                        break;

                    case NUMBER:
                        columns[i] = new Column(i, colName, NativeTypes.numberOf(255), false);
                        tuple.set(colName, reader.numberValue(valIdx));
                        break;

                    case BITMASK:
                        columns[i] = new Column(i, colName, NativeTypes.bitmaskOf(32), false);
                        tuple.set(colName, reader.bitmaskValue(valIdx));
                        break;

                    case DATE:
                        columns[i] = new Column(i, colName, NativeTypes.DATE, false);
                        tuple.set(colName, reader.dateValue(valIdx));
                        break;

                    case TIME:
                        columns[i] = new Column(i, colName, NativeTypes.time(timePrecision), false);
                        tuple.set(colName, reader.timeValue(valIdx));
                        break;

                    case DATETIME:
                        columns[i] = new Column(i, colName, NativeTypes.datetime(timePrecision), false);
                        tuple.set(colName, reader.dateTimeValue(valIdx));
                        break;

                    case TIMESTAMP:
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

    /**
     * Compute job that computes row colocation hash according to the current table schema.
     */
    @SuppressWarnings("unused") // Used by platform tests.
    private static class TableRowColocationHashJob implements ComputeJob<Integer> {
        @Override
        public Integer execute(JobExecutionContext context, Object... args) {
            String tableName = (String) args[0];
            int i = (int) args[1];
            Tuple key = Tuple.create().set("id", 1 + i).set("id0", 2L + i).set("id1", "3" + i);

            @SuppressWarnings("resource")
            Table table = context.ignite().tables().table(tableName);
            RecordBinaryViewImpl view = (RecordBinaryViewImpl) table.recordView();
            TupleMarshallerImpl marsh = IgniteTestUtils.getFieldValue(view, "marsh");

            try {
                return marsh.marshal(key).colocationHash();
            } catch (TupleMarshallerException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Compute job that enables or disables client authentication.
     */
    @SuppressWarnings("unused") // Used by platform tests.
    private static class EnableAuthenticationJob implements ComputeJob<Void> {
        @Override
        public Void execute(JobExecutionContext context, Object... args) {
            boolean enable = ((Integer) args[0]) != 0;
            @SuppressWarnings("resource") IgniteImpl ignite = (IgniteImpl) context.ignite();

            CompletableFuture<Void> changeFuture = ignite.clusterConfiguration().change(
                    root -> root.changeRoot(SecurityConfiguration.KEY).changeAuthentication(
                            change -> {
                                change.changeEnabled(enable);
                                change.changeProviders().delete("basic");

                                if (enable) {
                                    change.changeProviders().create("basic", authenticationProviderChange -> {
                                        authenticationProviderChange.convert(BasicAuthenticationProviderChange.class)
                                                .changeUsername("user-1")
                                                .changePassword("password-1");
                                    });
                                }
                            }
                    ));

            assertThat(changeFuture, willCompleteSuccessfully());

            return null;
        }
    }
}
