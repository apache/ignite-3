/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.TableDefinition;
import org.apache.ignite.table.Table;

/**
 * Helper class for non-Java platform tests (.NET, C++, Python, ...). Starts nodes, populates tables and data for tests.
 */
public class PlatformTestNodeRunner {
    /** Test node name. */
    private static final String NODE_NAME = PlatformTestNodeRunner.class.getCanonicalName();

    /** Test node name 2. */
    private static final String NODE_NAME2 = PlatformTestNodeRunner.class.getCanonicalName() + "_2";

    private static final String SCHEMA_NAME = "PUB";

    private static final String TABLE_NAME = "tbl1";

    /** Time to keep the node alive. */
    private static final int RUN_TIME_MINUTES = 30;

    /** Nodes bootstrap configuration. */
    private static final Map<String, String> nodesBootstrapCfg = Map.of(
            NODE_NAME, "{\n"
                    + "  \"clientConnector\":{\"port\": 10942,\"portRange\":10,\"idleTimeout\":1000},"
                    + "  \"network\": {\n"
                    + "    \"port\":3344,\n"
                    + "    \"nodeFinder\": {\n"
                    + "      \"netClusterNodes\":[ \"localhost:3344\", \"localhost:3345\" ]\n"
                    + "    }\n"
                    + "  }\n"
                    + "}",

            NODE_NAME2, "{\n"
                    + "  \"clientConnector\":{\"port\": 10942,\"portRange\":10,\"idleTimeout\":1000},"
                    + "  \"network\": {\n"
                    + "    \"port\":3345,\n"
                    + "    \"nodeFinder\": {\n"
                    + "      \"netClusterNodes\":[ \"localhost:3344\", \"localhost:3345\" ]\n"
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

        IgniteUtils.deleteIfExists(BASE_PATH);
        Files.createDirectories(BASE_PATH);

        List<CompletableFuture<Ignite>> igniteFutures = nodesBootstrapCfg.entrySet().stream()
                .map(e -> {
                    String nodeName = e.getKey();
                    String config = e.getValue();

                    return IgnitionManager.start(nodeName, config, BASE_PATH.resolve(nodeName));
                })
                .collect(toList());

        String metaStorageNodeName = nodesBootstrapCfg.keySet().iterator().next();

        IgnitionManager.init(metaStorageNodeName, List.of(metaStorageNodeName));

        System.out.println("Initialization complete");

        List<Ignite> startedNodes = igniteFutures.stream().map(CompletableFuture::join).collect(toList());

        System.out.println("Ignite nodes started");

        var keyCol = "key";
        var valCol = "val";

        TableDefinition schTbl = SchemaBuilders.tableBuilder(SCHEMA_NAME, TABLE_NAME).columns(
                SchemaBuilders.column(keyCol, ColumnType.INT64).build(),
                SchemaBuilders.column(valCol, ColumnType.string()).asNullable(true).build()
        ).withPrimaryKey(keyCol).build();

        startedNodes.get(0).tables().createTable(schTbl.canonicalName(), tblCh ->
                SchemaConfigurationConverter.convert(schTbl, tblCh)
                        .changeReplicas(1)
                        .changePartitions(10)
        );

        String ports = startedNodes.stream()
                .map(n -> String.valueOf(getPort((IgniteImpl) n)))
                .collect(Collectors.joining(","));

        System.out.println("THIN_CLIENT_PORTS=" + ports);

        Thread.sleep(RUN_TIME_MINUTES * 60_000);

        System.out.println("Exiting after " + RUN_TIME_MINUTES + " minutes.");
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
     * Compute job that creates a table.
     */
    @SuppressWarnings({"unused"}) // Used by platform tests.
    private static class CreateTableJob implements ComputeJob<String> {
        @Override
        public String execute(JobExecutionContext context, Object... args) {
            String tableName = (String) args[0];

            Table table = context.ignite().tables().createTable(
                    tableName,
                    tblChanger -> tblChanger
                            .changeColumns(cols ->
                                    cols.create("key", col -> col.changeType(t -> t.changeType("INT64")).changeNullable(false)))
                            .changePrimaryKey(pk -> pk.changeColumns("key").changeColocationColumns("key"))
            );

            return table.name();
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
            context.ignite().tables().dropTable(tableName);

            return tableName;
        }
    }
}
