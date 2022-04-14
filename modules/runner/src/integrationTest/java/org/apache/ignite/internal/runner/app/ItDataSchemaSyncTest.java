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
import static org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter.convert;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.test.WatchListenerInhibitor;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnDefinition;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.TableDefinition;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * There is a test of table schema synchronization.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class ItDataSchemaSyncTest extends IgniteAbstractTest {
    /**
     * Schema name.
     */
    public static final String SCHEMA = "PUBLIC";

    /**
     * Short table name.
     */
    public static final String SHORT_TABLE_NAME = "tbl1";

    /**
     * Table name.
     */
    public static final String TABLE_NAME = SCHEMA + "." + SHORT_TABLE_NAME;

    /**
     * Nodes bootstrap configuration.
     */
    private static final Map<String, String> nodesBootstrapCfg = Map.of(
            "node0", "{\n"
                    + "  \"network\": {\n"
                    + "    \"port\":3344,\n"
                    + "    \"nodeFinder\": {\n"
                    + "      \"netClusterNodes\":[ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n"
                    + "    }\n"
                    + "  }\n"
                    + "}",

            "node1", "{\n"
                    + "  \"network\": {\n"
                    + "    \"port\":3345,\n"
                    + "    \"nodeFinder\": {\n"
                    + "      \"netClusterNodes\":[ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n"
                    + "    }\n"
                    + "  }\n"
                    + "}",

            "node2", "{\n"
                    + "  \"network\": {\n"
                    + "    \"port\":3346,\n"
                    + "    \"nodeFinder\": {\n"
                    + "      \"netClusterNodes\":[ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n"
                    + "    }\n"
                    + "  }\n"
                    + "}"
    );

    /**
     * Cluster nodes.
     */
    private final List<Ignite> clusterNodes = new ArrayList<>();

    /**
     * Starts a cluster before every test started.
     */
    @BeforeEach
    void beforeEach() throws Exception {
        List<CompletableFuture<Ignite>> futures = nodesBootstrapCfg.entrySet().stream()
                .map(e -> IgnitionManager.start(e.getKey(), e.getValue(), workDir.resolve(e.getKey())))
                .collect(toList());

        String metaStorageNode = nodesBootstrapCfg.keySet().iterator().next();

        IgnitionManager.init(metaStorageNode, List.of(metaStorageNode));

        for (CompletableFuture<Ignite> future : futures) {
            assertThat(future, willCompleteSuccessfully());

            clusterNodes.add(future.join());
        }
    }

    /**
     * Stops a cluster after every test finished.
     */
    @AfterEach
    void afterEach() throws Exception {
        List<AutoCloseable> closeables = nodesBootstrapCfg.keySet().stream()
                .map(name -> (AutoCloseable) () -> IgnitionManager.stop(name))
                .collect(toList());

        IgniteUtils.closeAll(closeables);
    }

    /**
     * The test executes various operation over the lagging node. The operations can be executed only the node overtakes a distributed
     * cluster state.
     */
    @Test
    public void test() throws Exception {
        Ignite ignite0 = clusterNodes.get(0);
        IgniteImpl ignite1 = (IgniteImpl) clusterNodes.get(1);

        createTable(ignite0, SCHEMA, SHORT_TABLE_NAME);

        TableImpl table = (TableImpl) ignite0.tables().table(TABLE_NAME);

        assertEquals(1, table.schemaView().schema().version());

        for (int i = 0; i < 10; i++) {
            table.recordView().insert(
                    null,
                    Tuple.create()
                            .set("key", (long) i)
                            .set("valInt", i)
                            .set("valStr", "str_" + i)
            );
        }

        WatchListenerInhibitor listenerInhibitor = WatchListenerInhibitor.metastorageEventsInhibitor(ignite1);

        listenerInhibitor.startInhibit();

        ColumnDefinition columnDefinition = SchemaBuilders.column("valStr2", ColumnType.string())
                .withDefaultValueExpression("default")
                .build();

        ignite0.tables().alterTable(TABLE_NAME,
                tblChanger -> tblChanger.changeColumns(cols ->
                        cols.create(columnDefinition.name(), colChg -> convert(columnDefinition, colChg))
                )
        );

        for (Ignite node : clusterNodes) {
            if (node == ignite1) {
                continue;
            }

            TableImpl tableOnNode = (TableImpl) node.tables().table(TABLE_NAME);

            IgniteTestUtils.waitForCondition(() -> tableOnNode.schemaView().lastSchemaVersion() == 2, 10_000);
        }

        TableImpl table1 = (TableImpl) ignite1.tables().table(TABLE_NAME);

        for (int i = 10; i < 20; i++) {
            table.recordView().insert(
                    null,
                    Tuple.create()
                            .set("key", (long) i)
                            .set("valInt", i)
                            .set("valStr", "str_" + i)
                            .set("valStr2", "str2_" + i)
            );
        }

        CompletableFuture<?> insertFut = IgniteTestUtils.runAsync(() ->
                table1.recordView().insert(
                        null,
                        Tuple.create()
                                .set("key", 0L)
                                .set("valInt", 0)
                                .set("valStr", "str_" + 0)
                                .set("valStr2", "str2_" + 0)
                ));

        CompletableFuture<?> getFut = IgniteTestUtils.runAsync(() -> {
            table1.recordView().get(null, Tuple.create().set("key", 10L));
        });

        CompletableFuture<?> checkDefaultFut = IgniteTestUtils.runAsync(() -> {
            assertEquals("default",
                    table1.recordView().get(null, Tuple.create().set("key", 0L))
                            .value("valStr2"));
        });

        assertEquals(1, table1.schemaView().lastSchemaVersion());

        assertFalse(getFut.isDone());
        assertFalse(insertFut.isDone());
        assertFalse(checkDefaultFut.isDone());

        listenerInhibitor.stopInhibit();

        getFut.get(10, TimeUnit.SECONDS);
        insertFut.get(10, TimeUnit.SECONDS);
        checkDefaultFut.get(10, TimeUnit.SECONDS);

        for (Ignite node : clusterNodes) {
            Table tableOnNode = node.tables().table(TABLE_NAME);

            for (int i = 0; i < 20; i++) {
                Tuple row = tableOnNode.recordView().get(null, Tuple.create().set("key", (long) i));

                assertNotNull(row);

                assertEquals(i, row.intValue("valInt"));
                assertEquals("str_" + i, row.value("valStr"));
                assertEquals(i < 10 ? "default" : ("str2_" + i), row.value("valStr2"));
            }
        }
    }

    /**
     * Creates a table with the passed name on the specific schema.
     *
     * @param node Cluster node.
     * @param schemaName Schema name.
     * @param shortTableName Table name.
     */
    protected void createTable(Ignite node, String schemaName, String shortTableName) {
        // Create table on node 0.
        TableDefinition schTbl1 = SchemaBuilders.tableBuilder(schemaName, shortTableName).columns(
                SchemaBuilders.column("key", ColumnType.INT64).build(),
                SchemaBuilders.column("valInt", ColumnType.INT32).asNullable(true).build(),
                SchemaBuilders.column("valStr", ColumnType.string()).withDefaultValueExpression("default").build()
        ).withPrimaryKey("key").build();

        node.tables().createTable(
                schTbl1.canonicalName(),
                tblCh -> convert(schTbl1, tblCh).changeReplicas(2).changePartitions(10)
        );
    }
}
