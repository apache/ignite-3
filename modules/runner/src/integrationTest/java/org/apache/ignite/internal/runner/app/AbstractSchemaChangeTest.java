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
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter.convert;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.configuration.schemas.table.ColumnChange;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteObjectName;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnDefinition;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.TableDefinition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;

/**
 * Ignition interface tests.
 */
@ExtendWith(WorkDirectoryExtension.class)
abstract class AbstractSchemaChangeTest {
    /** Table name. */
    public static final String TABLE = "PUBLIC.tbl1";

    /** Network ports of the test nodes. */
    private static final int[] PORTS = {3344, 3345, 3346};

    /** Nodes bootstrap configuration. */
    private final Map<String, String> nodesBootstrapCfg = new LinkedHashMap<>();

    /** Work directory. */
    @WorkDirectory
    private Path workDir;

    /**
     * Before each.
     */
    @BeforeEach
    void setUp(TestInfo testInfo) {
        String node0Name = testNodeName(testInfo, PORTS[0]);
        String node1Name = testNodeName(testInfo, PORTS[1]);
        String node2Name = testNodeName(testInfo, PORTS[2]);

        nodesBootstrapCfg.put(
                node0Name,
                "{\n"
                        + "  network: {\n"
                        + "    port: " + PORTS[0] + ",\n"
                        + "    nodeFinder: {\n"
                        + "      netClusterNodes: [ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n"
                        + "    }\n"
                        + "  }\n"
                        + "}"
        );

        nodesBootstrapCfg.put(
                node1Name,
                "{\n"
                        + "  network: {\n"
                        + "    port: " + PORTS[1] + ",\n"
                        + "    nodeFinder: {\n"
                        + "      netClusterNodes: [ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n"
                        + "    }\n"
                        + "  }\n"
                        + "}"
        );

        nodesBootstrapCfg.put(
                node2Name,
                "{\n"
                        + "  network: {\n"
                        + "    port: " + PORTS[2] + ",\n"
                        + "    nodeFinder: {\n"
                        + "      netClusterNodes: [ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n"
                        + "    }\n"
                        + "  }\n"
                        + "}"
        );
    }

    /**
     * After each.
     */
    @AfterEach
    void afterEach() throws Exception {
        List<AutoCloseable> closeables = nodesBootstrapCfg.keySet().stream()
                .map(nodeName -> (AutoCloseable) () -> IgnitionManager.stop(nodeName))
                .collect(toList());

        IgniteUtils.closeAll(closeables);
    }

    /**
     * Check unsupported column type change.
     */
    @Test
    public void testChangeColumnType() throws Exception {
        List<Ignite> grid = startGrid();

        createTable(grid);

        assertColumnChangeFailed(grid, "valStr", c -> c.changeType(t -> t.changeType("UNKNOWN_TYPE")));

        assertColumnChangeFailed(grid, "valInt",
                colChanger -> colChanger.changeType(t -> t.changeType(ColumnType.blobOf().typeSpec().name())));

        assertColumnChangeFailed(grid, "valInt", colChanger -> colChanger.changeType(t -> t.changePrecision(10)));
        assertColumnChangeFailed(grid, "valInt", colChanger -> colChanger.changeType(t -> t.changeScale(10)));
        assertColumnChangeFailed(grid, "valInt", colChanger -> colChanger.changeType(t -> t.changeLength(1)));

        assertColumnChangeFailed(grid, "valBigInt", colChanger -> colChanger.changeType(t -> t.changePrecision(-1)));
        assertColumnChangeFailed(grid, "valBigInt", colChanger -> colChanger.changeType(t -> t.changePrecision(10)));
        assertColumnChangeFailed(grid, "valBigInt", colChanger -> colChanger.changeType(t -> t.changeScale(2)));
        assertColumnChangeFailed(grid, "valBigInt", colChanger -> colChanger.changeType(t -> t.changeLength(10)));

        assertColumnChangeFailed(grid, "valDecimal", colChanger -> colChanger.changeType(c -> c.changePrecision(-1)));
        assertColumnChangeFailed(grid, "valDecimal", colChanger -> colChanger.changeType(c -> c.changePrecision(0)));
        assertColumnChangeFailed(grid, "valDecimal", colChanger -> colChanger.changeType(c -> c.changeScale(-2)));
        assertColumnChangeFailed(grid, "valDecimal", colChanger -> colChanger.changeType(c -> c.changePrecision(10)));
        assertColumnChangeFailed(grid, "valDecimal", colChanger -> colChanger.changeType(c -> c.changeScale(2)));
        assertColumnChangeFailed(grid, "valDecimal", colChanger -> colChanger.changeType(c -> c.changeLength(10)));
    }

    /**
     * Check unsupported nullability change.
     */
    @Test
    public void testChangeColumnsNullability() throws Exception {
        List<Ignite> grid = startGrid();

        createTable(grid);

        assertColumnChangeFailed(grid, "valStr", colChanger -> colChanger.changeNullable(true));
        assertColumnChangeFailed(grid, "valInt", colChanger -> colChanger.changeNullable(false));
    }

    /**
     * Returns grid nodes.
     */
    protected List<Ignite> startGrid() throws Exception {
        List<CompletableFuture<Ignite>> futures = nodesBootstrapCfg.entrySet().stream()
                .map(e -> IgnitionManager.start(e.getKey(), e.getValue(), workDir.resolve(e.getKey())))
                .collect(toList());

        String metaStorageNode = nodesBootstrapCfg.keySet().iterator().next();

        IgnitionManager.init(metaStorageNode, List.of(metaStorageNode), "cluster");

        return futures.stream()
                .map(CompletableFuture::join)
                .collect(toUnmodifiableList());
    }

    /**
     * Creates tables.
     *
     * @param nodes Cluster nodes.
     */
    protected static void createTable(List<Ignite> nodes) {
        // Create table on node 0.
        TableDefinition schTbl1 = SchemaBuilders.tableBuilder("PUBLIC", "tbl1").columns(
                SchemaBuilders.column("key", ColumnType.INT64).build(),
                SchemaBuilders.column("valInt", ColumnType.INT32).asNullable(true).build(),
                SchemaBuilders.column("valBlob", ColumnType.blobOf()).asNullable(true).build(),
                SchemaBuilders.column("valDecimal", ColumnType.decimalOf()).asNullable(true).build(),
                SchemaBuilders.column("valBigInt", ColumnType.numberOf()).asNullable(true).build(),
                SchemaBuilders.column("valStr", ColumnType.string()).withDefaultValueExpression("default").build()
        ).withPrimaryKey("key").build();

        nodes.get(0).tables().createTable(
                schTbl1.canonicalName(),
                tblCh -> convert(schTbl1, tblCh).changeReplicas(1).changePartitions(10)
        );
    }

    /**
     * Adds column.
     *
     * @param nodes Cluster nodes.
     * @param columnToAdd Column to add.
     */
    protected static void addColumn(List<Ignite> nodes, ColumnDefinition columnToAdd) {
        nodes.get(0).tables().alterTable(TABLE,
                chng -> chng.changeColumns(cols -> cols.create(columnToAdd.name(), colChg -> convert(columnToAdd, colChg)))
        );
    }

    /**
     * Drops column.
     *
     * @param nodes Cluster nodes.
     * @param colName Name of column to drop.
     */
    protected static void dropColumn(List<Ignite> nodes, String colName) {
        nodes.get(0).tables()
                .alterTable(TABLE, chng -> chng.changeColumns(cols -> cols.delete(IgniteObjectName.parse(colName))));
    }

    /**
     * Renames column.
     *
     * @param nodes Cluster nodes.
     * @param oldName Old column name.
     * @param newName New column name.
     */
    protected static void renameColumn(List<Ignite> nodes, String oldName, String newName) {
        nodes.get(0).tables().alterTable(TABLE,
                tblChanger -> tblChanger.changeColumns(
                        colListChanger -> colListChanger
                                .rename(IgniteObjectName.parse(oldName), IgniteObjectName.parse(newName))
                )
        );
    }

    /**
     * Changes column default.
     *
     * @param nodes Cluster nodes.
     * @param colName Column name.
     * @param defSup Default value supplier.
     */
    protected static void changeDefault(List<Ignite> nodes, String colName, Supplier<Object> defSup) {
        nodes.get(0).tables().alterTable(TABLE,
                tblChanger -> tblChanger.changeColumns(
                        colListChanger -> colListChanger
                                .update(
                                        IgniteObjectName.parse(colName),
                                        colChanger -> colChanger.changeDefaultValue(defSup.get().toString())
                                )
                )
        );
    }

    /**
     * Ensure configuration validation failed.
     *
     * @param grid Grid.
     * @param colName Column to change.
     * @param colChanger Column configuration changer.
     */
    private static void assertColumnChangeFailed(List<Ignite> grid, String colName, Consumer<ColumnChange> colChanger) {
        assertThrows(IgniteException.class, () ->
                grid.get(0).tables().alterTable(TABLE,
                        tblChanger -> tblChanger.changeColumns(
                                listChanger -> listChanger.update(IgniteObjectName.parse(colName), colChanger)
                        )
                )
        );
    }

    protected static <T extends Throwable> void assertThrowsWithCause(Class<T> expectedType, Executable executable) {
        Throwable ex = assertThrows(IgniteException.class, executable);

        while (ex.getCause() != null) {
            if (expectedType.isInstance(ex.getCause())) {
                return;
            }

            ex = ex.getCause();
        }

        fail("Expected cause wasn't found.");
    }
}
