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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.configuration.schemas.table.ColumnChange;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.internal.ItUtils;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.TableDefinition;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Ignition interface tests.
 */
@ExtendWith(WorkDirectoryExtension.class)
class ItDynamicTableCreationTest {
    /** Network ports of the test nodes. */
    private static final int[] PORTS = {3344, 3345, 3346};

    /** Nodes bootstrap configuration. */
    private final Map<String, String> nodesBootstrapCfg = new LinkedHashMap<>();

    private final List<Ignite> clusterNodes = new ArrayList<>();

    @WorkDirectory
    private Path workDir;

    /**
     * Before each.
     */
    @BeforeEach
    void setUp(TestInfo testInfo) {
        for (int i = 0; i < 3; ++i) {
            String nodeName = testNodeName(testInfo, PORTS[i]);

            nodesBootstrapCfg.put(
                    nodeName,
                    "{\n"
                    + "  network: {\n"
                    + "    port: " + PORTS[i] + ",\n"
                    + "    nodeFinder:{\n"
                    + "      netClusterNodes: [ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ] \n"
                    + "    }\n"
                    + "  }\n"
                    + "}"
            );
        }
    }

    /**
     * After each.
     */
    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.closeAll(ItUtils.reverse(clusterNodes));
    }

    /**
     * Returns grid nodes.
     */
    protected List<Ignite> startGrid() throws Exception {
        nodesBootstrapCfg.forEach((nodeName, configStr) ->
                clusterNodes.add(IgnitionManager.start(nodeName, configStr, workDir.resolve(nodeName)))
        );

        assertEquals(3, clusterNodes.size());

        IgniteImpl metastorageNode = (IgniteImpl) clusterNodes.get(0);

        metastorageNode.init(List.of(metastorageNode.name()));

        return clusterNodes;
    }

    /**
     * Check dynamic table creation.
     */
    @Test
    void testDynamicSimpleTableCreation() throws Exception {
        startGrid();

        // Create table on node 0.
        TableDefinition schTbl1 = SchemaBuilders.tableBuilder("PUBLIC", "tbl1").columns(
                SchemaBuilders.column("key", ColumnType.INT64).build(),
                SchemaBuilders.column("val", ColumnType.INT32).asNullable(true).build()
        ).withPrimaryKey("key").build();

        clusterNodes.get(0).tables().createTable(schTbl1.canonicalName(), tblCh ->
                SchemaConfigurationConverter.convert(schTbl1, tblCh)
                        .changeReplicas(1)
                        .changePartitions(10)
        );

        // Put data on node 1.
        Table tbl1 = clusterNodes.get(1).tables().table(schTbl1.canonicalName());
        RecordView<Tuple> recView1 = tbl1.recordView();
        KeyValueView<Tuple, Tuple> kvView1 = tbl1.keyValueView();

        recView1.insert(null, Tuple.create().set("key", 1L).set("val", 111));
        kvView1.put(null, Tuple.create().set("key", 2L), Tuple.create().set("val", 222));

        // Get data on node 2.
        Table tbl2 = clusterNodes.get(2).tables().table(schTbl1.canonicalName());
        RecordView<Tuple> recView2 = tbl2.recordView();
        KeyValueView<Tuple, Tuple> kvView2 = tbl2.keyValueView();

        Tuple keyTuple1 = Tuple.create().set("key", 1L);
        Tuple keyTuple2 = Tuple.create().set("key", 2L);

        assertThrows(IllegalArgumentException.class, () -> kvView2.get(null, keyTuple1).value("key"));
        assertThrows(IllegalArgumentException.class, () -> kvView2.get(null, keyTuple2).value("key"));
        assertEquals(1, (Long) recView2.get(null, keyTuple1).value("key"));
        assertEquals(2, (Long) recView2.get(null, keyTuple2).value("key"));

        assertEquals(111, (Integer) recView2.get(null, keyTuple1).value("val"));
        assertEquals(111, (Integer) kvView2.get(null, keyTuple1).value("val"));
        assertEquals(222, (Integer) recView2.get(null, keyTuple2).value("val"));
        assertEquals(222, (Integer) kvView2.get(null, keyTuple2).value("val"));

        assertThrows(IllegalArgumentException.class, () -> kvView1.get(null, keyTuple1).value("key"));
        assertThrows(IllegalArgumentException.class, () -> kvView1.get(null, keyTuple2).value("key"));
    }

    /**
     * Check dynamic table creation.
     */
    @Test
    void testDynamicTableCreation() throws Exception {
        startGrid();

        // Create table on node 0.
        TableDefinition scmTbl1 = SchemaBuilders.tableBuilder("PUBLIC", "tbl1").columns(
                SchemaBuilders.column("key", ColumnType.UUID).build(),
                SchemaBuilders.column("affKey", ColumnType.INT64).build(),
                SchemaBuilders.column("valStr", ColumnType.string()).asNullable(true).build(),
                SchemaBuilders.column("valInt", ColumnType.INT32).asNullable(true).build(),
                SchemaBuilders.column("valNull", ColumnType.INT16).asNullable(true).build()
        ).withPrimaryKey(
                SchemaBuilders.primaryKey()
                        .withColumns("key", "affKey")
                        .withColocationColumns("affKey")
                        .build()
        ).build();

        clusterNodes.get(0).tables().createTable(scmTbl1.canonicalName(), tblCh ->
                SchemaConfigurationConverter.convert(scmTbl1, tblCh)
                        .changeReplicas(1)
                        .changePartitions(10));

        UUID uuid = UUID.randomUUID();
        UUID uuid2 = UUID.randomUUID();

        // Put data on node 1.
        Table tbl1 = clusterNodes.get(1).tables().table(scmTbl1.canonicalName());
        RecordView<Tuple> recView1 = tbl1.recordView();
        KeyValueView<Tuple, Tuple> kvView1 = tbl1.keyValueView();

        recView1.insert(null, Tuple.create().set("key", uuid).set("affKey", 42L)
                .set("valStr", "String value").set("valInt", 73).set("valNull", null));

        kvView1.put(null, Tuple.create().set("key", uuid2).set("affKey", 4242L),
                Tuple.create().set("valStr", "String value 2").set("valInt", 7373).set("valNull", null));

        // Get data on node 2.
        Table tbl2 = clusterNodes.get(2).tables().table(scmTbl1.canonicalName());
        RecordView<Tuple> recView2 = tbl2.recordView();
        KeyValueView<Tuple, Tuple> kvView2 = tbl2.keyValueView();

        Tuple keyTuple1 = Tuple.create().set("key", uuid).set("affKey", 42L);
        Tuple keyTuple2 = Tuple.create().set("key", uuid2).set("affKey", 4242L);

        // KV view must NOT return key columns in value.
        assertThrows(IllegalArgumentException.class, () -> kvView2.get(null, keyTuple1).value("key"));
        assertThrows(IllegalArgumentException.class, () -> kvView2.get(null, keyTuple1).value("affKey"));
        assertThrows(IllegalArgumentException.class, () -> kvView2.get(null, keyTuple2).value("key"));
        assertThrows(IllegalArgumentException.class, () -> kvView2.get(null, keyTuple2).value("affKey"));

        // Record binary view MUST return key columns in value.
        assertEquals(uuid, recView2.get(null, keyTuple1).value("key"));
        assertEquals(42L, (Long) recView2.get(null, keyTuple1).value("affKey"));
        assertEquals(uuid2, recView2.get(null, keyTuple2).value("key"));
        assertEquals(4242L, (Long) recView2.get(null, keyTuple2).value("affKey"));

        assertEquals("String value", recView2.get(null, keyTuple1).value("valStr"));
        assertEquals(73, (Integer) recView2.get(null, keyTuple1).value("valInt"));
        assertNull(recView2.get(null, keyTuple1).value("valNull"));

        assertEquals("String value 2", recView2.get(null, keyTuple2).value("valStr"));
        assertEquals(7373, (Integer) recView2.get(null, keyTuple2).value("valInt"));
        assertNull(recView2.get(null, keyTuple2).value("valNull"));

        assertEquals("String value", kvView2.get(null, keyTuple1).value("valStr"));
        assertEquals(73, (Integer) kvView2.get(null, keyTuple1).value("valInt"));
        assertNull(kvView2.get(null, keyTuple1).value("valNull"));

        assertEquals("String value 2", kvView2.get(null, keyTuple2).value("valStr"));
        assertEquals(7373, (Integer) kvView2.get(null, keyTuple2).value("valInt"));
        assertNull(kvView2.get(null, keyTuple2).value("valNull"));
    }

    /**
     * Check unsupported column type change.
     */
    @Test
    public void testChangeColumnType() throws Exception {
        List<Ignite> grid = startGrid();

        assertTableCreationFailed(grid, c -> c.changeType(t -> t.changeType("UNKNOWN_TYPE")));

        assertTableCreationFailed(grid, colChanger -> colChanger.changeType(t -> t.changeType("STRING").changeLength(-1)));
        assertTableCreationFailed(grid, colChanger -> colChanger.changeType(t -> t.changeType("BYTES").changeLength(-1)));

        assertTableCreationFailed(grid, colChanger -> colChanger.changeType(t -> t.changeType("INT32").changePrecision(-1)));
        assertTableCreationFailed(grid, colChanger -> colChanger.changeType(t -> t.changeType("INT32").changeScale(-1)));
        assertTableCreationFailed(grid, colChanger -> colChanger.changeType(t -> t.changeType("BYTES").changeLength(-1)));

        assertTableCreationFailed(grid, colChanger -> colChanger.changeType(t -> t.changeType("NUMBER").changePrecision(-1)));
        assertTableCreationFailed(grid, colChanger -> colChanger.changeType(t -> t.changeType("NUMBER").changeScale(-2)));
        assertTableCreationFailed(grid, colChanger -> colChanger.changeType(t -> t.changeType("BYTES").changeLength(-1)));

        assertTableCreationFailed(grid, colChanger -> colChanger.changeType(c -> c.changeType("DECIMAL").changePrecision(-1)));
        assertTableCreationFailed(grid, colChanger -> colChanger.changeType(c -> c.changeType("DECIMAL").changePrecision(0)));
        assertTableCreationFailed(grid, colChanger -> colChanger.changeType(c -> c.changeType("DECIMAL").changeScale(-2)));
        assertTableCreationFailed(grid, colChanger -> colChanger.changeType(t -> t.changeType("BYTES").changeLength(-1)));
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-15747")
    @Test
    void testMissedPk() throws Exception {
        List<Ignite> grid = startGrid();

        // Missed PK.
        assertThrows(ConfigurationValidationException.class, () -> {
            try {
                grid.get(0).tables().createTable(
                        "PUBLIC.tbl1",
                        tblChanger -> tblChanger.changeName("PUBLIC.tbl1")
                                .changeColumns(cols -> {
                                    cols.create("0",
                                            col -> col.changeName("key").changeType(t -> t.changeType("INT64")).changeNullable(false));
                                    cols.create("1",
                                            col -> col.changeName("val").changeNullable(true).changeType(t -> t.changeType("INT32")));
                                })
                                .changeReplicas(1)
                                .changePartitions(10)
                );
            } catch (CompletionException ex) {
                throw ex.getCause();
            }
        });

        //Missed affinity cols.
        assertThrows(ConfigurationValidationException.class, () -> {
            try {
                grid.get(0).tables().createTable(
                        "PUBLIC.tbl1",
                        tblChanger -> tblChanger.changeName("PUBLIC.tbl1")
                                .changeColumns(cols -> {
                                    cols.create("0",
                                            col -> col.changeName("key").changeType(t -> t.changeType("INT64")).changeNullable(false));
                                    cols.create("1",
                                            col -> col.changeName("val").changeNullable(true).changeType(t -> t.changeType("INT32")));
                                })
                                .changePrimaryKey(pk -> pk.changeColumns("key"))
                                .changeReplicas(1)
                                .changePartitions(10)
                );
            } catch (CompletionException ex) {
                throw ex.getCause();
            }
        });

        //Missed key cols.
        assertThrows(ConfigurationValidationException.class, () -> {
            try {
                grid.get(0).tables().createTable(
                        "PUBLIC.tbl1",
                        tblChanger -> tblChanger.changeName("PUBLIC.tbl1")
                                .changeColumns(cols -> {
                                    cols.create("0",
                                            col -> col.changeName("key").changeType(t -> t.changeType("INT64")).changeNullable(false));
                                    cols.create("1",
                                            col -> col.changeName("val").changeNullable(true).changeType(t -> t.changeType("INT32")));
                                })
                                .changePrimaryKey(pk -> pk.changeColocationColumns("key"))
                                .changeReplicas(1)
                                .changePartitions(10)
                );
            } catch (CompletionException ex) {
                throw ex.getCause();
            }
        });
    }

    /**
     * Ensure configuration validation failed.
     *
     * @param grid       Grid.
     * @param colChanger Column configuration changer.
     */
    private void assertTableCreationFailed(List<Ignite> grid, Consumer<ColumnChange> colChanger) {
        assertThrows(IgniteException.class, () -> {
            try {
                grid.get(0).tables().createTable(
                        "PUBLIC.tbl1",
                        tblChanger -> tblChanger.changeName("PUBLIC.tbl1")
                                .changeColumns(cols -> {
                                    cols.create("0",
                                            col -> col.changeName("key").changeType(t -> t.changeType("INT64")).changeNullable(false));

                                    cols.create("1", col -> colChanger.accept(col.changeName("val").changeNullable(true)));
                                })
                                .changePrimaryKey(pk -> pk.changeColumns("key").changeColocationColumns("key"))
                                .changeReplicas(1)
                                .changePartitions(10)
                );
            } catch (CompletionException ex) {
                throw ex.getCause();
            }
        });
    }
}
