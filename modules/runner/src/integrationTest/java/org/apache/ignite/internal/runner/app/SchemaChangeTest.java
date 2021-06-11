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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.app.Ignite;
import org.apache.ignite.app.IgnitionManager;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.table.ColumnNotFoundException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.schema.Column;
import org.apache.ignite.schema.ColumnType;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.SchemaTable;
import org.apache.ignite.table.KeyValueBinaryView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Test;

import static org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter.convert;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Ignition interface tests.
 */
//@Disabled("https://issues.apache.org/jira/browse/IGNITE-14581")
class SchemaChangeTest {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(SchemaManager.class);

    /** Nodes bootstrap configuration. */
    private final Map<String, String> nodesBootstrapCfg = new LinkedHashMap<>() {{
        put("node0", "{\n" +
            "  \"node\": {\n" +
            "    \"metastorageNodes\":[ \"node0\" ]\n" +
            "  },\n" +
            "  \"network\": {\n" +
            "    \"port\":3344,\n" +
            "    \"netClusterNodes\":[ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n" +
            "  }\n" +
            "}");

        put("node1", "{\n" +
            "  \"node\": {\n" +
            "    \"metastorageNodes\":[ \"node0\" ]\n" +
            "  },\n" +
            "  \"network\": {\n" +
            "    \"port\":3345,\n" +
            "    \"netClusterNodes\":[ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n" +
            "  }\n" +
            "}");

        put("node2", "{\n" +
            "  \"node\": {\n" +
            "    \"metastorageNodes\":[ \"node0\"]\n" +
            "  },\n" +
            "  \"network\": {\n" +
            "    \"port\":3346,\n" +
            "    \"netClusterNodes\":[ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n" +
            "  }\n" +
            "}");
    }};

    /**
     * Check dynamic table creation.
     */
    @Test
    void addNewColumn() {
        List<Ignite> clusterNodes = new ArrayList<>();

        for (Map.Entry<String, String> nodeBootstrapCfg : nodesBootstrapCfg.entrySet())
            clusterNodes.add(IgnitionManager.start(nodeBootstrapCfg.getKey(), nodeBootstrapCfg.getValue()));

        assertEquals(3, clusterNodes.size());

        // Create table on node 0.
        SchemaTable schTbl1 = SchemaBuilders.tableBuilder("PUBLIC", "tbl1").columns(
            SchemaBuilders.column("key", ColumnType.INT64).asNonNull().build(),
            SchemaBuilders.column("val1", ColumnType.INT32).asNullable().build()
        ).withPrimaryKey("key").build();

        clusterNodes.get(0).tables().createTable(schTbl1.canonicalName(), tblCh ->
            convert(schTbl1, tblCh)
                .changeReplicas(1)
                .changePartitions(10)
        );

        Table tbl1 = clusterNodes.get(1).tables().table(schTbl1.canonicalName());
        Table tbl2 = clusterNodes.get(2).tables().table(schTbl1.canonicalName());

        // Put data on node 1.
        KeyValueBinaryView kvView1 = tbl1.kvView();

        tbl1.insert(tbl1.tupleBuilder().set("key", 1L).set("val1", 111).build());
        kvView1.put(tbl1.tupleBuilder().set("key", 2L).build(), tbl1.tupleBuilder().set("val1", 222).build());

        assertThrows(ColumnNotFoundException.class, () -> kvView1.put(tbl1.tupleBuilder().set("key", 2L).build(), tbl1.tupleBuilder().set("val1", 222).set("val2", "str").build()));

        clusterNodes.get(1).tables().alterTable(schTbl1.canonicalName(),
            chng -> chng.changeColumns(cols -> {
                final int colIdx = chng.columns().size();
                final Column col = SchemaBuilders.column("val2", ColumnType.string())
                    .asNullable()
                    .withDefaultValue("default")
                    .build();

                //TODO: avoid 'colIdx' or replace with correct last colIdx.
                cols.create(String.valueOf(colIdx), colChg -> convert(col, colChg));
            }));

        kvView1.put(tbl1.tupleBuilder().set("key", 3L).build(), tbl1.tupleBuilder().set("val1", 333).set("val2", "str").build());

        // Get data on node 2.
        KeyValueBinaryView kvView2 = tbl2.kvView();

        final Tuple keyTuple3 = tbl2.tupleBuilder().set("key", 3L).build();

        assertEquals(3, (Long)tbl2.get(keyTuple3).value("key"));
        assertEquals(333, (Integer)tbl2.get(keyTuple3).value("val1"));
        assertEquals(333, (Integer)kvView2.get(keyTuple3).value("val1"));
        assertEquals("str", tbl2.get(keyTuple3).value("val2"));
        assertEquals("str", kvView2.get(keyTuple3).value("val2"));

        // Check old row conversion.
        final Tuple keyTuple1 = tbl2.tupleBuilder().set("key", 1L).build();
        final Tuple keyTuple2 = kvView2.tupleBuilder().set("key", 2L).build();

        assertEquals(1, (Long)tbl2.get(keyTuple1).value("key"));
        assertEquals(2, (Long)tbl2.get(keyTuple2).value("key"));

        assertEquals(111, (Integer)tbl2.get(keyTuple1).value("val1"));
        assertEquals(111, (Integer)kvView2.get(keyTuple1).value("val1"));

        //TODO: https://issues.apache.org/jira/browse/IGNITE-14896 Add evolution converter for default values.
//        assertEquals("default", tbl2.get(keyTuple1).value("val2"));
//        assertEquals("default", kvView2.get(keyTuple1).value("val2"));

        assertEquals(222, (Integer)tbl2.get(keyTuple2).value("val1"));
        assertEquals(222, (Integer)kvView2.get(keyTuple2).value("val1"));

        //TODO: https://issues.apache.org/jira/browse/IGNITE-14896 Add evolution converter for default values.
//        assertEquals("default", tbl2.get(keyTuple2).value("val2"));
//        assertEquals("default", kvView2.get(keyTuple2).value("val2"));
    }
}
