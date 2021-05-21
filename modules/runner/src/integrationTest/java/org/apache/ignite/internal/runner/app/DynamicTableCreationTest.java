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
import java.util.List;
import java.util.UUID;
import org.apache.ignite.app.Ignite;
import org.apache.ignite.app.IgnitionManager;
import org.apache.ignite.table.KeyValueBinaryView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Ignition interface tests.
 */
@Disabled("https://issues.apache.org/jira/browse/IGNITE-14389")
class DynamicTableCreationTest {
    /** Nodes bootstrap configuration. */
    private final String[] nodesBootstrapCfg =
        {
            "{\n" +
                "  \"node\": {\n" +
                "    \"name\":node0,\n" +
                "    \"metastorageNodes\":[ \"node0\", \"node1\" ]\n" +
                "  },\n" +
                "  \"network\": {\n" +
                "    \"port\":3344,\n" +
                "    \"netClusterNodes\":[ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n" +
                "  }\n" +
                "}",

            "{\n" +
                "  \"node\": {\n" +
                "    \"name\":node1,\n" +
                "    \"metastorageNodes\":[ \"node0\", \"node1\" ]\n" +
                "  },\n" +
                "  \"network\": {\n" +
                "    \"port\":3345,\n" +
                "    \"netClusterNodes\":[ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n" +
                "  }\n" +
                "}",

            "{\n" +
                "  \"node\": {\n" +
                "    \"name\":node2,\n" +
                "    \"metastorageNodes\":[ \"node0\", \"node1\" ]\n" +
                "  },\n" +
                "  \"network\": {\n" +
                "    \"port\":3346,\n" +
                "    \"netClusterNodes\":[ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n" +
                "  }\n" +
                "}",
        };

    /**
     * Check dynamic table creation.
     */
    @Test
    void testDynamicSimpleTableCreation() {
        List<Ignite> clusterNodes = new ArrayList<>();

        for (String nodeBootstrapCfg : nodesBootstrapCfg)
            clusterNodes.add(IgnitionManager.start(nodeBootstrapCfg));

        assertEquals(3, clusterNodes.size());

        // Create table on node 0.
        clusterNodes.get(0).tables().createTable("tbl1", tbl -> tbl
            .changeName("tbl1")
            .changeReplicas(1)
            .changePartitions(10)
            .changeColumns(cols -> cols
                .create("key", c -> c.changeName("key").changeNullable(false).changeType(t -> t.changeType("INT64")))
                .create("val", c -> c.changeName("val").changeNullable(true).changeType(t -> t.changeType("INT64")))
            )
            .changeIndices(idxs -> idxs
                .create("PK", idx -> idx
                    .changeName("PK")
                    .changeType("PRIMARY")
                    .changeColNames(new String[] {"key"})
                    .changeColumns(c -> c
                        .create("key", t -> t.changeName("key")))
                    .changeAffinityColumns(new String[] {"key"}))
            ));

        // Put data on node 1.
        Table tbl1 = clusterNodes.get(1).tables().table("tbl1");
        KeyValueBinaryView kvView1 = tbl1.kvView();

        tbl1.insert(tbl1.tupleBuilder().set("key", 1L).set("val", 111).build());
        kvView1.put(tbl1.tupleBuilder().set("key", 2L).build(), tbl1.tupleBuilder().set("val", 222).build());

        // Get data on node 2.
        Table tbl2 = clusterNodes.get(2).tables().table("tbl1");
        KeyValueBinaryView kvView2 = tbl2.kvView();

        final Tuple keyTuple1 = tbl2.tupleBuilder().set("key", 1L).build();
        final Tuple keyTuple2 = kvView2.tupleBuilder().set("key", 2L).build();

        assertEquals(111, (Integer)kvView2.get(keyTuple1).value("key"));
        assertEquals(222, (Integer)kvView2.get(keyTuple2).value("key"));

        assertEquals(111, (Integer)tbl2.get(keyTuple1).value("val"));
        assertEquals(111, (Integer)kvView2.get(keyTuple1).value("val"));
        assertEquals(222, (Integer)tbl2.get(keyTuple2).value("val"));
        assertEquals(222, (Integer)kvView2.get(keyTuple2).value("val"));
    }

    /**
     * Check dynamic table creation.
     */
    @Test
    void testDynamicTableCreation() {
        List<Ignite> clusterNodes = new ArrayList<>();

        for (String nodeBootstrapCfg : nodesBootstrapCfg)
            clusterNodes.add(IgnitionManager.start(nodeBootstrapCfg));

        assertEquals(3, clusterNodes.size());

        // Create table on node 0.
        clusterNodes.get(0).tables().createTable("tbl1", tbl -> tbl
            .changeName("tbl1")
            .changeReplicas(1)
            .changePartitions(10)
            .changeColumns(cols -> cols
                .create("key", c -> c.changeName("key").changeNullable(false).changeType(t -> t.changeType("UUID")))
                .create("affKey", c -> c.changeName("affKey").changeNullable(false).changeType(t -> t.changeType("INT64")))
                .create("valStr", c -> c.changeName("valStr").changeNullable(true).changeType(t -> t.changeType("STRING")))
                .create("valInt", c -> c.changeName("valInt").changeNullable(true).changeType(t -> t.changeType("INT32")))
                .create("valNull", c -> c.changeName("valNull").changeNullable(true).changeType(t -> t.changeType("INT16")).changeNullable(true))
            )
            .changeIndices(idxs -> idxs
                .create("PK", idx -> idx
                    .changeName("PK")
                    .changeType("PRIMARY")
                    .changeColNames(new String[] {"key", "affKey"})
                    .changeColumns(c -> c
                        .create("key", t -> t.changeName("key").changeAsc(true))
                        .create("affKey", t -> t.changeName("affKey").changeAsc(false)))
                    .changeAffinityColumns(new String[] {"affKey"}))
            ));

        final UUID uuid = UUID.randomUUID();
        final UUID uuid2 = UUID.randomUUID();

        // Put data on node 1.
        Table tbl1 = clusterNodes.get(1).tables().table("tbl1");
        KeyValueBinaryView kvView1 = tbl1.kvView();

        tbl1.insert(tbl1.tupleBuilder().set("key", uuid).set("affKey", 42L)
            .set("valStr", "String value").set("valInt", 73).set("valNull", null).build());

        kvView1.put(kvView1.tupleBuilder().set("key", uuid2).set("affKey", 4242L).build(),
            kvView1.tupleBuilder().set("valStr", "String value 2").set("valInt", 7373).set("valNull", null).build());

        // Get data on node 2.
        Table tbl2 = clusterNodes.get(2).tables().table("tbl1");
        KeyValueBinaryView kvView2 = tbl2.kvView();

        final Tuple keyTuple1 = tbl2.tupleBuilder().set("key", uuid).set("affKey", 42L).build();
        final Tuple keyTuple2 = kvView2.tupleBuilder().set("key", uuid2).set("affKey", 4242L).build();

        // KV view must NOT return key columns in value.
        assertNull(kvView2.get(keyTuple1).value("key"));
        assertNull(kvView2.get(keyTuple1).value("affKey"));
        assertNull(kvView2.get(keyTuple2).value("key"));
        assertNull(kvView2.get(keyTuple2).value("affKey"));

        // Record binary view MUST return key columns in value.
        assertEquals(uuid, tbl2.get(keyTuple1).value("key"));
        assertEquals(42L, (Long)tbl2.get(keyTuple1).value("affKey"));
        assertEquals(uuid2, tbl2.get(keyTuple2).value("key"));
        assertEquals(4242L, (Long)tbl2.get(keyTuple2).value("affKey"));

        assertEquals("String value", tbl2.get(keyTuple1).value("valStr"));
        assertEquals(73, (Integer)tbl2.get(keyTuple1).value("valInt"));
        assertNull(tbl2.get(keyTuple1).value("valNull"));

        assertEquals("String value 2", tbl2.get(keyTuple2).value("valStr"));
        assertEquals(7373, (Integer)tbl2.get(keyTuple2).value("valInt"));
        assertNull(tbl2.get(keyTuple2).value("valNull"));

        assertEquals("String value", kvView2.get(keyTuple1).value("valStr"));
        assertEquals(73, (Integer)kvView2.get(keyTuple1).value("valInt"));
        assertNull(kvView2.get(keyTuple1).value("valNull"));

        assertEquals("String value 2", kvView2.get(keyTuple2).value("valStr"));
        assertEquals(7373, (Integer)kvView2.get(keyTuple2).value("valInt"));
        assertNull(kvView2.get(keyTuple2).value("valNull"));
    }
}
