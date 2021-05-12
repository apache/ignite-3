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
import java.util.function.Consumer;
import org.apache.ignite.app.Ignite;
import org.apache.ignite.app.IgnitionManager;
import org.apache.ignite.table.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Ignition interface tests.
 */
class TableTest {
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
                "  },\n" +
                "  \"table\": {\n" +
                "       \"tbl1\": {\n" +
                "           \"partitions\": 10,\n" +
                "           \"replicas\": 2,\n" +
                "           \"columns\": { \n" +
                "                \"key\": {\n" +
                "                   \"type\":INT64,\n" +
                "                   \"nullable\":false\n" +
                "                },\n" +
                "                \"val\": {\n" +
                "                   \"type\":INT64,\n" +
                "                   \"nullable\":false\n" +
                "                }\n" +
                "           },\n" +
                "           \"indices\": {\n" +
                "                \"PK\": {\n" +
                "                   \"type\":PRIMARY,\n" +
                "                   \"columns\": {\n" +
                "                       \"key\": {\n" +
                "                           \"asc\":true\n" +
                "                       }\n" +
                "                   },\n" +
                "                   \"affinityColumns\": [\"key\"]\n" +
                "                }\n" +
                "           }\n" +
                "       }\n" +
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
     * Check table creation via bootstrap configuration with pre-configured table.
     */
    @Test
    void testInitialTableConfiguration() {
        List<Ignite> startedNodes = new ArrayList<>();

        for (String nodeBootstrapCfg : nodesBootstrapCfg)
            startedNodes.add(IgnitionManager.start(nodeBootstrapCfg));

        assertEquals(3, startedNodes.size());

        startedNodes.forEach(Assertions::assertNotNull);

        Table tbl1 = startedNodes.get(1).tables().table("tbl1");
        tbl1.insert(tbl1.tupleBuilder().set("key", 1L).set("val", 111L).build());

        Table tbl2 = startedNodes.get(2).tables().table("tbl1");
        assertEquals(111L, tbl2.get(tbl2.tupleBuilder().set("key", 1L).build()));
    }

    /**
     * Check dynamic table creation.
     */
    @Test
    void testDynamicTableCreation() {
        List<Ignite> startedNodes = new ArrayList<>();

        for (String nodeBootstrapCfg : nodesBootstrapCfg)
            startedNodes.add(IgnitionManager.start(nodeBootstrapCfg));

        assertEquals(3, startedNodes.size());

        startedNodes.forEach(Assertions::assertNotNull);

        startedNodes.get(0).tables().createTable("tbl2", tbl -> tbl
            .changeReplicas(1)
            .changePartitions(10)
            .changeColumns(cols -> cols
                .create("key", c -> c.changeType(t -> t.changeType("INT64")))
                .create("val", c -> c.changeType(t -> t.changeType("INT64")))
            )
            .changeIndices(idxs -> idxs
                .create("PK", idx -> idx
                    .changeType("PRIMARY")
                    .changeColumns(c -> c
                        .create("key", t -> {}))
                    .changeAffinityColumns(new String[] {"key"}))
            ));

        Table tbl1 = startedNodes.get(1).tables().table("tbl2");
        tbl1.insert(tbl1.tupleBuilder().set("key", 1L).set("val", 111L).build());

        Table tbl2 = startedNodes.get(2).tables().table("tbl2");
        assertEquals(111L, tbl2.get(tbl2.tupleBuilder().set("key", 1L).build()));
    }
}
