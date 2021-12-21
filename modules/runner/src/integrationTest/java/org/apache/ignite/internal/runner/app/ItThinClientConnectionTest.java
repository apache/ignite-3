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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.ItUtils;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.TableDefinition;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests thin client connecting to a real server node.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class ItThinClientConnectionTest extends IgniteAbstractTest {
    private static final String SCHEMA_NAME = "PUB";

    private static final String TABLE_NAME = "tbl1";

    /** Nodes bootstrap configuration. */
    private final Map<String, String> nodesBootstrapCfg = new LinkedHashMap<>();

    private final List<Ignite> startedNodes = new ArrayList<>();

    /**
     * Before each.
     */
    @BeforeEach
    void setup(TestInfo testInfo) {
        String node0Name = testNodeName(testInfo, 3344);
        String node1Name = testNodeName(testInfo, 3345);

        nodesBootstrapCfg.put(
                node0Name,
                "{\n"
                        + "  node.metastorageNodes: [ \"" + node0Name + "\" ],\n"
                        + "  network: {\n"
                        + "    port: " + 3344 + ",\n"
                        + "    nodeFinder: {\n"
                        + "      netClusterNodes: [ \"localhost:3344\", \"localhost:3345\" ]\n"
                        + "    }\n"
                        + "  }\n"
                        + "}"
        );

        nodesBootstrapCfg.put(
                node1Name,
                "{\n"
                        + "  node.metastorageNodes: [ \"" + node0Name + "\" ],\n"
                        + "  network: {\n"
                        + "    port: " + 3345 + ",\n"
                        + "    nodeFinder: {\n"
                        + "      netClusterNodes: [ \"localhost:3344\", \"localhost:3345\" ]\n"
                        + "    }\n"
                        + "  }\n"
                        + "}"
        );
    }

    /**
     * After each.
     */
    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.closeAll(ItUtils.reverse(startedNodes));
    }

    /**
     * Check that thin client can connect to any server node and work with table API.
     */
    @Test
    void testThinClientConnectsToServerNodesAndExecutesBasicTableOperations() throws Exception {
        nodesBootstrapCfg.forEach((nodeName, configStr) ->
                startedNodes.add(IgnitionManager.start(nodeName, configStr, workDir.resolve(nodeName)))
        );

        var keyCol = "key";
        var valCol = "val";

        TableDefinition schTbl = SchemaBuilders.tableBuilder(SCHEMA_NAME, TABLE_NAME).columns(
                SchemaBuilders.column(keyCol, ColumnType.INT32).build(),
                SchemaBuilders.column(valCol, ColumnType.string()).asNullable(true).build()
        ).withPrimaryKey(keyCol).build();

        startedNodes.get(0).tables().createTable(schTbl.canonicalName(), tblCh ->
                SchemaConfigurationConverter.convert(schTbl, tblCh)
                        .changeReplicas(1)
                        .changePartitions(10)
        );

        var addrs = new String[]{"127.0.0.1:"
                + ((InetSocketAddress) ((IgniteImpl) startedNodes.get(0)).clientHandlerModule().localAddress()).getPort()};

        for (var addr : addrs) {
            try (var client = IgniteClient.builder().addresses(addr).build()) {
                List<Table> tables = client.tables().tables();
                assertEquals(1, tables.size());

                Table table = tables.get(0);
                assertEquals(String.format("%s.%s", SCHEMA_NAME, TABLE_NAME), table.name());

                var tuple = Tuple.create().set(keyCol, 1).set(valCol, "Hello");
                var keyTuple = Tuple.create().set(keyCol, 1);

                RecordView<Tuple> recView = table.recordView();

                recView.upsert(null, tuple);
                assertEquals("Hello", recView.get(null, keyTuple).stringValue(valCol));

                var kvView = table.keyValueView();
                assertEquals("Hello", kvView.get(null, keyTuple).stringValue(valCol));

                var pojoView = table.recordView(TestPojo.class);
                assertEquals("Hello", pojoView.get(null, new TestPojo(1)).val);

                assertTrue(recView.delete(null, keyTuple));
            }
        }
    }

    private static class TestPojo {
        public TestPojo() {
            // No-op.
        }

        public TestPojo(int key) {
            this.key = key;
        }

        public int key;

        public String val;
    }
}
