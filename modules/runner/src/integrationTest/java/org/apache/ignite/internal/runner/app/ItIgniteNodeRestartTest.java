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
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.function.IntFunction;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.configuration.schemas.network.NetworkConfiguration;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.TableDefinition;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * These tests check node restart scenarios.
 */
public class ItIgniteNodeRestartTest extends IgniteAbstractTest {
    /** Default node port. */
    private static final int DEFAULT_NODE_PORT = 47500;

    /** Test table name. */
    private static final String TABLE_NAME = "Table1";

    /** Test table name. */
    private static final String TABLE_NAME_2 = "Table2";

    /**
     * Restarts empty node.
     */
    @Test
    public void emptyNodeTest(TestInfo testInfo) {
        String nodeName = testNodeName(testInfo, DEFAULT_NODE_PORT);

        IgniteImpl ignite = (IgniteImpl) IgnitionManager.start(nodeName, null, workDir);

        int nodePort = ignite.nodeConfiguration().getConfiguration(NetworkConfiguration.KEY).port().value();

        assertEquals(DEFAULT_NODE_PORT, nodePort);

        IgnitionManager.stop(ignite.name());

        ignite = (IgniteImpl) IgnitionManager.start(nodeName, null, workDir);

        nodePort = ignite.nodeConfiguration().getConfiguration(NetworkConfiguration.KEY).port().value();

        assertEquals(DEFAULT_NODE_PORT, nodePort);

        IgnitionManager.stop(ignite.name());
    }

    /**
     * Restarts a node with changing configuration.
     */
    @Test
    public void changeConfigurationOnStartTest(TestInfo testInfo) {
        String nodeName = testNodeName(testInfo, DEFAULT_NODE_PORT);

        IgniteImpl ignite = (IgniteImpl) IgnitionManager.start(nodeName, null, workDir);

        int nodePort = ignite.nodeConfiguration().getConfiguration(NetworkConfiguration.KEY).port().value();

        assertEquals(DEFAULT_NODE_PORT, nodePort);

        IgnitionManager.stop(ignite.name());

        int newPort = 3322;

        String updateCfg = "network.port=" + newPort;

        ignite = (IgniteImpl) IgnitionManager.start(nodeName, updateCfg, workDir);

        nodePort = ignite.nodeConfiguration().getConfiguration(NetworkConfiguration.KEY).port().value();

        assertEquals(newPort, nodePort);

        IgnitionManager.stop(ignite.name());
    }

    /**
     * Checks that the only one non-default property overwrites after another configuration is passed on the node restart.
     */
    @Test
    public void twoCustomPropertiesTest(TestInfo testInfo) {
        String nodeName = testNodeName(testInfo, 3344);

        String startCfg = "network: {\n"
                + "  port:3344,\n"
                + "  nodeFinder: {netClusterNodes:[ \"localhost:3344\" ]}\n"
                + "}";

        IgniteImpl ignite = (IgniteImpl) IgnitionManager.start(nodeName, startCfg, workDir);

        assertEquals(
                3344,
                ignite.nodeConfiguration().getConfiguration(NetworkConfiguration.KEY).port().value()
        );

        assertArrayEquals(
                new String[]{"localhost:3344"},
                ignite.nodeConfiguration().getConfiguration(NetworkConfiguration.KEY).nodeFinder().netClusterNodes().value()
        );

        IgnitionManager.stop(ignite.name());

        ignite = (IgniteImpl) IgnitionManager.start(
                nodeName,
                "network.nodeFinder.netClusterNodes=[ \"localhost:3344\", \"localhost:3343\" ]",
                workDir
        );

        assertEquals(
                3344,
                ignite.nodeConfiguration().getConfiguration(NetworkConfiguration.KEY).port().value()
        );

        assertArrayEquals(
                new String[]{"localhost:3344", "localhost:3343"},
                ignite.nodeConfiguration().getConfiguration(NetworkConfiguration.KEY).nodeFinder().netClusterNodes().value()
        );

        IgnitionManager.stop(ignite.name());
    }

    /**
     * Restarts the node which stores some data.
     */
    @Test
    public void nodeWithDataTest(TestInfo testInfo) {
        String nodeName = testNodeName(testInfo, 3344);

        Ignite ignite = IgnitionManager.start(nodeName, "{\n"
                + "  \"node\": {\n"
                + "    \"metastorageNodes\":[ " + nodeName + " ]\n"
                + "  },\n"
                + "  \"network\": {\n"
                + "    \"port\":3344,\n"
                + "    \"nodeFinder\": {\n"
                + "      \"netClusterNodes\":[ \"localhost:3344\" ] \n"
                + "    }\n"
                + "  }\n"
                + "}", workDir);

        createTableWithData(ignite, TABLE_NAME, i -> "name " + i);

        IgnitionManager.stop(nodeName);

        ignite = IgnitionManager.start(nodeName, null, workDir);

        checkTableWithData(ignite, TABLE_NAME,  i -> "name " + i);

        IgnitionManager.stop(nodeName);
    }

    /**
     * Starts two nodes and checks that the data are storing through restarts.
     * Nodes restart in the same order when they started at first.
     *
     * @param testInfo Test information object.
     */
    @Test
    public void testTwoNodesRestartDirect(TestInfo testInfo) {
        twoNodesRestart(testInfo, true);
    }

    /**
     * Starts two nodes and checks that the data are storing through restarts.
     * Nodes restart in reverse order when they started at first.
     *
     * @param testInfo Test information object.
     */
    @Test
    @Disabled("IGNITE-16034 Unblock a node start that happenes before Metastorage is ready")
    public void testTwoNodesRestartReverse(TestInfo testInfo) {
        twoNodesRestart(testInfo, false);
    }

    /**
     * Starts two nodes and checks that the data are storing through restarts.
     *
     * @param testInfo Test information object.
     * @param directOrder When the parameter is true, nodes restart in direct order, otherwise they restart in reverse order.
     */
    private void twoNodesRestart(TestInfo testInfo, boolean directOrder) {
        String metastorageNode = testNodeName(testInfo, 3344);

        Ignite ignite = IgnitionManager.start(metastorageNode, "{\n"
                + "  \"node\": {\n"
                + "    \"metastorageNodes\":[ " + metastorageNode + " ]\n"
                + "  },\n"
                + "  \"network\": {\n"
                + "    \"port\":3344,\n"
                + "    \"nodeFinder\": {\n"
                + "      \"netClusterNodes\":[ \"localhost:3344\" ] \n"
                + "    }\n"
                + "  }\n"
                + "}", workDir.resolve(metastorageNode));

        String nodeName = testNodeName(testInfo, 3345);

        IgnitionManager.start(nodeName, "{\n"
                + "  \"node\": {\n"
                + "    \"metastorageNodes\":[ " + metastorageNode + " ]\n"
                + "  },\n"
                + "  \"network\": {\n"
                + "    \"port\":3345,\n"
                + "    \"nodeFinder\": {\n"
                + "      \"netClusterNodes\":[ \"localhost:3344\" ] \n"
                + "    }\n"
                + "  }\n"
                + "}", workDir.resolve(nodeName));

        createTableWithData(ignite, TABLE_NAME, i -> "name " + i);
        createTableWithData(ignite, TABLE_NAME_2, i -> "val " + i);

        IgnitionManager.stop(metastorageNode);
        IgnitionManager.stop(nodeName);

        if (directOrder) {
            IgnitionManager.start(metastorageNode, null, workDir.resolve(metastorageNode));
            ignite = IgnitionManager.start(nodeName, null, workDir.resolve(nodeName));
        } else {
            ignite = IgnitionManager.start(nodeName, null, workDir.resolve(nodeName));
            IgnitionManager.start(metastorageNode, null, workDir.resolve(metastorageNode));
        }

        checkTableWithData(ignite, TABLE_NAME,  i -> "name " + i);
        checkTableWithData(ignite, TABLE_NAME_2,  i -> "val " + i);

        IgnitionManager.stop(metastorageNode);
        IgnitionManager.stop(nodeName);
    }

    /**
     * Checks the table exists and validates all data in it.
     *
     * @param ignite Ignite.
     * @param valueProducer Producer to predict a value.
     */
    private void checkTableWithData(Ignite ignite, String name, IntFunction<String> valueProducer) {
        Table table = ignite.tables().table("PUBLIC." + name);

        assertNotNull(table);

        for (int i = 0; i < 100; i++) {
            Tuple row = table.keyValueView().get(null, Tuple.create().set("id", i));

            assertEquals(valueProducer.apply(i), row.stringValue("name"));
        }
    }

    /**
     * Creates a table and load data to it.
     *
     * @param ignite Ignite.
     */
    private void createTableWithData(Ignite ignite, String name, IntFunction<String> valueProducer) {
        TableDefinition scmTbl1 = SchemaBuilders.tableBuilder("PUBLIC", name).columns(
                SchemaBuilders.column("id", ColumnType.INT32).build(),
                SchemaBuilders.column("name", ColumnType.string()).asNullable(true).build()
        ).withPrimaryKey(
                SchemaBuilders.primaryKey()
                        .withColumns("id")
                        .build()
        ).build();

        Table table = ignite.tables().createTable(
                scmTbl1.canonicalName(), tbl -> SchemaConfigurationConverter.convert(scmTbl1, tbl).changePartitions(10));

        for (int i = 0; i < 100; i++) {
            Tuple key = Tuple.create().set("id", i);
            Tuple val = Tuple.create().set("name", valueProducer.apply(i));

            table.keyValueView().put(null, key, val);
        }
    }
}
