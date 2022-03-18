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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntFunction;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.configuration.schemas.network.NetworkConfiguration;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.TableDefinition;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * These tests check node restart scenarios.
 */
public class ItIgniteNodeRestartTest extends IgniteAbstractTest {
    /** Default node port. */
    private static final int DEFAULT_NODE_PORT = 3344;

    /** Test table name. */
    private static final String TABLE_NAME = "Table1";

    /** Test table name. */
    private static final String TABLE_NAME_2 = "Table2";

    /** Nodes bootstrap configuration pattern. */
    private static final String NODE_BOOTSTRAP_CFG = "{\n"
            + "  \"node\": {\n"
            + "    \"metastorageNodes\":[ {} ]\n"
            + "  },\n"
            + "  \"network\": {\n"
            + "    \"port\":{},\n"
            + "    \"nodeFinder\":{\n"
            + "      \"netClusterNodes\": [ {} ]\n"
            + "    }\n"
            + "  }\n"
            + "}";

    /** Cluster nodes. */
    protected static final List<Ignite> CLUSTER_NODES = new ArrayList<>();

    /**
     * Stops all started nodes.
     */
    @AfterEach
    public void afterAll() {
        for (int i = 0; i < CLUSTER_NODES.size(); i++) {
            stopNode(i);
        }

        CLUSTER_NODES.clear();
    }

    /**
     * Start node with the given parameters.
     *
     * @param idx Node index, is used to stop the node later, see {@link #stopNode(int)}.
     * @param nodeName Node name.
     * @param cfgString Configuration string.
     * @param workDir Working directory.
     * @return Created node instance.
     */
    private IgniteImpl startNode(int idx, String nodeName, String cfgString, Path workDir) {
        IgniteImpl ignite = (IgniteImpl) IgnitionManager.start(nodeName, cfgString, workDir);

        assertTrue(CLUSTER_NODES.size() == idx || CLUSTER_NODES.get(idx) == null);

        CLUSTER_NODES.add(idx, ignite);

        return ignite;
    }

    /**
     * Start node with the given parameters.
     *
     * @param testInfo Test info.
     * @param idx Node index, is used to stop the node later, see {@link #stopNode(int)}.
     * @param predefinedNodeName Predefined node name, can be null.
     * @param predefinedPort Predefined port, is {@code null} then default port is used.
     * @param cfg Configuration string, can be auto-generated if {@code null}.
     * @return Created node instance.
     */
    private IgniteImpl startNode(
            TestInfo testInfo,
            int idx,
            @Nullable String predefinedNodeName,
            @Nullable Integer predefinedPort,
            @Nullable String cfg
    ) {
        int port = predefinedPort == null ? DEFAULT_NODE_PORT + idx : predefinedPort;
        String nodeName = predefinedNodeName == null ? testNodeName(testInfo, port) : predefinedNodeName;
        int connectPort = predefinedPort == null ? DEFAULT_NODE_PORT : predefinedPort;
        String connectAddr = "\"localhost:" + connectPort + '\"';
        String metastorageNodeName = testNodeName(testInfo, connectPort);
        String cfgString = cfg == null
                ? IgniteStringFormatter.format(NODE_BOOTSTRAP_CFG, metastorageNodeName, port, connectAddr)
                : cfg;

        return startNode(idx, nodeName, cfgString, workDir.resolve(nodeName));
    }

    /**
     * Start node with the given parameters.
     *
     * @param testInfo Test info.
     * @param idx Node index, is used to stop the node later, see {@link #stopNode(int)}.
     * @return Created node instance.
     */
    private IgniteImpl startNode(TestInfo testInfo, int idx) {
        return startNode(testInfo, idx, null, null, null);
    }

    /**
     * Stop the node with given index.
     *
     * @param idx Node index.
     */
    private void stopNode(int idx) {
        Ignite node = CLUSTER_NODES.get(idx);

        if (node != null) {
            IgnitionManager.stop(node.name());

            CLUSTER_NODES.set(idx, null);
        }
    }

    /**
     * Restarts empty node.
     */
    @Test
    public void emptyNodeTest(TestInfo testInfo) {
        IgniteImpl ignite = startNode(testInfo, 0);

        int nodePort = ignite.nodeConfiguration().getConfiguration(NetworkConfiguration.KEY).port().value();

        assertEquals(DEFAULT_NODE_PORT, nodePort);

        stopNode(0);

        ignite = startNode(testInfo, 0);

        nodePort = ignite.nodeConfiguration().getConfiguration(NetworkConfiguration.KEY).port().value();

        assertEquals(DEFAULT_NODE_PORT, nodePort);
    }

    /**
     * Restarts a node with changing configuration.
     */
    @Test
    public void changeConfigurationOnStartTest(TestInfo testInfo) {
        IgniteImpl ignite = startNode(testInfo, 0);

        int nodePort = ignite.nodeConfiguration().getConfiguration(NetworkConfiguration.KEY).port().value();

        assertEquals(DEFAULT_NODE_PORT, nodePort);

        stopNode(0);

        int newPort = 3322;

        String updateCfg = "network.port=" + newPort;

        ignite = startNode(testInfo, 0, null, newPort, updateCfg);

        nodePort = ignite.nodeConfiguration().getConfiguration(NetworkConfiguration.KEY).port().value();

        assertEquals(newPort, nodePort);

        IgnitionManager.stop(ignite.name());
    }

    /**
     * Checks that the only one non-default property overwrites after another configuration is passed on the node restart.
     */
    @Test
    public void twoCustomPropertiesTest(TestInfo testInfo) {
        String startCfg = "network: {\n"
                + "  port:3344,\n"
                + "  nodeFinder: {netClusterNodes:[ \"localhost:3344\" ]}\n"
                + "}";

        IgniteImpl ignite = startNode(testInfo, 0, null, 3344, startCfg);

        String nodeName = ignite.name();

        assertEquals(
                3344,
                ignite.nodeConfiguration().getConfiguration(NetworkConfiguration.KEY).port().value()
        );

        assertArrayEquals(
                new String[]{"localhost:3344"},
                ignite.nodeConfiguration().getConfiguration(NetworkConfiguration.KEY).nodeFinder().netClusterNodes().value()
        );

        stopNode(0);

        ignite = startNode(testInfo, 0, nodeName, null, "network.nodeFinder.netClusterNodes=[ \"localhost:3344\", \"localhost:3343\" ]");

        assertEquals(
                3344,
                ignite.nodeConfiguration().getConfiguration(NetworkConfiguration.KEY).port().value()
        );

        assertArrayEquals(
                new String[]{"localhost:3344", "localhost:3343"},
                ignite.nodeConfiguration().getConfiguration(NetworkConfiguration.KEY).nodeFinder().netClusterNodes().value()
        );
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
        Ignite ignite = startNode(testInfo, 0);

        startNode(testInfo, 1);

        createTableWithData(ignite, TABLE_NAME, i -> "name " + i);
        createTableWithData(ignite, TABLE_NAME_2, i -> "val " + i);

        stopNode(0);
        stopNode(1);

        if (directOrder) {
            startNode(testInfo, 0);
            ignite = startNode(testInfo, 1);
        } else {
            ignite = startNode(testInfo, 1);
            startNode(testInfo, 0);
        }

        checkTableWithData(ignite, TABLE_NAME,  i -> "name " + i);
        checkTableWithData(ignite, TABLE_NAME_2,  i -> "val " + i);
    }

    /**
     * The test for node restart when there is a gap between the node local configuration and distributed configuration.
     */
    @Test
    @Disabled
    public void testRestartNodeWithConfigurationGap(TestInfo testInfo) {
        final int nodes = 4;

        for (int i = 0; i < nodes; i++) {
            startNode(testInfo, i);
        }

        IntFunction<String> valueProducer = String::valueOf;

        createTableWithData(CLUSTER_NODES.get(0), "t1", valueProducer);

        final int nodeToStop = nodes - 1;

        log.info("Stopping the node.");

        stopNode(nodeToStop);

        createTableWithData(CLUSTER_NODES.get(0), "t2", valueProducer);

        log.info("Starting the node.");

        startNode(testInfo, nodeToStop);

        checkTableWithData(CLUSTER_NODES.get(0), "t1", valueProducer);
        checkTableWithData(CLUSTER_NODES.get(0), "t1", valueProducer);

        checkTableWithData(CLUSTER_NODES.get(nodeToStop), "t1", valueProducer);
        checkTableWithData(CLUSTER_NODES.get(nodeToStop), "t2", valueProducer);
    }


    /**
     * Checks the table exists and validates all data in it.
     *
     * @param ignite Ignite.
     * @param name Table name.
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
     * @param name Table name.
     * @param valueProducer Producer of the values.
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
