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

import java.util.List;
import java.util.Map;
import org.apache.ignite.app.Ignite;
import org.apache.ignite.app.IgnitionManager;
import org.apache.ignite.configuration.schemas.network.NetworkConfiguration;
import org.apache.ignite.internal.app.IgnitionImpl;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.schema.ColumnType;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.SchemaTable;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.ReflectionUtils;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * These tests check node restart scenarios.
 */
public class ITIgniteNodeRestartTest extends IgniteAbstractTest {
    /** Test node name. */
    public static final String NODE_NAME = "TestNode";

    /** Test table name. */
    public static final String TABLE_NAME = "Table1";

    /**
     * Restarts empty node.
     *
     * @throws Exception If failed.
     */
    @Test
    public void emptyNodeTest() throws Exception {
        Ignite ignite = IgnitionManager.start(NODE_NAME, null, workDir.resolve(NODE_NAME));

        int nodePort = getNodePort(NODE_NAME);

        assertEquals(47500, nodePort);

        IgnitionManager.stop(ignite.name());

        ignite = IgnitionManager.start(NODE_NAME, null, workDir.resolve(NODE_NAME));

        nodePort = getNodePort(NODE_NAME);

        assertEquals(47500, nodePort);

        IgnitionManager.stop(ignite.name());
    }

    /**
     * Restarts a node with changing configuration.
     *
     * @throws Exception If failed.
     */
    @Test
    public void changeConfigurationOnStartTest() throws Exception {
        Ignite ignite = IgnitionManager.start(NODE_NAME, null, workDir.resolve(NODE_NAME));

        int nodePort = getNodePort(NODE_NAME);

        assertEquals(47500, nodePort);

        IgnitionManager.stop(ignite.name());

        int newPort = 3322;

        String updateCfg = "network.port=" + newPort;

        ignite = IgnitionManager.start(NODE_NAME, updateCfg, workDir.resolve(NODE_NAME));

        nodePort = getNodePort(NODE_NAME);

        assertEquals(newPort, nodePort);

        IgnitionManager.stop(ignite.name());
    }

    /**
     * Checks that the only one non-default property overwrites after another configuration is passed on the node
     * restart.
     *
     * @throws Exception If failed.
     */
    @Test
    public void twoCustomPropertiesTest() throws Exception {
        String startCfg = "network: {\n" +
            "  port:3344,\n" +
            "  netClusterNodes:[ \"localhost:3344\" ]\n" +
            "}";

        Ignite ignite = IgnitionManager.start(NODE_NAME, startCfg, workDir.resolve(NODE_NAME));

        assertEquals(3344, getNodePort(NODE_NAME));
        assertArrayEquals(new String[] {"localhost:3344"}, getClusterNodes(NODE_NAME));

        IgnitionManager.stop(ignite.name());

        ignite = IgnitionManager.start(NODE_NAME, "network.netClusterNodes=[ \"localhost:3344\", \"localhost:3343\" ]", workDir.resolve(NODE_NAME));

        assertEquals(3344, getNodePort(NODE_NAME));
        assertArrayEquals(new String[] {"localhost:3344", "localhost:3343"}, getClusterNodes(NODE_NAME));

        IgnitionManager.stop(ignite.name());
    }

    /**
     * Gets current network port.
     *
     * @param nodeName Node name.
     * @return Network port.
     * @throws Exception If some issue has happened when getting the port.
     */
    private int getNodePort(String nodeName) throws Exception {
        NetworkConfiguration networkCfgTree = getNetworkCfgTree(nodeName);

        return networkCfgTree.port().value();
    }

    /**
     * Gets an array of nodes.
     *
     * @param nodeName Node name.
     * @return Array of node's hosts.
     * @throws Exception If some issue has happened when getting the nodes.
     */
    private String[] getClusterNodes(String nodeName) throws Exception {
        NetworkConfiguration networkCfgTree = getNetworkCfgTree(nodeName);

        return networkCfgTree.netClusterNodes().value();
    }

    /**
     * Gets a network configuration for a node with the name specified.
     *
     * @param nodeName Node name.
     * @return Network configuration.
     * @throws Exception If failed.
     */
    private NetworkConfiguration getNetworkCfgTree(String nodeName) throws Exception {
        Map<String, List<IgniteComponent>> nodesStartedComponents = (Map<String, List<IgniteComponent>>)ReflectionUtils.tryToReadFieldValue(IgnitionImpl.class, "nodesStartedComponents", null).get();

        assertNotNull(nodesStartedComponents.get(nodeName));

        MetaStorageManager metaStorageManager = (MetaStorageManager)nodesStartedComponents.get(NODE_NAME).stream().filter(component -> component instanceof MetaStorageManager).findFirst().get();

        ConfigurationManager locCfgMgr = (ConfigurationManager)ReflectionUtils.tryToReadFieldValue(MetaStorageManager.class, "locCfgMgr", metaStorageManager).get();

        return locCfgMgr.configurationRegistry().getConfiguration(NetworkConfiguration.KEY);
    }

    /**
     * Restarts the node which stores some data.
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-15255")
    public void nodeWithDataTest() {
        Ignite ignite = IgnitionManager.start(NODE_NAME, "{\n" +
            "  \"node\": {\n" +
            "    \"metastorageNodes\":[ " + NODE_NAME + " ]\n" +
            "  },\n" +
            "  \"network\": {\n" +
            "    \"port\":3344,\n" +
            "    \"netClusterNodes\":[ \"localhost:3344\" ]\n" +
            "  }\n" +
            "}", workDir.resolve(NODE_NAME));

        SchemaTable scmTbl1 = SchemaBuilders.tableBuilder("PUBLIC", TABLE_NAME).columns(
            SchemaBuilders.column("id", ColumnType.INT32).asNonNull().build(),
            SchemaBuilders.column("name", ColumnType.string()).asNullable().build()
        ).withIndex(
            SchemaBuilders.pkIndex()
                .addIndexColumn("id").done()
                .build()
        ).build();

        Table table = ignite.tables().getOrCreateTable(scmTbl1.canonicalName(), tbl -> SchemaConfigurationConverter.convert(scmTbl1, tbl)
            .changePartitions(10));

        for (int i = 0; i < 100; i++) {
            Tuple key = table.tupleBuilder()
                .set("id", i)
                .build();

            Tuple vaul = table.tupleBuilder()
                .set("name", "name " + i)
                .build();

            table.kvView().put(key, vaul);
        }

        IgnitionManager.stop(NODE_NAME);

        ignite = IgnitionManager.start(NODE_NAME, null, workDir.resolve(NODE_NAME));

        assertNotNull(ignite.tables().table(TABLE_NAME));

        for (int i = 0; i < 100; i++) {
            assertEquals("name " + i, table.kvView().get(table.tupleBuilder()
                .set("id", i)
                .build())
                .stringValue("name"));
        }

        IgnitionManager.stop(NODE_NAME);
    }
}
