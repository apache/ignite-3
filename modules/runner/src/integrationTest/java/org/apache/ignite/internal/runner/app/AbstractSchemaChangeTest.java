/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.EmbeddedNode;
import org.apache.ignite.Ignite;
import org.apache.ignite.InitParameters;
import org.apache.ignite.internal.IgniteIntegrationTest;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.function.Executable;

/**
 * Ignition interface tests.
 */
abstract class AbstractSchemaChangeTest extends IgniteIntegrationTest {
    /** Table name. */
    public static final String TABLE = "TBL1";

    /** Network ports of the test nodes. */
    private static final int[] PORTS = {3344, 3345, 3346};

    /** Nodes bootstrap configuration. */
    private final Map<String, String> nodesBootstrapCfg = new LinkedHashMap<>();

    /** Work directory. */
    @WorkDirectory
    private Path workDir;
    private List<EmbeddedNode> nodes;

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
                        + "  },\n"
                        + "  clientConnector: { port:10901 },\n"
                        + "  rest.port: 10300\n"
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
                        + "  },\n"
                        + "  clientConnector: { port:10902 },\n"
                        + "  rest.port: 10301\n"
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
                        + "  },\n"
                        + "  clientConnector: { port:10903 },\n"
                        + "  rest.port: 10302\n"
                        + "}"
        );
    }

    /**
     * After each.
     */
    @AfterEach
    void afterEach() throws Exception {
        IgniteUtils.closeAll(nodes.stream().map(node -> node::stop));
    }

    /**
     * Returns grid nodes.
     */
    protected List<Ignite> startGrid() {
        nodes = nodesBootstrapCfg.entrySet().stream()
                .map(e -> TestIgnitionManager.start(e.getKey(), e.getValue(), workDir.resolve(e.getKey())))
                .collect(toList());

        EmbeddedNode node = nodes.get(0);

        InitParameters initParameters = InitParameters.builder()
                .metaStorageNodes(node)
                .clusterName("cluster")
                .build();

        TestIgnitionManager.init(node, initParameters);

        CompletableFuture<Ignite>[] futures = nodes.stream()
                .map(EmbeddedNode::joinClusterAsync)
                .toArray(CompletableFuture[]::new);

        return await(CompletableFutures.allOf(futures));
    }

    /**
     * Creates tables.
     *
     * @param nodes Cluster nodes.
     */
    protected static void createTable(List<Ignite> nodes) {
        nodes.get(0).sql().execute(null, "CREATE TABLE tbl1(key BIGINT PRIMARY KEY, valint INT, valblob BINARY,"
                + "valdecimal DECIMAL, valbigint BIGINT, valstr VARCHAR NOT NULL DEFAULT 'default')");
    }

    /**
     * Adds column.
     *
     * @param nodes Cluster nodes.
     * @param columnToAdd Column to add.
     */
    protected static void addColumn(List<Ignite> nodes, String columnToAdd) {
        nodes.get(0).sql().execute(null, "ALTER TABLE " + TABLE + " ADD COLUMN " + columnToAdd);
    }

    /**
     * Drops column.
     *
     * @param nodes Cluster nodes.
     * @param colName Name of column to drop.
     */
    protected static void dropColumn(List<Ignite> nodes, String colName) {
        nodes.get(0).sql().execute(null, "ALTER TABLE " + TABLE + " DROP COLUMN " + colName + "");
    }

    /**
     * Renames column.
     *
     * @param nodes Cluster nodes.
     * @param oldName Old column name.
     * @param newName New column name.
     */
    // TODO: IGNITE-20315 syntax may change
    protected static void renameColumn(List<Ignite> nodes, String oldName, String newName) {
        nodes.get(0).sql().execute(null, String.format("ALTER TABLE %s RENAME COLUMN %s TO %s", TABLE, oldName, newName));
    }

    /**
     * Changes column default.
     *
     * @param nodes Cluster nodes.
     * @param colName Column name.
     * @param def Default value.
     */
    protected static void changeDefault(List<Ignite> nodes, String colName, String def) {
        nodes.get(0).sql().execute(null, String.format("ALTER TABLE %s ALTER COLUMN %s SET DEFAULT '%s'", TABLE, colName, def));
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
