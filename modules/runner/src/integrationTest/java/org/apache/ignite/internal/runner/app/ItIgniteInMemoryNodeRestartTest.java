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

import static org.apache.ignite.internal.recovery.ConfigurationCatchUpListener.CONFIGURATION_CATCH_UP_DIFFERENCE_PROPERTY;
import static org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter.convert;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.VolatilePageMemoryDataStorageChange;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.TableDefinition;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * These tests check in-memory node restart scenarios.
 */
@WithSystemProperty(key = CONFIGURATION_CATCH_UP_DIFFERENCE_PROPERTY, value = "0")
public class ItIgniteInMemoryNodeRestartTest extends IgniteAbstractTest {
    /** Default node port. */
    private static final int DEFAULT_NODE_PORT = 3344;

    /** Value producer for table data, is used to create data and check it later. */
    private static final IntFunction<String> VALUE_PRODUCER = i -> "val " + i;

    /** Prefix for full table name. */
    private static final String SCHEMA_PREFIX = "PUBLIC.";

    /** Test table name. */
    private static final String TABLE_NAME = "Table1";

    /** Nodes bootstrap configuration pattern. */
    private static final String NODE_BOOTSTRAP_CFG = "{\n"
            + "  network.port: {},\n"
            + "  network.nodeFinder.netClusterNodes: {}\n"
            + "}";

    /** Cluster nodes. */
    private static final List<Ignite> CLUSTER_NODES = new ArrayList<>();

    private static final List<String> CLUSTER_NODES_NAMES = new ArrayList<>();

    /**
     * Stops all started nodes.
     */
    @AfterEach
    public void afterEach() throws Exception {
        var closeables = new ArrayList<AutoCloseable>();

        for (String name : CLUSTER_NODES_NAMES) {
            if (name != null) {
                closeables.add(() -> IgnitionManager.stop(name));
            }
        }

        CLUSTER_NODES.clear();
        CLUSTER_NODES_NAMES.clear();

        IgniteUtils.closeAll(closeables);
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
    private static IgniteImpl startNode(int idx, String nodeName, @Nullable String cfgString, Path workDir) {
        assertTrue(CLUSTER_NODES.size() == idx || CLUSTER_NODES.get(idx) == null);

        CLUSTER_NODES_NAMES.add(idx, nodeName);

        CompletableFuture<Ignite> future = IgnitionManager.start(nodeName, cfgString, workDir.resolve(nodeName));

        if (CLUSTER_NODES.isEmpty()) {
            IgnitionManager.init(nodeName, List.of(nodeName), "cluster");
        }

        assertThat(future, willCompleteSuccessfully());

        Ignite ignite = future.join();

        CLUSTER_NODES.add(idx, ignite);

        return (IgniteImpl) ignite;
    }

    /**
     * Start node with the given parameters.
     *
     * @param testInfo Test info.
     * @param idx Node index, is used to stop the node later, see {@link #stopNode(int)}.
     * @return Created node instance.
     */
    private IgniteImpl startNode(TestInfo testInfo, int idx) {
        int port = DEFAULT_NODE_PORT + idx;
        String nodeName = testNodeName(testInfo, port);
        String cfgString = configurationString(idx, null, null);

        return startNode(idx, nodeName, cfgString, workDir.resolve(nodeName));
    }

    /**
     * Build a configuration string.
     *
     * @param idx Node index.
     * @param cfg Optional configuration string.
     * @param predefinedPort Predefined port.
     * @return Configuration string.
     */
    private static String configurationString(int idx, @Nullable String cfg, @Nullable Integer predefinedPort) {
        int port = predefinedPort == null ? DEFAULT_NODE_PORT + idx : predefinedPort;
        int connectPort = predefinedPort == null ? DEFAULT_NODE_PORT : predefinedPort;
        String connectAddr = "[\"localhost:" + connectPort + "\"]";

        return cfg == null ? IgniteStringFormatter.format(NODE_BOOTSTRAP_CFG, port, connectAddr) : cfg;
    }

    /**
     * Stop the node with given index.
     *
     * @param idx Node index.
     */
    private static void stopNode(int idx) {
        Ignite node = CLUSTER_NODES.get(idx);

        if (node != null) {
            IgnitionManager.stop(node.name());

            CLUSTER_NODES.set(idx, null);
            CLUSTER_NODES_NAMES.set(idx, null);
        }
    }

    /**
     * Restarts an in-memory node that is not a leader of the table's partition.
     */
    @Test
    public void inMemoryNodeRestartNotLeader(TestInfo testInfo) throws Exception {
        IgniteImpl ignite = startNode(testInfo, 0);
        startNode(testInfo, 1);
        startNode(testInfo, 2);

        createTableWithData(ignite, TABLE_NAME, 3, 1);

        String tableName = SCHEMA_PREFIX + TABLE_NAME;

        TableImpl table = (TableImpl) ignite.tables().table(tableName);
        String tableId = table.tableId().toString();
        RaftGroupService raftGroupService = table.internalTable().partitionRaftGroupService(0);
        IgniteBiTuple<Peer, Long> leaderWithTerm = raftGroupService.refreshAndGetLeaderWithTerm().join();
        NetworkAddress leaderAddress = leaderWithTerm.get1().address();

        var idxToStop = IntStream.range(1, 3)
                .filter(idx -> !leaderAddress.equals(ignite(idx).node().address()))
                .findFirst().getAsInt();

        stopNode(idxToStop);

        IgniteImpl restartingNode = startNode(testInfo, idxToStop);

        Loza loza = restartingNode.raftManager();

        assertTrue(IgniteTestUtils.waitForCondition(
                () -> loza.startedGroups().stream().anyMatch(grpName -> grpName.contains(tableId)),
                TimeUnit.SECONDS.toMillis(20)
        ));

        checkTableWithData(restartingNode, TABLE_NAME);
    }

    /**
     * Checks the table exists and validates all data in it.
     *
     * @param ignite Ignite.
     * @param name Table name.
     */
    private static void checkTableWithData(Ignite ignite, String name) {
        Table table = ignite.tables().table(SCHEMA_PREFIX + name);

        assertNotNull(table);

        for (int i = 0; i < 100; i++) {
            Tuple row = table.keyValueView().get(null, Tuple.create().set("id", i));

            assertEquals(VALUE_PRODUCER.apply(i), row.stringValue("name"));
        }
    }

    /**
     * Creates a table and load data to it.
     *
     * @param ignite Ignite.
     * @param name Table name.
     * @param replicas Replica factor.
     * @param partitions Partitions count.
     */
    private static void createTableWithData(Ignite ignite, String name, int replicas, int partitions) {
        TableDefinition scmTbl1 = SchemaBuilders.tableBuilder("PUBLIC", name).columns(
                SchemaBuilders.column("id", ColumnType.INT32).build(),
                SchemaBuilders.column("name", ColumnType.string()).asNullable(true).build()
        ).withPrimaryKey(
                SchemaBuilders.primaryKey()
                        .withColumns("id")
                        .build()
        ).build();

        Table table = ignite.tables().createTable(
                scmTbl1.canonicalName(),
                tbl -> convert(scmTbl1, tbl).changeReplicas(replicas).changePartitions(partitions)
                        .changeDataStorage(dsc -> dsc.convert(VolatilePageMemoryDataStorageChange.class))
        );

        for (int i = 0; i < 100; i++) {
            Tuple key = Tuple.create().set("id", i);
            Tuple val = Tuple.create().set("name", VALUE_PRODUCER.apply(i));

            table.keyValueView().put(null, key, val);
        }
    }

    private static IgniteImpl ignite(int idx) {
        return (IgniteImpl) CLUSTER_NODES.get(idx);
    }
}
