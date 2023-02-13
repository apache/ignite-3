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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.service.LeaderWithTerm;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.replicator.TablePartitionId;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.sql.Session;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * These tests check in-memory node restart scenarios.
 */
public class ItIgniteInMemoryNodeRestartTest extends IgniteAbstractTest {
    /** Default node port. */
    private static final int DEFAULT_NODE_PORT = 3344;

    /** Value producer for table data, is used to create data and check it later. */
    private static final IntFunction<String> VALUE_PRODUCER = i -> "val " + i;

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

        IgniteUtils.closeAll(closeables);

        CLUSTER_NODES.clear();
        CLUSTER_NODES_NAMES.clear();
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
        String cfgString = configurationString(idx);

        return startNode(idx, nodeName, cfgString, workDir.resolve(nodeName));
    }

    /**
     * Build a configuration string.
     *
     * @param idx Node index.
     * @return Configuration string.
     */
    private static String configurationString(int idx) {
        int port = DEFAULT_NODE_PORT + idx;

        // The address of the first node.
        @Language("HOCON") String connectAddr = "[localhost\":\"" + DEFAULT_NODE_PORT + "]";

        return IgniteStringFormatter.format(NODE_BOOTSTRAP_CFG, port, connectAddr);
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
        // Start three nodes, the first one is going to be CMG and MetaStorage leader.
        IgniteImpl ignite = startNode(testInfo, 0);
        startNode(testInfo, 1);
        startNode(testInfo, 2);

        // Create a table with replica on every node.
        createTableWithData(ignite, TABLE_NAME, 3, 1);

        TableImpl table = (TableImpl) ignite.tables().table(TABLE_NAME);
        UUID tableId = table.tableId();

        // Find the leader of the table's partition group.
        RaftGroupService raftGroupService = table.internalTable().partitionRaftGroupService(0);
        LeaderWithTerm leaderWithTerm = raftGroupService.refreshAndGetLeaderWithTerm().join();
        String leaderId = leaderWithTerm.leader().consistentId();

        // Find the index of any node that is not a leader of the partition group.
        int idxToStop = IntStream.range(1, 3)
                .filter(idx -> !leaderId.equals(ignite(idx).node().name()))
                .findFirst().getAsInt();

        // Restart the node.
        stopNode(idxToStop);

        IgniteImpl restartingNode = startNode(testInfo, idxToStop);

        Loza loza = restartingNode.raftManager();

        String restartingNodeConsistentId = restartingNode.name();

        TableImpl restartingTable = (TableImpl) restartingNode.tables().table(TABLE_NAME);
        InternalTableImpl internalTable = (InternalTableImpl) restartingTable.internalTable();

        // Check that it restarts.
        assertTrue(IgniteTestUtils.waitForCondition(
                () -> {
                    boolean raftNodeStarted = loza.localNodes().stream().anyMatch(nodeId -> {
                        if (nodeId.groupId() instanceof TablePartitionId) {
                            return ((TablePartitionId) nodeId.groupId()).tableId().equals(tableId);
                        }

                        return false;
                    });

                    if (!raftNodeStarted) {
                        return false;
                    }

                    Map<Integer, List<String>> assignments = internalTable.peersAndLearners();

                    List<String> partitionAssignments = assignments.get(0);

                    return partitionAssignments.contains(restartingNodeConsistentId);
                },
                TimeUnit.SECONDS.toMillis(10)
        ));

        // Check the data rebalanced correctly.
        checkTableWithData(restartingNode, TABLE_NAME);
    }

    /**
     * Restarts multiple nodes so the majority is lost.
     */
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-17586")
    @Test
    public void inMemoryNodeRestartNoMajority(TestInfo testInfo) throws Exception {
        // Start three nodes, the first one is going to be CMG and MetaStorage leader.
        IgniteImpl ignite0 = startNode(testInfo, 0);
        startNode(testInfo, 1);
        startNode(testInfo, 2);

        // Create a table with replica on every node.
        createTableWithData(ignite0, TABLE_NAME, 3, 1);

        TableImpl table = (TableImpl) ignite0.tables().table(TABLE_NAME);
        UUID tableId = table.tableId();

        // Lose the majority.
        stopNode(1);
        stopNode(2);

        IgniteImpl restartingNode = startNode(testInfo, 1);

        Loza loza = restartingNode.raftManager();

        // Check that it restarts.
        assertTrue(IgniteTestUtils.waitForCondition(
                () -> loza.localNodes().stream().anyMatch(nodeId -> {
                    if (nodeId.groupId() instanceof TablePartitionId) {
                        return ((TablePartitionId) nodeId.groupId()).tableId().equals(tableId);
                    }

                    return true;
                }),
                TimeUnit.SECONDS.toMillis(10)
        ));

        // Check the data rebalanced correctly.
        checkTableWithData(restartingNode, TABLE_NAME);
    }

    /**
     * Restarts all the nodes with the partition.
     */
    @Test
    public void inMemoryNodeFullPartitionRestart(TestInfo testInfo) throws Exception {
        // Start three nodes, the first one is going to be CMG and MetaStorage leader.
        IgniteImpl ignite0 = startNode(testInfo, 0);
        startNode(testInfo, 1);
        startNode(testInfo, 2);

        // Create a table with replicas on every node.
        createTableWithData(ignite0, TABLE_NAME, 3, 1);

        TableImpl table = (TableImpl) ignite0.tables().table(TABLE_NAME);
        UUID tableId = table.tableId();

        stopNode(0);
        stopNode(1);
        stopNode(2);

        startNode(testInfo, 0);
        startNode(testInfo, 1);
        startNode(testInfo, 2);

        // Check that full partition restart happens.
        for (int i = 0; i < 3; i++) {
            Loza loza = ignite(i).raftManager();

            assertTrue(IgniteTestUtils.waitForCondition(
                    () -> loza.localNodes().stream().anyMatch(nodeId -> {
                        if (nodeId.groupId() instanceof TablePartitionId) {
                            return ((TablePartitionId) nodeId.groupId()).tableId().equals(tableId);
                        }

                        return true;
                    }),
                    TimeUnit.SECONDS.toMillis(10)
            ));
        }
    }

    /**
     * Checks the table exists and validates all data in it.
     *
     * @param ignite Ignite.
     * @param name Table name.
     */
    private static void checkTableWithData(Ignite ignite, String name) {
        Table table = ignite.tables().table(name);

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
        try (Session session = ignite.sql().createSession()) {
            session.execute(null, "CREATE TABLE " + name
                    + " (id INT PRIMARY KEY, name VARCHAR)"
                    + " ENGINE aimem"
                    + " WITH replicas=" + replicas + ", partitions=" + partitions);

            for (int i = 0; i < 100; i++) {
                session.execute(null, "INSERT INTO " + name + "(id, name) VALUES (?, ?)",
                        i, VALUE_PRODUCER.apply(i));
            }
        }

        var table = (TableImpl) ignite.tables().table(name);

        assertThat(table.internalTable().storage().isVolatile(), is(true));
    }

    private static IgniteImpl ignite(int idx) {
        return (IgniteImpl) CLUSTER_NODES.get(idx);
    }
}
