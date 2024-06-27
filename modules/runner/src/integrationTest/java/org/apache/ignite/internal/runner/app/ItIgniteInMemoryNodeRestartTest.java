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

import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIMEM_PROFILE_NAME;
import static org.apache.ignite.internal.TestWrappers.unwrapTableViewInternal;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
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
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteServer;
import org.apache.ignite.InitParameters;
import org.apache.ignite.internal.BaseIgniteRestartTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.service.LeaderWithTerm;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * These tests check in-memory node restart scenarios.
 */
public class ItIgniteInMemoryNodeRestartTest extends BaseIgniteRestartTest {

    /** Value producer for table data, is used to create data and check it later. */
    private static final IntFunction<String> VALUE_PRODUCER = i -> "val " + i;

    /** Test table name. */
    private static final String TABLE_NAME = "Table1";

    /** Cluster nodes. */
    private static final List<Ignite> CLUSTER_NODES = new ArrayList<>();

    /**
     * Stops all started nodes.
     */
    @AfterEach
    public void afterEach() throws Exception {
        var closeables = new ArrayList<AutoCloseable>();

        for (IgniteServer node : IGNITE_SERVERS) {
            if (node != null) {
                closeables.add(node::shutdown);
            }
        }

        IgniteUtils.closeAll(closeables);

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
    private static IgniteImpl startNode(int idx, String nodeName, @Nullable String cfgString, Path workDir) {
        assertTrue(CLUSTER_NODES.size() == idx || CLUSTER_NODES.get(idx) == null);
        assertTrue(IGNITE_SERVERS.size() == idx || IGNITE_SERVERS.get(idx) == null);

        IgniteServer node = TestIgnitionManager.start(nodeName, cfgString, workDir.resolve(nodeName));

        IGNITE_SERVERS.add(idx, node);

        if (CLUSTER_NODES.isEmpty()) {
            InitParameters initParameters = InitParameters.builder()
                    .metaStorageNodes(node)
                    .clusterName("cluster")
                    .build();

            TestIgnitionManager.init(node, initParameters);
        }

        assertThat(node.waitForInitAsync(), willCompleteSuccessfully());

        Ignite ignite = node.api();

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

    /** {@inheritDoc} */
    @Override
    protected void stopNode(int idx) {
        IgniteServer node = IGNITE_SERVERS.get(idx);

        if (node != null) {
            node.shutdown();

            CLUSTER_NODES.set(idx, null);
            IGNITE_SERVERS.set(idx, null);
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

        TableViewInternal table = unwrapTableViewInternal(ignite.tables().table(TABLE_NAME));

        // Find the leader of the table's partition group.
        RaftGroupService raftGroupService = table.internalTable().tableRaftService().partitionRaftGroupService(0);
        LeaderWithTerm leaderWithTerm = raftGroupService.refreshAndGetLeaderWithTerm().join();
        String leaderId = leaderWithTerm.leader().consistentId();

        log.info("Leader is {}", leaderId);

        // Find the index of any node that is not a leader of the partition group.
        int idxToStop = IntStream.range(1, 3)
                .filter(idx -> !leaderId.equals(ignite(idx).node().name()))
                .findFirst().getAsInt();

        log.info("Stopping node {}", idxToStop);

        // Restart the node.
        stopNode(idxToStop);

        IgniteImpl restartingNode = startNode(testInfo, idxToStop);

        log.info("Restarted node {}", restartingNode.name());

        Loza loza = restartingNode.raftManager();

        String restartingNodeConsistentId = restartingNode.name();

        TableViewInternal restartingTable = unwrapTableViewInternal(restartingNode.tables().table(TABLE_NAME));
        InternalTableImpl internalTable = (InternalTableImpl) restartingTable.internalTable();

        // Check that it restarts.
        waitForCondition(
                () -> isRaftNodeStarted(table, loza) && solePartitionAssignmentsContain(restartingNodeConsistentId, internalTable),
                TimeUnit.SECONDS.toMillis(10)
        );

        assertTrue(isRaftNodeStarted(table, loza), "Raft node of the partition is not started on " + restartingNodeConsistentId);
        assertTrue(
                solePartitionAssignmentsContain(restartingNodeConsistentId, internalTable),
                "Assignments do not contain node " + restartingNodeConsistentId
        );

        // Check the data rebalanced correctly.
        checkTableWithData(restartingNode, TABLE_NAME);
    }

    private static boolean solePartitionAssignmentsContain(String restartingNodeConsistentId, InternalTableImpl internalTable) {
        Map<Integer, List<String>> assignments = internalTable.tableRaftService().peersAndLearners();

        List<String> partitionAssignments = assignments.get(0);

        return partitionAssignments.contains(restartingNodeConsistentId);
    }

    private static boolean isRaftNodeStarted(TableViewInternal table, Loza loza) {
        return loza.localNodes().stream().anyMatch(nodeId ->
                nodeId.groupId() instanceof TablePartitionId && ((TablePartitionId) nodeId.groupId()).tableId() == table.tableId());
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

        TableViewInternal table = (TableViewInternal) ignite0.tables().table(TABLE_NAME);

        // Lose the majority.
        stopNode(1);
        stopNode(2);

        IgniteImpl restartingNode = startNode(testInfo, 1);

        Loza loza = restartingNode.raftManager();

        // Check that it restarts.
        assertTrue(waitForCondition(
                () -> loza.localNodes().stream().anyMatch(nodeId -> {
                    if (nodeId.groupId() instanceof TablePartitionId) {
                        return ((TablePartitionId) nodeId.groupId()).tableId() == table.tableId();
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

        TableViewInternal table = unwrapTableViewInternal(ignite0.tables().table(TABLE_NAME));

        stopNode(0);
        stopNode(1);
        stopNode(2);

        startNode(testInfo, 0);
        startNode(testInfo, 1);
        startNode(testInfo, 2);

        // Check that full partition restart happens.
        for (int i = 0; i < 3; i++) {
            Loza loza = ignite(i).raftManager();

            assertTrue(waitForCondition(
                    () -> loza.localNodes().stream().anyMatch(nodeId -> {
                        if (nodeId.groupId() instanceof TablePartitionId) {
                            return ((TablePartitionId) nodeId.groupId()).tableId() == table.tableId();
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
    private static void createTableWithData(Ignite ignite, String name, int replicas, int partitions) throws InterruptedException {
        IgniteSql sql = ignite.sql();

        sql.execute(null, String.format("CREATE ZONE IF NOT EXISTS ZONE_%s WITH REPLICAS=%d, PARTITIONS=%d, STORAGE_PROFILES='%s'",
                name, replicas, partitions, DEFAULT_AIMEM_PROFILE_NAME));
        sql.execute(null, "CREATE TABLE " + name
                + " (id INT PRIMARY KEY, name VARCHAR)"
                + " WITH PRIMARY_ZONE='ZONE_" + name.toUpperCase() + "';");

        for (int i = 0; i < 100; i++) {
            sql.execute(null, "INSERT INTO " + name + "(id, name) VALUES (?, ?)",
                    i, VALUE_PRODUCER.apply(i));
        }

        TableViewInternal table = unwrapTableViewInternal(ignite.tables().table(name));

        assertThat(table.internalTable().storage().isVolatile(), is(true));

        waitTillTableDataPropagatesToAllNodes(name, partitions);
    }

    private static void waitTillTableDataPropagatesToAllNodes(String name, int partitions) throws InterruptedException {
        assertTrue(
                waitForCondition(() -> tableHasDataOnAllIgnites(name, partitions), TimeUnit.SECONDS.toMillis(10)),
                "Did not see tuples propagate to all Ignites in time"
        );
    }

    private static boolean tableHasDataOnAllIgnites(String name, int partitions) {
        return CLUSTER_NODES.stream()
                .allMatch(igniteNode -> tableHasAnyData(unwrapTableViewInternal(igniteNode.tables().table(name)), partitions));
    }

    private static boolean tableHasAnyData(TableViewInternal nodeTable, int partitions) {
        return IgniteTestUtils.bypassingThreadAssertions(() -> {
            return IntStream.range(0, partitions)
                    .mapToObj(partition -> new IgniteBiTuple<>(
                            partition, nodeTable.internalTable().storage().getMvPartition(partition)
                    ))
                    .filter(pair -> pair.get2() != null)
                    .anyMatch(pair -> pair.get2().closestRowId(RowId.lowestRowId(pair.get1())) != null);
        });
    }

    private static IgniteImpl ignite(int idx) {
        return (IgniteImpl) CLUSTER_NODES.get(idx);
    }
}
