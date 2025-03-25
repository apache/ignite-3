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

package org.apache.ignite.internal.partition;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIPERSIST_PROFILE_NAME;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapTableImpl;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.io.FileMatchers.anExistingFile;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.TestWrappers;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.TokenizedAssignments;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.LogStorage;
import org.apache.ignite.table.Table;
import org.junit.jupiter.api.Test;

class ItPartitionDestructionTest extends ClusterPerTestIntegrationTest {
    private static final String ZONE_NAME = "TEST_ZONE";
    private static final String TABLE_NAME = "TEST_TABLE";

    private static final int PARTITION_ID = 0;

    @Override
    protected int initialNodes() {
        return 0;
    }

    private static void aggressiveLowWatermarkIncrease(InitParametersBuilder builder) {
        builder.clusterConfiguration("{\n"
                + "  ignite.gc.lowWatermark {\n"
                + "    dataAvailabilityTime: 1000,\n"
                + "    updateInterval: 100\n"
                + "  }\n"
                + "}");
    }

    @Test
    void partitionIsDestroyedOnTableDestruction() throws Exception {
        cluster.startAndInit(1, ItPartitionDestructionTest::aggressiveLowWatermarkIncrease);

        createZoneAndTableWith1Partition(1);
        makePutToTestTable();

        int tableId = testTableId();
        IgniteImpl ignite0 = unwrapIgniteImpl(cluster.node(0));

        makeSurePartition0ExistsOnDisk(ignite0, tableId);

        executeUpdate("DROP TABLE " + TABLE_NAME);

        verifyPartition0GetsRemovedFromDisk(ignite0, tableId);
    }

    private void createZoneAndTableWith1Partition(int replicas) {
        executeUpdate(
                "CREATE ZONE " + ZONE_NAME + " (REPLICAS " + replicas + ", PARTITIONS 1) STORAGE PROFILES ['"
                        + DEFAULT_AIPERSIST_PROFILE_NAME + "']"
        );
        executeUpdate("CREATE TABLE " + TABLE_NAME + " (id INT PRIMARY KEY, val VARCHAR) ZONE test_zone");
    }

    private void executeUpdate(String query) {
        SqlTestUtils.executeUpdate(query, cluster.aliveNode().sql());
    }

    private void makePutToTestTable() {
        Table table = cluster.aliveNode().tables().table(TABLE_NAME);
        table.keyValueView(Integer.class, String.class).put(null, 1, "one");
    }

    private int testTableId() {
        IgniteImpl ignite = unwrapIgniteImpl(cluster.node(0));
        TableImpl tableImpl = unwrapTableImpl(ignite.tables().table(TABLE_NAME));
        return tableImpl.tableId();
    }

    private static void makeSurePartition0ExistsOnDisk(IgniteImpl ignite, int tableId) {
        File partitionFile = testTablePartition0File(ignite, tableId);
        assertThat(partitionFile, is(anExistingFile()));

        LogStorage logStorage = partition0LogStorage(ignite, tableId);
        assertThat(logStorage.getLastLogIndex(), is(greaterThan(0L)));

        File raftMetaFile = partition0RaftMetaFile(ignite, tableId);
        assertThat(raftMetaFile, is(anExistingFile()));
    }

    private static File partition0RaftMetaFile(IgniteImpl ignite, int tableId) {
        String relativePath = partition0RaftNodeIdStringForStorage(ignite, tableId) + "/meta/raft_meta";
        Path raftMetaFilePath = ignite.partitionsWorkDir().metaPath().resolve(relativePath);
        return raftMetaFilePath.toFile();
    }

    private static LogStorage partition0LogStorage(IgniteImpl ignite, int tableId) {
        return ignite.partitionsLogStorageFactory().createLogStorage(
                partition0RaftNodeIdStringForStorage(ignite, tableId),
                new RaftOptions()
        );
    }

    private static String partition0RaftNodeIdStringForStorage(IgniteImpl ignite, int tableId) {
        RaftNodeId partitionRaftNodeId = new RaftNodeId(partition0ReplicationGroupId(tableId), new Peer(ignite.name()));
        return partitionRaftNodeId.nodeIdStringForStorage();
    }

    private static TablePartitionId partition0ReplicationGroupId(int tableId) {
        return new TablePartitionId(tableId, PARTITION_ID);
    }

    private static File testTablePartition0File(IgniteImpl ignite, int tableId) {
        return ignite.partitionsWorkDir().dbPath().resolve("db/table-" + tableId + "/part-0.bin").toFile();
    }

    private static void verifyPartition0GetsRemovedFromDisk(IgniteImpl ignite, int tableId)
            throws InterruptedException {
        File partitionFile = testTablePartition0File(ignite, tableId);
        assertTrue(
                waitForCondition(() -> !partitionFile.exists(), SECONDS.toMillis(10)),
                "Partition file " + partitionFile.getAbsolutePath() + " was not removed in time"
        );

        assertTrue(
                waitForCondition(() -> partition0LogStorage(ignite, tableId).getLastLogIndex() == 0L, SECONDS.toMillis(10)),
                "Partition Raft log was not removed in time"
        );

        File raftMetaFile = partition0RaftMetaFile(ignite, tableId);
        assertTrue(
                waitForCondition(() -> !raftMetaFile.exists(), SECONDS.toMillis(10)),
                "Partition Raft meta file " + raftMetaFile.getAbsolutePath() + " was not removed in time"
        );
    }

    @Test
    void partitionIsDestroyedWhenItIsEvictedFromNode() throws Exception {
        cluster.startAndInit(2);

        createZoneAndTableWith1Partition(2);
        makePutToTestTable();

        int tableId = testTableId();
        IgniteImpl ignite0 = unwrapIgniteImpl(cluster.node(0));
        IgniteImpl ignite1 = unwrapIgniteImpl(cluster.node(1));

        waitTillAssignmentCountReaches(2, tableId);

        makeSurePartition0ExistsOnDisk(ignite0, tableId);
        makeSurePartition0ExistsOnDisk(ignite1, tableId);

        executeUpdate("ALTER ZONE " + ZONE_NAME + " SET REPLICAS = 1");

        IgniteImpl notHostingIgnite = nodeNotHostingPartition0(tableId);
        verifyPartition0GetsRemovedFromDisk(notHostingIgnite, tableId);
    }

    private void waitTillAssignmentCountReaches(int targetAssignmentCount, int tableId) throws InterruptedException {
        assertTrue(
                waitForCondition(() -> partition0Assignments(tableId).size() == targetAssignmentCount, SECONDS.toMillis(10)),
                "Did not see assignments count reaching " + targetAssignmentCount
        );
    }

    private Set<Assignment> partition0Assignments(int tableId) {
        IgniteImpl ignite = unwrapIgniteImpl(cluster.aliveNode());

        CompletableFuture<TokenizedAssignments> assignmentsFuture = ignite.placementDriver().getAssignments(partition0ReplicationGroupId(
                tableId), ignite.clock().now());
        assertThat(assignmentsFuture, willCompleteSuccessfully());
        return assignmentsFuture.join().nodes();
    }

    private IgniteImpl nodeNotHostingPartition0(int tableId) throws InterruptedException {
        waitTillAssignmentCountReaches(1, tableId);

        Set<Assignment> assignments = partition0Assignments(tableId);
        assertThat(assignments, hasSize(1));
        Assignment assignment = assignments.iterator().next();

        return cluster.runningNodes()
                .filter(node -> !node.name().equals(assignment.consistentId()))
                .findAny()
                .map(TestWrappers::unwrapIgniteImpl)
                .orElseThrow();
    }
}
