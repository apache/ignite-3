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

package org.apache.ignite.internal.sql.engine;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.SessionUtils.executeUpdate;
import static org.apache.ignite.internal.sql.engine.schema.IgniteTableImpl.STATS_CLI_UPDATE_THRESHOLD;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.apache.ignite.internal.Cluster;
import org.apache.ignite.internal.Cluster.NodeKnockout;
import org.apache.ignite.internal.IgniteIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.table.distributed.replicator.TablePartitionId;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.raft.jraft.RaftGroupService;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.core.NodeImpl;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Test ensures that the SQL planner does not crash during a rebalance.
 */
public class ItSqlPlanningOnRebalanceTest extends IgniteIntegrationTest {
    private static final IgniteLogger LOG = Loggers.forClass(ItSqlPlanningOnRebalanceTest.class);

    @WorkDirectory
    private Path workDir;

    private Cluster cluster;

    @BeforeEach
    void createCluster(TestInfo testInfo) {
        cluster = new Cluster(testInfo, workDir);
    }

    @AfterEach
    void shutdownCluster() {
        cluster.shutdown();
    }

    /**
     * Test ensures that SQL planner ignores storage rebalance exception.
     */
    @Test
    public void testSqlPlanningDuringRebalance() throws InterruptedException {
        cluster.startAndInit(3);

        createTestTable();

        transferLeadershipOnSolePartitionTo(0);

        int rowsCnt = 5_000;

        cluster.doInSession(0, session -> {
            executeUpdate("insert into test(id, value) select x, x::VARCHAR from TABLE(system_range(1, " + rowsCnt + "))", session);
        });

        cluster.knockOutNode(2, NodeKnockout.PARTITION_NETWORK);

        causeLogTruncationOnSolePartitionLeader();

        cluster.reanimateNode(2, NodeKnockout.PARTITION_NETWORK);

        cluster.doInSession(2, session -> {
            String query = "select count(*) from test";

            // Ensures that planner ignores storage rebalance exception.
            for (int i = 0; i < STATS_CLI_UPDATE_THRESHOLD; i++) {
                session.execute(null, "EXPLAIN PLAN FOR " + query);
            }

            ResultSet<SqlRow> res = session.execute(null, query);

            assertTrue(res.hasNext());

            SqlRow rs = res.next();

            assertEquals(rowsCnt, rs.longValue(0));
        });
    }

    private void transferLeadershipOnSolePartitionTo(int nodeIndex) throws InterruptedException {
        String nodeConsistentId = cluster.node(nodeIndex).node().name();

        int maxAttempts = 3;

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            boolean transferred = tryTransferLeadershipOnSolePartitionTo(nodeConsistentId);

            if (transferred) {
                break;
            }

            if (attempt < maxAttempts) {
                LOG.info("Did not transfer leadership after " + attempt + " attempts, going to retry...");
            } else {
                fail("Did not transfer leadership in time after " + maxAttempts + " attempts");
            }
        }
    }

    private boolean tryTransferLeadershipOnSolePartitionTo(String targetLeaderConsistentId) throws InterruptedException {
        NodeImpl leaderBeforeTransfer = (NodeImpl) cluster.leaderServiceFor(solePartitionId()).getRaftNode();

        initiateLeadershipTransferTo(targetLeaderConsistentId, leaderBeforeTransfer);

        BooleanSupplier leaderTransferred = () -> {
            PeerId leaderId = leaderBeforeTransfer.getLeaderId();
            return leaderId != null && leaderId.getConsistentId().equals(targetLeaderConsistentId);
        };

        return waitForCondition(leaderTransferred, 10_000);
    }

    private static void initiateLeadershipTransferTo(String targetLeaderConsistentId, NodeImpl leaderBeforeTransfer) {
        long startedMillis = System.currentTimeMillis();

        while (true) {
            Status status = leaderBeforeTransfer.transferLeadershipTo(new PeerId(targetLeaderConsistentId));

            if (status.getRaftError() != RaftError.EBUSY) {
                break;
            }

            if (System.currentTimeMillis() - startedMillis > 10_000) {
                throw new IllegalStateException("Could not initiate leadership transfer to " + targetLeaderConsistentId + " in time");
            }
        }
    }

    /**
     * Returns the ID of the sole table partition that exists in the cluster.
     */
    private TablePartitionId solePartitionId() {
        List<TablePartitionId> tablePartitionIds = tablePartitionIds(cluster.aliveNode());

        assertThat(tablePartitionIds.size(), is(1));

        return tablePartitionIds.get(0);
    }

    /**
     * Causes log truncation on the RAFT leader of the sole table partition that exists in the cluster.
     * After such truncation, when a knocked-out follower gets reanimated, the leader will not be able to feed it
     * with AppendEntries (because the leader does not already have the index that is required to send AppendEntries
     * to the lagging follower), so the leader will have to send InstallSnapshot instead.
     */
    private void causeLogTruncationOnSolePartitionLeader() throws InterruptedException {
        // Doing this twice because first snapshot creation does not trigger log truncation.
        doSnapshotOnSolePartitionLeader();
        doSnapshotOnSolePartitionLeader();
    }

    /**
     * Causes a RAFT snapshot to be taken on the RAFT leader of the sole table partition that exists in the cluster.
     */
    private void doSnapshotOnSolePartitionLeader() throws InterruptedException {
        TablePartitionId tablePartitionId = solePartitionId();

        doSnapshotOn(tablePartitionId);
    }

    /**
     * Takes a RAFT snapshot on the leader of the RAFT group corresponding to the given table partition.
     */
    private void doSnapshotOn(TablePartitionId tablePartitionId) throws InterruptedException {
        RaftGroupService raftGroupService = cluster.leaderServiceFor(tablePartitionId);

        CountDownLatch snapshotLatch = new CountDownLatch(1);
        AtomicReference<Status> snapshotStatus = new AtomicReference<>();

        raftGroupService.getRaftNode().snapshot(status -> {
            snapshotStatus.set(status);
            snapshotLatch.countDown();
        });

        assertTrue(snapshotLatch.await(10, TimeUnit.SECONDS), "Snapshot was not finished in time");

        assertTrue(snapshotStatus.get().isOk(), "Snapshot failed: " + snapshotStatus.get());
    }

    private void createTestTable() throws InterruptedException {
        String sql1 = "create zone test_zone with "
                + "partitions=1,"
                + "replicas=3";
        String sql2 = "create table test (id int primary key, value varchar(20))"
                + " with primary_zone='TEST_ZONE'";

        cluster.doInSession(0, session -> {
            executeUpdate(sql1, session);
            executeUpdate(sql2, session);
        });

        waitForTableToStart();
    }

    private void waitForTableToStart() throws InterruptedException {
        // TODO: IGNITE-18733 - remove this wait because when a table creation query is executed, the table must be fully ready.

        BooleanSupplier tableStarted = () -> {
            int numberOfStartedRaftNodes = cluster.runningNodes()
                    .map(ItSqlPlanningOnRebalanceTest::tablePartitionIds)
                    .mapToInt(List::size)
                    .sum();
            return numberOfStartedRaftNodes == 3;
        };

        assertTrue(waitForCondition(tableStarted, 10_000), "Did not see all table RAFT nodes started");
    }

    /**
     * Returns the IDs of all table partitions that exist on the given node.
     */
    private static List<TablePartitionId> tablePartitionIds(IgniteImpl node) {
        return node.raftManager().localNodes().stream()
                .map(RaftNodeId::groupId)
                .filter(TablePartitionId.class::isInstance)
                .map(TablePartitionId.class::cast)
                .collect(toList());
    }
}
