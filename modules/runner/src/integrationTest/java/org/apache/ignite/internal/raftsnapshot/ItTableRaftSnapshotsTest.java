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

package org.apache.ignite.internal.raftsnapshot;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.hasCause;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedIn;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.stream.IntStream;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.ignite.internal.Cluster;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.Cluster.NodeKnockout;
import org.apache.ignite.internal.replicator.exception.ReplicationTimeoutException;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine;
import org.apache.ignite.internal.storage.pagememory.VolatilePageMemoryStorageEngine;
import org.apache.ignite.internal.storage.rocksdb.RocksDbStorageEngine;
import org.apache.ignite.internal.table.distributed.replicator.TablePartitionId;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.testframework.jul.NoOpHandler;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.raft.jraft.RaftGroupService;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.core.NodeImpl;
import org.apache.ignite.raft.jraft.core.Replicator;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests how RAFT snapshots installation works for table partitions.
 */
@SuppressWarnings("resource")
@ExtendWith(WorkDirectoryExtension.class)
@Timeout(60)
class ItTableRaftSnapshotsTest {
    private static final IgniteLogger LOG = Loggers.forClass(ItTableRaftSnapshotsTest.class);

    /**
     * Nodes bootstrap configuration pattern.
     *
     * <p>rpcInstallSnapshotTimeout is changed to 10 seconds so that sporadic snapshot installation failures still
     * allow tests pass thanks to retries.
     */
    private static final String NODE_BOOTSTRAP_CFG = "{\n"
            + "  \"network\": {\n"
            + "    \"port\":{},\n"
            + "    \"nodeFinder\":{\n"
            + "      \"netClusterNodes\": [ {} ]\n"
            + "    }\n"
            + "  },\n"
            + "  \"raft\": {"
            + "    \"rpcInstallSnapshotTimeout\": 10000"
            + "  }"
            + "}";

    /**
     * Marker that instructs to create a table with the default storage engine. Used in tests that are indifferent
     * to a storage engine used.
     */
    private static final String DEFAULT_STORAGE_ENGINE = "<default>";

    /**
     * {@link NodeKnockout} that is used by tests that are indifferent for the knockout strategy being used.
     */
    private static final NodeKnockout DEFAULT_KNOCKOUT = NodeKnockout.PARTITION_NETWORK;

    @WorkDirectory
    private Path workDir;

    private Cluster cluster;

    @BeforeEach
    void createCluster(TestInfo testInfo) {
        cluster = new Cluster(testInfo, workDir, NODE_BOOTSTRAP_CFG);
    }

    @AfterEach
    void shutdownCluster() {
        cluster.shutdown();
    }

    private void doInSession(int nodeIndex, Consumer<Session> action) {
        try (Session session = cluster.openSession(nodeIndex)) {
            action.accept(session);
        }
    }

    private <T> T doInSession(int nodeIndex, Function<Session, T> action) {
        try (Session session = cluster.openSession(nodeIndex)) {
            return action.apply(session);
        }
    }

    private static void executeUpdate(String sql, Session session) {
        executeUpdate(sql, session, null);
    }

    private static void executeUpdate(String sql, Session session, @Nullable Transaction transaction) {
        try (ResultSet ignored = session.execute(transaction, sql)) {
            // Do nothing, just adhere to the syntactic ceremony...
        }
    }

    /**
     * Executes the given action, retrying it up to a few times if a transient failure occurs (like node unavailability).
     */
    private static <T> T withRetry(Supplier<T> action) {
        // TODO: IGNITE-18423 remove this retry machinery when the networking bug is fixed as replication timeout seems to be caused by it.

        int maxAttempts = 4;
        int sleepMillis = 500;

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                return action.get();
            } catch (RuntimeException e) {
                if (attempt < maxAttempts && isTransientFailure(e)) {
                    LOG.warn("Attempt " + attempt + " failed, going to retry", e);
                } else {
                    throw e;
                }
            }

            try {
                Thread.sleep(sleepMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                fail("Interrupted while waiting for next attempt");
            }

            sleepMillis = sleepMillis * 2;
        }

        throw new AssertionError("Should not reach here");
    }

    private static boolean isTransientFailure(RuntimeException e) {
        return hasCause(e, ReplicationTimeoutException.class, null)
                || hasCause(e, IgniteInternalException.class, "Failed to send message to node")
                || hasCause(e, IgniteInternalCheckedException.class, "Failed to execute query, node left")
                || hasCause(e, SqlValidatorException.class, "Object 'TEST' not found");
    }

    private <T> T query(int nodeIndex, String sql, Function<ResultSet, T> extractor) {
        return doInSession(nodeIndex, session -> {
            try (ResultSet resultSet = session.execute(null, sql)) {
                return extractor.apply(resultSet);
            }
        });
    }

    private <T> T queryWithRetry(int nodeIndex, String sql, Function<ResultSet, T> extractor) {
        return withRetry(() -> query(nodeIndex, sql, extractor));
    }

    /**
     * Reads all rows from TEST table.
     */
    private static List<IgniteBiTuple<Integer, String>> readRows(ResultSet rs) {
        List<IgniteBiTuple<Integer, String>> rows = new ArrayList<>();

        while (rs.hasNext()) {
            SqlRow sqlRow = rs.next();

            rows.add(new IgniteBiTuple<>(sqlRow.intValue(0), sqlRow.stringValue(1)));
        }

        return rows;
    }

    /**
     * Tests that a leader successfully feeds a follower with a RAFT snapshot (using {@link NodeKnockout#STOP} strategy
     * to knock-out the follower to make it require a snapshot installation).
     */
    @Test
    @Disabled("Enable when the IGNITE-18170 deadlock is fixed")
    void leaderFeedsFollowerWithSnapshotWithKnockoutStop() throws Exception {
        testLeaderFeedsFollowerWithSnapshot(Cluster.NodeKnockout.STOP, DEFAULT_STORAGE_ENGINE);
    }

    /**
     * Tests that a leader successfully feeds a follower with a RAFT snapshot (using {@link NodeKnockout#PARTITION_NETWORK} strategy
     * to knock-out the follower to make it require a snapshot installation).
     */
    @Test
    void leaderFeedsFollowerWithSnapshotWithKnockoutPartitionNetwork() throws Exception {
        testLeaderFeedsFollowerWithSnapshot(Cluster.NodeKnockout.PARTITION_NETWORK, DEFAULT_STORAGE_ENGINE);
    }

    /**
     * Tests that a leader successfully feeds a follower with a RAFT snapshot (using the given {@link NodeKnockout} strategy
     * to knock-out the follower to make it require a snapshot installation and the given storage engine).
     */
    private void testLeaderFeedsFollowerWithSnapshot(NodeKnockout knockout, String storageEngine) throws Exception {
        feedNode2WithSnapshotOfOneRow(knockout, storageEngine);

        transferLeadershipOnSolePartitionTo(2);

        List<IgniteBiTuple<Integer, String>> rows = queryWithRetry(2, "select * from test", ItTableRaftSnapshotsTest::readRows);

        assertThat(rows, is(List.of(new IgniteBiTuple<>(1, "one"))));
    }

    private void feedNode2WithSnapshotOfOneRow(NodeKnockout knockout) throws InterruptedException {
        feedNode2WithSnapshotOfOneRow(knockout, DEFAULT_STORAGE_ENGINE);
    }

    private void feedNode2WithSnapshotOfOneRow(NodeKnockout knockout, String storageEngine) throws InterruptedException {
        prepareClusterForInstallingSnapshotToNode2(knockout, storageEngine);

        reanimateNode2AndWaitForSnapshotInstalled(knockout);
    }

    /**
     * Transfer the cluster to a state in which, when node 2 is reanimated from being knocked-out, the only partition
     * of the only table (called TEST) is transferred to it using RAFT snapshot installation mechanism.
     *
     * @param knockout The knock-out strategy that was used to knock-out node 2 and that will be used to reanimate it.
     * @see NodeKnockout
     */
    private void prepareClusterForInstallingSnapshotToNode2(NodeKnockout knockout) throws InterruptedException {
        prepareClusterForInstallingSnapshotToNode2(knockout, DEFAULT_STORAGE_ENGINE);
    }

    /**
     * Transfer the cluster to a state in which, when node 2 is reanimated from being knocked-out, the only partition
     * of the only table (called TEST) is transferred to it using RAFT snapshot installation mechanism.
     *
     * @param knockout The knock-out strategy that should be used to knock-out node 2.
     * @param storageEngine Storage engine for the TEST table.
     * @see NodeKnockout
     */
    private void prepareClusterForInstallingSnapshotToNode2(NodeKnockout knockout, String storageEngine) throws InterruptedException {
        cluster.startAndInit(3);

        createTestTableWith3Replicas(storageEngine);

        transferLeadershipOnSolePartitionTo(0);

        cluster.knockOutNode(2, knockout);

        doInSession(0, session -> {
            executeUpdate("insert into test(key, value) values (1, 'one')", session);
        });

        causeLogTruncationOnSolePartitionLeader();
    }

    private void createTestTableWith3Replicas(String storageEngine) throws InterruptedException {
        String sql = "create table test (key int primary key, value varchar(20))"
                + (DEFAULT_STORAGE_ENGINE.equals(storageEngine) ? "" : " engine " + storageEngine)
                + " with partitions=1, replicas=3";

        doInSession(0, session -> {
            executeUpdate(sql, session);
        });

        waitForTableToStart();
    }

    private void waitForTableToStart() throws InterruptedException {
        // TODO: IGNITE-18203 - remove this wait because when a table creation query is executed, the table must be fully ready.

        BooleanSupplier tableStarted = () -> {
            int numberOfStartedRaftNodes = cluster.aliveNodes()
                    .map(ItTableRaftSnapshotsTest::tablePartitionIds)
                    .mapToInt(List::size)
                    .sum();
            return numberOfStartedRaftNodes == 3;
        };

        assertTrue(waitForCondition(tableStarted, 10_000), "Did not see all table RAFT nodes started");
    }

    /**
     * Causes log truncation on the RAFT leader of the sole table partition that exists in the cluster.
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
     * Returns the ID of the sole table partition that exists in the cluster.
     */
    private TablePartitionId solePartitionId() {
        List<TablePartitionId> tablePartitionIds = tablePartitionIds(cluster.entryNode());

        assertThat(tablePartitionIds.size(), is(1));

        return tablePartitionIds.get(0);
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

    /**
     * Reanimates (that is, reverts the effects of a knock out) node 2 and waits until a RAFT snapshot is installed
     * on it for the sole table partition in the cluster.
     */
    private void reanimateNode2AndWaitForSnapshotInstalled(NodeKnockout knockout) throws InterruptedException {
        reanimateNodeAndWaitForSnapshotInstalled(2, knockout);
    }

    /**
     * Reanimates (that is, reverts the effects of a knock out) a node with the given index and waits until a RAFT snapshot is installed
     * on it for the sole table partition in the cluster.
     */
    private void reanimateNodeAndWaitForSnapshotInstalled(int nodeIndex, NodeKnockout knockout) throws InterruptedException {
        CountDownLatch snapshotInstalledLatch = new CountDownLatch(1);

        Logger replicatorLogger = Logger.getLogger(Replicator.class.getName());

        var handler = new NoOpHandler() {
            @Override
            public void publish(LogRecord record) {
                if (record.getMessage().matches("Node .+ received InstallSnapshotResponse from .+_" + nodeIndex + " .+ success=true")) {
                    snapshotInstalledLatch.countDown();
                }
            }
        };

        replicatorLogger.addHandler(handler);

        try {
            cluster.reanimateNode(nodeIndex, knockout);

            assertTrue(snapshotInstalledLatch.await(60, TimeUnit.SECONDS), "Did not install a snapshot in time");
        } finally {
            replicatorLogger.removeHandler(handler);
        }
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
     * Tests that, if first part of a transaction (everything before COMMIT) arrives using AppendEntries, and later the whole
     * partition state arrives in a RAFT snapshot, then the transaction is seen as committed (i.e. its effects are seen).
     *
     * <p>{@link NodeKnockout#STOP} is used to knock out the follower which will accept the snapshot.
     */
    @Test
    @Disabled("Enable when the IGNITE-18170 deadlock is resolved")
    void txSemanticsIsMaintainedWithKnockoutStop() throws Exception {
        txSemanticsIsMaintainedAfterInstallingSnapshot(Cluster.NodeKnockout.STOP);
    }

    /**
     * Tests that, if first part of a transaction (everything before COMMIT) arrives using AppendEntries, and later the whole
     * partition state arrives in a RAFT snapshot, then the transaction is seen as committed (i.e. its effects are seen).
     *
     * <p>{@link NodeKnockout#PARTITION_NETWORK} is used to knock out the follower which will accept the snapshot.
     */
    @Test
    void txSemanticsIsMaintainedWithKnockoutPartitionNetwork() throws Exception {
        txSemanticsIsMaintainedAfterInstallingSnapshot(Cluster.NodeKnockout.PARTITION_NETWORK);
    }

    /**
     * Tests that, if first part of a transaction (everything before COMMIT) arrives using AppendEntries, and later the whole
     * partition state arrives in a RAFT snapshot, then the transaction is seen as committed (i.e. its effects are seen).
     *
     * <p>The given {@link NodeKnockout} is used to knock out the follower which will accept the snapshot.
     */
    private void txSemanticsIsMaintainedAfterInstallingSnapshot(NodeKnockout knockout) throws Exception {
        cluster.startAndInit(3);

        createTestTableWith3Replicas(DEFAULT_STORAGE_ENGINE);

        transferLeadershipOnSolePartitionTo(0);

        Transaction tx = cluster.node(0).transactions().begin();

        doInSession(0, session -> {
            executeUpdate("insert into test(key, value) values (1, 'one')", session, tx);

            cluster.knockOutNode(2, knockout);

            tx.commit();
        });

        causeLogTruncationOnSolePartitionLeader();

        reanimateNode2AndWaitForSnapshotInstalled(knockout);

        transferLeadershipOnSolePartitionTo(2);

        List<IgniteBiTuple<Integer, String>> rows = queryWithRetry(2, "select * from test", ItTableRaftSnapshotsTest::readRows);

        assertThat(rows, is(List.of(new IgniteBiTuple<>(1, "one"))));
    }

    /**
     * Tests that a leader successfully feeds a follower with a RAFT snapshot on any of the supported storage engines.
     */
    @ParameterizedTest
    @ValueSource(strings = {
            RocksDbStorageEngine.ENGINE_NAME,
            PersistentPageMemoryStorageEngine.ENGINE_NAME,
            VolatilePageMemoryStorageEngine.ENGINE_NAME
    })
    void leaderFeedsFollowerWithSnapshot(String storageEngine) throws Exception {
        testLeaderFeedsFollowerWithSnapshot(DEFAULT_KNOCKOUT, storageEngine);
    }

    /**
     * Tests that entries can still be added to a follower using AppendEntries after it gets fed with a RAFT snapshot.
     */
    @Test
    @Disabled("Enable when IGNITE-18451 is fixed")
    void entriesKeepAppendedAfterSnapshotInstallation() throws Exception {
        feedNode2WithSnapshotOfOneRow(DEFAULT_KNOCKOUT);

        doInSession(0, session -> {
            executeUpdate("insert into test(key, value) values (2, 'two')", session);
        });

        transferLeadershipOnSolePartitionTo(2);

        List<IgniteBiTuple<Integer, String>> rows = queryWithRetry(2, "select * from test order by key",
                ItTableRaftSnapshotsTest::readRows);

        assertThat(rows, is(List.of(new IgniteBiTuple<>(1, "one"), new IgniteBiTuple<>(2, "two"))));
    }

    /**
     * Tests that, if commands are added to a leader while it installs a RAFT snapshot on a follower, these commands
     * reach the follower and get applied after the snapshot is installed.
     */
    @Test
    // TODO: IGNITE-18423 - enable when ReplicationTimeoutException is fixed
    @Disabled("IGNITE-18423")
    void entriesKeepAppendedDuringSnapshotInstallation() throws Exception {
        NodeKnockout knockout = DEFAULT_KNOCKOUT;

        prepareClusterForInstallingSnapshotToNode2(knockout);

        AtomicBoolean installedSnapshot = new AtomicBoolean(false);
        AtomicInteger lastLoadedKey = new AtomicInteger();

        CompletableFuture<?> loadingFuture = IgniteTestUtils.runAsync(() -> {
            for (int i = 2; !installedSnapshot.get(); i++) {
                int key = i;
                doInSession(0, session -> {
                    executeUpdate("insert into test(key, value) values (" + key + ", 'extra')", session);
                    lastLoadedKey.set(key);
                });
            }
        });

        reanimateNode2AndWaitForSnapshotInstalled(knockout);

        installedSnapshot.set(true);

        assertThat(loadingFuture, willSucceedIn(30, TimeUnit.SECONDS));

        transferLeadershipOnSolePartitionTo(2);

        List<Integer> keys = queryWithRetry(2, "select * from test order by key", ItTableRaftSnapshotsTest::readRows)
                .stream().map(IgniteBiTuple::get1).collect(toList());

        assertThat(keys, equalTo(IntStream.rangeClosed(1, lastLoadedKey.get()).boxed().collect(toList())));
    }

    /**
     * Tests that, after a node gets a RAFT snapshot installed to it, and it switches to a leader, it can act as a leader
     * (and can install a RAFT snapshot on the ex-leader).
     */
    @Test
    // TODO: IGNITE-18423 - enable when ReplicationTimeoutException is fixed
    @Disabled("IGNITE-18423")
    void nodeCanInstallSnapshotsAfterSnapshotInstalledToIt() throws Exception {
        feedNode2WithSnapshotOfOneRow(DEFAULT_KNOCKOUT);

        transferLeadershipOnSolePartitionTo(2);

        cluster.knockOutNode(0, DEFAULT_KNOCKOUT);

        doInSession(2, session -> {
            executeUpdate("insert into test(key, value) values (2, 'two')", session);
        });

        causeLogTruncationOnSolePartitionLeader();

        reanimateNodeAndWaitForSnapshotInstalled(0, DEFAULT_KNOCKOUT);

        transferLeadershipOnSolePartitionTo(0);

        List<IgniteBiTuple<Integer, String>> rows = queryWithRetry(0, "select * from test order by key",
                ItTableRaftSnapshotsTest::readRows);

        assertThat(rows, is(List.of(new IgniteBiTuple<>(1, "one"), new IgniteBiTuple<>(2, "two"))));
    }
}
