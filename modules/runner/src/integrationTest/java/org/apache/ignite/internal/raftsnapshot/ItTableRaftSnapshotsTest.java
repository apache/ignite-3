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
import static org.apache.ignite.internal.SessionUtils.executeUpdate;
import static org.apache.ignite.internal.sql.engine.ClusterPerClassIntegrationTest.getIndexConfiguration;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.hasCause;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedIn;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.ignite.internal.Cluster;
import org.apache.ignite.internal.Cluster.NodeKnockout;
import org.apache.ignite.internal.IgniteIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.replicator.command.SafeTimeSyncCommand;
import org.apache.ignite.internal.replicator.exception.ReplicationTimeoutException;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine;
import org.apache.ignite.internal.storage.pagememory.VolatilePageMemoryStorageEngine;
import org.apache.ignite.internal.storage.rocksdb.RocksDbStorageEngine;
import org.apache.ignite.internal.table.distributed.raft.snapshot.message.SnapshotMetaResponse;
import org.apache.ignite.internal.table.distributed.replicator.TablePartitionId;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.jul.NoOpHandler;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.raft.jraft.RaftGroupService;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.core.NodeImpl;
import org.apache.ignite.raft.jraft.core.Replicator;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.rpc.ActionRequest;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotExecutorImpl;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests how RAFT snapshots installation works for table partitions.
 */
@SuppressWarnings("resource")
@Timeout(90)
class ItTableRaftSnapshotsTest extends IgniteIntegrationTest {
    private static final IgniteLogger LOG = Loggers.forClass(ItTableRaftSnapshotsTest.class);

    /**
     * Nodes bootstrap configuration pattern.
     *
     * <p>rpcInstallSnapshotTimeout is changed to 10 seconds so that sporadic snapshot installation failures still
     * allow tests pass thanks to retries.
     */
    private static final String NODE_BOOTSTRAP_CFG = "{\n"
            + "  network: {\n"
            + "    port:{},\n"
            + "    nodeFinder:{\n"
            + "      netClusterNodes: [ {} ]\n"
            + "    }\n"
            + "  },\n"
            + "  raft.rpcInstallSnapshotTimeout: 10000"
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

    private Logger replicatorLogger;

    private @Nullable Handler replicaLoggerHandler;

    @BeforeEach
    void createCluster(TestInfo testInfo) {
        cluster = new Cluster(testInfo, workDir, NODE_BOOTSTRAP_CFG);

        replicatorLogger = Logger.getLogger(Replicator.class.getName());
    }

    @AfterEach
    @Timeout(60)
    void shutdownCluster() {
        if (replicaLoggerHandler != null) {
            replicatorLogger.removeHandler(replicaLoggerHandler);
        }

        cluster.shutdown();
    }

    @BeforeEach
    public void setup(TestInfo testInfo) throws Exception {
        setupBase(testInfo, workDir);
    }

    @AfterEach
    public void tearDown(TestInfo testInfo) throws Exception {
        tearDownBase(testInfo);
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

    private <T> T queryWithRetry(int nodeIndex, String sql, Function<ResultSet<SqlRow>, T> extractor) {
        return withRetry(() -> cluster.query(nodeIndex, sql, extractor));
    }

    /**
     * Reads all rows from TEST table.
     */
    private static List<IgniteBiTuple<Integer, String>> readRows(ResultSet<SqlRow> rs) {
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
    // Hangs at org.apache.ignite.internal.sql.engine.message.MessageServiceImpl.send(MessageServiceImpl.java:98)
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-19088")
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
        prepareClusterForInstallingSnapshotToNode2(knockout, storageEngine, theCluster -> {});
    }

    /**
     * Transfer the cluster to a state in which, when node 2 is reanimated from being knocked-out, the only partition
     * of the only table (called TEST) is transferred to it using RAFT snapshot installation mechanism.
     *
     * @param knockout The knock-out strategy that should be used to knock-out node 2.
     * @param storageEngine Storage engine for the TEST table.
     * @param doOnClusterAfterInit Action executed just after the cluster is started and initialized.
     * @see NodeKnockout
     */
    private void prepareClusterForInstallingSnapshotToNode2(
            NodeKnockout knockout,
            String storageEngine,
            Consumer<Cluster> doOnClusterAfterInit
    ) throws InterruptedException {
        cluster.startAndInit(3);

        doOnClusterAfterInit.accept(cluster);

        createTestTableWith3Replicas(storageEngine);

        // Prepare the scene: force node 0 to be a leader, and node 2 to be a follower.

        transferLeadershipOnSolePartitionTo(0);

        cluster.knockOutNode(2, knockout);
        System.out.println("Test::run_dml");

        cluster.doInSession(0, session -> {
            executeUpdate("insert into test(key, value) values (1, 'one')", session);
        });

        // Make sure AppendEntries from leader to follower is impossible, making the leader to use InstallSnapshot.
        causeLogTruncationOnSolePartitionLeader();
    }

    private void createTestTableWith3Replicas(String storageEngine) throws InterruptedException {
        String sql = "create table test (key int primary key, value varchar(20))"
                + (DEFAULT_STORAGE_ENGINE.equals(storageEngine) ? "" : " engine " + storageEngine)
                + " with partitions=1, replicas=3";

        cluster.doInSession(0, session -> {
            executeUpdate(sql, session);
        });

        waitForIndex(cluster::nodes, "test" + "_PK");
        System.out.println("Test::indexes_ready");

        waitForTableToStart();
    }

    protected static void waitForIndex(Supplier<List<IgniteImpl>> nodes, String indexName) throws InterruptedException {
        // FIXME: Wait for the index to be created on all nodes,
        //  this is a workaround for https://issues.apache.org/jira/browse/IGNITE-18733 to avoid missed updates to the index.
        assertTrue(waitForCondition(
                () -> nodes.get().stream().filter(Objects::nonNull)
                        .map(node -> getIndexConfiguration(node, indexName)).allMatch(Objects::nonNull),
                10_000)
        );
    }

    private void waitForTableToStart() throws InterruptedException {
        // TODO: IGNITE-18733 - remove this wait because when a table creation query is executed, the table must be fully ready.

        BooleanSupplier tableStarted = () -> {
            int numberOfStartedRaftNodes = cluster.runningNodes()
                    .map(ItTableRaftSnapshotsTest::tablePartitionIds)
                    .mapToInt(List::size)
                    .sum();
            return numberOfStartedRaftNodes == 3;
        };

        assertTrue(waitForCondition(tableStarted, 10_000), "Did not see all table RAFT nodes started");
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
     * Returns the ID of the sole table partition that exists in the cluster.
     */
    private TablePartitionId solePartitionId() {
        List<TablePartitionId> tablePartitionIds = tablePartitionIds(cluster.aliveNode());

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

        // Prepare the scene: force node 0 to be a leader, and node 2 to be a follower.
        transferLeadershipOnSolePartitionTo(0);

        Transaction tx = cluster.node(0).transactions().begin();

        cluster.doInSession(0, session -> {
            executeUpdate("insert into test(key, value) values (1, 'one')", session, tx);

            cluster.knockOutNode(2, knockout);

            tx.commit();
        });

        // Make sure AppendEntries from leader to follower is impossible, making the leader to use InstallSnapshot.
        causeLogTruncationOnSolePartitionLeader();

        reanimateNode2AndWaitForSnapshotInstalled(knockout);

        transferLeadershipOnSolePartitionTo(2);

        List<IgniteBiTuple<Integer, String>> rows = queryWithRetry(2, "select * from test", ItTableRaftSnapshotsTest::readRows);

        assertThat(rows, is(List.of(new IgniteBiTuple<>(1, "one"))));
    }

    /**
     * Tests that a leader successfully feeds a follower with a RAFT snapshot on any of the supported storage engines.
     */
    // TODO: IGNITE-18481 - make sure we don't forget to add new storage engines here
    @ParameterizedTest
    @ValueSource(strings = {
            RocksDbStorageEngine.ENGINE_NAME,
            PersistentPageMemoryStorageEngine.ENGINE_NAME,
            VolatilePageMemoryStorageEngine.ENGINE_NAME
    })
    // Hangs at org.apache.ignite.internal.sql.engine.message.MessageServiceImpl.send(MessageServiceImpl.java:98)
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-19088")
    void leaderFeedsFollowerWithSnapshot(String storageEngine) throws Exception {
        testLeaderFeedsFollowerWithSnapshot(DEFAULT_KNOCKOUT, storageEngine);
    }

    /**
     * Tests that entries can still be added to a follower using AppendEntries after it gets fed with a RAFT snapshot.
     */
    @Test
    @Disabled("Enable when IGNITE-18485 is fixed")
    void entriesKeepAppendedAfterSnapshotInstallation() throws Exception {
        feedNode2WithSnapshotOfOneRow(DEFAULT_KNOCKOUT);

        cluster.doInSession(0, session -> {
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
                cluster.doInSession(0, session -> {
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

        // The leader (0) has fed the follower (2). Now, change roles: the new leader will be node 2, it will feed node 0.

        transferLeadershipOnSolePartitionTo(2);

        cluster.knockOutNode(0, DEFAULT_KNOCKOUT);

        cluster.doInSession(2, session -> {
            executeUpdate("insert into test(key, value) values (2, 'two')", session);
        });

        // Make sure AppendEntries from leader to follower is impossible, making the leader to use InstallSnapshot.
        causeLogTruncationOnSolePartitionLeader();

        reanimateNodeAndWaitForSnapshotInstalled(0, DEFAULT_KNOCKOUT);

        transferLeadershipOnSolePartitionTo(0);

        List<IgniteBiTuple<Integer, String>> rows = queryWithRetry(0, "select * from test order by key",
                ItTableRaftSnapshotsTest::readRows);

        assertThat(rows, is(List.of(new IgniteBiTuple<>(1, "one"), new IgniteBiTuple<>(2, "two"))));
    }

    /**
     * Tests that, if a snapshot installation fails for some reason, a subsequent retry due to a timeout happens successfully.
     */
    @Test
    // Hangs at org.apache.ignite.internal.sql.engine.message.MessageServiceImpl.send(MessageServiceImpl.java:98)
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-19088")
    void snapshotInstallationRepeatsOnTimeout() throws Exception {
        prepareClusterForInstallingSnapshotToNode2(DEFAULT_KNOCKOUT, DEFAULT_STORAGE_ENGINE, theCluster -> {
            theCluster.node(0).dropMessages(dropFirstSnapshotMetaResponse());
        });

        reanimateNode2AndWaitForSnapshotInstalled(DEFAULT_KNOCKOUT);
    }

    @Test
    // Hangs at org.apache.ignite.internal.sql.engine.message.MessageServiceImpl.send(MessageServiceImpl.java:98)
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-19088")
    void snapshotInstallationRepeatsOnTimeout2() throws Exception {
        prepareClusterForInstallingSnapshotToNode2(DEFAULT_KNOCKOUT, DEFAULT_STORAGE_ENGINE, theCluster -> {
            theCluster.node(0).dropMessages(dropFirstSnapshotMetaResponse());
        });

        reanimateNode2AndWaitForSnapshotInstalled(DEFAULT_KNOCKOUT);
    }

    @Test
    // Hangs at org.apache.ignite.internal.sql.engine.message.MessageServiceImpl.send(MessageServiceImpl.java:98)
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-19088")
    void snapshotInstallationRepeatsOnTimeout3() throws Exception {
        prepareClusterForInstallingSnapshotToNode2(DEFAULT_KNOCKOUT, DEFAULT_STORAGE_ENGINE, theCluster -> {
            theCluster.node(0).dropMessages(dropFirstSnapshotMetaResponse());
        });

        reanimateNode2AndWaitForSnapshotInstalled(DEFAULT_KNOCKOUT);
    }

    private BiPredicate<String, NetworkMessage> dropFirstSnapshotMetaResponse() {
        AtomicBoolean sentSnapshotMetaResponse = new AtomicBoolean(false);

        return dropFirstSnapshotMetaResponse(sentSnapshotMetaResponse);
    }

    private BiPredicate<String, NetworkMessage> dropFirstSnapshotMetaResponse(AtomicBoolean sentSnapshotMetaResponse) {
        return (targetConsistentId, message) -> {
            if (Objects.equals(targetConsistentId, cluster.node(2).name()) && message instanceof SnapshotMetaResponse) {
                return sentSnapshotMetaResponse.compareAndSet(false, true);
            } else {
                return false;
            }
        };
    }

    private BiPredicate<String, NetworkMessage> dropSnapshotMetaResponse(CompletableFuture<Void> sentFirstSnapshotMetaResponse) {
        return (targetConsistentId, message) -> {
            if (Objects.equals(targetConsistentId, cluster.node(2).name()) && message instanceof SnapshotMetaResponse) {
                sentFirstSnapshotMetaResponse.complete(null);

                // Always drop.
                return true;
            } else {
                return false;
            }
        };
    }

    /**
     * This is a test for a tricky scenario:
     *
     * <ol>
     *     <li>First InstallSnapshot request is sent, its processing starts hanging forever (it will be cancelled on step 3</li>
     *     <li>After a timeout, second InstallSnapshot request is sent with same index+term as the first had; in JRaft, it causes
     *     a special handling (previous request processing is NOT cancelled)</li>
     *     <li>After a timeout, third InstallSnapshot request is sent with DIFFERENT index, so it cancels the first snapshot processing
     *     effectively unblocking the first thread</li>
     * </ol>
     *
     * <p>In the original JRaft implementation, after being unblocked, the first thread fails to clean up, so subsequent retries will
     * always see a phantom of an unfinished snapshot, so the snapshotting process will be jammed. Also, node stop might
     * stuck because one 'download' task will remain unfinished forever.
     */
    @Test
    // Hangs at org.apache.ignite.internal.sql.engine.message.MessageServiceImpl.send(MessageServiceImpl.java:98)
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-19088")
    void snapshotInstallTimeoutDoesNotBreakSubsequentInstallsWhenSecondAttemptIsIdenticalToFirst() throws Exception {
        AtomicBoolean snapshotInstallFailedDueToIdenticalRetry = new AtomicBoolean(false);

        Logger snapshotExecutorLogger = Logger.getLogger(SnapshotExecutorImpl.class.getName());

        var snapshotInstallFailedDueToIdenticalRetryHandler = new NoOpHandler() {
            @Override
            public void publish(LogRecord record) {
                if (record.getMessage().contains("Register DownloadingSnapshot failed: interrupted by retry installing request")) {
                    snapshotInstallFailedDueToIdenticalRetry.set(true);
                }
            }
        };

        snapshotExecutorLogger.addHandler(snapshotInstallFailedDueToIdenticalRetryHandler);

        try {
            prepareClusterForInstallingSnapshotToNode2(DEFAULT_KNOCKOUT, DEFAULT_STORAGE_ENGINE, theCluster -> {
                BiPredicate<String, NetworkMessage> dropSafeTimeUntilSecondInstallSnapshotRequestIsProcessed = (recipientId, message) ->
                        message instanceof ActionRequest
                                && ((ActionRequest) message).command() instanceof SafeTimeSyncCommand
                                && !snapshotInstallFailedDueToIdenticalRetry.get();

                theCluster.node(0).dropMessages(
                        dropFirstSnapshotMetaResponse().or(dropSafeTimeUntilSecondInstallSnapshotRequestIsProcessed)
                );

                theCluster.node(1).dropMessages(dropSafeTimeUntilSecondInstallSnapshotRequestIsProcessed);
                theCluster.node(2).dropMessages(dropSafeTimeUntilSecondInstallSnapshotRequestIsProcessed);
            });

            reanimateNode2AndWaitForSnapshotInstalled(DEFAULT_KNOCKOUT);
        } finally {
            snapshotExecutorLogger.removeHandler(snapshotInstallFailedDueToIdenticalRetryHandler);
        }
    }

    @Test
    // Hangs at org.apache.ignite.internal.sql.engine.message.MessageServiceImpl.send(MessageServiceImpl.java:98)
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-19088")
    void testChangeLeaderOnInstallSnapshotInMiddle() throws Exception {
        CompletableFuture<Void> sentSnapshotMetaResponseFormNode1Future = new CompletableFuture<>();

        prepareClusterForInstallingSnapshotToNode2(NodeKnockout.PARTITION_NETWORK, DEFAULT_STORAGE_ENGINE, cluster -> {
            // Let's hang the InstallSnapshot in the "middle" from the leader with index 1.
            cluster.node(1).dropMessages(dropSnapshotMetaResponse(sentSnapshotMetaResponseFormNode1Future));
        });

        // Change the leader and truncate its log so that InstallSnapshot occurs instead of AppendEntries.
        transferLeadershipOnSolePartitionTo(1);

        causeLogTruncationOnSolePartitionLeader();

        CompletableFuture<Void> installSnapshotSuccessfulFuture = new CompletableFuture<>();

        listenForSnapshotInstalledSuccessFromLogger(0, 2, installSnapshotSuccessfulFuture);

        // Return node 2.
        cluster.reanimateNode(2, NodeKnockout.PARTITION_NETWORK);

        // Waiting for the InstallSnapshot from node 2 to hang in the "middle".
        assertThat(sentSnapshotMetaResponseFormNode1Future, willSucceedIn(1, TimeUnit.MINUTES));

        // Change the leader to node 0.
        transferLeadershipOnSolePartitionTo(0);

        // Waiting for the InstallSnapshot successfully from node 0 to node 2.
        assertThat(installSnapshotSuccessfulFuture, willSucceedIn(1, TimeUnit.MINUTES));

        // Make sure the rebalancing is complete.
        List<IgniteBiTuple<Integer, String>> rows = queryWithRetry(2, "select * from test", ItTableRaftSnapshotsTest::readRows);

        assertThat(rows, is(List.of(new IgniteBiTuple<>(1, "one"))));
    }

    /**
     * Adds a listener for the {@link #replicatorLogger} to hear the success of the snapshot installation.
     */
    private void listenForSnapshotInstalledSuccessFromLogger(
            int nodeIndexFrom,
            int nodeIndexTo,
            CompletableFuture<Void> snapshotInstallSuccessfullyFuture
    ) {
        String regexp = "Node .+" + nodeIndexFrom + " received InstallSnapshotResponse from .+_" + nodeIndexTo + " .+ success=true";

        replicaLoggerHandler = new NoOpHandler() {
            @Override
            public void publish(LogRecord record) {
                if (record.getMessage().matches(regexp)) {
                    snapshotInstallSuccessfullyFuture.complete(null);

                    replicatorLogger.removeHandler(this);
                    replicaLoggerHandler = null;
                }
            }
        };

        replicatorLogger.addHandler(replicaLoggerHandler);
    }

    /**
     * This tests the following schenario.
     *
     * <ol>
     *     <li>
     *         A snapshot installation is started from Node A that is a leader because A does not have enough RAFT log to feed a follower
     *         with AppendEntries
     *     </li>
     *     <li>It is cancelled in the middle</li>
     *     <li>Node B is elected as a leader; B has enough log to feed the follower with AppendEntries</li>
     *     <li>The follower gets data from the leader using AppendEntries, not using InstallSnapshot</li>>
     * </ol>
     */
    @Test
    // Hangs at org.apache.ignite.internal.sql.engine.message.MessageServiceImpl.send(MessageServiceImpl.java:98)
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-19088")
    void testChangeLeaderDuringSnapshotInstallationToLeaderWithEnoughLog() throws Exception {
        CompletableFuture<Void> sentSnapshotMetaResponseFormNode0Future = new CompletableFuture<>();

        prepareClusterForInstallingSnapshotToNode2(NodeKnockout.PARTITION_NETWORK, DEFAULT_STORAGE_ENGINE, cluster -> {
            // Let's hang the InstallSnapshot in the "middle" from the leader with index 0.
            cluster.node(0).dropMessages(dropSnapshotMetaResponse(sentSnapshotMetaResponseFormNode0Future));
        });

        CompletableFuture<Void> installSnapshotSuccessfulFuture = new CompletableFuture<>();

        listenForSnapshotInstalledSuccessFromLogger(1, 2, installSnapshotSuccessfulFuture);

        // Return node 2.
        cluster.reanimateNode(2, NodeKnockout.PARTITION_NETWORK);

        // Waiting for the InstallSnapshot from node 2 to hang in the "middle".
        assertThat(sentSnapshotMetaResponseFormNode0Future, willSucceedIn(1, TimeUnit.MINUTES));

        // Change the leader to node 1.
        transferLeadershipOnSolePartitionTo(1);

        boolean replicated = waitForCondition(() -> {
            List<IgniteBiTuple<Integer, String>> rows = queryWithRetry(2, "select * from test", ItTableRaftSnapshotsTest::readRows);
            return rows.size() == 1;
        }, 20_000);

        assertTrue(replicated, "Data has not been replicated to node 2 in time");

        // No snapshot must be installed.
        assertFalse(installSnapshotSuccessfulFuture.isDone());

        // Make sure the rebalancing is complete.
        List<IgniteBiTuple<Integer, String>> rows = queryWithRetry(2, "select * from test", ItTableRaftSnapshotsTest::readRows);

        assertThat(rows, is(List.of(new IgniteBiTuple<>(1, "one"))));
    }
}
