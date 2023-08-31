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
import static org.apache.ignite.internal.testframework.IgniteTestUtils.getFieldValue;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.hasCause;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedIn;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.ConnectException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.ignite.internal.Cluster;
import org.apache.ignite.internal.IgniteIntegrationTest;
import org.apache.ignite.internal.ReplicationGroupsUtils;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.server.raft.MetastorageGroupId;
import org.apache.ignite.internal.raft.server.RaftServer;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.command.SafeTimeSyncCommand;
import org.apache.ignite.internal.replicator.exception.ReplicationTimeoutException;
import org.apache.ignite.internal.storage.StorageRebalanceException;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine;
import org.apache.ignite.internal.storage.rocksdb.RocksDbStorageEngine;
import org.apache.ignite.internal.table.distributed.raft.snapshot.incoming.IncomingSnapshotCopier;
import org.apache.ignite.internal.table.distributed.raft.snapshot.message.SnapshotMetaResponse;
import org.apache.ignite.internal.test.WatchListenerInhibitor;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.log4j2.LogInspector;
import org.apache.ignite.internal.testframework.log4j2.LogInspector.Handler;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.raft.jraft.RaftGroupService;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.core.Replicator;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.rpc.ActionRequest;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RaftRpcFactory;
import org.apache.ignite.raft.jraft.rpc.RaftServerService;
import org.apache.ignite.raft.jraft.rpc.RpcProcessor;
import org.apache.ignite.raft.jraft.rpc.RpcRequestClosure;
import org.apache.ignite.raft.jraft.rpc.RpcRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.AppendEntriesRequest;
import org.apache.ignite.raft.jraft.rpc.RpcServer;
import org.apache.ignite.raft.jraft.rpc.impl.IgniteRpcServer;
import org.apache.ignite.raft.jraft.rpc.impl.core.AppendEntriesRequestProcessor;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotExecutorImpl;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
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
            + "  raft.rpcInstallSnapshotTimeout: 10000,\n"
            + "  clientConnector.port: {}\n"
            + "}";

    /**
     * Marker that instructs to create a table with the default storage engine. Used in tests that are indifferent
     * to a storage engine used.
     */
    private static final String DEFAULT_STORAGE_ENGINE = "<default>";

    @WorkDirectory
    private Path workDir;

    private Cluster cluster;

    private LogInspector replicatorLogInspector;

    private LogInspector copierLogInspector;

    @BeforeEach
    void createCluster(TestInfo testInfo) {
        cluster = new Cluster(testInfo, workDir, NODE_BOOTSTRAP_CFG);

        replicatorLogInspector = LogInspector.create(Replicator.class, true);
        copierLogInspector = LogInspector.create(IncomingSnapshotCopier.class, true);
    }

    @AfterEach
    @Timeout(60)
    void shutdownCluster() {
        replicatorLogInspector.stop();
        copierLogInspector.stop();

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
     * Executes the given action, retrying it up to a few times if a transient failure occurs (like node unavailability) or
     * until {@code shouldStop} returns {@code true}, in that case this method throws {@link UnableToRetry} exception.
     */
    private static <T> T withRetry(Supplier<T> action, Predicate<RuntimeException> shouldStop) {
        // The following allows to retry for up to 16 seconds (we need so much time to account
        // for a node restart).
        int maxAttempts = 10;
        float backoffFactor = 1.3f;
        int sleepMillis = 500;

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                return action.get();
            } catch (RuntimeException e) {
                if (shouldStop.test(e)) {
                    throw new UnableToRetry(e);
                }
                if (attempt < maxAttempts && isTransientFailure(e)) {
                    LOG.warn("Attempt {} failed, going to retry", e, attempt);
                } else {
                    LOG.error("Attempt {} failed, not going to retry anymore, rethrowing", e, attempt);

                    throw e;
                }
            }

            try {
                Thread.sleep(sleepMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                fail("Interrupted while waiting for next attempt");
            }

            //noinspection NumericCastThatLosesPrecision
            sleepMillis = (int) (sleepMillis * backoffFactor);
        }

        throw new AssertionError("Should not reach here");
    }

    /**
     * Executes the given UPDATE/INSERT statement until it succeed or receives duplicate key error.
     */
    private void executeDmlWithRetry(int nodeIndex, String statement) {
        // We should retry a DML statement until we either succeed or receive a duplicate key error.
        // The number of attempts is bounded because we know that node is going to recover.
        Predicate<RuntimeException> stopOnDuplicateKeyError = (e) -> {
            if (e instanceof IgniteException) {
                IgniteException ie = (IgniteException) e;
                return ie.code() == Sql.CONSTRAINT_VIOLATION_ERR;
            } else {
                return false;
            }
        };

        try {
            withRetry(() -> {
                cluster.doInSession(nodeIndex, session -> {
                    executeUpdate(statement, session);
                });
                return null;
            }, stopOnDuplicateKeyError);

        } catch (UnableToRetry ignore) {
            // Duplicate key exception was caught.
        }
    }

    private static boolean isTransientFailure(RuntimeException e) {
        return hasCause(e, ReplicationTimeoutException.class, null)
                || hasCause(e, IgniteInternalException.class, "Failed to send message to node")
                || hasCause(e, IgniteInternalCheckedException.class, "Failed to execute query, node left")
                || hasCause(e, SqlValidatorException.class, "Object 'TEST' not found")
                // TODO: remove after https://issues.apache.org/jira/browse/IGNITE-18848 is implemented.
                || hasCause(e, StorageRebalanceException.class, "process of rebalancing")
                || hasCause(e, ConnectException.class, null);
    }

    private <T> T queryWithRetry(int nodeIndex, String sql, Function<ResultSet<SqlRow>, T> extractor) {
        Predicate<RuntimeException> retryForever = (e) -> false;
        // TODO: IGNITE-18423 remove this retry machinery when the networking bug is fixed as replication timeout seems to be caused by it.
        return withRetry(() -> cluster.query(nodeIndex, sql, extractor), retryForever);
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
     * Tests that a leader successfully feeds a follower with a RAFT snapshot on any of the supported storage engines.
     */
    // TODO: IGNITE-18481 - make sure we don't forget to add new storage engines here
    @ParameterizedTest
    @ValueSource(strings = {
            RocksDbStorageEngine.ENGINE_NAME,
            PersistentPageMemoryStorageEngine.ENGINE_NAME
            // TODO: uncomment when https://issues.apache.org/jira/browse/IGNITE-19234 is fixed
//            VolatilePageMemoryStorageEngine.ENGINE_NAME
    })
    void leaderFeedsFollowerWithSnapshot(String storageEngine) throws Exception {
        testLeaderFeedsFollowerWithSnapshot(storageEngine);
    }

    /**
     * Tests that a leader successfully feeds a follower with a RAFT snapshot (using the given storage engine).
     */
    private void testLeaderFeedsFollowerWithSnapshot(String storageEngine) throws Exception {
        feedNode2WithSnapshotOfOneRow(storageEngine);

        transferLeadershipOnSolePartitionTo(2);

        List<IgniteBiTuple<Integer, String>> rows = queryWithRetry(2, "select * from test", ItTableRaftSnapshotsTest::readRows);

        assertThat(rows, is(List.of(new IgniteBiTuple<>(1, "one"))));
    }

    private void feedNode2WithSnapshotOfOneRow() throws InterruptedException {
        feedNode2WithSnapshotOfOneRow(DEFAULT_STORAGE_ENGINE);
    }

    private void feedNode2WithSnapshotOfOneRow(String storageEngine) throws InterruptedException {
        prepareClusterForInstallingSnapshotToNode2(storageEngine);

        reanimateNode2AndWaitForSnapshotInstalled();
    }

    /**
     * Transfer the cluster to a state in which, when node 2 is reanimated from being knocked-out, the only partition
     * of the only table (called TEST) is transferred to it using RAFT snapshot installation mechanism.
     */
    private void prepareClusterForInstallingSnapshotToNode2() throws InterruptedException {
        prepareClusterForInstallingSnapshotToNode2(DEFAULT_STORAGE_ENGINE);
    }

    /**
     * Transfer the cluster to a state in which, when node 2 is reanimated from being knocked-out, the only partition
     * of the only table (called TEST) is transferred to it using RAFT snapshot installation mechanism.
     *
     * @param storageEngine Storage engine for the TEST table.
     */
    private void prepareClusterForInstallingSnapshotToNode2(String storageEngine) throws InterruptedException {
        prepareClusterForInstallingSnapshotToNode2(storageEngine, theCluster -> {});
    }

    /**
     * Transfer the cluster to a state in which, when node 2 is reanimated from being knocked-out, the only partition
     * of the only table (called TEST) is transferred to it using RAFT snapshot installation mechanism.
     *
     * @param storageEngine Storage engine for the TEST table.
     * @param doOnClusterAfterInit Action executed just after the cluster is started and initialized.
     */
    private void prepareClusterForInstallingSnapshotToNode2(
            String storageEngine,
            Consumer<Cluster> doOnClusterAfterInit
    ) throws InterruptedException {
        cluster.startAndInit(3);

        doOnClusterAfterInit.accept(cluster);

        createTestTableWith3Replicas(storageEngine);

        // Prepare the scene: force node 0 to be a leader, and node 2 to be a follower.

        transferLeadershipOnSolePartitionTo(0);

        knockoutNode(2);

        executeDmlWithRetry(0, "insert into test(key, val) values (1, 'one')");

        // Make sure AppendEntries from leader to follower is impossible, making the leader to use InstallSnapshot.
        causeLogTruncationOnSolePartitionLeader(0);
    }

    private void knockoutNode(int nodeIndex) {
        cluster.stopNode(nodeIndex);

        LOG.info("Node {} knocked out", nodeIndex);
    }

    private void createTestTableWith3Replicas(String storageEngine) throws InterruptedException {
        String zoneSql = "create zone test_zone"
                + (DEFAULT_STORAGE_ENGINE.equals(storageEngine) ? "" : " engine " + storageEngine)
                + " with partitions=1, replicas=3;";
        String sql = "create table test (key int primary key, val varchar(20))"
                + " with primary_zone='TEST_ZONE'";

        cluster.doInSession(0, session -> {
            executeUpdate(zoneSql, session);
            executeUpdate(sql, session);
        });

        waitForTableToStart();
    }

    private void waitForTableToStart() throws InterruptedException {
        // TODO: IGNITE-18733 - remove this wait because when a table creation query is executed, the table must be fully ready.

        BooleanSupplier tableStarted = () -> {
            int numberOfStartedRaftNodes = cluster.runningNodes()
                    .map(ReplicationGroupsUtils::tablePartitionIds)
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
    private void causeLogTruncationOnSolePartitionLeader(int expectedLeaderNodeIndex) throws InterruptedException {
        // Doing this twice because first snapshot creation does not trigger log truncation.
        doSnapshotOnSolePartitionLeader(expectedLeaderNodeIndex);
        doSnapshotOnSolePartitionLeader(expectedLeaderNodeIndex);
    }

    /**
     * Causes a RAFT snapshot to be taken on the RAFT leader of the sole table partition that exists in the cluster.
     */
    private void doSnapshotOnSolePartitionLeader(int expectedLeaderNodeIndex) throws InterruptedException {
        TablePartitionId tablePartitionId = cluster.solePartitionId();

        doSnapshotOn(tablePartitionId, expectedLeaderNodeIndex);
    }

    /**
     * Takes a RAFT snapshot on the leader of the RAFT group corresponding to the given table partition.
     */
    private void doSnapshotOn(TablePartitionId tablePartitionId, int expectedLeaderNodeIndex) throws InterruptedException {
        RaftGroupService raftGroupService = cluster.leaderServiceFor(tablePartitionId);

        assertThat(
                "Unexpected leadership change",
                raftGroupService.getServerId().getConsistentId(), is(cluster.node(expectedLeaderNodeIndex).name())
        );

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
    private void reanimateNode2AndWaitForSnapshotInstalled() throws InterruptedException {
        reanimateNodeAndWaitForSnapshotInstalled(2);
    }

    /**
     * Reanimates (that is, reverts the effects of a knock out) a node with the given index and waits until a RAFT snapshot is installed
     * on it for the sole table partition in the cluster.
     */
    private void reanimateNodeAndWaitForSnapshotInstalled(int nodeIndex) throws InterruptedException {
        CountDownLatch snapshotInstalledLatch = snapshotInstalledLatch(nodeIndex);

        reanimateNode(nodeIndex);

        assertTrue(snapshotInstalledLatch.await(60, TimeUnit.SECONDS), "Did not install a snapshot in time");
    }

    private CountDownLatch snapshotInstalledLatch(int nodeIndex) {
        CountDownLatch snapshotInstalledLatch = new CountDownLatch(1);

        replicatorLogInspector.addHandler(
                evt -> evt.getMessage().getFormattedMessage().matches(
                        "Node \\S+ received InstallSnapshotResponse from \\S+_" + nodeIndex + " .+ success=true"),
                snapshotInstalledLatch::countDown
        );

        return snapshotInstalledLatch;
    }

    private void reanimateNode(int nodeIndex) {
        cluster.startNode(nodeIndex);
    }

    private void transferLeadershipOnSolePartitionTo(int nodeIndex) throws InterruptedException {
        cluster.transferLeadershipTo(nodeIndex, cluster.solePartitionId());
    }

    /**
     * Tests that, if first part of a transaction (everything before COMMIT) arrives using AppendEntries, and later the whole
     * partition state arrives in a RAFT snapshot, then the transaction is seen as committed (i.e. its effects are seen).
     */
    @Test
    void txSemanticsIsMaintained() throws Exception {
        txSemanticsIsMaintainedAfterInstallingSnapshot();
    }

    /**
     * Tests that, if first part of a transaction (everything before COMMIT) arrives using AppendEntries, and later the whole
     * partition state arrives in a RAFT snapshot, then the transaction is seen as committed (i.e. its effects are seen).
     */
    private void txSemanticsIsMaintainedAfterInstallingSnapshot() throws Exception {
        cluster.startAndInit(3);

        createTestTableWith3Replicas(DEFAULT_STORAGE_ENGINE);

        // Prepare the scene: force node 0 to be a leader, and node 2 to be a follower.
        transferLeadershipOnSolePartitionTo(0);

        Transaction tx = cluster.node(0).transactions().begin();

        cluster.doInSession(0, session -> {
            executeUpdate("insert into test(key, val) values (1, 'one')", session, tx);

            knockoutNode(2);

            tx.commit();
        });

        // Make sure AppendEntries from leader to follower is impossible, making the leader to use InstallSnapshot.
        causeLogTruncationOnSolePartitionLeader(0);

        reanimateNode2AndWaitForSnapshotInstalled();

        transferLeadershipOnSolePartitionTo(2);

        List<IgniteBiTuple<Integer, String>> rows = queryWithRetry(2, "select * from test", ItTableRaftSnapshotsTest::readRows);

        assertThat(rows, is(List.of(new IgniteBiTuple<>(1, "one"))));
    }

    /**
     * Tests that entries can still be added to a follower using AppendEntries after it gets fed with a RAFT snapshot.
     */
    @Test
    void entriesKeepAppendedAfterSnapshotInstallation() throws Exception {
        feedNode2WithSnapshotOfOneRow();

        // this should be possibly replaced with executeDmlWithRetry.
        cluster.doInSession(0, session -> {
            executeUpdate("insert into test(key, val) values (2, 'two')", session);
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
        prepareClusterForInstallingSnapshotToNode2();

        AtomicBoolean installedSnapshot = new AtomicBoolean(false);
        AtomicInteger lastLoadedKey = new AtomicInteger();

        CompletableFuture<?> loadingFuture = IgniteTestUtils.runAsync(() -> {
            for (int i = 2; !installedSnapshot.get(); i++) {
                int key = i;
                cluster.doInSession(0, session -> {
                    executeUpdate("insert into test(key, val) values (" + key + ", 'extra')", session);
                    lastLoadedKey.set(key);
                });
            }
        });

        reanimateNode2AndWaitForSnapshotInstalled();

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
        feedNode2WithSnapshotOfOneRow();

        // The leader (0) has fed the follower (2). Now, change roles: the new leader will be node 2, it will feed node 0.

        transferLeadershipOnSolePartitionTo(2);

        knockoutNode(0);

        cluster.doInSession(2, session -> {
            executeUpdate("insert into test(key, val) values (2, 'two')", session);
        });

        // Make sure AppendEntries from leader to follower is impossible, making the leader to use InstallSnapshot.
        causeLogTruncationOnSolePartitionLeader(2);

        reanimateNodeAndWaitForSnapshotInstalled(0);

        transferLeadershipOnSolePartitionTo(0);

        List<IgniteBiTuple<Integer, String>> rows = queryWithRetry(0, "select * from test order by key",
                ItTableRaftSnapshotsTest::readRows);

        assertThat(rows, is(List.of(new IgniteBiTuple<>(1, "one"), new IgniteBiTuple<>(2, "two"))));
    }

    /**
     * Tests that, if a snapshot installation fails for some reason, a subsequent retry due to a timeout happens successfully.
     */
    @Test
    void snapshotInstallationRepeatsOnTimeout() throws Exception {
        prepareClusterForInstallingSnapshotToNode2(DEFAULT_STORAGE_ENGINE, theCluster -> {
            theCluster.node(0).dropMessages(dropFirstSnapshotMetaResponse());
        });

        reanimateNode2AndWaitForSnapshotInstalled();
    }

    private BiPredicate<String, NetworkMessage> dropFirstSnapshotMetaResponse() {
        AtomicBoolean sentSnapshotMetaResponse = new AtomicBoolean(false);

        return dropFirstSnapshotMetaResponse(sentSnapshotMetaResponse);
    }

    private BiPredicate<String, NetworkMessage> dropFirstSnapshotMetaResponse(AtomicBoolean sentSnapshotMetaResponse) {
        String node2Name = cluster.node(2).name();

        return (targetConsistentId, message) -> {
            if (Objects.equals(targetConsistentId, node2Name) && message instanceof SnapshotMetaResponse) {
                return sentSnapshotMetaResponse.compareAndSet(false, true);
            } else {
                return false;
            }
        };
    }

    private BiPredicate<String, NetworkMessage> dropSnapshotMetaResponse(CompletableFuture<Void> sentFirstSnapshotMetaResponse) {
        String node2Name = cluster.node(2).name();

        return (targetConsistentId, message) -> {
            if (Objects.equals(targetConsistentId, node2Name) && message instanceof SnapshotMetaResponse) {
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
    void snapshotInstallTimeoutDoesNotBreakSubsequentInstallsWhenSecondAttemptIsIdenticalToFirst() throws Exception {
        AtomicBoolean snapshotInstallFailedDueToIdenticalRetry = new AtomicBoolean(false);

        LogInspector snapshotExecutorLogInspector = LogInspector.create(SnapshotExecutorImpl.class);

        Handler snapshotInstallFailedDueToIdenticalRetryHandler =
                snapshotExecutorLogInspector.addHandler(
                        evt -> evt.getMessage().getFormattedMessage().contains(
                                "Register DownloadingSnapshot failed: interrupted by retry installing request"),
                        () -> snapshotInstallFailedDueToIdenticalRetry.set(true));

        snapshotExecutorLogInspector.start();

        try {
            prepareClusterForInstallingSnapshotToNode2(DEFAULT_STORAGE_ENGINE, theCluster -> {
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

            reanimateNode2AndWaitForSnapshotInstalled();
        } finally {
            snapshotExecutorLogInspector.removeHandler(snapshotInstallFailedDueToIdenticalRetryHandler);
            snapshotExecutorLogInspector.stop();
        }
    }

    @Test
    void testChangeLeaderOnInstallSnapshotInMiddle() throws Exception {
        CompletableFuture<Void> sentSnapshotMetaResponseFormNode1Future = new CompletableFuture<>();

        prepareClusterForInstallingSnapshotToNode2(DEFAULT_STORAGE_ENGINE, cluster -> {
            // Let's hang the InstallSnapshot in the "middle" from the leader with index 1.
            cluster.node(1).dropMessages(dropSnapshotMetaResponse(sentSnapshotMetaResponseFormNode1Future));
        });

        // Change the leader and truncate its log so that InstallSnapshot occurs instead of AppendEntries.
        transferLeadershipOnSolePartitionTo(1);

        causeLogTruncationOnSolePartitionLeader(1);

        CompletableFuture<Void> installSnapshotSuccessfulFuture = new CompletableFuture<>();

        listenForSnapshotInstalledSuccessFromLogger(0, 2, installSnapshotSuccessfulFuture);

        // Return node 2.
        reanimateNode(2);

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
     * Adds a listener for the {@link #replicatorLogInspector} to hear the success of the snapshot installation.
     */
    private void listenForSnapshotInstalledSuccessFromLogger(
            int nodeIndexFrom,
            int nodeIndexTo,
            CompletableFuture<Void> snapshotInstallSuccessfullyFuture
    ) {
        String regexp = "Node \\S+" + nodeIndexFrom + " received InstallSnapshotResponse from \\S+_" + nodeIndexTo + " .+ success=true";

        replicatorLogInspector.addHandler(
                evt -> evt.getMessage().getFormattedMessage().matches(regexp),
                () -> snapshotInstallSuccessfullyFuture.complete(null)
        );
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
    void testChangeLeaderDuringSnapshotInstallationToLeaderWithEnoughLog() throws Exception {
        CompletableFuture<Void> sentSnapshotMetaResponseFormNode0Future = new CompletableFuture<>();

        prepareClusterForInstallingSnapshotToNode2(DEFAULT_STORAGE_ENGINE, cluster -> {
            // Let's hang the InstallSnapshot in the "middle" from the leader with index 0.
            cluster.node(0).dropMessages(dropSnapshotMetaResponse(sentSnapshotMetaResponseFormNode0Future));
        });

        CompletableFuture<Void> installSnapshotSuccessfulFuture = new CompletableFuture<>();

        listenForSnapshotInstalledSuccessFromLogger(1, 2, installSnapshotSuccessfulFuture);

        // Return node 2.
        reanimateNode(2);

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

    /**
     * The replication mechanism must not replicate commands for which schemas are not yet available on the node
     * to which replication happens (in Raft, it means that followers/learners cannot receive commands that they
     * cannot execute without waiting for schemas). This method tests that snapshots bringing such commands are
     * rejected, and that, when metadata catches up, the snapshot gets successfully installed.
     */
    @Test
    void laggingSchemasOnFollowerPreventSnapshotInstallation() throws Exception {
        cluster.startAndInit(3);

        createTestTableWith3Replicas(DEFAULT_STORAGE_ENGINE);

        // Prepare the scene: force node 0 to be a leader, and node 2 to be a follower.
        final int leaderIndex = 0;
        final int followerIndex = 2;

        transferLeadershipOnSolePartitionTo(leaderIndex);
        cluster.transferLeadershipTo(leaderIndex, MetastorageGroupId.INSTANCE);

        // Block AppendEntries from being accepted on the follower so that the leader will have to use a snapshot.
        blockIncomingAppendEntriesAt(followerIndex);

        // Inhibit the MetaStorage on the follower to make snapshots not eligible for installation.
        WatchListenerInhibitor listenerInhibitor = inhibitMetastorageListenersAt(followerIndex);

        try {
            // Add some data in a schema that is not yet available on the follower
            updateTableSchemaAt(leaderIndex);
            putToTableAt(leaderIndex);

            CountDownLatch installationRejected = installationRejectedLatch();
            CountDownLatch snapshotInstalled = snapshotInstalledLatch(followerIndex);

            // Force InstallSnapshot to be used.
            causeLogTruncationOnSolePartitionLeader(leaderIndex);

            assertTrue(installationRejected.await(20, TimeUnit.SECONDS), "Did not see snapshot installation rejection");

            assertThat("Snapshot was installed before unblocking", snapshotInstalled.getCount(), is(not(0L)));

            listenerInhibitor.stopInhibit();

            assertTrue(snapshotInstalled.await(20, TimeUnit.SECONDS), "Did not see a snapshot installed");
        } finally {
            listenerInhibitor.stopInhibit();
        }
    }

    private void updateTableSchemaAt(int nodeIndex) {
        cluster.doInSession(nodeIndex, session -> {
            session.execute(null, "alter table test add column added int");
        });
    }

    private void putToTableAt(int nodeIndex) {
        KeyValueView<Tuple, Tuple> kvView = cluster.node(nodeIndex)
                .tables()
                .table("test")
                .keyValueView();
        kvView.put(null, Tuple.create().set("key", 1), Tuple.create().set("val", "one"));
    }

    private void blockIncomingAppendEntriesAt(int nodeIndex) {
        BlockingAppendEntriesRequestProcessor blockingProcessorOnFollower = installBlockingAppendEntriesProcessor(nodeIndex);

        blockingProcessorOnFollower.startBlocking();
    }

    private WatchListenerInhibitor inhibitMetastorageListenersAt(int nodeIndex) {
        IgniteImpl nodeToInhibitMetaStorage = cluster.node(nodeIndex);

        WatchListenerInhibitor listenerInhibitor = WatchListenerInhibitor.metastorageEventsInhibitor(nodeToInhibitMetaStorage);
        listenerInhibitor.startInhibit();

        return listenerInhibitor;
    }

    private CountDownLatch installationRejectedLatch() {
        CountDownLatch installationRejected = new CountDownLatch(1);

        copierLogInspector.addHandler(
                event -> event.getMessage().getFormattedMessage().startsWith("Metadata not yet available, rejecting snapshot installation"),
                installationRejected::countDown
        );

        return installationRejected;
    }

    private BlockingAppendEntriesRequestProcessor installBlockingAppendEntriesProcessor(int nodeIndex) {
        RaftServer raftServer = cluster.node(nodeIndex).raftManager().server();
        RpcServer<?> rpcServer = getFieldValue(raftServer, JraftServerImpl.class, "rpcServer");
        Map<String, RpcProcessor<?>> processors = getFieldValue(rpcServer, IgniteRpcServer.class, "processors");

        AppendEntriesRequestProcessor originalProcessor =
                (AppendEntriesRequestProcessor) processors.get(AppendEntriesRequest.class.getName());
        Executor appenderExecutor = getFieldValue(originalProcessor, RpcRequestProcessor.class, "executor");
        RaftMessagesFactory raftMessagesFactory = getFieldValue(originalProcessor, RpcRequestProcessor.class, "msgFactory");

        BlockingAppendEntriesRequestProcessor blockingProcessor = new BlockingAppendEntriesRequestProcessor(
                appenderExecutor,
                raftMessagesFactory,
                cluster.solePartitionId().toString()
        );

        rpcServer.registerProcessor(blockingProcessor);

        return blockingProcessor;
    }

    /**
     * This exception is thrown to indicate that an operation can not possibly succeed after some error condition.
     * For example there is no reason to retry an operation that inserts a certain key after receiving a duplicate key error.
     */
    private static final class UnableToRetry extends RuntimeException {

        private static final long serialVersionUID = -504618429083573198L;

        private UnableToRetry(Throwable cause) {
            super(cause);
        }
    }

    /**
     * {@link AppendEntriesRequestProcessor} that, when blocking is enabled, blocks all AppendEntriesRequests of
     * the given group (that is, returns EBUSY error code, which makes JRaft repeat them).
     */
    private static class BlockingAppendEntriesRequestProcessor extends AppendEntriesRequestProcessor {
        private final String idOfGroupToBlock;
        private volatile boolean block;

        public BlockingAppendEntriesRequestProcessor(Executor executor,
                RaftMessagesFactory msgFactory, String idOfGroupToBlock) {
            super(executor, msgFactory);

            this.idOfGroupToBlock = idOfGroupToBlock;
        }

        @Override
        public Message processRequest0(RaftServerService service, AppendEntriesRequest request, RpcRequestClosure done) {
            if (block && idOfGroupToBlock.equals(request.groupId())) {
                return RaftRpcFactory.DEFAULT //
                    .newResponse(done.getMsgFactory(), RaftError.EBUSY,
                            "Blocking AppendEntries on '%s'.", request.groupId());
            }

            return super.processRequest0(service, request, done);
        }

        public void startBlocking() {
            block = true;
        }
    }
}
