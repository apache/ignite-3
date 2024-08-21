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
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIMEM_PROFILE_NAME;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIPERSIST_PROFILE_NAME;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_ROCKSDB_PROFILE_NAME;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.raft.util.OptimizedMarshaller.NO_POOL;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.executeUpdate;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.getFieldValue;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedIn;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.Cluster;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.cluster.management.CmgGroupId;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.server.raft.MetastorageGroupId;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.internal.partition.replicator.network.raft.SnapshotMetaResponse;
import org.apache.ignite.internal.raft.server.RaftServer;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.command.SafeTimeSyncCommand;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine;
import org.apache.ignite.internal.storage.pagememory.VolatilePageMemoryStorageEngine;
import org.apache.ignite.internal.storage.rocksdb.RocksDbStorageEngine;
import org.apache.ignite.internal.table.distributed.raft.snapshot.incoming.IncomingSnapshotCopier;
import org.apache.ignite.internal.table.distributed.schema.PartitionCommandsMarshallerImpl;
import org.apache.ignite.internal.test.WatchListenerInhibitor;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.testframework.log4j2.LogInspector;
import org.apache.ignite.internal.testframework.log4j2.LogInspector.Handler;
import org.apache.ignite.raft.jraft.RaftGroupService;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.core.Replicator;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RaftRpcFactory;
import org.apache.ignite.raft.jraft.rpc.RaftServerService;
import org.apache.ignite.raft.jraft.rpc.RpcProcessor;
import org.apache.ignite.raft.jraft.rpc.RpcRequestClosure;
import org.apache.ignite.raft.jraft.rpc.RpcRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.AppendEntriesRequest;
import org.apache.ignite.raft.jraft.rpc.RpcServer;
import org.apache.ignite.raft.jraft.rpc.WriteActionRequest;
import org.apache.ignite.raft.jraft.rpc.impl.IgniteRpcServer;
import org.apache.ignite.raft.jraft.rpc.impl.core.AppendEntriesRequestProcessor;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotExecutorImpl;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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
@Timeout(90)
@ExtendWith(WorkDirectoryExtension.class)
class ItTableRaftSnapshotsTest extends BaseIgniteAbstractTest {
    private static final IgniteLogger LOG = Loggers.forClass(ItTableRaftSnapshotsTest.class);

    /**
     * Nodes bootstrap configuration pattern.
     *
     * <p>rpcInstallSnapshotTimeout is changed to 10 seconds so that sporadic snapshot installation failures still
     * allow tests pass thanks to retries.
     */
    private static final String NODE_BOOTSTRAP_CFG = "ignite {\n"
            + "  network: {\n"
            + "    port: {},\n"
            + "    nodeFinder.netClusterNodes: [ {} ]\n"
            + "  },\n"
            + "  raft.rpcInstallSnapshotTimeout: 10000,\n"
            + "  storage.profiles: {"
            + "        " + DEFAULT_AIPERSIST_PROFILE_NAME + ".engine: aipersist, "
            + "        " + DEFAULT_AIMEM_PROFILE_NAME + ".engine: aimem, "
            + "        " + DEFAULT_ROCKSDB_PROFILE_NAME + ".engine: rocksdb"
            + "  },\n"
            + "  clientConnector.port: {},\n"
            + "  rest.port: {}\n"
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
    void leaderFeedsFollowerWithSnapshot(String storageEngine) throws Exception {
        testLeaderFeedsFollowerWithSnapshot(storageEngine);
    }

    /**
     * Tests that a leader successfully feeds a follower with a RAFT snapshot (using the given storage engine).
     */
    private void testLeaderFeedsFollowerWithSnapshot(String storageEngine) throws Exception {
        feedNode2WithSnapshotOfOneRow(storageEngine);

        transferLeadershipOnSolePartitionTo(2);

        assertThat(getFromNode(2, 1), is("one"));
    }

    private @Nullable String getFromNode(int clusterNode, int key) {
        return tableViewAt(clusterNode).get(null, key);
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
        startAndInitCluster();

        doOnClusterAfterInit.accept(cluster);

        createTestTableWith3Replicas(storageEngine);

        // Prepare the scene: force node 0 to be a leader, and node 2 to be a follower.

        transferLeadershipOnSolePartitionTo(0);

        knockoutNode(2);

        putToNode(0, 1, "one");

        // Make sure AppendEntries from leader to follower is impossible, making the leader to use InstallSnapshot.
        causeLogTruncationOnSolePartitionLeader(0);
    }

    private void startAndInitCluster() {
        cluster.startAndInit(3, IntStream.range(0, 3).toArray());
    }

    private void putToNode(int nodeIndex, int key, String value) {
        putToNode(nodeIndex, key, value, null);
    }

    private void putToNode(int nodeIndex, int key, String value, @Nullable Transaction tx) {
        tableViewAt(nodeIndex).put(tx, key, value);
    }

    private KeyValueView<Integer, String> tableViewAt(int nodeIndex) {
        Table table = cluster.node(nodeIndex).tables().table("test");
        return table.keyValueView(Integer.class, String.class);
    }

    private void knockoutNode(int nodeIndex) {
        cluster.stopNode(nodeIndex);

        LOG.info("Node {} knocked out", nodeIndex);
    }

    private void createTestTableWith3Replicas(String storageEngine) {
        String storageProfile =
                DEFAULT_STORAGE_ENGINE.equals(storageEngine) ? DEFAULT_STORAGE_PROFILE : "default_" + storageEngine.toLowerCase();

        String zoneSql = "create zone test_zone with partitions=1, replicas=3, storage_profiles='" + storageProfile + "';";

        String sql = "create table test (key int primary key, val varchar(20))"
                + " with primary_zone='TEST_ZONE', storage_profile='" + storageProfile + "';";

        cluster.doInSession(0, session -> {
            executeUpdate(zoneSql, session);
            executeUpdate(sql, session);
        });
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
                "Unexpected leadership change on group: " + tablePartitionId,
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
        startAndInitCluster();

        createTestTableWith3Replicas(DEFAULT_STORAGE_ENGINE);

        // Prepare the scene: force node 0 to be a leader, and node 2 to be a follower.
        transferLeadershipOnSolePartitionTo(0);

        Transaction tx = cluster.node(0).transactions().begin();

        putToNode(0, 1, "one", tx);

        knockoutNode(2);

        tx.commit();

        // Make sure AppendEntries from leader to follower is impossible, making the leader to use InstallSnapshot.
        causeLogTruncationOnSolePartitionLeader(0);

        reanimateNode2AndWaitForSnapshotInstalled();

        transferLeadershipOnSolePartitionTo(2);

        assertThat(getFromNode(2, 1), is("one"));
    }

    /**
     * Tests that entries can still be added to a follower using AppendEntries after it gets fed with a RAFT snapshot.
     */
    @Test
    void entriesKeepAppendedAfterSnapshotInstallation() throws Exception {
        feedNode2WithSnapshotOfOneRow();

        putToNode(0, 2, "two");

        transferLeadershipOnSolePartitionTo(2);

        assertThat(getFromNode(0, 1), is("one"));
        assertThat(getFromNode(0, 2), is("two"));
    }

    /**
     * Tests that, if commands are added to a leader while it installs a RAFT snapshot on a follower, these commands
     * reach the follower and get applied after the snapshot is installed.
     */
    @Test
    void entriesKeepAppendedDuringSnapshotInstallation() throws Exception {
        prepareClusterForInstallingSnapshotToNode2();

        AtomicBoolean installedSnapshot = new AtomicBoolean(false);
        AtomicInteger lastLoadedKey = new AtomicInteger();

        CompletableFuture<?> loadingFuture = IgniteTestUtils.runAsync(() -> {
            for (int key = 2; !installedSnapshot.get(); key++) {
                putToNode(0, key, "extra");
                lastLoadedKey.set(key);
            }
        });

        reanimateNode2AndWaitForSnapshotInstalled();

        installedSnapshot.set(true);

        assertThat(loadingFuture, willSucceedIn(30, TimeUnit.SECONDS));

        transferLeadershipOnSolePartitionTo(2);

        assertThat(getFromNode(2, 1), is("one"));

        List<Integer> expectedKeysAndNextKey = IntStream.rangeClosed(2, lastLoadedKey.get() + 1).boxed().collect(toList());
        Map<Integer, String> keysToValues = tableViewAt(2).getAll(null, expectedKeysAndNextKey);

        Set<Integer> expectedKeys = IntStream.rangeClosed(2, lastLoadedKey.get()).boxed().collect(toSet());
        assertThat(keysToValues.keySet(), equalTo(expectedKeys));
        assertThat(keysToValues.values(), everyItem(is("extra")));
    }

    /**
     * Tests that, after a node gets a RAFT snapshot installed to it, and it switches to a leader, it can act as a leader
     * (and can install a RAFT snapshot on the ex-leader).
     */
    @Test
    void nodeCanInstallSnapshotsAfterSnapshotInstalledToIt() throws Exception {
        feedNode2WithSnapshotOfOneRow();

        // The leader (0) has fed the follower (2). Now, change roles: the new leader will be node 2, it will feed node 0.

        transferLeadershipOnSolePartitionTo(2);

        knockoutNode(0);

        putToNode(2, 2, "two");

        // Make sure AppendEntries from leader to follower is impossible, making the leader to use InstallSnapshot.
        causeLogTruncationOnSolePartitionLeader(2);

        reanimateNodeAndWaitForSnapshotInstalled(0);

        transferLeadershipOnSolePartitionTo(0);

        assertThat(getFromNode(0, 1), is("one"));
        assertThat(getFromNode(0, 2), is("two"));
    }

    /**
     * Tests that, if a snapshot installation fails for some reason, a subsequent retry due to a timeout happens successfully.
     */
    @Test
    void snapshotInstallationRepeatsOnTimeout() throws Exception {
        prepareClusterForInstallingSnapshotToNode2(DEFAULT_STORAGE_ENGINE, theCluster -> {
            unwrapIgniteImpl(theCluster.node(0)).dropMessages(dropFirstSnapshotMetaResponse());
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
                IgniteImpl node = unwrapIgniteImpl(theCluster.node(0));
                MessageSerializationRegistry serializationRegistry = node.raftManager().service().serializationRegistry();

                BiPredicate<String, NetworkMessage> dropSafeTimeUntilSecondInstallSnapshotRequestIsProcessed = (recipientId, message) ->
                        message instanceof WriteActionRequest
                                && isSafeTimeSyncCommand((WriteActionRequest) message, serializationRegistry)
                                && !snapshotInstallFailedDueToIdenticalRetry.get();

                unwrapIgniteImpl(theCluster.node(0)).dropMessages(
                        dropFirstSnapshotMetaResponse().or(dropSafeTimeUntilSecondInstallSnapshotRequestIsProcessed)
                );

                unwrapIgniteImpl(theCluster.node(1)).dropMessages(dropSafeTimeUntilSecondInstallSnapshotRequestIsProcessed);
                unwrapIgniteImpl(theCluster.node(2)).dropMessages(dropSafeTimeUntilSecondInstallSnapshotRequestIsProcessed);
            });

            reanimateNode2AndWaitForSnapshotInstalled();
        } finally {
            snapshotExecutorLogInspector.removeHandler(snapshotInstallFailedDueToIdenticalRetryHandler);
            snapshotExecutorLogInspector.stop();
        }
    }

    private static boolean isSafeTimeSyncCommand(WriteActionRequest request, MessageSerializationRegistry serializationRegistry) {
        String groupId = request.groupId();

        if (groupId.equals(MetastorageGroupId.INSTANCE.toString()) || groupId.equals(CmgGroupId.INSTANCE.toString())) {
            return false;
        }

        var commandsMarshaller = new PartitionCommandsMarshallerImpl(serializationRegistry, NO_POOL);
        return commandsMarshaller.unmarshall(request.command()) instanceof SafeTimeSyncCommand;
    }

    @Test
    void testChangeLeaderOnInstallSnapshotInMiddle() throws Exception {
        CompletableFuture<Void> sentSnapshotMetaResponseFormNode1Future = new CompletableFuture<>();

        prepareClusterForInstallingSnapshotToNode2(DEFAULT_STORAGE_ENGINE, cluster -> {
            // Let's hang the InstallSnapshot in the "middle" from the leader with index 1.
            unwrapIgniteImpl(cluster.node(1)).dropMessages(dropSnapshotMetaResponse(sentSnapshotMetaResponseFormNode1Future));
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
        assertThat(getFromNode(2, 1), is("one"));
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
            unwrapIgniteImpl(cluster.node(0)).dropMessages(dropSnapshotMetaResponse(sentSnapshotMetaResponseFormNode0Future));
        });

        CompletableFuture<Void> installSnapshotSuccessfulFuture = new CompletableFuture<>();

        listenForSnapshotInstalledSuccessFromLogger(1, 2, installSnapshotSuccessfulFuture);

        // Return node 2.
        reanimateNode(2);

        // Waiting for the InstallSnapshot from node 2 to hang in the "middle".
        assertThat(sentSnapshotMetaResponseFormNode0Future, willSucceedIn(1, TimeUnit.MINUTES));

        // Change the leader to node 1.
        transferLeadershipOnSolePartitionTo(1);

        boolean replicated = waitForCondition(() -> getFromNode(2, 1) != null, 20_000);

        assertTrue(replicated, "Data has not been replicated to node 2 in time");

        // No snapshot must be installed.
        assertFalse(installSnapshotSuccessfulFuture.isDone());

        // Make sure the rebalancing is complete.
        assertThat(getFromNode(2, 1), is("one"));
    }

    /**
     * The replication mechanism must not replicate commands for which schemas are not yet available on the node
     * to which replication happens (in Raft, it means that followers/learners cannot receive commands that they
     * cannot execute without waiting for schemas). This method tests that snapshots bringing such commands are
     * rejected, and that, when metadata catches up, the snapshot gets successfully installed.
     */
    @Test
    void laggingSchemasOnFollowerPreventSnapshotInstallation() throws Exception {
        startAndInitCluster();

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
        Ignite nodeToInhibitMetaStorage = cluster.node(nodeIndex);

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
        RaftServer raftServer = unwrapIgniteImpl(cluster.node(nodeIndex)).raftManager().server();
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
