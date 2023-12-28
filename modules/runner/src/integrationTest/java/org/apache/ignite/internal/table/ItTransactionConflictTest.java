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

package org.apache.ignite.internal.table;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.SessionUtils.executeUpdate;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.ExceptionUtils.extractCodeFrom;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.message.ErrorTimestampAwareReplicaResponse;
import org.apache.ignite.internal.replicator.message.TimestampAwareReplicaResponse;
import org.apache.ignite.internal.table.distributed.command.MarkLocksReleasedCommand;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.tx.HybridTimestampTracker;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TransactionAlreadyFinishedException;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
import org.apache.ignite.internal.tx.message.TxFinishReplicaRequest;
import org.apache.ignite.internal.tx.message.TxRecoveryMessage;
import org.apache.ignite.internal.tx.message.TxStateCommitPartitionRequest;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.ErrorGroups.Transactions;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.DefaultMessagingService;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionOptions;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Abandoned transactions integration tests.
 */
public class ItTransactionConflictTest extends ClusterPerTestIntegrationTest {
    /** Table name. */
    private static final String TABLE_NAME = "test_table";

    @BeforeEach
    @Override
    public void setup(TestInfo testInfo) throws Exception {
        super.setup(testInfo);

        String zoneSql = "create zone test_zone with partitions=1, replicas=3";
        String sql = "create table " + TABLE_NAME + " (key int primary key, val varchar(20)) with primary_zone='TEST_ZONE'";

        cluster.doInSession(0, session -> {
            executeUpdate(zoneSql, session);
            executeUpdate(sql, session);
        });
    }

    @Override
    protected void customizeInitParameters(InitParametersBuilder builder) {
        super.customizeInitParameters(builder);

        builder.clusterConfiguration("{\n"
                + "  \"transaction\": {\n"
                + "  \"abandonedCheckTs\": 600000\n"
                + "  }\n"
                + "}\n");
    }

    @Test
    public void testMultipleRecoveryRequestsIssued() throws Exception {
        TableImpl tbl = (TableImpl) node(0).tables().table(TABLE_NAME);

        var tblReplicationGrp = new TablePartitionId(tbl.tableId(), 0);

        CompletableFuture<ReplicaMeta> primaryReplicaFut = node(0).placementDriver().awaitPrimaryReplica(
                tblReplicationGrp,
                node(0).clock().now(),
                10,
                SECONDS
        );

        assertThat(primaryReplicaFut, willCompleteSuccessfully());

        String leaseholder = primaryReplicaFut.join().getLeaseholder();

        IgniteImpl commitPartNode = IntStream.range(0, initialNodes()).mapToObj(this::node).filter(n -> leaseholder.equals(n.name()))
                .findFirst().get();

        log.info("Transaction commit partition is determined [node={}].", commitPartNode.name());

        IgniteImpl txCrdNode = IntStream.range(1, initialNodes()).mapToObj(this::node).filter(n -> !leaseholder.equals(n.name()))
                .findFirst().get();

        log.info("Transaction coordinator is chosen [node={}].", txCrdNode.name());

        Transaction oldRwTx = node(0).transactions().begin();

        UUID orphanTxId = startTransactionAndStopNode(txCrdNode);

        CompletableFuture<UUID> recoveryTxMsgCaptureFut = new CompletableFuture<>();
        AtomicInteger msgCount = new AtomicInteger();

        commitPartNode.dropMessages((nodeName, msg) -> {
            if (msg instanceof TxRecoveryMessage) {
                var recoveryTxMsg = (TxRecoveryMessage) msg;

                recoveryTxMsgCaptureFut.complete(recoveryTxMsg.txId());

                // Drop only the first recovery to emulate a lost message.
                // Another one should be issued eventually.
                return msgCount.incrementAndGet() == 1;
            }

            return false;
        });

        runConflictingTransaction(node(0), oldRwTx);
        runConflictingTransaction(node(0), node(0).transactions().begin());

        assertThat(recoveryTxMsgCaptureFut, willCompleteSuccessfully());

        assertEquals(orphanTxId, recoveryTxMsgCaptureFut.join());
        assertEquals(1, msgCount.get());

        node(0).clusterConfiguration().getConfiguration(TransactionConfiguration.KEY).change(transactionChange ->
                transactionChange.changeAbandonedCheckTs(1));

        assertTrue(waitForCondition(() -> {
            runConflictingTransaction(node(0), node(0).transactions().begin());

            return msgCount.get() > 1;
        }, 10_000));
    }

    @Test
    public void testAbandonedTxIsAborted() throws Exception {
        TableImpl tbl = (TableImpl) node(0).tables().table(TABLE_NAME);

        var tblReplicationGrp = new TablePartitionId(tbl.tableId(), 0);

        CompletableFuture<ReplicaMeta> primaryReplicaFut = node(0).placementDriver().awaitPrimaryReplica(
                tblReplicationGrp,
                node(0).clock().now(),
                10,
                SECONDS
        );

        assertThat(primaryReplicaFut, willCompleteSuccessfully());

        String leaseholder = primaryReplicaFut.join().getLeaseholder();

        IgniteImpl commitPartNode = IntStream.range(0, initialNodes()).mapToObj(this::node).filter(n -> leaseholder.equals(n.name()))
                .findFirst().get();

        log.info("Transaction commit partition is determined [node={}].", commitPartNode.name());

        IgniteImpl txCrdNode = IntStream.range(1, initialNodes()).mapToObj(this::node).filter(n -> !leaseholder.equals(n.name()))
                .findFirst().get();

        log.info("Transaction coordinator is chosen [node={}].", txCrdNode.name());

        UUID orphanTxId = startTransactionAndStopNode(txCrdNode);

        CompletableFuture<UUID> recoveryTxMsgCaptureFut = new CompletableFuture<>();
        AtomicInteger msgCount = new AtomicInteger();

        commitPartNode.dropMessages((nodeName, msg) -> {
            if (msg instanceof TxRecoveryMessage) {
                var recoveryTxMsg = (TxRecoveryMessage) msg;

                recoveryTxMsgCaptureFut.complete(recoveryTxMsg.txId());

                msgCount.incrementAndGet();
            }

            return false;
        });

        runConflictingTransaction(node(0), node(0).transactions().begin());

        assertThat(recoveryTxMsgCaptureFut, willCompleteSuccessfully());

        assertEquals(orphanTxId, recoveryTxMsgCaptureFut.join());
        assertEquals(1, msgCount.get());

        assertTrue(waitForCondition(() -> txStoredState(commitPartNode, orphanTxId) == TxState.ABORTED, 10_000));
    }

    @Test
    public void testWriteIntentRecoverNoCoordinator() throws Exception {
        TableImpl tbl = (TableImpl) node(0).tables().table(TABLE_NAME);

        var tblReplicationGrp = new TablePartitionId(tbl.tableId(), 0);

        CompletableFuture<ReplicaMeta> primaryReplicaFut = node(0).placementDriver().awaitPrimaryReplica(
                tblReplicationGrp,
                node(0).clock().now(),
                10,
                SECONDS
        );

        assertThat(primaryReplicaFut, willCompleteSuccessfully());

        String leaseholder = primaryReplicaFut.join().getLeaseholder();

        IgniteImpl commitPartNode = IntStream.range(0, initialNodes()).mapToObj(this::node).filter(n -> leaseholder.equals(n.name()))
                .findFirst().get();

        log.info("Transaction commit partition is determined [node={}].", commitPartNode.name());

        IgniteImpl txCrdNode = IntStream.range(1, initialNodes()).mapToObj(this::node).filter(n -> !leaseholder.equals(n.name()))
                .findFirst().get();

        log.info("Transaction coordinator is chosen [node={}].", txCrdNode.name());

        UUID orphanTxId = startTransactionAndStopNode(txCrdNode);

        AtomicInteger msgCount = new AtomicInteger();

        IgniteImpl roCoordNode = node(0);

        log.info("RO Transaction coordinator is chosen [node={}].", roCoordNode.name());

        commitPartNode.dropMessages((nodeName, msg) -> {
            if (msg instanceof TxStateCommitPartitionRequest) {
                msgCount.incrementAndGet();

                assertEquals(TxState.ABANDONED, txVolatileState(commitPartNode, orphanTxId));
            }

            return false;
        });

        Transaction recoveryTxReadOnly = roCoordNode.transactions().begin(new TransactionOptions().readOnly(true));

        runReadOnlyTransaction(roCoordNode, recoveryTxReadOnly);

        assertEquals(1, msgCount.get());

        assertTrue(waitForCondition(() -> txStoredState(commitPartNode, orphanTxId) == TxState.ABORTED, 10_000));
    }

    /**
     * Coordinator is alive, no recovery expeted.
     */
    @Test
    public void testWriteIntentNoRecovery() throws Exception {
        TableImpl tbl = (TableImpl) node(0).tables().table(TABLE_NAME);

        var tblReplicationGrp = new TablePartitionId(tbl.tableId(), 0);

        CompletableFuture<ReplicaMeta> primaryReplicaFut = node(0).placementDriver().awaitPrimaryReplica(
                tblReplicationGrp,
                node(0).clock().now(),
                10,
                SECONDS
        );

        assertThat(primaryReplicaFut, willCompleteSuccessfully());

        String leaseholder = primaryReplicaFut.join().getLeaseholder();

        IgniteImpl commitPartNode = IntStream.range(0, initialNodes()).mapToObj(this::node).filter(n -> leaseholder.equals(n.name()))
                .findFirst().get();

        log.info("Transaction commit partition is determined [node={}].", commitPartNode.name());

        IgniteImpl txCrdNode = IntStream.range(1, initialNodes()).mapToObj(this::node).filter(n -> !leaseholder.equals(n.name()))
                .findFirst().get();

        log.info("Transaction coordinator is chosen [node={}].", txCrdNode.name());

        Transaction rwTransaction = createRwTransaction(txCrdNode);

        AtomicInteger msgCount = new AtomicInteger();

        IgniteImpl roCoordNode = node(0);

        log.info("RO Transaction coordinator is chosen [node={}].", roCoordNode.name());

        commitPartNode.dropMessages((nodeName, msg) -> {
            if (msg instanceof TxRecoveryMessage) {
                msgCount.incrementAndGet();
            }

            return false;
        });

        Transaction recoveryTxReadOnly = roCoordNode.transactions().begin(new TransactionOptions().readOnly(true));

        runReadOnlyTransaction(roCoordNode, recoveryTxReadOnly);

        assertEquals(0, msgCount.get());

        rwTransaction.commit();

        UUID rwId = ((InternalTransaction) rwTransaction).id();

        assertTrue(waitForCondition(() -> txStoredState(commitPartNode, rwId) == TxState.COMMITTED, 10_000));
    }

    @Test
    public void testWriteIntentRecoveryAndLockConflict() throws Exception {
        TableImpl tbl = (TableImpl) node(0).tables().table(TABLE_NAME);

        var tblReplicationGrp = new TablePartitionId(tbl.tableId(), 0);

        CompletableFuture<ReplicaMeta> primaryReplicaFut = node(0).placementDriver().awaitPrimaryReplica(
                tblReplicationGrp,
                node(0).clock().now(),
                10,
                SECONDS
        );

        assertThat(primaryReplicaFut, willCompleteSuccessfully());

        String leaseholder = primaryReplicaFut.join().getLeaseholder();

        IgniteImpl commitPartNode = IntStream.range(0, initialNodes()).mapToObj(this::node).filter(n -> leaseholder.equals(n.name()))
                .findFirst().get();

        log.info("Transaction commit partition is determined [node={}].", commitPartNode.name());

        IgniteImpl txCrdNode = IntStream.range(1, initialNodes()).mapToObj(this::node).filter(n -> !leaseholder.equals(n.name()))
                .findFirst().get();

        log.info("Transaction coordinator is chosen [node={}].", txCrdNode.name());

        UUID orphanTxId = startTransactionAndStopNode(txCrdNode);

        AtomicInteger msgCount = new AtomicInteger();

        IgniteImpl roCoordNode = node(0);

        log.info("RO Transaction coordinator is chosen [node={}].", roCoordNode.name());

        CompletableFuture<UUID> txMsgCaptureFut = new CompletableFuture<>();

        commitPartNode.dropMessages((nodeName, msg) -> {
            if (msg instanceof TxStateCommitPartitionRequest) {
                msgCount.incrementAndGet();

                assertEquals(TxState.ABANDONED, txVolatileState(commitPartNode, orphanTxId));

                txMsgCaptureFut.complete(((TxStateCommitPartitionRequest) msg).txId());
            }

            return false;
        });

        Transaction recoveryTxReadOnly = roCoordNode.transactions().begin(new TransactionOptions().readOnly(true));

        RecordView view = roCoordNode.tables().table(TABLE_NAME).recordView();

        try {
            view.getAsync(recoveryTxReadOnly, Tuple.create().set("key", 42));
        } catch (Exception e) {
            assertEquals(Transactions.ACQUIRE_LOCK_ERR, extractCodeFrom(e));

            log.info("Expected lock conflict.", e);
        }

        assertThat(txMsgCaptureFut, willCompleteSuccessfully());

        runConflictingTransaction(commitPartNode, commitPartNode.transactions().begin());

        assertEquals(1, msgCount.get());

        assertTrue(waitForCondition(() -> txStoredState(commitPartNode, orphanTxId) == TxState.ABORTED, 10_000));
    }

    /**
     * Coordinator sends a commit message and dies. The message eventually reaches the commit partition and gets executed.
     * The expected outcome of the transaction is COMMIT.
     */
    @Test
    public void testSendCommitAndDie() throws Exception {
        TableImpl tbl = (TableImpl) node(0).tables().table(TABLE_NAME);

        var tblReplicationGrp = new TablePartitionId(tbl.tableId(), 0);

        CompletableFuture<ReplicaMeta> primaryReplicaFut = node(0).placementDriver().awaitPrimaryReplica(
                tblReplicationGrp,
                node(0).clock().now(),
                10,
                SECONDS
        );

        assertThat(primaryReplicaFut, willCompleteSuccessfully());

        String leaseholder = primaryReplicaFut.join().getLeaseholder();

        IgniteImpl commitPartNode = IntStream.range(0, initialNodes()).mapToObj(this::node).filter(n -> leaseholder.equals(n.name()))
                .findFirst().get();

        log.info("Transaction commit partition is determined [node={}].", commitPartNode.name());

        IgniteImpl txCrdNode = IntStream.range(1, initialNodes()).mapToObj(this::node).filter(n -> !leaseholder.equals(n.name()))
                .findFirst().get();

        log.info("Transaction coordinator is chosen [node={}].", txCrdNode.name());

        InternalTransaction orphanTx = (InternalTransaction) createRwTransaction(txCrdNode);

        CompletableFuture<TxFinishReplicaRequest> finishRequestCaptureFut = new CompletableFuture<>();
        AtomicReference<String> targetName = new AtomicReference<>();

        // Intercept the commit message, prevent it form being sent. We will kill this node anyway.
        txCrdNode.dropMessages((nodeName, msg) -> {
            if (msg instanceof TxFinishReplicaRequest) {
                var finishTxMsg = (TxFinishReplicaRequest) msg;

                finishRequestCaptureFut.complete(finishTxMsg);
                targetName.set(nodeName);

                return true;
            }

            return false;
        });

        // Initiate commit.
        orphanTx.commitAsync();

        assertThat(finishRequestCaptureFut, willCompleteSuccessfully());

        // Stop old coordinator.
        String txCrdNodeId = txCrdNode.id();

        txCrdNode.stop();

        assertTrue(waitForCondition(
                () -> node(0).clusterNodes().stream().filter(n -> txCrdNodeId.equals(n.id())).count() == 0,
                10_000)
        );

        // The state on the commit partition is still PENDING.
        assertEquals(TxState.PENDING, txVolatileState(commitPartNode, orphanTx.id()));

        // Continue the COMMIT message flow.
        CompletableFuture<NetworkMessage> finishRequest =
                messaging(commitPartNode).invoke(targetName.get(), finishRequestCaptureFut.join(), 3000);

        assertThat(finishRequest, willCompleteSuccessfully());

        // The conflicting transaction should see an already committed TX.
        runRwTransactionNoError(node(0), node(0).transactions().begin());

        assertTrue(waitForCondition(() -> txStoredState(commitPartNode, orphanTx.id()) == TxState.COMMITTED, 10_000));
    }

    /**
     * Coordinator sends a commit message and dies. Another tx initiates recovery and aborts this transaction.
     * The commit message eventually reaches the commit partition and gets executed but the outcome is ABORTED.
     */
    @Test
    public void testCommitAndDieRecoveryFirst() throws Exception {
        TableImpl tbl = (TableImpl) node(0).tables().table(TABLE_NAME);

        var tblReplicationGrp = new TablePartitionId(tbl.tableId(), 0);

        CompletableFuture<ReplicaMeta> primaryReplicaFut = node(0).placementDriver().awaitPrimaryReplica(
                tblReplicationGrp,
                node(0).clock().now(),
                10,
                SECONDS
        );

        assertThat(primaryReplicaFut, willCompleteSuccessfully());

        String leaseholder = primaryReplicaFut.join().getLeaseholder();

        IgniteImpl commitPartNode = IntStream.range(0, initialNodes()).mapToObj(this::node).filter(n -> leaseholder.equals(n.name()))
                .findFirst().get();

        log.info("Transaction commit partition is determined [node={}].", commitPartNode.name());

        IgniteImpl txCrdNode = IntStream.range(1, initialNodes()).mapToObj(this::node).filter(n -> !leaseholder.equals(n.name()))
                .findFirst().get();

        log.info("Transaction coordinator is chosen [node={}].", txCrdNode.name());

        InternalTransaction orphanTx = (InternalTransaction) createRwTransaction(txCrdNode);

        CompletableFuture<TxFinishReplicaRequest> finishRequestCaptureFut = new CompletableFuture<>();
        AtomicReference<String> targetName = new AtomicReference<>();

        // Intercept the commit message, prevent it form being sent. We will kill this node anyway.
        txCrdNode.dropMessages((nodeName, msg) -> {
            if (msg instanceof TxFinishReplicaRequest) {
                var finishTxMsg = (TxFinishReplicaRequest) msg;

                finishRequestCaptureFut.complete(finishTxMsg);
                targetName.set(nodeName);

                return true;
            }

            return false;
        });

        // Initiate commit.
        orphanTx.commitAsync();

        assertThat(finishRequestCaptureFut, willCompleteSuccessfully());

        // Stop old coordinator.
        String txCrdNodeId = txCrdNode.id();

        txCrdNode.stop();

        assertTrue(waitForCondition(
                () -> node(0).clusterNodes().stream().filter(n -> txCrdNodeId.equals(n.id())).count() == 0,
                10_000)
        );

        // The state on the commit partition is still PENDING.
        assertEquals(TxState.PENDING, txVolatileState(commitPartNode, orphanTx.id()));

        IgniteImpl newTxCoord = node(0);

        commitPartNode.dropMessages((nodeName, msg) -> {
            if (msg instanceof MarkLocksReleasedCommand) {

                UUID txId = ((MarkLocksReleasedCommand) msg).txId();

                if (orphanTx.equals(txId)) {
                    log.info("Dropping MarkLocksReleasedCommand");

                    return true;
                }
            }

            return false;
        });

        runRwTransactionNoError(newTxCoord, newTxCoord.transactions().begin());

        assertTrue(waitForCondition(() -> txStoredState(commitPartNode, orphanTx.id()) == TxState.ABORTED, 10_000));

        CompletableFuture<NetworkMessage> commitRequest =
                messaging(commitPartNode).invoke(targetName.get(), finishRequestCaptureFut.join(), 3000);

        assertThat(commitRequest, willCompleteSuccessfully());

        NetworkMessage response = commitRequest.join();

        assertInstanceOf(ErrorTimestampAwareReplicaResponse.class, response);

        ErrorTimestampAwareReplicaResponse errorResponse = (ErrorTimestampAwareReplicaResponse) response;

        assertInstanceOf(TransactionAlreadyFinishedException.class, ExceptionUtils.unwrapCause(errorResponse.throwable()));

        assertTrue(waitForCondition(() -> txStoredState(commitPartNode, orphanTx.id()) == TxState.ABORTED, 10_000));
    }

    /**
     * Coordinator sends a commit message and dies. Another tx initiates recovery and aborts this transaction.
     * The commit message eventually reaches the commit partition and gets executed but the outcome is ABORTED.
     * Here COMMIT is sent after the locks were released.
     */
    @Test
    public void testCommitAndDieRecoveryFirstWaitLocksReleased() throws Exception {
        TableImpl tbl = (TableImpl) node(0).tables().table(TABLE_NAME);

        var tblReplicationGrp = new TablePartitionId(tbl.tableId(), 0);

        CompletableFuture<ReplicaMeta> primaryReplicaFut = node(0).placementDriver().awaitPrimaryReplica(
                tblReplicationGrp,
                node(0).clock().now(),
                10,
                SECONDS
        );

        assertThat(primaryReplicaFut, willCompleteSuccessfully());

        String leaseholder = primaryReplicaFut.join().getLeaseholder();

        IgniteImpl commitPartNode = IntStream.range(0, initialNodes()).mapToObj(this::node).filter(n -> leaseholder.equals(n.name()))
                .findFirst().get();

        log.info("Transaction commit partition is determined [node={}].", commitPartNode.name());

        IgniteImpl txCrdNode = IntStream.range(1, initialNodes()).mapToObj(this::node).filter(n -> !leaseholder.equals(n.name()))
                .findFirst().get();

        log.info("Transaction coordinator is chosen [node={}].", txCrdNode.name());

        InternalTransaction orphanTx = (InternalTransaction) createRwTransaction(txCrdNode);

        CompletableFuture<TxFinishReplicaRequest> finishRequestCaptureFut = new CompletableFuture<>();
        AtomicReference<String> targetName = new AtomicReference<>();

        // Intercept the commit message, prevent it form being sent. We will kill this node anyway.
        txCrdNode.dropMessages((nodeName, msg) -> {
            if (msg instanceof TxFinishReplicaRequest) {
                var finishTxMsg = (TxFinishReplicaRequest) msg;

                finishRequestCaptureFut.complete(finishTxMsg);
                targetName.set(nodeName);

                return true;
            }

            return false;
        });

        // Initiate commit.
        orphanTx.commitAsync();

        assertThat(finishRequestCaptureFut, willCompleteSuccessfully());

        // Stop old coordinator.
        String txCrdNodeId = txCrdNode.id();

        txCrdNode.stop();

        assertTrue(waitForCondition(
                () -> node(0).clusterNodes().stream().filter(n -> txCrdNodeId.equals(n.id())).count() == 0,
                10_000)
        );

        // The state on the commit partition is still PENDING.
        assertEquals(TxState.PENDING, txVolatileState(commitPartNode, orphanTx.id()));

        IgniteImpl newTxCoord = node(0);

        runRwTransactionNoError(newTxCoord, newTxCoord.transactions().begin());

        assertTrue(waitForCondition(() -> txStoredState(commitPartNode, orphanTx.id()) == TxState.ABORTED, 10_000));

        assertTrue(waitForCondition(() -> txStoredMeta(commitPartNode, orphanTx.id()).locksReleased(), 10_000));

        CompletableFuture<NetworkMessage> commitRequest =
                messaging(commitPartNode).invoke(targetName.get(), finishRequestCaptureFut.join(), 3000);

        assertThat(commitRequest, willCompleteSuccessfully());

        NetworkMessage response = commitRequest.join();

        assertInstanceOf(TimestampAwareReplicaResponse.class, response);

        assertTrue(waitForCondition(() -> txStoredState(commitPartNode, orphanTx.id()) == TxState.ABORTED, 10_000));
    }

    @Test
    public void testRecoveryIsTriggeredOnce() throws Exception {
        TableImpl tbl = (TableImpl) node(0).tables().table(TABLE_NAME);

        var tblReplicationGrp = new TablePartitionId(tbl.tableId(), 0);

        CompletableFuture<ReplicaMeta> primaryReplicaFut = node(0).placementDriver().awaitPrimaryReplica(
                tblReplicationGrp,
                node(0).clock().now(),
                10,
                SECONDS
        );

        assertThat(primaryReplicaFut, willCompleteSuccessfully());

        String leaseholder = primaryReplicaFut.join().getLeaseholder();

        IgniteImpl commitPartNode = IntStream.range(0, initialNodes()).mapToObj(this::node).filter(n -> leaseholder.equals(n.name()))
                .findFirst().get();

        log.info("Transaction commit partition is determined [node={}].", commitPartNode.name());

        IgniteImpl txCrdNode = IntStream.range(1, initialNodes()).mapToObj(this::node).filter(n -> !leaseholder.equals(n.name()))
                .findFirst().get();

        log.info("Transaction coordinator is chosen [node={}].", txCrdNode.name());

        UUID orphanTxId = startTransactionAndStopNode(txCrdNode);

        log.info("Orphan tx [id={}]", orphanTxId);

        CompletableFuture<UUID> recoveryTxMsgCaptureFut = new CompletableFuture<>();
        AtomicInteger msgCount = new AtomicInteger();

        commitPartNode.dropMessages((nodeName, msg) -> {
            if (msg instanceof TxRecoveryMessage) {
                var recoveryTxMsg = (TxRecoveryMessage) msg;

                recoveryTxMsgCaptureFut.complete(recoveryTxMsg.txId());

                msgCount.incrementAndGet();
            }

            return false;
        });

        IgniteImpl newCoordNode = node(0);

        log.info("New Transaction coordinator is chosen [node={}].", newCoordNode.name());

        // Run RW transaction.
        Transaction rwTx1 = commitPartNode.transactions().begin();

        UUID rwTx1Id = ((InternalTransaction) rwTx1).id();

        log.info("First concurrent tx [id={}]", rwTx1Id);

        runConflictingTransaction(commitPartNode, rwTx1);

        Transaction rwTx2 = newCoordNode.transactions().begin();

        UUID rwTx2Id = ((InternalTransaction) rwTx2).id();

        log.info("Second concurrent tx [id={}]", rwTx2Id);

        runRwTransactionNoError(newCoordNode, rwTx2);

        assertThat(recoveryTxMsgCaptureFut, willCompleteSuccessfully());

        assertEquals(orphanTxId, recoveryTxMsgCaptureFut.join());
        assertEquals(1, msgCount.get());

        rwTx2.commit();

        assertTrue(waitForCondition(() -> txStoredState(commitPartNode, orphanTxId) == TxState.ABORTED, 10_000));
        assertTrue(waitForCondition(() -> txStoredState(commitPartNode, rwTx1Id) == TxState.ABORTED, 10_000));
        assertTrue(waitForCondition(() -> txStoredState(commitPartNode, rwTx2Id) == TxState.COMMITTED, 10_000));

        assertTrue(waitForCondition(() -> txStoredMeta(commitPartNode, orphanTxId).locksReleased(), 10_000));
        assertTrue(waitForCondition(() -> txStoredMeta(commitPartNode, rwTx1Id).locksReleased(), 10_000));
        assertTrue(waitForCondition(() -> txStoredMeta(commitPartNode, rwTx2Id).locksReleased(), 10_000));

        Transaction rwTx3 = newCoordNode.transactions().begin();

        log.info("Start RW tx {}", ((InternalTransaction) rwTx3).id());

        runRwTransactionNoError(newCoordNode, rwTx3);

        rwTx3.commit();

        assertTrue(waitForCondition(() -> txStoredState(commitPartNode, ((InternalTransaction) rwTx3).id()) == TxState.COMMITTED, 10_000));
    }

    @Test
    public void testFinishAlreadyFinishedTx() throws Exception {
        TableImpl tbl = (TableImpl) node(0).tables().table(TABLE_NAME);

        var tblReplicationGrp = new TablePartitionId(tbl.tableId(), 0);

        CompletableFuture<ReplicaMeta> primaryReplicaFut = node(0).placementDriver().awaitPrimaryReplica(
                tblReplicationGrp,
                node(0).clock().now(),
                10,
                SECONDS
        );

        assertThat(primaryReplicaFut, willCompleteSuccessfully());

        String leaseholder = primaryReplicaFut.join().getLeaseholder();

        IgniteImpl commitPartNode = IntStream.range(0, initialNodes()).mapToObj(this::node).filter(n -> leaseholder.equals(n.name()))
                .findFirst().get();

        log.info("Transaction commit partition is determined [node={}].", commitPartNode.name());

        IgniteImpl txCrdNode = IntStream.range(1, initialNodes()).mapToObj(this::node).filter(n -> !leaseholder.equals(n.name()))
                .findFirst().get();

        log.info("Transaction coordinator is chosen [node={}].", txCrdNode.name());

        Transaction rwTx1 = createRwTransaction(txCrdNode);

        rwTx1.commit();

        UUID rwTx1Id = ((InternalTransaction) rwTx1).id();

        assertTrue(waitForCondition(() -> txStoredState(commitPartNode, rwTx1Id) == TxState.COMMITTED, 10_000));
        assertTrue(waitForCondition(() -> txStoredMeta(commitPartNode, rwTx1Id).locksReleased(), 10_000));

        IgniteImpl txCrdNode2 = node(0);

        CompletableFuture<Void> finish2 = txCrdNode2.txManager().finish(
                new HybridTimestampTracker(),
                ((InternalTransaction) rwTx1).commitPartition(),
                false,
                Map.of(((InternalTransaction) rwTx1).commitPartition(), 0L),
                rwTx1Id
        );

        assertThat(finish2, willThrow(TransactionAlreadyFinishedException.class));
    }

    private DefaultMessagingService messaging(IgniteImpl node) {
        ClusterService coordinatorService = IgniteTestUtils.getFieldValue(node, IgniteImpl.class, "clusterSvc");

        return (DefaultMessagingService) coordinatorService.messagingService();
    }

    private @Nullable TxState txVolatileState(IgniteImpl node, UUID txId) {
        TxStateMeta txMeta = node.txManager().stateMeta(txId);

        return txMeta == null ? null : txMeta.txState();
    }

    private @Nullable TxState txStoredState(IgniteImpl node, UUID txId) {
        TxMeta txMeta = txStoredMeta(node, txId);

        return txMeta == null ? null : txMeta.txState();
    }

    private @Nullable TxMeta txStoredMeta(IgniteImpl node, UUID txId) {
        InternalTable internalTable = ((TableViewInternal) node.tables().table(TABLE_NAME)).internalTable();

        return internalTable.txStateStorage().getTxStateStorage(0).get(txId);
    }

    /**
     * Runs a transaction that was expectedly finished with the lock conflict exception.
     *
     * @param node Transaction coordinator node.
     * @param rwTx A transaction to create a lock conflict with an abandoned one.
     */
    private void runConflictingTransaction(IgniteImpl node, Transaction rwTx) {
        RecordView view = node.tables().table(TABLE_NAME).recordView();

        try {
            view.upsert(rwTx, Tuple.create().set("key", 42).set("val", "val2"));

            fail("Lock conflict have to be detected.");
        } catch (Exception e) {
            assertEquals(Transactions.ACQUIRE_LOCK_ERR, extractCodeFrom(e));

            log.info("Expected lock conflict.", e);
        }
    }

    private void runRwTransactionNoError(IgniteImpl node, Transaction rwTx) {
        RecordView view = node.tables().table(TABLE_NAME).recordView();

        try {
            view.upsert(rwTx, Tuple.create().set("key", 42).set("val", "val2"));
        } catch (Exception e) {
            assertEquals(Transactions.ACQUIRE_LOCK_ERR, extractCodeFrom(e));

            log.info("Expected lock conflict.", e);
        }
    }

    /**
     * Runs a RO transaction to trigger recovery on write intent resolutioin.
     *
     * @param node Transaction coordinator node.
     * @param roTx A transaction to resolve write intents from the abandoned TX.
     */
    private void runReadOnlyTransaction(IgniteImpl node, Transaction roTx) {
        RecordView view = node.tables().table(TABLE_NAME).recordView();

        try {
            view.get(roTx, Tuple.create().set("key", 42));
        } catch (Exception e) {
            assertEquals(Transactions.ACQUIRE_LOCK_ERR, extractCodeFrom(e));

            log.info("Expected lock conflict.", e);
        }
    }

    /**
     * Starts the transaction, takes a lock, and stops the transaction coordinator. The stopped node leaves the transaction in the pending
     * state.
     *
     * @param node Transaction coordinator node.
     * @return Transaction id.
     * @throws InterruptedException If interrupted.
     */
    private UUID startTransactionAndStopNode(IgniteImpl node) throws InterruptedException {
        Transaction rwTx1 = createRwTransaction(node);

        String txCrdNodeId = node.id();

        node.stop();

        assertTrue(waitForCondition(
                () -> node(0).clusterNodes().stream().filter(n -> txCrdNodeId.equals(n.id())).count() == 0,
                10_000)
        );
        return ((InternalTransaction) rwTx1).id();
    }

    /**
     * Creates RW the transaction.
     *
     * @param node Transaction coordinator node.
     * @return Transaction id.
     */
    private Transaction createRwTransaction(IgniteImpl node) {
        RecordView<Tuple> view = node.tables().table(TABLE_NAME).recordView();

        Transaction rwTx1 = node.transactions().begin();

        view.upsert(rwTx1, Tuple.create().set("key", 42).set("val", "val1"));

        return rwTx1;
    }
}
