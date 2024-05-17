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

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.SessionUtils.executeUpdate;
import static org.apache.ignite.internal.TestWrappers.unwrapTableImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapTableViewInternal;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.bypassingThreadAssertions;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.tx.TxState.ABORTED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.DefaultMessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.MismatchingTransactionOutcomeException;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.message.CleanupReplicatedInfo;
import org.apache.ignite.internal.tx.message.TxCleanupMessage;
import org.apache.ignite.internal.tx.message.TxCleanupMessageErrorResponse;
import org.apache.ignite.internal.tx.message.TxCleanupMessageResponse;
import org.apache.ignite.internal.tx.message.TxFinishReplicaRequest;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/**
 * Test resending the finish request from the coordinator when the previous attempts failed for any reason.
 */
@WithSystemProperty(key = "IGNITE_ALWAYS_FORCE", value = "false")
public class ItDurableFinishTest extends ClusterPerTestIntegrationTest {
    private static final int AWAIT_PRIMARY_REPLICA_TIMEOUT = 10;

    private static final String TABLE_NAME = "TEST_FINISH";

    private final Collection<CompletableFuture<?>> futures = new ArrayList<>();

    private void createTestTableWith3Replicas() {
        String zoneSql = "create zone test_zone with partitions=1, replicas=3, storage_profiles='" + DEFAULT_STORAGE_PROFILE + "'";
        String sql = "create table " + TABLE_NAME + " (key int primary key, val varchar(20))"
                + " with primary_zone='TEST_ZONE'";

        cluster.doInSession(0, session -> {
            executeUpdate(zoneSql, session);
            executeUpdate(sql, session);
        });
    }

    private Context prepareTransactionData() throws ExecutionException, InterruptedException {
        createTestTableWith3Replicas();

        var tblReplicationGrp = defaultTablePartitionId(node(0));

        CompletableFuture<ReplicaMeta> primaryReplicaFut = node(0).placementDriver().awaitPrimaryReplica(
                tblReplicationGrp,
                node(0).clock().now(),
                AWAIT_PRIMARY_REPLICA_TIMEOUT,
                SECONDS
        );

        assertThat(primaryReplicaFut, willCompleteSuccessfully());

        String primary = primaryReplicaFut.get().getLeaseholder();

        int primaryIndex = nodeIndex(primary);

        IgniteImpl primaryNode = node(primaryIndex);
        IgniteImpl coordinatorNode = node((primaryIndex + 1) % 3);

        InternalTransaction rwTx = (InternalTransaction) coordinatorNode.transactions().begin();

        Tuple keyTpl = Tuple.create().set("key", 42);
        Tuple tpl = Tuple.create().set("key", 42).set("val", "val 42");

        Table publicTable = coordinatorNode.tables().table(TABLE_NAME);

        publicTable.recordView().upsert(rwTx, tpl);

        return new Context(primaryNode, coordinatorNode, publicTable, rwTx, keyTpl);
    }

    private TablePartitionId defaultTablePartitionId(IgniteImpl node) {
        TableViewInternal table = unwrapTableViewInternal(node.tables().table(TABLE_NAME));

        return new TablePartitionId(table.tableId(), 0);
    }

    private void commitAndValidate(InternalTransaction rwTx, Table publicTable, Tuple keyTpl) {
        rwTx.commit();

        Tuple storedData = publicTable.recordView().get(null, keyTpl);

        assertNotNull(storedData);

        assertEquals("val 42", storedData.stringValue("val"));
    }

    private DefaultMessagingService messaging(IgniteImpl node) {
        ClusterService coordinatorService = IgniteTestUtils.getFieldValue(node, IgniteImpl.class, "clusterSvc");

        return (DefaultMessagingService) coordinatorService.messagingService();
    }

    @Test
    void testChangedPrimaryOnFinish() throws Exception {
        Context context = prepareTransactionData();

        // Drop all finish messages to the old primary, pick a new one.
        // The coordinator will get a response from the new primary.
        CompletableFuture<Void> transferPrimaryFuture = changePrimaryOnFinish(context.coordinatorNode);

        // The primary is changed after calculating the outcome and commit timestamp.
        // The new primary successfully commits such transaction.
        commitAndValidate(context.tx, context.publicTable, context.keyTpl);

        assertThat(transferPrimaryFuture, willCompleteSuccessfully());
    }

    private CompletableFuture<Void> changePrimaryOnFinish(IgniteImpl coordinatorNode) {
        DefaultMessagingService coordinatorMessaging = messaging(coordinatorNode);

        AtomicBoolean dropMessage = new AtomicBoolean(true);

        CountDownLatch commitStartedLatch = new CountDownLatch(1);

        // Make sure the finish message is prepared, i.e. the outcome, commit timestamp, primary node, etc. have been set,
        // and then temporarily block the messaging to simulate network issues.
        coordinatorMessaging.dropMessages((s, networkMessage) -> {
            if (networkMessage instanceof TxFinishReplicaRequest && dropMessage.get()) {
                logger().info("Dropping: {}.", networkMessage);

                commitStartedLatch.countDown();

                return true;
            }

            return false;
        });

        // Now change the commit primary and run tx.commit().
        // The transfer is performed asynchronously because the message processing block we added earlier
        // will run in the current thread.
        return CompletableFuture.runAsync(() -> {
            try {
                commitStartedLatch.await();

                logger().info("Start transferring primary.");

                NodeUtils.transferPrimary(cluster.runningNodes().collect(toSet()), defaultTablePartitionId(node(0)));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                logger().info("Finished transferring primary.");

                dropMessage.set(false);
            }
        });
    }

    @Test
    void testCommitOverCommit() throws ExecutionException, InterruptedException {
        Context context = prepareTransactionData();

        // The coordinator does not get the response from the first commit message, but it anyway reaches the primary and succeeds.
        // The coordinator has to retry the finish request and survive a COMMIT over COMMIT.
        coordinatorDropsFirstFinishMessage(context.coordinatorNode);

        commitAndValidate(context.tx, context.publicTable, context.keyTpl);

        for (CompletableFuture<?> future : futures) {
            assertThat(future, willCompleteSuccessfully());
        }
    }

    private void coordinatorDropsFirstFinishMessage(IgniteImpl coordinatorNode) {
        DefaultMessagingService coordinatorMessaging = messaging(coordinatorNode);
        // Make sure the finish message is prepared, i.e. the outcome, commit timestamp, primary node, etc. have been set,
        // and then temporarily block the messaging to simulate network issues.
        AtomicBoolean messageHandled = new AtomicBoolean();
        coordinatorMessaging.dropMessages((s, networkMessage) -> {
            if (networkMessage instanceof TxFinishReplicaRequest && !messageHandled.get()) {
                messageHandled.set(true);

                logger().info("Drop message [msg={}].", networkMessage);

                // Here we act as a man-in-the-middle: the finish request is intercepted and further routed to
                // the commit partition as normal. The coordinator instead fails with a timeout (see DefaultMessagingService.invoke0)
                // and has to retry the finish request according to the durable finish logic.
                // The test checks that the second coordinator attempt to commit succeeds
                // and the server is able to apply a COMMIT over COMMIT without exceptions.

                CompletableFuture<NetworkMessage> finish = coordinatorMessaging.invoke(s, networkMessage, 3000);

                futures.add(finish);

                return true;
            }

            return false;
        });
    }

    @Test
    void testChangePrimaryOnCleanup() throws ExecutionException, InterruptedException {
        node(0).clusterConfiguration().getConfiguration(ReplicationConfiguration.KEY).change(replicationChange ->
                replicationChange.changeRpcTimeout(3000));

        Context context = prepareTransactionData();

        // The transaction is committed but the primary expires right before applying the cleanup message.
        CompletableFuture<Void> transferPrimaryFuture = changePrimaryOnCleanup(context.primaryNode);

        commitAndValidate(context.tx, context.publicTable, context.keyTpl);

        assertThat(transferPrimaryFuture, willCompleteSuccessfully());
    }

    private CompletableFuture<Void> changePrimaryOnCleanup(IgniteImpl primaryNode) {
        DefaultMessagingService primaryMessaging = messaging(primaryNode);

        AtomicBoolean dropMessage = new AtomicBoolean(true);

        CountDownLatch cleanupStartedLatch = new CountDownLatch(1);

        // Make sure the finish message is prepared, i.e. the outcome, commit timestamp, primary node, etc. have been set,
        // and then temporarily block the messaging to simulate network issues.
        primaryMessaging.dropMessages((s, networkMessage) -> {
            if (networkMessage instanceof TxCleanupMessage && dropMessage.get()) {
                logger().info("Dropping message: {}.", networkMessage);

                cleanupStartedLatch.countDown();

                return true;
            }

            return false;
        });

        // Now change the commit primary and run tx.commit().
        // The transfer is performed asynchronously because the message processing block we added earlier
        // will run in the current thread.
        return CompletableFuture.runAsync(() -> {
            try {
                cleanupStartedLatch.await();

                logger().info("Start transferring primary.");

                NodeUtils.transferPrimary(cluster.runningNodes().collect(toSet()), defaultTablePartitionId(node(0)));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                logger().info("Finished transferring primary.");

                dropMessage.set(false);
            }
        });
    }

    @Test
    void testCommitAlreadyAbortedTx() throws ExecutionException, InterruptedException {
        Context context = prepareTransactionData();

        // Simulate the state when a tx has already been committed by writing a corresponding state into tx state storage.
        markTxAbortedInTxStateStorage(context.primaryNode, context.tx, context.publicTable);

        // Tx.commit should throw MismatchingTransactionOutcomeException.
        TransactionException transactionException = assertThrows(TransactionException.class, context.tx::commit);

        Throwable cause = ExceptionUtils.unwrapCause(transactionException.getCause());

        assertInstanceOf(MismatchingTransactionOutcomeException.class, cause);
    }

    private void markTxAbortedInTxStateStorage(IgniteImpl primaryNode, InternalTransaction tx, Table publicTable) {
        TableImpl tableImpl = unwrapTableImpl(publicTable);

        TableViewInternal primaryTbl = unwrapTableViewInternal(primaryNode.tables().table(TABLE_NAME));

        TxStateStorage storage = primaryTbl.internalTable().txStateStorage().getTxStateStorage(0);

        TxMeta txMetaToSet = new TxMeta(
                ABORTED,
                asList(new ZonePartitionId(tableImpl.internalTable().zoneId(), tableImpl.tableId(), 0)),
                null
        );
        bypassingThreadAssertions(() -> storage.put(tx.id(), txMetaToSet));
    }

    @Test
    void testCleanupReplicatedMessage() throws ExecutionException, InterruptedException {
        Context context = prepareTransactionData();

        DefaultMessagingService primaryMessaging = messaging(context.primaryNode);

        CompletableFuture<Void> cleanupReplicatedFuture = new CompletableFuture<>();

        primaryMessaging.dropMessages((s, networkMessage) -> {
            if (networkMessage instanceof TxCleanupMessageResponse) {
                logger().info("Received message: {}.", networkMessage);

                TxCleanupMessageResponse message = (TxCleanupMessageResponse) networkMessage;

                if (message instanceof TxCleanupMessageErrorResponse) {
                    TxCleanupMessageErrorResponse error = (TxCleanupMessageErrorResponse) message;

                    logger().error("Cleanup Error: ", error);

                    return false;
                }

                CleanupReplicatedInfo result = message.result();

                if (result != null) {
                    cleanupReplicatedFuture.complete(null);
                }
            }

            return false;
        });

        commitAndValidate(context.tx, context.publicTable, context.keyTpl);

        assertThat(cleanupReplicatedFuture, willCompleteSuccessfully());

        assertTrue(waitForCondition(
                () -> {
                    TxStateMeta stateMeta = context.primaryNode.txManager().stateMeta(context.tx.id());

                    return stateMeta != null && stateMeta.cleanupCompletionTimestamp() != null;
                },
                10_000)
        );
    }

    private @Nullable Integer nodeIndex(String name) {
        for (int i = 0; i < initialNodes(); i++) {
            if (node(i).name().equals(name)) {
                return i;
            }
        }

        return null;
    }

    private static class Context {
        private final IgniteImpl primaryNode;
        private final IgniteImpl coordinatorNode;
        private final Table publicTable;
        private final InternalTransaction tx;
        private final Tuple keyTpl;

        private Context(IgniteImpl primaryNode, IgniteImpl coordinatorNode, Table publicTable, InternalTransaction tx, Tuple keyTpl) {
            this.primaryNode = primaryNode;
            this.coordinatorNode = coordinatorNode;
            this.publicTable = publicTable;
            this.tx = tx;
            this.keyTpl = keyTpl;
        }
    }

}
