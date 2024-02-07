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
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.tx.TxState.ABORTED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.lang.IgniteTriConsumer;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.DefaultMessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.message.TxFinishReplicaRequest;
import org.apache.ignite.internal.tx.message.WriteIntentSwitchReplicaRequest;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/**
 * Test resending the finish request from the coordinator when the previous attempts failed for any reason.
 */
public class ItDurableFinishTest extends ClusterPerTestIntegrationTest {
    private static final int AWAIT_PRIMARY_REPLICA_TIMEOUT = 10;

    private static final String TABLE_NAME = "TEST_FINISH";

    private final Collection<CompletableFuture<?>> futures = new ArrayList<>();

    private void createTestTableWith3Replicas() {
        String zoneSql = "create zone test_zone with partitions=1, replicas=3";
        String sql = "create table " + TABLE_NAME + " (key int primary key, val varchar(20))"
                + " with primary_zone='TEST_ZONE'";

        cluster.doInSession(0, session -> {
            executeUpdate(zoneSql, session);
            executeUpdate(sql, session);
        });
    }

    private void testFinishRow(Configurator msgConf, IgniteTriConsumer<InternalTransaction, TableImpl, Tuple> finisher)
            throws ExecutionException, InterruptedException {
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

        TableImpl tbl = (TableImpl) coordinatorNode.tables().table(TABLE_NAME);

        tbl.recordView().upsert(rwTx, tpl);

        msgConf.accept(primaryNode, coordinatorNode, tbl, rwTx);

        finisher.accept(rwTx, tbl, keyTpl);
    }

    private TablePartitionId defaultTablePartitionId(IgniteImpl node) {
        TableImpl table = (TableImpl) node.tables().table(TABLE_NAME);

        return new TablePartitionId(table.tableId(), 0);
    }

    private void commitRow(InternalTransaction rwTx, TableImpl tbl, Tuple keyTpl) {
        rwTx.commit();

        Tuple storedData = tbl.recordView().get(null, keyTpl);

        assertNotNull(storedData);

        assertEquals("val 42", storedData.stringValue("val"));
    }

    private DefaultMessagingService messaging(IgniteImpl node) {
        ClusterService coordinatorService = IgniteTestUtils.getFieldValue(node, IgniteImpl.class, "clusterSvc");

        return (DefaultMessagingService) coordinatorService.messagingService();
    }

    @Test
    void testChangedPrimaryOnFinish() throws ExecutionException, InterruptedException {
        testFinishRow(this::changedPrimaryOnFinish, this::commitRow);
    }

    private void changedPrimaryOnFinish(
            IgniteImpl primaryNode,
            IgniteImpl coordinatorNode,
            TableImpl tbl,
            InternalTransaction tx
    ) {
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
        CompletableFuture.runAsync(() -> {
            try {
                commitStartedLatch.await();

                logger().info("Start transferring primary.");

                NodeUtils.transferPrimary(tbl, null, this::node);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                logger().info("Finished transferring primary.");

                dropMessage.set(false);
            }
        });
    }

    @Test
    void testCoordinatorMissedResponse() throws ExecutionException, InterruptedException {
        testFinishRow(this::coordinatorMissedResponse, this::commitRow);

        for (CompletableFuture<?> future : futures) {
            assertThat(future, willCompleteSuccessfully());
        }
    }

    private void coordinatorMissedResponse(
            IgniteImpl primaryNode,
            IgniteImpl coordinatorNode,
            TableImpl tbl,
            InternalTransaction tx
    ) {
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
    void testWaitForCleanup() throws ExecutionException, InterruptedException {
        testFinishRow(this::waitForCleanup, this::commitRow);
    }

    private void waitForCleanup(
            IgniteImpl primaryNode,
            IgniteImpl coordinatorNode,
            TableImpl tbl,
            InternalTransaction tx
    ) {
        DefaultMessagingService primaryMessaging = messaging(primaryNode);

        AtomicBoolean dropMessage = new AtomicBoolean(true);

        // Make sure the finish message is prepared, i.e. the outcome, commit timestamp, primary node, etc. have been set,
        // and then temporarily block the messaging to simulate network issues.
        primaryMessaging.dropMessages((s, networkMessage) -> {
            if (networkMessage instanceof WriteIntentSwitchReplicaRequest && dropMessage.get()) {
                logger().info("Dropping message: {}.", networkMessage);

                return true;
            }

            return false;
        });

        // Now change the commit primary and run tx.commit().
        // The transfer is performed asynchronously because the message processing block we added earlier
        // will run in the current thread.
        CompletableFuture.runAsync(() -> {
            try {
                logger().info("Start transferring primary.");

                NodeUtils.transferPrimary(tbl, null, this::node);
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
        testFinishRow(this::commitAlreadyAbortedTx,
                (transaction, table, objects) -> assertThrows(TransactionException.class, transaction::commit));
    }

    private void commitAlreadyAbortedTx(
            IgniteImpl primaryNode,
            IgniteImpl coordinatorNode,
            TableImpl tbl,
            InternalTransaction tx
    ) {
        TableImpl primaryTbl = (TableImpl) primaryNode.tables().table(TABLE_NAME);

        TxStateStorage storage = primaryTbl.internalTable().txStateStorage().getTxStateStorage(0);

        TxMeta txMetaToSet = new TxMeta(
                ABORTED,
                null
        );
        storage.put(tx.id(), txMetaToSet);
    }

    /**
     * Gets Ignite instance by the name.
     *
     * @param name Node name.
     * @return Ignite instance.
     */
    private @Nullable IgniteImpl node(String name) {
        for (int i = 0; i < initialNodes(); i++) {
            if (node(i).name().equals(name)) {
                return node(i);
            }
        }

        return null;
    }

    private @Nullable Integer nodeIndex(String name) {
        for (int i = 0; i < initialNodes(); i++) {
            if (node(i).name().equals(name)) {
                return i;
            }
        }

        return null;
    }

    private interface Configurator {
        void accept(
                IgniteImpl primaryNode,
                IgniteImpl coordinatorNode,
                TableImpl tbl,
                InternalTransaction tx
        );
    }

}
