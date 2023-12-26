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
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.ExceptionUtils.extractCodeFrom;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
import org.apache.ignite.internal.tx.message.TxRecoveryMessage;
import org.apache.ignite.lang.ErrorGroups.Transactions;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
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

    /**
     * Starts the transaction, takes a lock, and stops the transaction coordinator. The stopped node leaves the transaction in the pending
     * state.
     *
     * @param node Transaction coordinator node.
     * @return Transaction id.
     * @throws InterruptedException If interrupted.
     */
    private UUID startTransactionAndStopNode(IgniteImpl node) throws InterruptedException {
        RecordView view = node.tables().table(TABLE_NAME).recordView();

        Transaction rwTx1 = node.transactions().begin();

        view.upsert(rwTx1, Tuple.create().set("key", 42).set("val", "val1"));

        String txCrdNodeId = node.id();

        node.stop();

        assertTrue(waitForCondition(
                () -> node(0).clusterNodes().stream().filter(n -> txCrdNodeId.equals(n.id())).count() == 0,
                10_000)
        );
        return ((InternalTransaction) rwTx1).id();
    }
}
