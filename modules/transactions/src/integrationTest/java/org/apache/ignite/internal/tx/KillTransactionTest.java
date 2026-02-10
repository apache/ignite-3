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

package org.apache.ignite.internal.tx;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowWithCauseOrSuppressed;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.tx.metrics.ResourceVacuumMetrics;
import org.apache.ignite.internal.wrapper.Wrappers;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionException;
import org.apache.ignite.tx.TransactionOptions;
import org.junit.jupiter.api.Test;

/**
 * The test class contains the internal API to kill a transaction.
 * The behavior of this API is similar to the rollback invocation,
 * but a client has to get a specific exception when trying to interact with the transaction object.
 */
public class KillTransactionTest extends ClusterPerClassIntegrationTest {
    @Override
    protected void configureInitParameters(InitParametersBuilder builder) {
        super.configureInitParameters(builder);

        builder.clusterConfiguration("ignite.system.properties.txnResourceTtl=\"0\"");
    }

    @Test
    public void testKillTransactionBeforeCommit() throws Exception {
        killTransactionBeforeFinishing(true);
    }

    @Test
    public void testKillTransactionBeforeRollback() throws Exception {
        killTransactionBeforeFinishing(false);
    }

    /**
     * The test creates a transaction on each node and then kills them.
     * After this scenario, this test checks invariants:
     * public API for the killed transaction throws an exception;
     * transaction states are vacuumed.
     *
     * @param commit True for commit, false for rollback.
     */
    private void killTransactionBeforeFinishing(boolean commit) throws Exception {
        node(0).sql().executeScript("CREATE TABLE IF NOT EXISTS test (id INT PRIMARY KEY, val VARCHAR)");

        ArrayList<Transaction> txs = new ArrayList<>();
        AtomicInteger keyGenerator = new AtomicInteger(ThreadLocalRandom.current().nextInt(100));

        CLUSTER.runningNodes().forEach(node -> {
            int key = keyGenerator.incrementAndGet();

            Transaction tx = node.transactions().begin(new TransactionOptions()
                    .readOnly(key % 2 == 0));

            KeyValueView<Integer, String> kvView = node.tables().table("test").keyValueView(Integer.class, String.class);

            if (!tx.isReadOnly()) {
                kvView.put(tx, key, "Test val");
            }

            String res = kvView.get(tx, key);

            if (tx.isReadOnly()) {
                assertNull(res);
            } else {
                assertEquals("Test val", res);
            }

            txs.add(tx);
        });

        CLUSTER.runningNodes().forEach(node -> {
            IgniteImpl igniteImpl = Wrappers.unwrap(node, IgniteImpl.class);

            boolean txIsKilled = false;

            for (Transaction tx : txs) {
                InternalTransaction internalTx = Wrappers.unwrap(tx, InternalTransaction.class);

                CompletableFuture<Boolean> killFut = igniteImpl.txManager().kill(internalTx.id());

                assertThat(killFut, willCompleteSuccessfully());

                if (killFut.join()) {
                    assertFalse(txIsKilled);

                    txIsKilled = true;
                }
            }

            assertTrue(txIsKilled);
        });

        for (Transaction tx : txs) {
            CompletableFuture<Void> finishFut = commit ? tx.commitAsync() : tx.rollbackAsync();

            if (tx.isReadOnly()) {
                assertThat(finishFut, willCompleteSuccessfully());
            } else {
                assertThat(finishFut, willThrowWithCauseOrSuppressed(
                        TransactionException.class,
                        "Transaction is killed"
                ));
            }
        }

        for (Transaction tx : txs) {
            CompletableFuture<Void> finishFut = commit ? tx.commitAsync() : tx.rollbackAsync();

            assertThat(finishFut, willCompleteSuccessfully());
        }

        waitForVacuum(txs);
    }

    /**
     * Waits for the removal of all transaction states from the volatile storage.
     *
     * @param txs List of transactions.
     * @throws InterruptedException If the waiting would be interrupted.
     */
    private void waitForVacuum(ArrayList<Transaction> txs) throws InterruptedException {
        for (int i = 0; i < initialNodes(); i++) {
            IgniteImpl igniteImpl = Wrappers.unwrap(node(i), IgniteImpl.class);

            assertTrue(IgniteTestUtils.waitForCondition(() -> {
                assertThat(igniteImpl.txManager().vacuum(mock(ResourceVacuumMetrics.class)), willCompleteSuccessfully());

                for (Transaction tx : txs) {
                    InternalTransaction internalTx = Wrappers.unwrap(tx, InternalTransaction.class);

                    if (igniteImpl.txManager().stateMeta(internalTx.id()) != null) {
                        return false;
                    }
                }

                return true;
            }, 10_000));
        }
    }
}
