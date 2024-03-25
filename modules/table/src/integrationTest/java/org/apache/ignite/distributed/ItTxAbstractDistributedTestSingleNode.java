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

package org.apache.ignite.distributed;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCode;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runMultiThreadedAsync;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_ALREADY_FINISHED_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.internal.table.TxAbstractTest;
import org.apache.ignite.internal.tx.impl.ReadWriteTransactionImpl;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionException;
import org.apache.ignite.tx.TransactionOptions;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Test class that is used for the tests that should be run on single node cluster only.
 */
public abstract class ItTxAbstractDistributedTestSingleNode extends TxAbstractTest {
    /**
     * The constructor.
     *
     * @param testInfo Test info.
     */
    public ItTxAbstractDistributedTestSingleNode(TestInfo testInfo) {
        super(testInfo);
    }

    @RepeatedTest(10)
    public void testTransactionMultiThreadedCommit() {
        testTransactionMultiThreadedFinish(1, false);
    }

    @RepeatedTest(10)
    public void testTransactionMultiThreadedCommitEmpty() {
        testTransactionMultiThreadedFinish(1, true);
    }

    @RepeatedTest(10)
    public void testTransactionMultiThreadedRollback() {
        testTransactionMultiThreadedFinish(0, false);
    }

    @RepeatedTest(10)
    public void testTransactionMultiThreadedRollbackEmpty() {
        testTransactionMultiThreadedFinish(0, true);
    }

    @RepeatedTest(10)
    public void testTransactionMultiThreadedMixed() {
        testTransactionMultiThreadedFinish(-1, false);
    }

    @RepeatedTest(10)
    public void testTransactionMultiThreadedMixedEmpty() {
        testTransactionMultiThreadedFinish(-1, true);
    }

    /**
     * Test trying to finish a tx in multiple threads simultaneously, and enlist new operations right after the first finish.
     *
     * @param finishMode 1 is commit, 0 is rollback, otherwise random outcome.
     * @param checkEmptyTx Whether the tx should be empty on finishing (no enlisted operations).
     */
    private void testTransactionMultiThreadedFinish(int finishMode, boolean checkEmptyTx) {
        var rv = accounts.recordView();

        rv.upsert(null, makeValue(1, 1.));

        Transaction tx = igniteTransactions.begin();

        var txId = ((ReadWriteTransactionImpl) tx).id();

        log.info("Started transaction {}", txId);

        if (!checkEmptyTx) {
            rv.upsert(tx, makeValue(1, 100.));
            rv.upsert(tx, makeValue(2, 200.));
        }

        int threadNum = Runtime.getRuntime().availableProcessors() * 5;

        CyclicBarrier b = new CyclicBarrier(threadNum);
        CountDownLatch finishLatch = new CountDownLatch(1);

        var futEnlists = runMultiThreadedAsync(() -> {
            finishLatch.await();
            var rnd = ThreadLocalRandom.current();

            assertThrowsWithCode(TransactionException.class, TX_ALREADY_FINISHED_ERR, () -> {
                if (rnd.nextBoolean()) {
                    rv.upsert(tx, makeValue(2, 200.));
                } else {
                    rv.get(tx, makeKey(1));
                }
            }, "Transaction is already finished");

            return null;
        }, threadNum, "txCommitTestThread");

        var futFinishes = runMultiThreadedAsync(() -> {
            b.await();

            finishTx(tx, finishMode);

            finishLatch.countDown();

            return null;
        }, threadNum, "txCommitTestThread");

        assertThat(futFinishes, willSucceedFast());
        assertThat(futEnlists, willSucceedFast());

        assertTrue(CollectionUtils.nullOrEmpty(txManager(accounts).lockManager().locks(txId)));
    }

    /**
     * Test trying to finish a read only tx in multiple threads simultaneously.
     */
    @RepeatedTest(10)
    public void testReadOnlyTransactionMultiThreadedFinish() {
        var rv = accounts.recordView();

        rv.upsert(null, makeValue(1, 1.));

        Transaction tx = igniteTransactions.begin(new TransactionOptions().readOnly(true));

        rv.get(tx, makeKey(1));

        int threadNum = Runtime.getRuntime().availableProcessors();

        CyclicBarrier b = new CyclicBarrier(threadNum);

        // TODO https://issues.apache.org/jira/browse/IGNITE-21411 Check enlists are prohibited.
        var futFinishes = runMultiThreadedAsync(() -> {
            b.await();

            finishTx(tx, -1);

            return null;
        }, threadNum, "txCommitTestThread");

        assertThat(futFinishes, willSucceedFast());
    }

    @Test
    public void testImplicitTransactionRetry() {
        var rv = accounts.recordView();

        Transaction tx = igniteTransactions.begin();

        assertNull(rv.get(tx, makeKey(1)));

        CompletableFuture<Void> implicitOpFut = runAsync(() -> rv.upsert(null, makeValue(1, 1.)));

        assertFalse(implicitOpFut.isDone());

        tx.commit();

        assertThat(implicitOpFut, willCompleteSuccessfully());

        assertNotNull(rv.get(null, makeKey(1)));
    }

    @Test
    public void testImplicitTransactionTimeout() {
        var rv = accounts.recordView();

        Transaction tx = igniteTransactions.begin();

        assertNull(rv.get(tx, makeKey(1)));

        CompletableFuture<Void> implicitOpFut = runAsync(() -> rv.upsert(null, makeValue(1, 1.)));

        assertFalse(implicitOpFut.isDone());

        assertThat(implicitOpFut, willThrow(TransactionException.class));

        assertNull(rv.get(null, makeKey(1)));
    }

    /**
     * Finish the tx.
     *
     * @param tx Transaction.
     * @param finishMode 1 is commit, 0 is rollback, otherwise random outcome.
     */
    private void finishTx(Transaction tx, int finishMode) {
        if (finishMode == 0) {
            tx.rollback();
        } else if (finishMode == 1) {
            tx.commit();
        } else {
            var rnd = ThreadLocalRandom.current();
            if (rnd.nextBoolean()) {
                tx.commit();
            } else {
                tx.rollback();
            }
        }
    }

    @Override
    protected int nodes() {
        return 1;
    }
}
