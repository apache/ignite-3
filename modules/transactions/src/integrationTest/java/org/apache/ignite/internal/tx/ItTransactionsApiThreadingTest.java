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

import static java.lang.Thread.currentThread;
import static org.apache.ignite.internal.PublicApiThreadingTests.anIgniteThread;
import static org.apache.ignite.internal.PublicApiThreadingTests.asyncContinuationPool;
import static org.apache.ignite.internal.PublicApiThreadingTests.tryToSwitchFromUserThreadWithDelayedSchemaSync;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteTransactionsImpl;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.bypassingThreadAssertionsAsync;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.is;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.tx.impl.IgniteTransactionsImpl;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.tx.IgniteTransactions;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionOptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@SuppressWarnings("resource")
class ItTransactionsApiThreadingTest extends ClusterPerClassIntegrationTest {
    private static final String TABLE_NAME = "test";

    private static KeyValueView<Integer, String> keyValueView;

    @Override
    protected int initialNodes() {
        return 1;
    }

    @BeforeAll
    void createTable() {
        sql("CREATE TABLE " + TABLE_NAME + " (id INT PRIMARY KEY, val VARCHAR)");

        //noinspection AssignmentToStaticFieldFromInstanceMethod
        keyValueView = CLUSTER.aliveNode().tables().table(TABLE_NAME).keyValueView(Integer.class, String.class);
    }

    @ParameterizedTest
    @EnumSource(TransactionsAsyncOperation.class)
    void transactionsFuturesCompleteInContinuationsPool(TransactionsAsyncOperation operation) {
        CompletableFuture<Thread> completerFuture = tryingToSwitchFromUserThread(
                () -> operation.executeOn(CLUSTER.aliveNode().transactions())
                        .thenApply(unused -> currentThread())
        );

        assertThat(completerFuture, willBe(either(is(currentThread())).or(asyncContinuationPool())));
    }

    @ParameterizedTest
    @EnumSource(TransactionsAsyncOperation.class)
    void transactionsFuturesFromInternalCallsAreNotResubmittedToContinuationsPool(TransactionsAsyncOperation operation) {
        CompletableFuture<Thread> completerFuture = tryingToSwitchFromUserThread(
                // Executing with everything allowed as this is what is handled by public API wrapper that we removed.
                // Internal components using the APIs directly (without the public API threading wrapper) should call the APIs
                // in IgniteThreads with corresponding permissions, so we do the same.
                () -> bypassingThreadAssertionsAsync(() -> operation.executeOn(igniteTransactionsForInternalUse()))
                        .thenApply(unused -> currentThread())
        );

        assertThat(completerFuture, willBe(either(is(currentThread())).or(anIgniteThread())));
    }

    @ParameterizedTest
    @EnumSource(TxAsyncOperation.class)
    void txFuturesCompleteInContinuationsPool(TxAsyncOperation operation) {
        Transaction tx = CLUSTER.aliveNode().transactions().begin();
        touchTestTableIn(tx);

        CompletableFuture<Thread> completerFuture = tryingToSwitchFromUserThread(
                () -> operation.executeOn(tx)
                        .thenApply(unused -> currentThread())
        );

        assertThat(completerFuture, willBe(asyncContinuationPool()));
    }

    private static void touchTestTableIn(Transaction tx) {
        keyValueView.put(tx, 1, "one");
    }

    private static CompletableFuture<Void> touchTestTableAsyncIn(Transaction tx) {
        return keyValueView.putAsync(tx, 1, "one");
    }

    @ParameterizedTest
    @EnumSource(TxAsyncOperation.class)
    void txFuturesFromInternalCallsAreNotResubmittedToContinuationsPool(TxAsyncOperation operation) {
        Transaction tx = igniteTransactionsForInternalUse().begin();
        touchTestTableIn(tx);

        CompletableFuture<Thread> completerFuture = tryingToSwitchFromUserThread(
                // Executing with everything allowed as this is what is handled by public API wrapper that we removed.
                // Internal components using the APIs directly (without the public API threading wrapper) should call the APIs
                // in IgniteThreads with corresponding permissions, so we do the same.
                () -> bypassingThreadAssertionsAsync(() -> operation.executeOn(tx))
                        .thenApply(unused -> currentThread())
        );

        assertThat(completerFuture, willBe(anIgniteThread()));
    }

    private static <T> T tryingToSwitchFromUserThread(Supplier<? extends T> action) {
        return tryToSwitchFromUserThreadWithDelayedSchemaSync(CLUSTER.aliveNode(), action);
    }

    private static IgniteTransactionsImpl igniteTransactionsForInternalUse() {
        return unwrapIgniteTransactionsImpl(CLUSTER.aliveNode().transactions());
    }

    private enum TransactionsAsyncOperation {
        BEGIN_ASYNC(transactions -> transactions.beginAsync()),
        BEGIN_WITH_OPTIONS_ASYNC(transactions -> transactions.beginAsync(new TransactionOptions())),
        RUN_IN_TRANSACTION_ASYNC(transactions -> transactions.runInTransactionAsync(tx -> touchTestTableAsyncIn(tx))),
        RUN_IN_TRANSACTION_WITH_OPTIONS_ASYNC(transactions -> transactions.runInTransactionAsync(
                tx -> touchTestTableAsyncIn(tx),
                new TransactionOptions()
        ));

        private final Function<IgniteTransactions, CompletableFuture<?>> action;

        TransactionsAsyncOperation(Function<IgniteTransactions, CompletableFuture<?>> action) {
            this.action = action;
        }

        CompletableFuture<?> executeOn(IgniteTransactions igniteTables) {
            return action.apply(igniteTables);
        }
    }

    private enum TxAsyncOperation {
        COMMIT_ASYNC(tx -> tx.commitAsync()),
        ROLLBACK_ASYNC(tx -> tx.rollbackAsync());

        private final Function<Transaction, CompletableFuture<?>> action;

        TxAsyncOperation(Function<Transaction, CompletableFuture<?>> action) {
            this.action = action;
        }

        CompletableFuture<?> executeOn(Transaction igniteTables) {
            return action.apply(igniteTables);
        }
    }
}
