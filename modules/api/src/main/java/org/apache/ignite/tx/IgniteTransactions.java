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

package org.apache.ignite.tx;

import static org.apache.ignite.tx.IgniteTransactionDefaults.DEFAULT_RW_TX_TIMEOUT_SECONDS;
import static org.apache.ignite.tx.RunInTransactionInternalImpl.runInTransactionAsyncInternal;
import static org.apache.ignite.tx.RunInTransactionInternalImpl.runInTransactionInternal;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.table.Table;
import org.jetbrains.annotations.Nullable;

/**
 * The Ignite Transactions facade that supports distributed transactions when working with tables.
 * This interface provides the ability to perform transactions in both synchronous and asynchronous ways.
 * <pre><code>
 *     // Synchronous transactional API to update the balance.
 *     client.transactions().runInTransaction(tx -&gt; {
 *          Account account = accounts.get(tx, key);
 *
 *          account.balance += 200.0d;
 *
 *          accounts.put(tx, key, account);
 *      });
 * </code></pre>
 * There is no need to call {@link Transaction#commit()} explicitly.
 * The transaction is automatically committed when the closure is successfully executed.
 *
 * <pre><code>
 *     // Using asynchronous transactional API to update the balance.
 *     CompletableFuture&lt;Void&gt; fut = client.transactions().beginAsync().thenCompose(tx -&gt;
 *             accounts
 *                 .getAsync(tx, key)
 *                 .thenCompose(account -&gt; {
 *                     account.balance += 200.0d;
 *
 *                     return accounts.putAsync(tx, key, account);
 *                 })
 *                 .thenCompose(ignored -&gt; tx.commitAsync())
 *     );
 *
 *     // Wait for completion.
 *     fut.join();
 * </code></pre>
 *
 * @see Table
 */
public interface IgniteTransactions {
    /**
     * Begins a transaction.
     *
     * @return The started transaction.
     */
    default Transaction begin() {
        return begin(null);
    }

    /**
     * Begins a transaction.
     *
     * @param options Transaction options.
     * @return The started transaction.
     */
    Transaction begin(@Nullable TransactionOptions options);

    /**
     * Begins a transaction.
     *
     * @return Started transaction.
     */
    default CompletableFuture<Transaction> beginAsync() {
        return beginAsync(null);
    }

    /**
     * Begins an asynchronous transaction.
     *
     * @param options Transaction options.
     * @return The future holding the started transaction.
     */
    CompletableFuture<Transaction> beginAsync(@Nullable TransactionOptions options);

    /**
     * Executes a closure within a transaction.
     *
     * <p>This method expects that all transaction operations are completed before the closure returns. The safest way to achieve that is
     * to use synchronous table API.
     *
     * <p>Take care then using the asynchronous operations inside the closure. For example, the following snippet is <b>incorrect</b>,
     * because the last operation goes out of the scope of the closure unfinished:
     * <pre>
     * {@code
     * igniteTransactions.runInTransaction(tx -> {
     *     var key = Tuple.create().set("accountId", 1);
     *     Tuple acc = view.get(tx, key);
     *     view.upsertAsync(tx, Tuple.create().set("accountId", 1).set("balance", acc.longValue("balance") + 100));
     * });
     * }
     * </pre>
     *
     * <p>The <b>correct</b> variant will be:
     * <pre>
     * {@code
     * igniteTransactions.runInTransaction(tx -> {
     *     view.getAsync(tx, Tuple.create().set("accountId", 1)).thenCompose(acc ->
     *         view.upsertAsync(tx, Tuple.create().set("accountId", 1).set("balance", acc.longValue("balance") + 100))).join();
     * });
     * }
     * </pre>
     *
     * <p>If the closure is executed normally (no exceptions) the transaction is automatically committed. In a case of exception, the
     * closure will be retried automatically within the transaction timeout, so it must be pure function. If the transaction timeout
     * expires before the closure completes successfully and the transaction has been committed, the transaction is rolled back instead.
     * <br>
     * The closure is retried only in cases of "expected" exceptions, like {@code LockException}, {@code TimeoutException},
     * exceptions related to the primary replica change, etc.
     *
     * @param clo The closure.
     *
     * @throws TransactionException If a transaction can't be finished successfully.
     */
    default void runInTransaction(Consumer<Transaction> clo) throws TransactionException {
        runInTransaction(clo, null);
    }

    /**
     * Executes a closure within a transaction.
     *
     * <p>This method expects that all transaction operations are completed before the closure returns. The safest way to achieve that is
     * to use synchronous table API.
     *
     * <p>Take care then using the asynchronous operations inside the closure. For example, the following snippet is <b>incorrect</b>,
     * because the last operation goes out of the scope of the closure unfinished:
     * <pre>
     * {@code
     * igniteTransactions.runInTransaction(tx -> {
     *     var key = Tuple.create().set("accountId", 1);
     *     Tuple acc = view.get(tx, key);
     *     view.upsertAsync(tx, Tuple.create().set("accountId", 1).set("balance", acc.longValue("balance") + 100));
     * });
     * }
     * </pre>
     *
     * <p>The <b>correct</b> variant will be:
     * <pre>
     * {@code
     * igniteTransactions.runInTransaction(tx -> {
     *     view.getAsync(tx, Tuple.create().set("accountId", 1)).thenCompose(acc ->
     *         view.upsertAsync(tx, Tuple.create().set("accountId", 1).set("balance", acc.longValue("balance") + 100))).join();
     * });
     * }
     * </pre>
     *
     * <p>If the closure is executed normally (no exceptions) the transaction is automatically committed. In a case of exception, the
     * closure will be retried automatically within the transaction timeout, so it must be pure function. If the transaction timeout
     * expires before the closure completes successfully and the transaction has been committed, the transaction is rolled back instead.
     * <br>
     * The closure is retried only in cases of "expected" exceptions, like {@code LockException}, {@code TimeoutException},
     * exceptions related to the primary replica change, etc.
     *
     * @param options Transaction options.
     * @param clo The closure.
     *
     * @throws TransactionException If a transaction can't be finished successfully.
     */
    default void runInTransaction(Consumer<Transaction> clo, @Nullable TransactionOptions options) throws TransactionException {
        Objects.requireNonNull(clo);

        runInTransaction(tx -> {
            clo.accept(tx);
            return null;
        }, options);
    }

    /**
     * Executes a closure within a transaction and returns a result.
     *
     * <p>This method expects that all transaction operations are completed before the closure returns. The safest way to achieve that is
     * to use synchronous table API.
     *
     * <p>Take care then using the asynchronous operations inside the closure. For example, the following snippet is <b>incorrect</b>,
     * because the last operation goes out of the scope of the closure unfinished:
     * <pre>
     * {@code
     * igniteTransactions.runInTransaction(tx -> {
     *     var key = Tuple.create().set("accountId", 1);
     *     Tuple acc = view.get(tx, key);
     *     view.upsertAsync(tx, Tuple.create().set("accountId", 1).set("balance", acc.longValue("balance") + 100));
     * });
     * }
     * </pre>
     *
     * <p>The <b>correct</b> variant will be:
     * <pre>
     * {@code
     * igniteTransactions.runInTransaction(tx -> {
     *     view.getAsync(tx, Tuple.create().set("accountId", 1)).thenCompose(acc ->
     *         view.upsertAsync(tx, Tuple.create().set("accountId", 1).set("balance", acc.longValue("balance") + 100))).join();
     * });
     * }
     * </pre>
     *
     * <p>If the closure is executed normally (no exceptions) the transaction is automatically committed. In a case of exception, the
     * closure will be retried automatically within the transaction timeout, so it must be pure function. If the transaction timeout
     * expires before the closure completes successfully and the transaction has been committed, the transaction is rolled back instead.
     * <br>
     * The closure is retried only in cases of "expected" exceptions, like {@code LockException}, {@code TimeoutException},
     * exceptions related to the primary replica change, etc.
     *
     * @param clo Closure.
     * @param <T> Closure result type.
     * @return Result.
     *
     * @throws TransactionException If a transaction can't be finished successfully.
     */
    default <T> T runInTransaction(Function<Transaction, T> clo) throws TransactionException {
        return runInTransaction(clo, null);
    }

    /**
     * Executes a closure within a transaction and returns a result.
     *
     * <p>This method expects that all transaction operations are completed before the closure returns. The safest way to achieve that is
     * to use synchronous table API.
     *
     * <p>Take care then using the asynchronous operations inside the closure. For example, the following snippet is <b>incorrect</b>,
     * because the last operation goes out of the scope of the closure unfinished:
     * <pre>
     * {@code
     * igniteTransactions.runInTransaction(tx -> {
     *     var key = Tuple.create().set("accountId", 1);
     *     Tuple acc = view.get(tx, key);
     *     view.upsertAsync(tx, Tuple.create().set("accountId", 1).set("balance", acc.longValue("balance") + 100));
     * });
     * }
     * </pre>
     *
     * <p>The <b>correct</b> variant will be:
     * <pre>
     * {@code
     * igniteTransactions.runInTransaction(tx -> {
     *     view.getAsync(tx, Tuple.create().set("accountId", 1)).thenCompose(acc ->
     *         view.upsertAsync(tx, Tuple.create().set("accountId", 1).set("balance", acc.longValue("balance") + 100))).join();
     * });
     * }
     * </pre>
     *
     * <p>If the closure is executed normally (no exceptions) the transaction is automatically committed. In a case of exception, the
     * closure will be retried automatically within the transaction timeout, so it must be pure function. If the transaction timeout
     * expires before the closure completes successfully and the transaction has been committed, the transaction is rolled back instead.
     * <br>
     * The closure is retried only in cases of "expected" exceptions, like {@code LockException}, {@code TimeoutException},
     * exceptions related to the primary replica change, etc.
     *
     * @param clo The closure.
     * @param options Transaction options.
     * @param <T> Closure result type.
     * @return The result.
     *
     * @throws TransactionException If a transaction can't be finished successfully.
     */
    default <T> T runInTransaction(Function<Transaction, T> clo, @Nullable TransactionOptions options) throws TransactionException {
        // This start timestamp is not related to transaction's begin timestamp and only serves as local time for counting the timeout of
        // possible retries.
        long startTimestamp = System.currentTimeMillis();
        long initialTimeout = options == null ? TimeUnit.SECONDS.toMillis(DEFAULT_RW_TX_TIMEOUT_SECONDS) : options.timeoutMillis();
        return runInTransactionInternal(this, clo, options, startTimestamp, initialTimeout);
    }

    /**
     * Executes a closure within a transaction asynchronously.
     *
     * <p>A returned future must be the last in the asynchronous chain. This means all transaction operations happen before the future
     * is completed.
     *
     * <p>Consider the example:
     * <pre>
     * {@code
     *     igniteTransactions.runInTransactionAsync(tx -> view.getAsync(tx, Tuple.create().set("accountId", 1)).thenCompose(
     *         acc -> view.upsertAsync(tx, Tuple.create().set("accountId", 1).set("balance", acc.longValue("balance") + 100))));
     * }
     * </pre>
     *
     * <p>If the asynchronous chain resulted with no exception, the commitAsync will be automatically called. In a case of exception, the
     * closure will be retried automatically within the transaction timeout, so it must be pure function. If the transaction timeout
     * expires before the closure completes successfully and the transaction has been committed, the transaction is rolled back instead.
     * <br>
     * The closure is retried only in cases of "expected" exceptions, like {@code LockException}, {@code TimeoutException},
     * exceptions related to the primary replica change, etc.
     *
     * @param clo The closure.
     * @param <T> Closure result type.
     * @return The result.
     */
    default <T> CompletableFuture<T> runInTransactionAsync(Function<Transaction, CompletableFuture<T>> clo) {
        return runInTransactionAsync(clo, null);
    }

    /**
     * Executes a closure within a transaction asynchronously.
     *
     * <p>A returned future must be the last in the asynchronous chain. This means all transaction operations happen before the future
     * is completed.
     *
     * <p>Consider the example:
     * <pre>
     * {@code
     *     igniteTransactions.runInTransactionAsync(tx -> view.getAsync(tx, Tuple.create().set("accountId", 1)).thenCompose(
     *         acc -> view.upsertAsync(tx, Tuple.create().set("accountId", 1).set("balance", acc.longValue("balance") + 100))));
     * }
     * </pre>
     *
     * <p>If the asynchronous chain resulted with no exception, the commitAsync will be automatically called. In a case of exception, the
     * closure will be retried automatically within the transaction timeout, so it must be pure function. If the transaction timeout
     * expires before the closure completes successfully and the transaction has been committed, the transaction is rolled back instead.
     * <br>
     * The closure is retried only in cases of "expected" exceptions, like {@code LockException}, {@code TimeoutException},
     * exceptions related to the primary replica change, etc.
     *
     *
     * @param clo The closure.
     * @param options Transaction options.
     * @param <T> Closure result type.
     * @return The result.
     */
    default <T> CompletableFuture<T> runInTransactionAsync(
            Function<Transaction, CompletableFuture<T>> clo,
            @Nullable TransactionOptions options
    ) {
        // This start timestamp is not related to transaction's begin timestamp and only serves as local time for counting the timeout of
        // possible retries.
        long startTimestamp = System.currentTimeMillis();
        long initialTimeout = options == null ? TimeUnit.SECONDS.toMillis(DEFAULT_RW_TX_TIMEOUT_SECONDS) : options.timeoutMillis();
        return runInTransactionAsyncInternal(this, clo, options, startTimestamp, initialTimeout, null);
    }
}
