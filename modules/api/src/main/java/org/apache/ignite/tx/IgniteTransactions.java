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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.function.Function.identity;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.table.Table;

/**
 * Ignite Transactions facade that allows to perform distributed transactions when working with tables.
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
     * Returns a facade with new default timeout.
     *
     * @param timeout The timeout in milliseconds.
     *
     * @return A facade using a new timeout.
     */
    IgniteTransactions withTimeout(long timeout);

    /**
     * Begins a transaction.
     *
     * @return The started transaction.
     */
    Transaction begin();

    /**
     * Begins an async transaction.
     *
     * @return The future holding the started transaction.
     */
    CompletableFuture<Transaction> beginAsync();

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
     * <p>The correct variant will be:
     * <pre>
     * {@code
     * igniteTransactions.runInTransaction(tx -> {
     *     view.getAsync(tx, Tuple.create().set("accountId", 1)).thenCompose(acc ->
     *         view.upsertAsync(tx, Tuple.create().set("accountId", 1).set("balance", acc.longValue("balance") + 100))).join();
     * });
     * }
     * </pre>
     *
     * <p>If the closure is executed normally (no exceptions) the transaction is automatically committed.
     *
     * @param clo The closure.
     *
     * @throws TransactionException If a transaction can't be finished successfully.
     */
    default void runInTransaction(Consumer<Transaction> clo) throws TransactionException {
        runInTransaction(tx -> {
            clo.accept(tx);
            return null;
        });
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
     * <p>The correct variant will be:
     * <pre>
     * {@code
     * igniteTransactions.runInTransaction(tx -> {
     *     view.getAsync(tx, Tuple.create().set("accountId", 1)).thenCompose(acc ->
     *         view.upsertAsync(tx, Tuple.create().set("accountId", 1).set("balance", acc.longValue("balance") + 100))).join();
     * });
     * }
     * </pre>
     *
     * <p>If the closure is executed normally (no exceptions) the transaction is automatically committed.
     *
     * @param clo The closure.
     * @param <T> Closure result type.
     * @return The result.
     *
     * @throws TransactionException If a transaction can't be finished successfully.
     */
    default <T> T runInTransaction(Function<Transaction, T> clo) throws TransactionException {
        Transaction tx = begin();

        try {
            T ret = clo.apply(tx);

            tx.commit();

            return ret;
        } catch (Throwable t) {
            // TODO FIXME https://issues.apache.org/jira/browse/IGNITE-17838 Implement auto retries
            try {
                tx.rollback(); // Try rolling back on user exception.
            } catch (Exception e) {
                t.addSuppressed(e);
            }

            throw t;
        }
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
     * <p>If the asynchronous chain resulted in no exception, the commitAsync will be automatically called.
     *
     * @param clo The closure.
     * @param <T> Closure result type.
     * @return The result.
     */
    default <T> CompletableFuture<T> runInTransactionAsync(Function<Transaction, CompletableFuture<T>> clo) {
        // TODO FIXME https://issues.apache.org/jira/browse/IGNITE-17838 Implement auto retries
        return beginAsync().thenCompose(tx -> {
            try {
                return clo.apply(tx).handle((res, e) -> {
                    if (e != null) {
                        return tx.rollbackAsync().exceptionally(e0 -> {
                            e.addSuppressed(e0);
                            return null;
                        }).thenCompose(ignored -> CompletableFuture.<T>failedFuture(e));
                    }

                    return completedFuture(res);
                }).thenCompose(identity()).thenCompose(val -> tx.commitAsync().thenApply(ignored -> val));
            } catch (Exception e) {
                return tx.rollbackAsync().exceptionally(e0 -> {
                    e.addSuppressed(e0);
                    return null;
                }).thenCompose(ignored -> CompletableFuture.failedFuture(e));
            }
        });
    }
}
