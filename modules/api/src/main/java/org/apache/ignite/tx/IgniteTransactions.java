/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.table.Table;

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
     * Returns a facade with the new default timeout.
     *
     * @param timeout Timeout in milliseconds.
     *
     * @return Facade with the new timeout.
     */
    IgniteTransactions withTimeout(long timeout);

    /**
     * Begins a transaction.
     *
     * @return Started transaction.
     */
    Transaction begin();

    /**
     * Begins an asynchronous transaction.
     *
     * @return The future that holds the started transaction.
     */
    CompletableFuture<Transaction> beginAsync();

    /**
     * Executes a closure within a transaction.
     *
     * <p>If the closure is executed normally (no exceptions), the transaction is automatically committed.
     *
     * @param clo Closure.
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
     * <p>If the closure is executed normally (no exceptions), the transaction is automatically committed.
     *
     * <p>This method automatically enlists all tables into the transaction. The transaction execution 
     * shouldn't leave the starting thread. If it does, an exception is thrown.
     *
     * @param clo Closure.
     * @param <T> Closure result type.
     * @return Result.
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
            try {
                tx.rollback(); // Try rolling back on user exception.
            } catch (Exception e) {
                t.addSuppressed(e);
            }

            throw t;
        }
    }
}
