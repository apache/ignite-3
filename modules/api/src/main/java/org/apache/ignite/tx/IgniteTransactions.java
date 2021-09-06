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

/**
 * Ignite Transactions facade.
 */
public interface IgniteTransactions {
    /**
     * @param timeout The timeout.
     * @return A facade with changed timeout.
     */
    IgniteTransactions withTimeout(long timeout);

    /**
     * Begins a transaction.
     * <p>
     * Tables must be enlisted into transaction using
     * {@link org.apache.ignite.table.Table#withTransaction(org.apache.ignite.tx.Transaction)}
     * <p>
     * Transaction logic is allowed for execute in any thread of control.
     * @return The future with a new transaction as a result.
     * @throws TransactionException If a transaction can't be started for a some reason.
     */
    Transaction begin() throws TransactionException;

    /**
     * Begins an async transaction.
     * <p>
     * Tables must be enlisted into transaction using
     * {@link org.apache.ignite.table.Table#withTransaction(org.apache.ignite.tx.Transaction)}
     * <p>
     * Transaction logic is allowed for execute in any thread of control.
     * @return The future with a new transaction as a result.
     */
    CompletableFuture<Transaction> beginAsync();

    /**
     * Executes a closure within a transaction.
     * <p>
     * If the closure is executed normally (no exceptions), the transaction is automatically committed.
     * <p>
     * This method will automatically enlist tables into the transaction, but the execution of
     * the transaction shouldn't leave starting thread or an exception will be thrown.
     * @param clo The closure.
     * @throws TransactionException If a transaction has failed to finish normally.
     */
    void runInTransaction(Consumer<Transaction> clo) throws TransactionException;

    /**
     * Executes a closure within a transaction and returns a result.
     * <p>
     * If the closure is executed normally (no exceptions), the transaction is automatically committed.
     * <p>
     * This method will automatically enlist all tables into the transaction, but the execution of
     * the transaction shouldn't leave starting thread or an exception will be thrown.
     * @param clo The closure.
     * @throws TransactionException If a transaction has failed to finish normally.
     */
    <T> T runInTransaction(Function<Transaction, T> clo) throws TransactionException;
}
