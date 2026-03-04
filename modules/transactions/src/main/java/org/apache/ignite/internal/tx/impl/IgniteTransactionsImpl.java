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

package org.apache.ignite.internal.tx.impl;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.InternalTxOptions;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxPriority;
import org.apache.ignite.tx.IgniteTransactions;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionException;
import org.apache.ignite.tx.TransactionOptions;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Transactions facade implementation.
 */
public class IgniteTransactionsImpl implements IgniteTransactions {
    private final TxManager txManager;

    private final HybridTimestampTracker observableTimestampTracker;

    /**
     * The constructor.
     *
     * @param txManager The manager.
     */
    public IgniteTransactionsImpl(TxManager txManager, HybridTimestampTracker observableTimestampTracker) {
        this.txManager = txManager;
        this.observableTimestampTracker = observableTimestampTracker;
    }

    /** {@inheritDoc} */
    @Override
    public Transaction begin(@Nullable TransactionOptions options) {
        // TODO move to begin exp
        InternalTxOptions internalTxOptions = options == null
                ? InternalTxOptions.defaults()
                : InternalTxOptions.builder()
                        .timeoutMillis(options.timeoutMillis())
                        .txLabel(options.label())
                        .build();

        return txManager.beginExplicit(observableTimestampTracker, options != null && options.readOnly(), internalTxOptions);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Transaction> beginAsync(@Nullable TransactionOptions options) {
        return CompletableFuture.completedFuture(begin(options));
    }

    /**
     * Begins a transaction.
     *
     * @param readOnly {@code true} in order to start a read-only transaction, {@code false} in order to start read-write one.
     * @return The started transaction.
     */
    public InternalTransaction beginImplicit(boolean readOnly) {
        return txManager.beginImplicit(observableTimestampTracker, readOnly, null);
    }

    @TestOnly
    public Transaction beginWithPriority(boolean readOnly, TxPriority priority) {
        return txManager.beginExplicit(observableTimestampTracker, readOnly, InternalTxOptions.defaultsWithPriority(priority));
    }

    @Override
    public <T> T runInTransaction(Function<Transaction, T> clo, @Nullable TransactionOptions options) throws TransactionException {
        return txManager.runInTransaction(clo, observableTimestampTracker, options);
    }

    @Override
    public <T> CompletableFuture<T> runInTransactionAsync(
            Function<Transaction, CompletableFuture<T>> clo,
            @Nullable TransactionOptions options
    ) {
        //return txManager.runInTransaction(clo, observableTimestampTracker, tx);
        return CompletableFuture.failedFuture(new Exception());
    }
}
