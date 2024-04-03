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

import static org.apache.ignite.internal.thread.PublicApiThreading.preventThreadHijack;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.ignite.internal.wrapper.Wrapper;
import org.apache.ignite.tx.IgniteTransactions;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionOptions;
import org.jetbrains.annotations.Nullable;

/**
 * Wrapper around {@link IgniteTransactions} that adds protection against thread hijacking by users.
 */
public class AntiHijackIgniteTransactions implements IgniteTransactions, Wrapper {
    private final IgniteTransactions transactions;
    private final Executor asyncContinuationExecutor;

    public AntiHijackIgniteTransactions(IgniteTransactions transactions, Executor asyncContinuationExecutor) {
        this.transactions = transactions;
        this.asyncContinuationExecutor = asyncContinuationExecutor;
    }

    @Override
    public Transaction begin(@Nullable TransactionOptions options) {
        return applyProtection(transactions.begin(options));
    }

    @Override
    public CompletableFuture<Transaction> beginAsync(@Nullable TransactionOptions options) {
        return preventThreadHijack(transactions.beginAsync(options), asyncContinuationExecutor)
                .thenApply(this::applyProtection);
    }

    private Transaction applyProtection(Transaction transaction) {
        return new AntiHijackTransaction(transaction, asyncContinuationExecutor);
    }

    @Override
    public <T> T unwrap(Class<T> classToUnwrap) {
        return classToUnwrap.cast(transactions);
    }
}
