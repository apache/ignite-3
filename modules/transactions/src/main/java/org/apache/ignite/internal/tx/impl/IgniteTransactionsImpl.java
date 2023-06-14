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
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.tx.IgniteTransactions;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionOptions;
import org.jetbrains.annotations.Nullable;

/**
 * Transactions facade implementation.
 */
public class IgniteTransactionsImpl implements IgniteTransactions {
    private final TxManager txManager;

    /**
     * The constructor.
     *
     * @param txManager The manager.
     */
    public IgniteTransactionsImpl(TxManager txManager) {
        this.txManager = txManager;
    }

    /** {@inheritDoc} */
    @Override
    public Transaction begin(@Nullable TransactionOptions options) {
        if (options != null && options.timeoutMillis() != 0) {
            // TODO: IGNITE-15936.
            throw new UnsupportedOperationException("Timeouts are not supported yet");
        }

        return txManager.begin(options != null && options.readOnly());
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Transaction> beginAsync(@Nullable TransactionOptions options) {
        if (options != null && options.timeoutMillis() != 0) {
            // TODO: IGNITE-15936.
            throw new UnsupportedOperationException("Timeouts are not supported yet");
        }

        return CompletableFuture.completedFuture(txManager.begin(options != null && options.readOnly()));
    }
}
