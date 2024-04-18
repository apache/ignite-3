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

package org.apache.ignite.internal.client.tx;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.client.ReliableChannel;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionException;
import org.apache.ignite.tx.TransactionOptions;
import org.jetbrains.annotations.Nullable;

/**
 * Lazy client transaction. Will be actually started on the first operation.
 */
public class ClientLazyTransaction implements Transaction {
    private final long observableTimestamp;

    private final @Nullable TransactionOptions options;

    private volatile CompletableFuture<ClientTransaction> tx;

    ClientLazyTransaction(long observableTimestamp, @Nullable TransactionOptions options) {
        this.observableTimestamp = observableTimestamp;
        this.options = options;
    }

    @Override
    public void commit() throws TransactionException {
        var tx0 = tx;

        if (tx0 == null) {
            // No operations were performed, nothing to commit.
            return;
        }

        tx0.join().commit();
    }

    @Override
    public CompletableFuture<Void> commitAsync() {
        var tx0 = tx;

        if (tx0 == null) {
            // No operations were performed, nothing to commit.
            return CompletableFuture.completedFuture(null);
        }

        return tx0.thenCompose(ClientTransaction::commitAsync);
    }

    @Override
    public void rollback() throws TransactionException {
        var tx0 = tx;

        if (tx0 == null) {
            // No operations were performed, nothing to rollback.
            return;
        }

        tx0.join().rollback();
    }

    @Override
    public CompletableFuture<Void> rollbackAsync() {
        var tx0 = tx;

        if (tx0 == null) {
            // No operations were performed, nothing to rollback.
            return CompletableFuture.completedFuture(null);
        }

        return tx0.thenCompose(ClientTransaction::rollbackAsync);
    }

    @Override
    public boolean isReadOnly() {
        return options != null && options.readOnly();
    }

    private synchronized CompletableFuture<ClientTransaction> ensureStarted(
            ReliableChannel ch,
            @Nullable String preferredNodeName) {
        var tx0 = tx;

        if (tx0 != null) {
            return tx0;
        }

        tx0 = ClientTransactions.beginAsync(ch, preferredNodeName, options, observableTimestamp);
        tx = tx0;

        return tx0;
    }

    public static CompletableFuture<?> ensureStarted(
            @Nullable Transaction tx,
            ReliableChannel ch,
            @Nullable String preferredNodeName) {
        if (tx == null) {
            return nullCompletedFuture();
        }

        if (!(tx instanceof ClientLazyTransaction)) {
            throw ClientTransaction.unsupportedTxTypeException(tx);
        }

        return ((ClientLazyTransaction) tx).ensureStarted(ch, preferredNodeName);
    }

    public ClientTransaction tx() {
        var tx0 = tx;

        assert tx0 != null : "Transaction is not started";
        assert tx0.isDone() : "Transaction is starting";

        return tx0.join();
    }
}
