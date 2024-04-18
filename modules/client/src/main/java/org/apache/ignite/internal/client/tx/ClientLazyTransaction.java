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

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.client.ReliableChannel;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;

/**
 * Lazy client transaction. Will be actually started on the first operation.
 */
public class ClientLazyTransaction implements Transaction {
    private final boolean readOnly;

    private volatile ClientTransaction tx;

    public ClientLazyTransaction(boolean readOnly) {
        // TODO: Preserve start timestamp for RO tx?
        this.readOnly = readOnly;
    }

    @Override
    public void commit() throws TransactionException {
        var tx0 = tx;

        if (tx0 == null) {
            // No operations were performed, nothing to commit.
            return;
        }

        tx0.commit();
    }

    @Override
    public CompletableFuture<Void> commitAsync() {
        var tx0 = tx;

        if (tx0 == null) {
            // No operations were performed, nothing to commit.
            return CompletableFuture.completedFuture(null);
        }

        return tx0.commitAsync();
    }

    @Override
    public void rollback() throws TransactionException {
        var tx0 = tx;

        if (tx0 == null) {
            // No operations were performed, nothing to rollback.
            return;
        }

        tx0.rollback();
    }

    @Override
    public CompletableFuture<Void> rollbackAsync() {
        var tx0 = tx;

        if (tx0 == null) {
            // No operations were performed, nothing to rollback.
            return CompletableFuture.completedFuture(null);
        }

        return tx0.rollbackAsync();
    }

    @Override
    public boolean isReadOnly() {
        return readOnly;
    }

    public CompletableFuture<Void> ensureStarted(
            ReliableChannel ch,
            @Nullable String preferredNodeName) {
        // TODO
        return CompletableFutures.nullCompletedFuture();
    }
}
