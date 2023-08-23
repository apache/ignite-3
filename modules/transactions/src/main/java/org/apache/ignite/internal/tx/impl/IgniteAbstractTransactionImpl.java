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

import static org.apache.ignite.internal.util.ExceptionUtils.withCause;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_COMMIT_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_ROLLBACK_ERR;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;

/**
 * An abstract implementation of an ignite internal transaction.
 */
public abstract class IgniteAbstractTransactionImpl implements InternalTransaction {
    /** The id. */
    private final UUID id;

    /** The transaction manager. */
    protected final TxManager txManager;

    /**
     * The constructor.
     *
     * @param txManager The tx manager.
     * @param id The id.
     */
    public IgniteAbstractTransactionImpl(TxManager txManager, UUID id) {
        this.txManager = txManager;
        this.id = id;
    }

    /** {@inheritDoc} */
    @Override
    public UUID id() {
        return id;
    }

    /** {@inheritDoc} */
    @Nullable
    @Override
    public TxState state() {
        return txManager.state(id);
    }

    /** {@inheritDoc} */
    @Override
    public void commit() throws TransactionException {
        try {
            commitAsync().get();
        } catch (Exception e) {
            throw withCause(TransactionException::new, TX_COMMIT_ERR, e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> commitAsync() {
        return finish(true);
    }

    /** {@inheritDoc} */
    @Override
    public void rollback() throws TransactionException {
        try {
            rollbackAsync().get();
        } catch (Exception e) {
            throw withCause(TransactionException::new, TX_ROLLBACK_ERR, e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> rollbackAsync() {
        return finish(false);
    }

    /**
     * Finishes a transaction. A finish of a completed or ending transaction has no effect
     * and always succeeds when the transaction is completed.
     *
     * @param commit {@code true} to commit, false to rollback.
     * @return The future.
     */
    protected abstract CompletableFuture<Void> finish(boolean commit);
}
