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

import static org.apache.ignite.internal.util.ExceptionUtils.copyExceptionWithCause;
import static org.apache.ignite.internal.util.ExceptionUtils.sneakyThrow;
import static org.apache.ignite.internal.util.ExceptionUtils.withCause;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_COMMIT_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_ROLLBACK_ERR;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.TxStateMeta;
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

    /** The tracker is used to track an observable timestamp. */
    protected final HybridTimestampTracker observableTsTracker;

    /** Transaction coordinator ephemeral ID. */
    private final UUID coordinatorId;

    /** Implicit transaction flag. */
    private final boolean implicit;

    /** Transaction timeout. */
    protected final long timeout;

    /** Flag indicating that the transaction was rolled back due to timeout. */
    protected volatile boolean timeoutExceeded;

    /**
     * The constructor.
     *
     * @param txManager The tx manager.
     * @param observableTsTracker Observation timestamp tracker.
     * @param id The id.
     * @param coordinatorId Transaction coordinator inconsistent ID.
     * @param implicit True for an implicit transaction, false for an ordinary one.
     * @param timeout Transaction timeout in milliseconds.
     */
    IgniteAbstractTransactionImpl(
            TxManager txManager,
            HybridTimestampTracker observableTsTracker,
            UUID id,
            UUID coordinatorId,
            boolean implicit,
            long timeout
    ) {
        this.txManager = txManager;
        this.observableTsTracker = observableTsTracker;
        this.id = id;
        this.coordinatorId = coordinatorId;
        this.implicit = implicit;
        this.timeout = timeout;
    }

    /** {@inheritDoc} */
    @Override
    public UUID id() {
        return id;
    }

    /**
     * Get the transaction coordinator inconsistent ID.
     *
     * @return Transaction coordinator inconsistent ID.
     */
    @Override
    public UUID coordinatorId() {
        return coordinatorId;
    }

    @Override
    public boolean implicit() {
        return implicit;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable TxState state() {
        TxStateMeta meta = txManager.stateMeta(id);

        return meta == null ? null : meta.txState();
    }

    /** {@inheritDoc} */
    @Override
    public void commit() throws TransactionException {
        try {
            commitAsync().get();
        } catch (ExecutionException e) {
            throw sneakyThrow(tryToCopyExceptionWithCause(e));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw withCause(TransactionException::new, TX_COMMIT_ERR, e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void rollback() throws TransactionException {
        try {
            rollbackAsync().get();
        } catch (ExecutionException e) {
            throw sneakyThrow(tryToCopyExceptionWithCause(e));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw withCause(TransactionException::new, TX_ROLLBACK_ERR, e);
        }
    }

    // TODO: remove after IGNITE-22721 gets resolved.
    private static Throwable tryToCopyExceptionWithCause(ExecutionException exception) {
        Throwable copy = copyExceptionWithCause(exception);

        if (copy == null) {
            return new TransactionException(INTERNAL_ERR, "Cannot make a proper copy of " + exception.getCause().getClass(), exception);
        }

        return copy;
    }

    /** {@inheritDoc} */
    @Override
    public long getTimeout() {
        return timeout;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isRolledBackWithTimeoutExceeded() {
        return timeoutExceeded;
    }
}
