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

package org.apache.ignite.internal.sql.engine.tx;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.lang.ErrorGroups.Sql.EXECUTION_CANCELLED_ERR;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.AsyncCursor;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.tx.Transaction;

/**
 * Wraps a transaction, which is managed by SQL engine via {@link SqlQueryType#TX_CONTROL} statements.
 * Responsible for tracking and releasing resources associated with this transaction.
 */
class ScriptTransactionWrapperImpl implements QueryTransactionWrapper {
    /** Script-driven transaction. */
    private final InternalTransaction managedTx;

    /**
     * The future is completed after transaction started from the script is committed or rolled back.
     *
     * <p>It is completed in the following cases.
     * <ol>
     * <li>All associated cursors have been closed and there was a COMMIT statement (the transaction is committed).</li>
     * <li>All associated cursors have been closed and we have reached the end of the script (the transaction is rolled back).</li>
     * <li>The transaction was rolled back due to an error while reading the cursor. In this case, all associated cursors are closed.</li>
     * </ol>
     */
    private final CompletableFuture<Void> txFinishFuture = new CompletableFuture<>();

    /** Opened cursors that must be closed before the transaction can complete. */
    private final Map<UUID, CompletableFuture<? extends AsyncCursor<?>>> openedCursors = new ConcurrentHashMap<>();

    /** Transaction action (commit or rollback) that will be performed when all dependent cursors are closed. */
    private final AtomicReference<Function<InternalTransaction, CompletableFuture<Void>>> finishTxAction = new AtomicReference<>();

    /** Error that caused the transaction to be rolled back. */
    private volatile Throwable rollbackCause;

    /** Mutex to synchronize registering a new cursor with closing the cursor. */
    private final Object mux = new Object();

    ScriptTransactionWrapperImpl(InternalTransaction managedTx) {
        this.managedTx = managedTx;
    }

    @Override
    public InternalTransaction unwrap() {
        return managedTx;
    }

    @Override
    public CompletableFuture<Void> commitImplicit() {
        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> rollback(Throwable cause) {
        assert cause != null;

        if (rollbackCause == null) {
            synchronized (mux) {
                if (rollbackCause != null) {
                    return txFinishFuture;
                }

                rollbackCause = cause;

                // Close all associated cursors on error.
                for (CompletableFuture<? extends AsyncCursor<?>> fut : openedCursors.values()) {
                    fut.whenComplete((cursor, ex) -> {
                        if (cursor != null) {
                            cursor.closeAsync();
                        }
                    });
                }
            }

            managedTx.rollbackAsync().whenComplete((r, e) -> {
                if (e != null) {
                    txFinishFuture.completeExceptionally(e);
                } else {
                    txFinishFuture.complete(null);
                }
            });
        }

        return txFinishFuture;
    }

    public CompletableFuture<Void> commit() {
        boolean success = finishTxAction.compareAndSet(null, Transaction::commitAsync);

        assert success;

        tryCompleteTx();

        return txFinishFuture;
    }

    void rollbackWhenCursorsClosed() {
        boolean success = finishTxAction.compareAndSet(null, InternalTransaction::rollbackAsync);

        assert success;

        // Wait until all cursors will be closed.
        tryCompleteTx();
    }

    void registerCursor(CompletableFuture<AsyncSqlCursor<List<Object>>> cursorFut) {
        UUID cursorId = UUID.randomUUID();

        synchronized (mux) {
            Throwable err = rollbackCause;

            if (err != null) {
                throw new SqlException(EXECUTION_CANCELLED_ERR,
                        "The transaction has already been rolled back due to an error in the previous statement.", err);
            }

            openedCursors.put(cursorId, cursorFut);
        }

        cursorFut.whenComplete((cur, ex) -> {
            if (cur != null) {
                cur.onClose(() -> {
                    if (openedCursors.remove(cursorId) != null) {
                        return tryCompleteTx();
                    }

                    return nullCompletedFuture();
                });
            }
        });
    }

    private CompletableFuture<Void> tryCompleteTx() {
        Function<InternalTransaction, CompletableFuture<Void>> action = finishTxAction.get();

        if (action != null && openedCursors.isEmpty()) {
            return action.apply(managedTx)
                    .whenComplete((r, e) -> {
                        if (e != null) {
                            txFinishFuture.completeExceptionally(e);
                        } else {
                            txFinishFuture.complete(null);
                        }
                    });
        }

        return nullCompletedFuture();
    }

}
