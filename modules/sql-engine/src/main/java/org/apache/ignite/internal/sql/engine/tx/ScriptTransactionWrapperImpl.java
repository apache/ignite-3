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

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.AsyncCursor;
import org.apache.ignite.sql.SqlException;

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
    private final Map<UUID, CompletableFuture<? extends AsyncCursor<?>>> openedCursors = new HashMap<>();

    private final Object mux = new Object();

    private volatile State txState;

    private Throwable rollbackCause;

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
        Collection<CompletableFuture<? extends AsyncCursor<?>>> cursorsToClose;

        synchronized (mux) {
            if (rollbackCause != null) {
                return txFinishFuture;
            }

            rollbackCause = cause;
            txState = State.ROLLBACK;

            cursorsToClose = List.copyOf(openedCursors.values());
        }

        // Close all associated cursors on error.
        for (CompletableFuture<? extends AsyncCursor<?>> fut : cursorsToClose) {
            fut.whenComplete((cursor, ex) -> {
                if (cursor != null) {
                    cursor.closeAsync();
                }
            });
        }

        completeTx();

        return txFinishFuture;
    }

    @Override
    public boolean implicit() {
        return false;
    }

    /** Returns a future that completes after the script-driven transaction commits. */
    CompletableFuture<Void> commit() {
        changeState(State.COMMIT);

        return txFinishFuture;
    }

    /** Rolls back the transaction when all cursors are closed. */
    void rollbackWhenCursorsClosed() {
        changeState(State.ROLLBACK);
    }

    /** Registers a new cursor associated with the current transaction. */
    void registerCursorFuture(CompletableFuture<AsyncSqlCursor<InternalSqlRow>> cursorFut) {
        UUID cursorId = UUID.randomUUID();

        synchronized (mux) {
            if (txState != null) {
                assert txState == State.ROLLBACK;

                throw new SqlException(EXECUTION_CANCELLED_ERR,
                        "The transaction has already been rolled back due to an error in the previous statement.", rollbackCause);
            }

            openedCursors.put(cursorId, cursorFut);
        }

        cursorFut.whenComplete((cur, ex) -> {
            if (cur != null) {
                cur.onClose().whenComplete((none, ignored) -> {
                    synchronized (mux) {
                        if (openedCursors.remove(cursorId) == null
                                || txState == null
                                || !openedCursors.isEmpty()) {
                            return;
                        }
                    }

                    completeTx();
                });
            }
        });
    }

    private void changeState(State newState) {
        synchronized (mux) {
            if (txState != null) {
                return;
            }

            txState = newState;

            if (!openedCursors.isEmpty()) {
                return;
            }
        }

        completeTx();
    }

    private void completeTx() {
        // Intentional volatile read outside of a synchronized block.
        switch (txState) {
            case COMMIT:
                managedTx.commitAsync().whenComplete(this::completeTxFuture);
                break;

            case ROLLBACK:
                managedTx.rollbackAsync().whenComplete(this::completeTxFuture);
                break;

            default:
                throw new IllegalStateException("Unknown transaction target state: " + txState);
        }
    }

    private void completeTxFuture(Void unused, Throwable e) {
        if (e != null) {
            txFinishFuture.completeExceptionally(e);
        } else {
            txFinishFuture.complete(null);
        }
    }

    private enum State {
        /** The transaction must be committed when all cursors are closed. */
        COMMIT,

        /** The transaction must be rolled back when all cursors are closed. */
        ROLLBACK
    }
}
