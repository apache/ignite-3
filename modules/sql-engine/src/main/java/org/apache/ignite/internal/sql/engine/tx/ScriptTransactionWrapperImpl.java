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

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
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
    private final InternalTransaction transaction;

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
    private final AtomicReference<Throwable> rollbackCause = new AtomicReference<>();

    ScriptTransactionWrapperImpl(InternalTransaction transaction) {
        this.transaction = transaction;
    }

    @Override
    public InternalTransaction unwrap() {
        return transaction;
    }

    @Override
    public CompletableFuture<Void> commitImplicit() {
        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> rollback(Throwable cause) {
        if (rollbackCause.compareAndSet(null, cause)) {
            // Close all associated cursors on error.
            for (CompletableFuture<? extends AsyncCursor<?>> fut : openedCursors.values()) {
                closeCursor(fut);
            }

            transaction.rollbackAsync().whenComplete((r, e) -> {
                if (e != null) {
                    txFinishFuture.completeExceptionally(e);
                } else {
                    txFinishFuture.complete(null);
                }
            });
        }

        return txFinishFuture;
    }

    QueryTransactionWrapper forCommit() {
        boolean success = finishTxAction.compareAndSet(null, Transaction::commitAsync);

        assert success;

        tryCompleteTx();

        return new ScriptCommitTxWrapper();
    }

    QueryTransactionWrapper forStatement(SqlQueryType queryType, CompletableFuture<? extends AsyncCursor<?>> cursorFut) {
        UUID cursorId = UUID.randomUUID();

        assert queryType != SqlQueryType.DDL;

        Throwable err = rollbackCause.get();
        if (err != null) {
            throwRollbackError(err);
        }

        if (queryType != SqlQueryType.EXPLAIN) {
            openedCursors.put(cursorId, cursorFut);
        }

        err = rollbackCause.get();
        if (err != null) {
            closeCursor(cursorFut);
            throwRollbackError(err);
        }

        return new ScriptStatementTxWrapper(cursorId);
    }

    private CompletableFuture<Void> tryCompleteTxOnCursorClose(UUID cursorId) {
        if (openedCursors.remove(cursorId) != null) {
            return tryCompleteTx();
        }

        return nullCompletedFuture();
    }

    private CompletableFuture<Void> tryCompleteTx() {
        Function<InternalTransaction, CompletableFuture<Void>> action = finishTxAction.get();

        if (action != null && openedCursors.isEmpty()) {
            return action.apply(transaction)
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

    private static void closeCursor(CompletableFuture<? extends AsyncCursor<?>> fut) {
        fut.whenComplete((cursor, ex) -> {
            if (cursor != null) {
                cursor.closeAsync();
            }
        });
    }

    private static void throwRollbackError(Throwable err) {
        throw new SqlException(EXECUTION_CANCELLED_ERR,
                "The transaction has already been rolled back due to an error in the previous statement.", err);
    }

    class ScriptStatementTxWrapper implements QueryTransactionWrapper {
        private final UUID cursorId;

        ScriptStatementTxWrapper(UUID cursorId) {
            this.cursorId = cursorId;
        }

        @Override
        public InternalTransaction unwrap() {
            return transaction;
        }

        @Override
        public CompletableFuture<Void> commitImplicit() {
            // Try finish transaction on cursor close.
            return tryCompleteTxOnCursorClose(cursorId);
        }

        @Override
        public CompletableFuture<Void> commitImplicitAfterPrefetch() {
            return nullCompletedFuture();
        }

        @Override
        public CompletableFuture<Void> rollback(Throwable cause) {
            if (cause != null) {
                return ScriptTransactionWrapperImpl.this.rollback(cause);
            } else {
                boolean success = finishTxAction.compareAndSet(null, InternalTransaction::rollbackAsync);

                assert success;

                // Wait until all cursors will be closed.
                tryCompleteTx();

                return txFinishFuture;
            }
        }
    }

    class ScriptCommitTxWrapper implements QueryTransactionWrapper {
        @Override
        public InternalTransaction unwrap() {
            return transaction;
        }

        @Override
        public CompletableFuture<Void> commitImplicit() {
            return nullCompletedFuture();
        }

        @Override
        public CompletableFuture<Void> rollback(Throwable cause) {
            return nullCompletedFuture();
        }

        @Override
        public CompletableFuture<Void> commitImplicitAfterPrefetch() {
            return txFinishFuture;
        }
    }
}
