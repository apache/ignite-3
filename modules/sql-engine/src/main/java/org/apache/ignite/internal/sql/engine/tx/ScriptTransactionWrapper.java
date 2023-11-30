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

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.AsyncCursor;
import org.apache.ignite.tx.Transaction;

/**
 * Wraps a transaction, which is managed by SQL engine via {@link SqlQueryType#TX_CONTROL} statements.
 * Responsible for tracking and releasing resources associated with this transaction.
 */
class ScriptTransactionWrapper implements QueryTransactionWrapper {
    private final InternalTransaction transaction;

    /** Future completes when all cursors associated with the current transaction are closed. */
    private final CompletableFuture<Void> txFinishFuture = new CompletableFuture<>();

    /** Opened cursors that must be closed before the transaction can complete. */
    private final Map<UUID, CompletableFuture<? extends AsyncCursor<?>>> openedCursors = new ConcurrentHashMap<>();

    /** Transaction action (commit or rollback) that will be performed when all dependent cursors are closed. */
    private final AtomicReference<Function<InternalTransaction, CompletableFuture<Void>>> finishTxAction = new AtomicReference<>();

    ScriptTransactionWrapper(InternalTransaction transaction) {
        this.transaction = transaction;
    }

    @Override
    public InternalTransaction unwrap() {
        return transaction;
    }

    @Override
    public CompletableFuture<Void> commitImplicit() {
        return Commons.completedFuture();
    }

    @Override
    public CompletableFuture<Void> rollback(Throwable cause) {
        return transaction.rollbackAsync();
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

        if (queryType != SqlQueryType.EXPLAIN) {
            openedCursors.put(cursorId, cursorFut);
        }

        return new ScriptStatementTxWrapper(cursorId);
    }

    private CompletableFuture<Void> tryCompleteTxOnCursorClose(UUID cursorId) {
        if (openedCursors.remove(cursorId) != null) {
            return tryCompleteTx();
        }

        return Commons.completedFuture();
    }

    private CompletableFuture<Void> tryCompleteTx() {
        Function<InternalTransaction, CompletableFuture<Void>> action = finishTxAction.get();

        if (action != null && openedCursors.isEmpty()) {
            return action.apply(transaction)
                    .whenComplete((r, e) -> txFinishFuture.complete(null));
        }

        return Commons.completedFuture();
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
            return Commons.completedFuture();
        }

        @Override
        public CompletableFuture<Void> rollback(Throwable cause) {
            if (cause != null) {
                // Close all associated cursors on error.
                for (CompletableFuture<? extends AsyncCursor<?>> fut : openedCursors.values()) {
                    fut.whenComplete((cursor, ex) -> {
                        if (cursor != null) {
                            cursor.closeAsync();
                        }
                    });
                }

                return transaction.rollbackAsync();
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
            return Commons.completedFuture();
        }

        @Override
        public CompletableFuture<Void> rollback(Throwable cause) {
            return Commons.completedFuture();
        }

        @Override
        public CompletableFuture<Void> commitImplicitAfterPrefetch() {
            return txFinishFuture;
        }
    }
}
