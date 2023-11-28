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

import static org.apache.ignite.lang.ErrorGroups.Sql.RUNTIME_ERR;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.calcite.sql.SqlNode;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlCommitTransaction;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlStartTransaction;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlStartTransactionMode;
import org.apache.ignite.internal.sql.engine.sql.ParsedResult;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.AsyncCursor;
import org.apache.ignite.sql.ExternalTransactionNotSupportedException;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.tx.IgniteTransactions;
import org.apache.ignite.tx.TransactionOptions;
import org.jetbrains.annotations.Nullable;

/**
 * Starts an implicit transaction if there is no external transaction. Supports script transaction management using
 * {@link SqlQueryType#TX_CONTROL} statements.
 */
public class ScriptTransactionHandler extends QueryTransactionHandler {
    /** No-op transaction wrapper. */
    private static final QueryTransactionWrapper NOOP_TX_WRAPPER = new NoopTransactionWrapper();

    /** Wraps a transaction, which is managed by SQL engine via {@link SqlQueryType#TX_CONTROL} statements. */
    private volatile @Nullable ManagedTransactionWrapper wrapper;

    public ScriptTransactionHandler(IgniteTransactions transactions, @Nullable InternalTransaction externalTransaction) {
        super(transactions, externalTransaction);
    }

    @Override
    protected @Nullable InternalTransaction activeTransaction() {
        return externalTransaction == null ? scriptTransaction() : externalTransaction;
    }

    /**
     * Starts a transaction if there is no external transaction.
     *
     * @param parsedResult Parse result.
     * @param cursorFut Cursor future for the current statement.
     * @return Transaction wrapper.
     */
    public QueryTransactionWrapper startScriptTxIfNeeded(
            ParsedResult parsedResult,
            CompletableFuture<? extends AsyncCursor<?>> cursorFut
    ) {
        try {
            SqlQueryType queryType = parsedResult.queryType();

            if (queryType == SqlQueryType.TX_CONTROL) {
                if (externalTransaction != null) {
                    throw new ExternalTransactionNotSupportedException();
                }

                return handleTxControlStatement(parsedResult.parsedTree());
            }

            ManagedTransactionWrapper wrapper = this.wrapper;

            if (wrapper == null) {
                return startTxIfNeeded(queryType);
            }

            validateStatement(parsedResult.queryType(), wrapper.unwrap());

            return wrapper.forStatement(queryType, cursorFut);
        } catch (SqlException e) {
            InternalTransaction scriptTx = scriptTransaction();

            if (scriptTx != null) {
                scriptTx.rollback();
            }

            throw e;
        }
    }

    private @Nullable InternalTransaction scriptTransaction() {
        ManagedTransactionWrapper hld = wrapper;

        return hld != null ? hld.unwrap() : null;
    }

    private QueryTransactionWrapper handleTxControlStatement(SqlNode node) {
        ManagedTransactionWrapper txWrapper = this.wrapper;

        if (node instanceof IgniteSqlCommitTransaction) {
            if (txWrapper == null) {
                return NOOP_TX_WRAPPER;
            }

            this.wrapper = null;

            return txWrapper.forCommit();
        } else {
            assert node instanceof IgniteSqlStartTransaction : node == null ? "null" : node.getClass().getName();

            if (txWrapper != null) {
                throw new SqlException(RUNTIME_ERR, "Nested transactions are not supported.");
            }

            IgniteSqlStartTransaction txStartNode = (IgniteSqlStartTransaction) node;

            TransactionOptions options =
                    new TransactionOptions().readOnly(txStartNode.getMode() == IgniteSqlStartTransactionMode.READ_ONLY);

            InternalTransaction tx = (InternalTransaction) transactions.begin(options);

            txWrapper = tx.isReadOnly() ? new ManagedTransactionWrapper(tx) : new ManagedReadWriteTransactionWrapper(tx);

            this.wrapper = txWrapper;

            return txWrapper;
        }
    }

    /** Wraps a transaction, which is managed by SQL engine via {@link SqlQueryType#TX_CONTROL} statements. */
    private static class ManagedTransactionWrapper implements QueryTransactionWrapper {
        final InternalTransaction transaction;

        private ManagedTransactionWrapper(InternalTransaction transaction) {
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
        public CompletableFuture<Void> rollback() {
            return transaction.rollbackAsync();
        }

        /** Returns a transaction wrapper that is responsible for committing script-driven transaction. */
        QueryTransactionWrapper forCommit() {
            return new TxCommitWrapper(transaction.commitAsync());
        }

        /** Returns a transaction wrapper that is responsible for processing a statement within a script-driven transaction. */
        QueryTransactionWrapper forStatement(SqlQueryType queryType, CompletableFuture<? extends AsyncCursor<?>> cursorFut) {
            return this;
        }

        class TxCommitWrapper implements QueryTransactionWrapper {
            private final CompletableFuture<Void> txCommitFuture;

            TxCommitWrapper(CompletableFuture<Void> txCommitFuture) {
                this.txCommitFuture = txCommitFuture;
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
            public CompletableFuture<Void> rollback() {
                return Commons.completedFuture();
            }

            @Override
            public CompletableFuture<Void> commitImplicitAfterPrefetch() {
                return txCommitFuture;
            }
        }
    }

    /**
     * Wraps a transaction, which is managed by SQL engine via {@link SqlQueryType#TX_CONTROL} statements.
     * Responsible for tracking and releasing resources associated with this transaction.
     */
    private static class ManagedReadWriteTransactionWrapper extends ManagedTransactionWrapper {
        private final CompletableFuture<Void> finishTxFuture = new CompletableFuture<>();
        private final List<CompletableFuture<? extends AsyncCursor<?>>> cursorsToCloseOnRollback = new CopyOnWriteArrayList<>();
        private final Set<UUID> cursorsToWaitBeforeCommit = ConcurrentHashMap.newKeySet();

        /**
         * Flag indicating that a {@code COMMIT} statement has been processed and
         * the transaction can be committed after the associated cursors are closed.
         */
        private volatile boolean canCommit;

        private ManagedReadWriteTransactionWrapper(InternalTransaction transaction) {
            super(transaction);
        }

        @Override
        QueryTransactionWrapper forCommit() {
            canCommit = true;

            commitIfPossible();

            return new TxCommitWrapper(finishTxFuture);
        }

        @Override
        QueryTransactionWrapper forStatement(SqlQueryType queryType, CompletableFuture<? extends AsyncCursor<?>> cursorFut) {
            cursorsToCloseOnRollback.add(cursorFut);

            UUID cursorId = UUID.randomUUID();

            if (queryType == SqlQueryType.QUERY) {
                cursorsToWaitBeforeCommit.add(cursorId);
            }

            return new ReadWriteTxStatementWrapper(cursorId);
        }

        private CompletableFuture<Void> tryCommit(UUID cursorId) {
            if (cursorsToWaitBeforeCommit.remove(cursorId)) {
                return commitIfPossible();
            }

            return Commons.completedFuture();
        }

        private CompletableFuture<Void> commitIfPossible() {
            if (canCommit && cursorsToWaitBeforeCommit.isEmpty()) {
                return transaction.commitAsync()
                        .whenComplete((r, e) -> finishTxFuture.complete(null));
            }

            return Commons.completedFuture();
        }

        class ReadWriteTxStatementWrapper implements QueryTransactionWrapper {
            private final UUID cursorId;

            ReadWriteTxStatementWrapper(UUID cursorId) {
                this.cursorId = cursorId;
            }

            @Override
            public InternalTransaction unwrap() {
                return transaction;
            }

            @Override
            public CompletableFuture<Void> commitImplicit() {
                // Try commit on cursor close.
                return tryCommit(cursorId);
            }

            @Override
            public CompletableFuture<Void> commitImplicitAfterPrefetch() {
                return Commons.completedFuture();
            }

            @Override
            public CompletableFuture<Void> rollback() {
                // Close all associated cursors.
                for (CompletableFuture<? extends AsyncCursor<?>> fut : cursorsToCloseOnRollback) {
                    fut.whenComplete((cursor, ex) -> {
                        if (cursor != null) {
                            cursor.closeAsync();
                        }
                    });
                }

                return transaction.rollbackAsync();
            }
        }
    }

    private static class NoopTransactionWrapper implements QueryTransactionWrapper {
        @Override
        public InternalTransaction unwrap() {
            return null;
        }

        @Override
        public CompletableFuture<Void> commitImplicit() {
            return Commons.completedFuture();
        }

        @Override
        public CompletableFuture<Void> rollback() {
            return Commons.completedFuture();
        }
    }
}
