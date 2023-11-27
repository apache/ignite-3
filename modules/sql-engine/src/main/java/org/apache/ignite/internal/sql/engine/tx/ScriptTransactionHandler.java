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
    /** Wraps a transaction initiated by a script control statement. */
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
    public QueryTransactionWrapper startTxIfNeeded(ParsedResult parsedResult, CompletableFuture<? extends AsyncCursor<?>> cursorFut) {
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
                return QueryTransactionWrapper.NOOP_TX_WRAPPER;
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

    /** Wraps an explicit transaction initiated by a script control statement. */
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
            return new ImplicitTransactionWrapper(transaction, true);
        }

        /** Returns a transaction wrapper that is responsible for processing a statement within a script-driven transaction. */
        QueryTransactionWrapper forStatement(SqlQueryType queryType, CompletableFuture<? extends AsyncCursor<?>> cursorFut) {
            return this;
        }
    }

    /**
     * Holds a transaction initiated by a script control statement. Responsible for tracking and releasing resources
     * associated with an explicit read-write transaction started from a script.
     */
    private static class ManagedReadWriteTransactionWrapper extends ManagedTransactionWrapper {
        private final CompletableFuture<Void> finishTxFuture = new CompletableFuture<>();
        private final List<CompletableFuture<? extends AsyncCursor<?>>> cursorsToCloseOnRollback = new CopyOnWriteArrayList<>();
        private final Set<UUID> cursorsToWaitBeforeCommit = ConcurrentHashMap.newKeySet();

        /** */
        private volatile boolean canCommit;

        private ManagedReadWriteTransactionWrapper(InternalTransaction transaction) {
            super(transaction);
        }

        @Override
        QueryTransactionWrapper forCommit() {
            canCommit = true;

            commitIfNeeded();

            return new ReadWriteTxCommitWrapper();
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
                return commitIfNeeded();
            }

            return Commons.completedFuture();
        }

        private CompletableFuture<Void> commitIfNeeded() {
            if (canCommit && cursorsToWaitBeforeCommit.isEmpty()) {
                return transaction.commitAsync()
                        .whenComplete((r, e) -> finishTxFuture.complete(null));
            }

            return Commons.completedFuture();
        }

        class ReadWriteTxCommitWrapper implements QueryTransactionWrapper {
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
                return finishTxFuture;
            }
        }

        class ReadWriteTxStatementWrapper extends ReadWriteTxCommitWrapper {
            private final UUID cursorId;

            ReadWriteTxStatementWrapper(UUID cursorId) {
                this.cursorId = cursorId;
            }

            @Override
            public CompletableFuture<Void> commitImplicit() {
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
}
