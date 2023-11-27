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
    /** Manages the explicit transaction initiated by the script and associated resources. */
    private volatile @Nullable ManagedTransactionHandler handler;

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

                return processTxControlStatement(parsedResult.parsedTree());
            }

            ManagedTransactionHandler handler = this.handler;

            if (handler == null) {
                return startTxIfNeeded(queryType);
            }

            validateStatement(parsedResult.queryType(), handler.transaction());

            return handler.handleStatement(queryType, cursorFut);
        } catch (SqlException e) {
            InternalTransaction scriptTx = scriptTransaction();

            if (scriptTx != null) {
                scriptTx.rollback();
            }

            throw e;
        }
    }

    private @Nullable InternalTransaction scriptTransaction() {
        ManagedTransactionHandler hld = handler;

        return hld != null ? hld.transaction() : null;
    }

    private QueryTransactionWrapper processTxControlStatement(SqlNode node) {
        ManagedTransactionHandler handler = this.handler;

        if (node instanceof IgniteSqlCommitTransaction) {
            if (handler == null) {
                return QueryTransactionWrapper.NOOP_TX_WRAPPER;
            }

            this.handler = null;

            return handler.handleCommit();
        } else {
            assert node instanceof IgniteSqlStartTransaction : node == null ? "null" : node.getClass().getName();

            if (handler != null) {
                throw new SqlException(RUNTIME_ERR, "Nested transactions are not supported.");
            }

            IgniteSqlStartTransaction txStartNode = (IgniteSqlStartTransaction) node;

            TransactionOptions options =
                    new TransactionOptions().readOnly(txStartNode.getMode() == IgniteSqlStartTransactionMode.READ_ONLY);

            InternalTransaction tx = (InternalTransaction) transactions.begin(options);

            handler = tx.isReadOnly() ? new ManagedTransactionHandler(tx) : new ManagedReadWriteTransactionHandler(tx);

            this.handler = handler;

            return handler.handleStart();
        }
    }

    /** Holds a transaction initiated by a script control statement. */
    private static class ManagedTransactionHandler {
        private final InternalTransaction transaction;
        private final QueryTransactionWrapper noCommitWrapper;

        private ManagedTransactionHandler(InternalTransaction transaction) {
            this.transaction = transaction;
            this.noCommitWrapper = new ImplicitTransactionWrapper(transaction, false);
        }

        InternalTransaction transaction() {
            return transaction;
        }

        QueryTransactionWrapper handleStart() {
            return noCommitWrapper;
        }

        QueryTransactionWrapper handleCommit() {
            return new ImplicitTransactionWrapper(transaction, true);
        }

        QueryTransactionWrapper handleStatement(SqlQueryType queryType, CompletableFuture<? extends AsyncCursor<?>> cursorFut) {
            return noCommitWrapper;
        }
    }

    /** Responsible for tracking and releasing resources associated with an explicit read-write transaction started from a script. */
    private static class ManagedReadWriteTransactionHandler extends ManagedTransactionHandler {
        private static final UUID commitId = UUID.randomUUID();
        private final CompletableFuture<Void> finishTxFuture = new CompletableFuture<>();
        private final List<CompletableFuture<? extends AsyncCursor<?>>> cursorsToCloseOnRollback = new CopyOnWriteArrayList<>();
        private final Set<UUID> cursorsToWaitBeforeCommit = ConcurrentHashMap.newKeySet();

        private ManagedReadWriteTransactionHandler(InternalTransaction transaction) {
            super(transaction);

            cursorsToWaitBeforeCommit.add(commitId);
        }

        @Override
        QueryTransactionWrapper handleCommit() {
            tryCommit(commitId);

            return new ReadWriteTxCommitWrapper();
        }

        @Override
        QueryTransactionWrapper handleStatement(SqlQueryType queryType, CompletableFuture<? extends AsyncCursor<?>> cursorFut) {
            cursorsToCloseOnRollback.add(cursorFut);

            UUID cursorId = UUID.randomUUID();

            if (queryType == SqlQueryType.QUERY) {
                cursorsToWaitBeforeCommit.add(cursorId);
            }

            return new ReadWriteTxStatementWrapper(cursorId);
        }

        CompletableFuture<Void> tryCommit(UUID cursorId) {
            if (cursorsToWaitBeforeCommit.remove(cursorId) && cursorsToWaitBeforeCommit.isEmpty()) {
                return transaction().commitAsync()
                        .whenComplete((r, e) -> finishTxFuture.complete(null));
            }

            return Commons.completedFuture();
        }

        class ReadWriteTxCommitWrapper implements QueryTransactionWrapper {
            @Override
            public InternalTransaction unwrap() {
                return transaction();
            }

            @Override
            public CompletableFuture<Void> commitImplicit() {
                return Commons.completedFuture();
            }

            @Override
            public CompletableFuture<Void> rollback() {
                return transaction().rollbackAsync();
            }

            @Override
            public CompletableFuture<Void> commitImplicitAfterPrefetch() {
                return finishTxFuture;
            }
        }

        class ReadWriteTxStatementWrapper implements QueryTransactionWrapper {
            private final UUID cursorId;

            ReadWriteTxStatementWrapper(UUID cursorId) {
                this.cursorId = cursorId;
            }

            @Override
            public InternalTransaction unwrap() {
                return transaction();
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
                // Close all cursors.
                for (CompletableFuture<? extends AsyncCursor<?>> fut : cursorsToCloseOnRollback) {
                    fut.whenComplete((cursor, ex) -> {
                        if (cursor != null) {
                            cursor.closeAsync();
                        }
                    });
                }

                return transaction().rollbackAsync();
            }
        }
    }
}
