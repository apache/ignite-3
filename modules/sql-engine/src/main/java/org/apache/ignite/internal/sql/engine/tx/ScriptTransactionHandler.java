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

import static org.apache.ignite.lang.ErrorGroups.Sql.EXECUTION_CANCELLED_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.RUNTIME_ERR;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.calcite.sql.SqlNode;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
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
    /** Manager for tracking and releasing resources associated with an explicit transaction started from a script. */
    private volatile @Nullable ScriptTxResourceManager scriptTxMgr;

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
    public QueryTransactionWrapper startTxIfNeeded(ParsedResult parsedResult, CompletableFuture<? extends AsyncSqlCursor<?>> cursorFut) {
        try {
            SqlQueryType queryType = parsedResult.queryType();

            if (queryType == SqlQueryType.TX_CONTROL) {
                if (externalTransaction != null) {
                    throw new ExternalTransactionNotSupportedException();
                }

                return processTransactionManagementStatement(parsedResult);
            }

            ScriptTxResourceManager scriptTxMgr0 = scriptTxMgr;

            if (scriptTxMgr0 != null && !scriptTxMgr0.transaction().isReadOnly()) {
                ensureStatementAllowedWithinExplicitTx(parsedResult.queryType(), false);

                UUID cursorId = scriptTxMgr0.trackStatementCursor(queryType, cursorFut);

                return new StatementTxWrapper(scriptTxMgr0, cursorId);
            }

            return startTxIfNeeded(queryType);
        } catch (SqlException e) {
            InternalTransaction scriptTx = scriptTransaction();

            if (scriptTx != null) {
                scriptTx.rollback();
            }

            throw e;
        }
    }

    private @Nullable InternalTransaction scriptTransaction() {
        ScriptTxResourceManager mgr = scriptTxMgr;

        return mgr != null ? mgr.transaction() : null;
    }

    private QueryTransactionWrapper processTransactionManagementStatement(ParsedResult parsedResult) {
        SqlNode node = parsedResult.parsedTree();

        ScriptTxResourceManager scriptTxMgr = this.scriptTxMgr;

        if (node instanceof IgniteSqlCommitTransaction) {
            if (scriptTxMgr == null) {
                return QueryTransactionWrapper.NOOP_TX_WRAPPER;
            }

            this.scriptTxMgr = null;

            scriptTxMgr.onCursorClose(ScriptTxResourceManager.commitId);

            return new CommitTxWrapper(scriptTxMgr.transaction(), scriptTxMgr.finishTxFuture);
        }

        assert node instanceof IgniteSqlStartTransaction : node == null ? "null" : node.getClass().getName();

        if (scriptTxMgr != null) {
            throw new SqlException(RUNTIME_ERR, "Nested transactions are not supported.");
        }

        IgniteSqlStartTransaction txStartNode = (IgniteSqlStartTransaction) node;

        TransactionOptions options =
                new TransactionOptions().readOnly(txStartNode.getMode() == IgniteSqlStartTransactionMode.READ_ONLY);

        scriptTxMgr = new ScriptTxResourceManager((InternalTransaction) transactions.begin(options));

        this.scriptTxMgr = scriptTxMgr;

        return new StartTxStatementWrapper(scriptTxMgr.transaction());
    }

    static class StartTxStatementWrapper implements QueryTransactionWrapper {
        private final InternalTransaction transaction;

        StartTxStatementWrapper(InternalTransaction transaction) {
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
        public CompletableFuture<Void> rollback(String reason) {
            return transaction.rollbackAsync();
        }
    }

    static class CommitTxWrapper extends StartTxStatementWrapper {
        private final CompletableFuture<Void> finishTxFuture;

        CommitTxWrapper(InternalTransaction transaction, CompletableFuture<Void> finishTxFuture) {
            super(transaction);

            this.finishTxFuture = finishTxFuture;
        }

        @Override
        public CompletableFuture<Void> commitScriptImplicit() {
            return finishTxFuture;
        }
    }

    /** Responsible for tracking and releasing resources associated with an explicit transaction started from a script. */
    private static class ScriptTxResourceManager {
        private static final UUID commitId = UUID.randomUUID();
        private final CompletableFuture<Void> finishTxFuture = new CompletableFuture<>();
        private final List<CompletableFuture<? extends AsyncSqlCursor<?>>> cursorsToCloseOnRollback = new CopyOnWriteArrayList<>();
        private final Set<UUID> cursorsToWaitBeforeCommit = ConcurrentHashMap.newKeySet();
        private final InternalTransaction transaction;

        private ScriptTxResourceManager(InternalTransaction transaction) {
            this.transaction = transaction;

            cursorsToWaitBeforeCommit.add(commitId);
        }

        CompletableFuture<Void> closeAllCursorsAndRollbackTx(String reason) {
            for (CompletableFuture<? extends AsyncSqlCursor<?>> cursor : cursorsToCloseOnRollback) {
                cursor.thenCompose(AsyncCursor::closeAsync);
            }

            return transaction.rollbackAsync()
                    .whenComplete((r, e) -> {
                        SqlException ex =
                                new SqlException(EXECUTION_CANCELLED_ERR, "Execution was canceled due to transaction rollback: " + reason);

                        if (e != null) {
                            ex.addSuppressed(e);
                        }

                        finishTxFuture.completeExceptionally(ex);
                    });
        }

        CompletableFuture<Void> onCursorClose(UUID queryId) {
            if (cursorsToWaitBeforeCommit.remove(queryId) && cursorsToWaitBeforeCommit.isEmpty()) {
                return transaction.commitAsync()
                        .whenComplete((r, e) -> finishTxFuture.complete(null));
            }

            return Commons.completedFuture();
        }

        UUID trackStatementCursor(SqlQueryType queryType, CompletableFuture<? extends AsyncSqlCursor<?>> cursorFut) {
            cursorsToCloseOnRollback.add(cursorFut);

            UUID cursorId = UUID.randomUUID();

            if (queryType == SqlQueryType.QUERY) {
                cursorsToWaitBeforeCommit.add(cursorId);
            }

            return cursorId;
        }

        InternalTransaction transaction() {
            return transaction;
        }
    }

    static class StatementTxWrapper implements QueryTransactionWrapper {
        private final ScriptTxResourceManager txManager;
        private final UUID cursorId;

        StatementTxWrapper(ScriptTxResourceManager txManager, UUID cursorId) {
            this.txManager = txManager;
            this.cursorId = cursorId;
        }

        @Override
        public InternalTransaction unwrap() {
            return txManager.transaction();
        }

        @Override
        public CompletableFuture<Void> commitScriptImplicit() {
            return Commons.completedFuture();
        }

        @Override
        public CompletableFuture<Void> rollback(String reason) {
            return txManager.closeAllCursorsAndRollbackTx(reason);
        }

        @Override
        public CompletableFuture<Void> commitImplicit() {
            return txManager.onCursorClose(cursorId);
        }
    }
}
