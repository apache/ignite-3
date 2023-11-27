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
    /** Manager for tracking and releasing resources associated with an explicit transaction started from a script. */
    private volatile @Nullable ScriptTxResourceManager scriptTxHolder;

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

            ScriptTxResourceManager txHld = scriptTxHolder;

            if (txHld == null || txHld.transaction().isReadOnly()) {
                return startTxIfNeeded(queryType);
            }

            ensureStatementAllowedWithinExplicitTx(parsedResult.queryType(), txHld.transaction());

            UUID cursorId = txHld.registerCursor(queryType, cursorFut);

            return new ManagedReadWriteTransactionWrapper(txHld, cursorId);
        } catch (SqlException e) {
            InternalTransaction scriptTx = scriptTransaction();

            if (scriptTx != null) {
                scriptTx.rollback();
            }

            throw e;
        }
    }

    private @Nullable InternalTransaction scriptTransaction() {
        ScriptTxResourceManager mgr = scriptTxHolder;

        return mgr != null ? mgr.transaction() : null;
    }

    private QueryTransactionWrapper processTxControlStatement(SqlNode node) {
        ScriptTxResourceManager scriptTxRsc = this.scriptTxHolder;

        if (node instanceof IgniteSqlCommitTransaction) {
            if (scriptTxRsc == null) {
                return QueryTransactionWrapper.NOOP_TX_WRAPPER;
            }

            this.scriptTxHolder = null;

            scriptTxRsc.onCursorClose(ScriptTxResourceManager.commitId);

            return new CommitTxStatementWrapper(scriptTxRsc.transaction(), scriptTxRsc.finishTxFuture());
        } else {
            assert node instanceof IgniteSqlStartTransaction : node == null ? "null" : node.getClass().getName();

            if (scriptTxRsc != null) {
                throw new SqlException(RUNTIME_ERR, "Nested transactions are not supported.");
            }

            IgniteSqlStartTransaction txStartNode = (IgniteSqlStartTransaction) node;

            TransactionOptions options =
                    new TransactionOptions().readOnly(txStartNode.getMode() == IgniteSqlStartTransactionMode.READ_ONLY);

            scriptTxRsc = new ScriptTxResourceManager((InternalTransaction) transactions.begin(options));

            this.scriptTxHolder = scriptTxRsc;

            return new StartTxStatementWrapper(scriptTxRsc.transaction());
        }
    }

    /** Responsible for tracking and releasing resources associated with an explicit transaction started from a script. */
    private static class ScriptTxResourceManager {
        private static final UUID commitId = UUID.randomUUID();
        private final CompletableFuture<Void> finishTxFuture = new CompletableFuture<>();
        private final List<CompletableFuture<? extends AsyncCursor<?>>> cursorsToCloseOnRollback = new CopyOnWriteArrayList<>();
        private final Set<UUID> cursorsToWaitBeforeCommit = ConcurrentHashMap.newKeySet();
        private final InternalTransaction transaction;

        private ScriptTxResourceManager(InternalTransaction transaction) {
            this.transaction = transaction;

            cursorsToWaitBeforeCommit.add(commitId);
        }

        InternalTransaction transaction() {
            return transaction;
        }

        CompletableFuture<Void> finishTxFuture() {
            return finishTxFuture;
        }

        UUID registerCursor(SqlQueryType queryType, CompletableFuture<? extends AsyncCursor<?>> cursorFut) {
            cursorsToCloseOnRollback.add(cursorFut);

            UUID cursorId = UUID.randomUUID();

            if (queryType == SqlQueryType.QUERY) {
                cursorsToWaitBeforeCommit.add(cursorId);
            }

            return cursorId;
        }

        CompletableFuture<Void> closeAllCursorsAndRollbackTx() {
            for (CompletableFuture<? extends AsyncCursor<?>> fut : cursorsToCloseOnRollback) {
                fut.whenComplete((cursor, ex) -> {
                    if (cursor != null) {
                        cursor.closeAsync();
                    }
                });
            }

            return transaction.rollbackAsync();
        }

        CompletableFuture<Void> onCursorClose(UUID queryId) {
            if (cursorsToWaitBeforeCommit.remove(queryId) && cursorsToWaitBeforeCommit.isEmpty()) {
                return transaction.commitAsync()
                        .whenComplete((r, e) -> finishTxFuture.complete(null));
            }

            return Commons.completedFuture();
        }
    }

    static class ManagedReadWriteTransactionWrapper implements QueryTransactionWrapper {
        private final ScriptTxResourceManager txRscManager;
        private final UUID cursorId;

        ManagedReadWriteTransactionWrapper(ScriptTxResourceManager txRscManager, UUID cursorId) {
            this.txRscManager = txRscManager;
            this.cursorId = cursorId;
        }

        @Override
        public InternalTransaction unwrap() {
            return txRscManager.transaction();
        }

        @Override
        public CompletableFuture<Void> commitImplicit() {
            return txRscManager.onCursorClose(cursorId);
        }

        @Override
        public CompletableFuture<Void> commitImplicitAfterPrefetch() {
            return Commons.completedFuture();
        }

        @Override
        public CompletableFuture<Void> rollback() {
            return txRscManager.closeAllCursorsAndRollbackTx();
        }
    }

    private static class StartTxStatementWrapper implements QueryTransactionWrapper {
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
        public CompletableFuture<Void> rollback() {
            return transaction.rollbackAsync();
        }
    }

    private static class CommitTxStatementWrapper extends StartTxStatementWrapper {
        private final CompletableFuture<Void> finishTxFuture;

        CommitTxStatementWrapper(InternalTransaction transaction, CompletableFuture<Void> finishTxFuture) {
            super(transaction);

            this.finishTxFuture = finishTxFuture;
        }

        @Override
        public CompletableFuture<Void> commitImplicitAfterPrefetch() {
            return finishTxFuture;
        }
    }
}
