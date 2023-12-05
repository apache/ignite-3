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

import static org.apache.ignite.internal.sql.engine.tx.QueryTransactionContext.validateStatement;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.lang.ErrorGroups.Sql.RUNTIME_ERR;

import java.util.concurrent.CompletableFuture;
import org.apache.calcite.sql.SqlNode;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.TxControlInsideExternalTxNotSupportedException;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlCommitTransaction;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlStartTransaction;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlStartTransactionMode;
import org.apache.ignite.internal.sql.engine.sql.ParsedResult;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.AsyncCursor;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.tx.TransactionOptions;
import org.jetbrains.annotations.Nullable;

/**
 * Starts an implicit transaction if there is no external transaction. Supports script transaction management using
 * {@link SqlQueryType#TX_CONTROL} statements.
 */
public class ScriptTransactionHandler {
    /** No-op transaction wrapper. */
    private static final QueryTransactionWrapper NOOP_TX_WRAPPER = new NoopTransactionWrapper();

    private final QueryTransactionContext ctx;

    /** Wraps a transaction, which is managed by SQL engine via {@link SqlQueryType#TX_CONTROL} statements. */
    private volatile @Nullable ScriptTransactionWrapperImpl wrapper;

    public ScriptTransactionHandler(QueryTransactionContext ctx) {
        this.ctx = ctx;
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
                if (ctx.tx() != null) {
                    throw new TxControlInsideExternalTxNotSupportedException();
                }

                return handleTxControlStatement(parsedResult.parsedTree());
            }

            ScriptTransactionWrapperImpl wrapper = this.wrapper;

            if (wrapper == null) {
                // Implicit transaction.
                return ctx.startTxIfNeeded(parsedResult.queryType());
            }

            validateStatement(parsedResult.queryType(), wrapper.unwrap());

            return wrapper.forStatement(queryType, cursorFut);
        } catch (SqlException e) {
            ScriptTransactionWrapperImpl wrapper = this.wrapper;

            if (wrapper != null) {
                wrapper.rollback(e);
            }

            throw e;
        }
    }

    private QueryTransactionWrapper handleTxControlStatement(SqlNode node) {
        ScriptTransactionWrapperImpl txWrapper = this.wrapper;

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

            txWrapper = new ScriptTransactionWrapperImpl((InternalTransaction) ctx.transactions().begin(options));

            this.wrapper = txWrapper;

            return txWrapper;
        }
    }

    private static class NoopTransactionWrapper implements QueryTransactionWrapper {
        @Override
        public InternalTransaction unwrap() {
            return null;
        }

        @Override
        public CompletableFuture<Void> commitImplicit() {
            return nullCompletedFuture();
        }

        @Override
        public CompletableFuture<Void> rollback(Throwable cause) {
            return nullCompletedFuture();
        }
    }
}
