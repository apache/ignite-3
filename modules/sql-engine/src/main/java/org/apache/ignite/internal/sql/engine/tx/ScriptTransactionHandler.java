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

import java.util.concurrent.CompletableFuture;
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
    private volatile @Nullable ScriptTransactionWrapper wrapper;

    public ScriptTransactionHandler(IgniteTransactions transactions, @Nullable InternalTransaction outerTransaction) {
        super(transactions, outerTransaction);
    }

    @Override
    protected @Nullable InternalTransaction activeTransaction() {
        return outerTransaction == null ? scriptTransaction() : outerTransaction;
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
                if (outerTransaction != null) {
                    throw new ExternalTransactionNotSupportedException();
                }

                return handleTxControlStatement(parsedResult.parsedTree());
            }

            ScriptTransactionWrapper wrapper = this.wrapper;

            if (wrapper == null) {
                return startTxIfNeeded(queryType);
            }

            validateStatement(parsedResult.queryType(), wrapper.unwrap());

            return wrapper.forStatement(queryType, cursorFut);
        } catch (SqlException e) {
            ScriptTransactionWrapper wrapper = this.wrapper;

            if (wrapper != null) {
                wrapper.rollback(e);
            }

            throw e;
        }
    }

    private @Nullable InternalTransaction scriptTransaction() {
        ScriptTransactionWrapper hld = wrapper;

        return hld != null ? hld.unwrap() : null;
    }

    private QueryTransactionWrapper handleTxControlStatement(SqlNode node) {
        ScriptTransactionWrapper txWrapper = this.wrapper;

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

            txWrapper = new ScriptTransactionWrapper((InternalTransaction) transactions.begin(options));

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
            return Commons.completedFuture();
        }

        @Override
        public CompletableFuture<Void> rollback(Throwable cause) {
            return Commons.completedFuture();
        }
    }
}
