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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.tx.QueryTransactionContext.validateStatement;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.lang.ErrorGroups.Sql.RUNTIME_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_ALREADY_FINISHED_ERR;

import java.util.concurrent.CompletableFuture;
import org.apache.calcite.sql.SqlNode;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.TxControlInsideExternalTxNotSupportedException;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlCommitTransaction;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlStartTransaction;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlStartTransactionMode;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.impl.TransactionInflights;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.tx.TransactionException;
import org.apache.ignite.tx.TransactionOptions;

/**
 * Starts an implicit or script-driven transaction if there is no external transaction.
 */
public class ScriptTransactionContext {
    public static final QueryTransactionWrapper NOOP_TX_WRAPPER = new NoopTransactionWrapper();

    private final QueryTransactionContext queryTxCtx;

    private volatile ScriptTransactionWrapperImpl wrapper;

    private final TransactionInflights transactionInflights;

    public ScriptTransactionContext(QueryTransactionContext queryTxCtx, TransactionInflights transactionInflights) {
        this.queryTxCtx = queryTxCtx;
        this.transactionInflights = transactionInflights;
    }

    /**
     * Starts a new implicit transaction if there is no external or script-driven transaction.
     *
     * @param queryType Query type.
     * @return Transaction wrapper.
     */
    public QueryTransactionWrapper getOrStartImplicit(SqlQueryType queryType) {
        QueryTransactionWrapper wrapper = this.wrapper;

        try {
            if (wrapper == null) {
                return queryTxCtx.getOrStartImplicit(queryType);
            }

            validateStatement(queryType, wrapper.unwrap().isReadOnly());

            return wrapper;
        } catch (SqlException e) {
            if (wrapper != null) {
                wrapper.rollback(e);
            }

            throw e;
        }
    }

    /**
     * Handles {@link SqlQueryType#TX_CONTROL} statement.
     * Depending on the type of operator, it starts a new transaction controlled by a script, or completes previously started transaction.
     *
     * @param node Tx control tree node.
     * @return Future representing result of execution. The next statement should not be executed until this future is completed.
     */
    public CompletableFuture<Void> handleControlStatement(SqlNode node) {
        if (queryTxCtx.transaction() != null) {
            throw new TxControlInsideExternalTxNotSupportedException();
        }

        ScriptTransactionWrapperImpl txWrapper = wrapper;

        if (node instanceof IgniteSqlStartTransaction) {
            if (txWrapper != null) {
                throw new SqlException(RUNTIME_ERR, "Nested transactions are not supported.");
            }

            boolean readOnly = ((IgniteSqlStartTransaction) node).getMode() == IgniteSqlStartTransactionMode.READ_ONLY;
            InternalTransaction tx = (InternalTransaction) queryTxCtx.transactions().begin(new TransactionOptions().readOnly(readOnly));

            // Adding inflights only for read-only transactions. See TransactionInflights.ReadOnlyTxContext for details.
            if (readOnly && !transactionInflights.addInflight(tx.id(), readOnly)) {
                throw new TransactionException(TX_ALREADY_FINISHED_ERR, format("Transaction is already finished [tx={}]", tx));
            }

            this.wrapper = new ScriptTransactionWrapperImpl(tx, transactionInflights);

            return nullCompletedFuture();
        } else {
            assert node instanceof IgniteSqlCommitTransaction : node == null ? "null" : node.getClass().getName();

            if (txWrapper == null) {
                return nullCompletedFuture();
            }

            wrapper = null;

            return txWrapper.commit();
        }
    }

    /** Registers a future statement cursor that must be closed before the transaction can be committed. */
    public void registerCursorFuture(SqlQueryType queryType, CompletableFuture<AsyncSqlCursor<InternalSqlRow>> cursorFut) {
        if (queryType == SqlQueryType.DDL || queryType == SqlQueryType.EXPLAIN) {
            return;
        }

        ScriptTransactionWrapperImpl txWrapper = wrapper;

        if (txWrapper != null) {
            wrapper.registerCursorFuture(cursorFut);
        }
    }

    /** Attempts to rollback a script-driven transaction if it has not finished. */
    public void rollbackUncommitted() {
        ScriptTransactionWrapperImpl txWrapper = wrapper;

        if (txWrapper != null) {
            txWrapper.rollbackWhenCursorsClosed();
        }
    }

    /** Closes all associated cursors and rolls back the script-driven transaction. */
    public void onError(Throwable t) {
        ScriptTransactionWrapperImpl txWrapper = wrapper;

        if (txWrapper != null) {
            txWrapper.rollback(t);
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

        @Override
        public boolean implicit() {
            return true;
        }
    }
}
