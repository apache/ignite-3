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
import static org.apache.ignite.lang.ErrorGroups.Sql.RUNTIME_ERR;

import java.util.concurrent.CompletableFuture;
import org.apache.calcite.sql.SqlNode;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.TxControlInsideExternalTxNotSupportedException;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlCommitTransaction;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlStartTransaction;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlStartTransactionMode;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.Nullable;

/**
 * Starts an implicit or script-driven transaction if there is no external transaction.
 */
public class ScriptTransactionContext implements QueryTransactionContext {
    private final QueryTransactionContextImpl txContext;

    private volatile @Nullable ScriptTransactionWrapperImpl wrapper;

    /** Constructor. */
    public ScriptTransactionContext(QueryTransactionContext txContext) {
        assert txContext instanceof QueryTransactionContextImpl : txContext;

        this.txContext = (QueryTransactionContextImpl) txContext;
    }

    /**
     * Starts a new implicit transaction if there is no external or script-driven transaction.
     *
     * @param readOnly Type of the transaction to start if none is started.
     * @return Transaction wrapper.
     */
    @Override
    public QueryTransactionWrapper getOrStartImplicit(boolean readOnly) {
        QueryTransactionWrapper wrapper = this.wrapper;

        if (wrapper == null) {
            return txContext.getOrStartImplicit(readOnly);
        }

        return wrapper;
    }

    @Override
    public void updateObservableTime(HybridTimestamp time) {
        txContext.updateObservableTime(time);
    }

    @Override
    public @Nullable QueryTransactionWrapper explicitTx() {
        QueryTransactionWrapper tx = wrapper;

        if (tx == null) {
            tx = txContext.explicitTx();
        }

        return tx;
    }

    /**
     * Handles {@link SqlQueryType#TX_CONTROL} statement.
     * Depending on the type of operator, it starts a new transaction controlled by a script, or completes previously started transaction.
     *
     * @param node Tx control tree node.
     * @return Future representing result of execution. The next statement should not be executed until this future is completed.
     */
    public CompletableFuture<Void> handleControlStatement(SqlNode node) {
        if (txContext.explicitTx() != null) {
            throw new TxControlInsideExternalTxNotSupportedException();
        }

        ScriptTransactionWrapperImpl txWrapper = wrapper;

        if (node instanceof IgniteSqlStartTransaction) {
            if (txWrapper != null) {
                throw new SqlException(RUNTIME_ERR, "Nested transactions are not supported.");
            }

            boolean readOnly = ((IgniteSqlStartTransaction) node).getMode() == IgniteSqlStartTransactionMode.READ_ONLY;
            InternalTransaction tx = txContext.getOrStartImplicit(readOnly).unwrap();

            this.wrapper = new ScriptTransactionWrapperImpl(tx);

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
            txWrapper.registerCursorFuture(cursorFut);
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
}
