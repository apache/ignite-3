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
import static org.apache.ignite.internal.tx.TransactionErrors.finishedTransactionErrorCode;
import static org.apache.ignite.internal.tx.TransactionErrors.finishedTransactionErrorMessage;
import static org.apache.ignite.internal.tx.TransactionLogUtils.formatTxInfo;

import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.sql.engine.exec.TransactionalOperationTracker;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.InternalTxOptions;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;

/**
 * Starts an implicit transaction if there is no external transaction.
 */
public class QueryTransactionContextImpl implements QueryTransactionContext {
    private final TxManager txManager;
    private final HybridTimestampTracker observableTimeTracker;
    private final @Nullable QueryTransactionWrapper tx;
    private final TransactionalOperationTracker txTracker;

    /** Constructor. */
    public QueryTransactionContextImpl(
            TxManager txManager,
            HybridTimestampTracker observableTimeTracker,
            @Nullable InternalTransaction tx,
            TransactionalOperationTracker txTracker
    ) {
        this.txManager = txManager;
        this.observableTimeTracker = observableTimeTracker;
        this.tx = tx != null ? new QueryTransactionWrapperImpl(tx, false, txTracker) : null;
        this.txTracker = txTracker;
    }


    /** {@inheritDoc} */
    @Override
    public QueryTransactionWrapper getOrStartSqlManaged(boolean readOnly, boolean implicit) {
        InternalTransaction transaction;
        QueryTransactionWrapper result;

        if (tx == null) {
            if (implicit) {
                transaction = txManager.beginImplicit(observableTimeTracker, readOnly, null);
            } else {
                transaction = txManager.beginExplicit(observableTimeTracker, readOnly, InternalTxOptions.defaults());
            }

            result = new QueryTransactionWrapperImpl(transaction, true, txTracker);
        } else {
            transaction = tx.unwrap();
            result = tx;

            TxStateMeta meta = txManager.stateMeta(transaction.id());

            if (meta != null && (meta.txState() == TxState.FINISHING || TxState.isFinalState(meta.txState()))) {
                Throwable cause = meta.lastException();
                boolean isFinishedDueToTimeout = meta.isFinishedDueToTimeoutOrFalse();
                boolean isFinishedDueToError = meta.isFinishedDueToErrorOrFalse();

                throw new TransactionException(
                        finishedTransactionErrorCode(isFinishedDueToTimeout, isFinishedDueToError),
                        format("{} [tx={}, {}].",
                                finishedTransactionErrorMessage(isFinishedDueToTimeout, isFinishedDueToError),
                                transaction,
                                formatTxInfo(transaction.id(), txManager, false)),
                        isFinishedDueToError ? cause : null
                );
            }
        }

        txTracker.registerOperationStart(transaction);

        return result;
    }

    @Override
    public void updateObservableTime(HybridTimestamp time) {
        observableTimeTracker.update(time);
    }

    /** Returns the external transaction if one has been started. */
    @Override
    public @Nullable QueryTransactionWrapper explicitTx() {
        return tx;
    }
}
