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

import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.tx.HybridTimestampTracker;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxManager;
import org.jetbrains.annotations.Nullable;

/**
 * Starts an implicit transaction if there is no external transaction.
 */
public class QueryTransactionContextImpl implements QueryTransactionContext {
    private final TxManager txManager;
    private final HybridTimestampTracker observableTimeTracker;
    private final @Nullable QueryTransactionWrapper tx;

    /** Constructor. */
    public QueryTransactionContextImpl(
            TxManager txManager,
            HybridTimestampTracker observableTimeTracker,
            @Nullable InternalTransaction tx
    ) {
        this.txManager = txManager;
        this.observableTimeTracker = observableTimeTracker;
        this.tx = tx != null ? new QueryTransactionWrapperImpl(tx, false) : null;
    }

    /**
     * Starts an implicit transaction if there is no external transaction.
     *
     * @param readOnly Query type.
     * @return Transaction wrapper.
     */
    @Override
    public QueryTransactionWrapper getOrStartImplicit(boolean readOnly) {
        if (tx == null) {
            return new QueryTransactionWrapperImpl(
                    txManager.begin(observableTimeTracker, readOnly), true
            );
        }

        return tx;
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
