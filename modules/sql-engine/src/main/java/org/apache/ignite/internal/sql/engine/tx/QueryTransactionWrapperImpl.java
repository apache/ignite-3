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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.sql.engine.exec.TransactionalOperationTracker;
import org.apache.ignite.internal.tx.InternalTransaction;

/**
 * Wrapper for the transaction that encapsulates the management of an implicit transaction.
 */
public class QueryTransactionWrapperImpl implements QueryTransactionWrapper {
    /**
     * This flag does not signify the type of transaction.
     * It means that the transaction was started in the SQL engine and was not passed from outside.
     */
    private final boolean queryImplicit;

    private final InternalTransaction transaction;

    private final TransactionalOperationTracker txTracker;

    private final AtomicBoolean committed = new AtomicBoolean();

    /**
     * Constructor.
     *
     * @param transaction Transaction.
     * @param implicit Whether tx is implicit.
     * @param txTracker Transaction tracker.
     */
    public QueryTransactionWrapperImpl(InternalTransaction transaction, boolean implicit, TransactionalOperationTracker txTracker) {
        this.transaction = transaction;
        this.queryImplicit = implicit;
        this.txTracker = txTracker;
    }

    @Override
    public InternalTransaction unwrap() {
        return transaction;
    }

    @Override
    public CompletableFuture<Void> finalise() {
        if (committed.compareAndSet(false, true)) {
            txTracker.registerOperationFinish(transaction);
        }

        if (queryImplicit) {
            return transaction.commitAsync();
        }

        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> finalise(Throwable error) {
        assert error != null;

        if (committed.compareAndSet(false, true)) {
            txTracker.registerOperationFinish(transaction);
        }

        if (transaction.remote()) {
            return nullCompletedFuture();
        }

        return transaction.rollbackAsync();
    }

    @Override
    public boolean implicit() {
        return queryImplicit;
    }
}
