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
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.impl.TransactionInflights;

/**
 * Wrapper for the transaction that encapsulates the management of an implicit transaction.
 */
public class QueryTransactionWrapperImpl implements QueryTransactionWrapper {
    private final boolean implicit;

    private final InternalTransaction transaction;

    private final TransactionInflights transactionInflights;

    private final AtomicBoolean committedImplicit = new AtomicBoolean();

    /**
     * Constructor.
     *
     * @param transaction Transaction.
     * @param implicit Whether tx is implicit.
     * @param transactionInflights Transaction inflights.
     */
    public QueryTransactionWrapperImpl(InternalTransaction transaction, boolean implicit, TransactionInflights transactionInflights) {
        this.transaction = transaction;
        this.implicit = implicit;
        this.transactionInflights = transactionInflights;
    }

    @Override
    public InternalTransaction unwrap() {
        return transaction;
    }

    @Override
    public CompletableFuture<Void> commitImplicit() {
        if (transaction.isReadOnly() && committedImplicit.compareAndSet(false, true)) {
            transactionInflights.removeInflight(transaction.id());
        }

        if (implicit) {
            return transaction.commitAsync();
        }

        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> rollback(Throwable cause) {
        return transaction.rollbackAsync();
    }

    @Override
    public boolean implicit() {
        return implicit;
    }
}
