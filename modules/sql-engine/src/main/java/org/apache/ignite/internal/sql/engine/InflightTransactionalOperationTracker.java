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

package org.apache.ignite.internal.sql.engine;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_ALREADY_FINISHED_ERR;

import org.apache.ignite.internal.sql.engine.exec.TransactionalOperationTracker;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.impl.TransactionInflights;
import org.apache.ignite.tx.TransactionException;

/**
 * Simple facade that propagates invocations to underlying {@link TransactionInflights} object.
 */
class InflightTransactionalOperationTracker implements TransactionalOperationTracker {
    private final TransactionInflights delegate;

    InflightTransactionalOperationTracker(TransactionInflights delegate) {
        this.delegate = delegate;
    }

    @Override
    public void registerOperationStart(InternalTransaction tx) {
        if (shouldBeTracked(tx)) {
            if (!delegate.addInflight(tx.id(), tx.isReadOnly())) {
                throw new TransactionException(TX_ALREADY_FINISHED_ERR, format("Transaction is already finished [tx={}]", tx));
            }
        }
    }

    @Override
    public void registerOperationFinish(InternalTransaction tx) {
        if (shouldBeTracked(tx)) {
            delegate.removeInflight(tx.id());
        }
    }

    private static boolean shouldBeTracked(InternalTransaction tx) {
        return tx.isReadOnly() && !tx.implicit();
    } 
}
