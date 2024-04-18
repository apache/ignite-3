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
import org.apache.ignite.internal.tx.InternalTransaction;

/**
 * Wrapper for the transaction that encapsulates the management of an implicit transaction.
 */
public class QueryTransactionWrapperImpl implements QueryTransactionWrapper {
    private final boolean implicit;

    private final InternalTransaction transaction;

    public QueryTransactionWrapperImpl(InternalTransaction transaction, boolean implicit) {
        this.transaction = transaction;
        this.implicit = implicit;
    }

    @Override
    public InternalTransaction unwrap() {
        return transaction;
    }

    @Override
    public CompletableFuture<Void> commitImplicit() {
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
