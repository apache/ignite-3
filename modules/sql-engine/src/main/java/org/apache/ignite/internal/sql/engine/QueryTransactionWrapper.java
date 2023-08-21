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

import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.lang.ErrorGroups;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.tx.IgniteTransactions;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionOptions;
import org.jetbrains.annotations.Nullable;

/**
 * Wrapper for the transaction that encapsulates the management of an implicit transaction.
 */
public class QueryTransactionWrapper {
    private final IgniteTransactions transactions;

    private final boolean implicitRequired;

    private volatile Transaction transaction;

    /**
     * Constructor.
     *
     * @param transactions Transaction facade.
     * @param transaction Explicit transaction.
     */
    public QueryTransactionWrapper(IgniteTransactions transactions, @Nullable Transaction transaction) {
        this.transactions = transactions;
        this.transaction = transaction;

        implicitRequired = transaction == null;
    }

    protected void commitImplicit() {
        if (!implicitRequired) {
            return;
        }

        Transaction transaction0 = transaction;

        if (transaction0 != null) {
            transaction0.commit();
        }
    }

    protected void rollbackImplicit() {
        if (!implicitRequired) {
            return;
        }

        Transaction transaction0 = transaction;

        if (transaction0 != null) {
            transaction0.rollback();
        }
    }

    void beginTxIfNeeded(SqlQueryType type) {
        if (!implicitRequired && SqlQueryType.DDL == type) {
            throw new SqlException(ErrorGroups.Sql.STMT_VALIDATION_ERR, "DDL doesn't support transactions.");
        }

        if (implicitRequired && type != SqlQueryType.DDL && type != SqlQueryType.EXPLAIN && transaction == null) {
            transaction = transactions.begin(new TransactionOptions().readOnly(type != SqlQueryType.DML));
        }
    }

    @Nullable InternalTransaction transaction() {
        return (InternalTransaction) transaction;
    }
}
