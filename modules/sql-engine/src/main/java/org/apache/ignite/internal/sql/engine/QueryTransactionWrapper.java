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

import org.apache.ignite.internal.hlc.HybridTimestamp;
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

    private volatile InternalTransaction transaction;

    /**
     * Constructor.
     *
     * @param transactions Transaction facade.
     * @param transaction Explicit transaction.
     */
    public QueryTransactionWrapper(IgniteTransactions transactions, @Nullable Transaction transaction) {
        this.transactions = transactions;
        this.transaction = (InternalTransaction) transaction;

        implicitRequired = transaction == null;
    }

    /**
     * Commits an implicit transaction, if one has been started.
     */
    protected void commitImplicit() {
        if (!implicitRequired) {
            return;
        }

        Transaction transaction0 = transaction;

        if (transaction0 != null) {
            transaction0.commit();
        }
    }

    /**
     * Rolls back an implicit transaction, if one has been started.
     */
    protected void rollbackImplicit() {
        if (!implicitRequired) {
            return;
        }

        Transaction transaction0 = transaction;

        if (transaction0 != null) {
            transaction0.rollback();
        }
    }

    /**
     * Each call starts a new "implicit" transaction if the query requires a new one and no "external" transaction exists.
     *
     * @param type Query type.
     * @return Transaction start time, or {@code null} if specified query type does not require transaction.
     * @throws SqlException If an external transaction was started for a {@link SqlQueryType#DDL DDL} query.
     */
    @Nullable HybridTimestamp beginTxIfNeeded(SqlQueryType type) {
        if (implicitRequired) {
            if (type == SqlQueryType.DDL || type == SqlQueryType.EXPLAIN) {
                return null;
            }

            transaction = (InternalTransaction) transactions.begin(new TransactionOptions().readOnly(type != SqlQueryType.DML));
        } else if (SqlQueryType.DDL == type) {
            throw new SqlException(ErrorGroups.Sql.STMT_VALIDATION_ERR, "DDL doesn't support transactions.");
        }

        return transaction.startTimestamp();
    }

    /**
     * Returns transaction if any has been started.
     */
    @Nullable InternalTransaction transaction() {
        return transaction;
    }
}
