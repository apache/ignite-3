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
import static org.apache.ignite.lang.ErrorGroups.Sql.RUNTIME_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_ALREADY_FINISHED_ERR;

import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.impl.TransactionInflights;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.tx.IgniteTransactions;
import org.apache.ignite.tx.TransactionException;
import org.apache.ignite.tx.TransactionOptions;
import org.jetbrains.annotations.Nullable;

/**
 * Starts an implicit transaction if there is no external transaction.
 */
public class QueryTransactionContext {
    private final IgniteTransactions transactions;
    private final @Nullable InternalTransaction tx;
    private final TransactionInflights transactionInflights;

    /**
     * Constructor.
     *
     * @param transactions Ignite transactions.
     * @param tx Transaction.
     * @param transactionInflights Transaction inflights.
     */
    public QueryTransactionContext(IgniteTransactions transactions, @Nullable InternalTransaction tx,
            TransactionInflights transactionInflights) {
        this.transactions = transactions;
        this.tx = tx;
        this.transactionInflights = transactionInflights;
    }

    /**
     * Starts an implicit transaction if there is no external transaction.
     *
     * @param queryType Query type.
     * @return Transaction wrapper.
     */
    public QueryTransactionWrapper getOrStartImplicit(SqlQueryType queryType) {
        InternalTransaction transaction = tx;
        boolean implicit;

        if (transaction == null) {
            implicit = true;
            transaction = (InternalTransaction) transactions.begin(
                    new TransactionOptions().readOnly(queryType != SqlQueryType.DML));
        } else {
            implicit = false;
            validateStatement(queryType, transaction.isReadOnly());
        }

        // Adding inflights only for read-only transactions. See TransactionInflights.ReadOnlyTxContext for details.
        if (transaction.isReadOnly() && !transactionInflights.addInflight(transaction.id(), transaction.isReadOnly())) {
            throw new TransactionException(TX_ALREADY_FINISHED_ERR, format("Transaction is already finished [tx={}]", transaction));
        }

        return new QueryTransactionWrapperImpl(transaction, implicit, transactionInflights);
    }

    /** Returns transactions facade. */
    IgniteTransactions transactions() {
        return transactions;
    }

    /** Returns the external transaction if one has been started. */
    @Nullable InternalTransaction transaction() {
        return tx;
    }

    /** Checks that the statement is allowed within an external/script transaction. */
    static void validateStatement(SqlQueryType queryType, boolean readOnly) {
        if (SqlQueryType.DDL == queryType) {
            throw new SqlException(RUNTIME_ERR, "DDL doesn't support transactions.");
        }

        if (SqlQueryType.DML == queryType && readOnly) {
            throw new SqlException(RUNTIME_ERR, "DML query cannot be started by using read only transactions.");
        }
    }
}
