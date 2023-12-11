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

import static org.apache.ignite.lang.ErrorGroups.Sql.RUNTIME_ERR;

import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.tx.IgniteTransactions;
import org.apache.ignite.tx.TransactionOptions;
import org.jetbrains.annotations.Nullable;

/**
 * Starts an implicit transaction if there is no external transaction.
 */
public class QueryTransactionContext {
    private final IgniteTransactions transactions;
    private final @Nullable InternalTransaction tx;

    public QueryTransactionContext(IgniteTransactions transactions, @Nullable InternalTransaction tx) {
        this.transactions = transactions;
        this.tx = tx;
    }

    /**
     * Starts an implicit transaction if there is no external transaction.
     *
     * @param queryType Query type.
     * @return Transaction wrapper.
     */
    public QueryTransactionWrapper getOrStartImplicit(SqlQueryType queryType) {
        InternalTransaction outerTx = tx;

        if (outerTx == null) {
            return new QueryTransactionWrapperImpl((InternalTransaction) transactions.begin(
                    new TransactionOptions().readOnly(queryType != SqlQueryType.DML)), true);
        }

        validateStatement(queryType, outerTx.isReadOnly());

        return new QueryTransactionWrapperImpl(outerTx, false);
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
