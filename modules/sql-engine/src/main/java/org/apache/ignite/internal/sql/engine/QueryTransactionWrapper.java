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

import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;

import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.tx.IgniteTransactions;
import org.apache.ignite.tx.TransactionOptions;
import org.jetbrains.annotations.Nullable;

/**
 * Wrapper for the transaction that encapsulates the management of an implicit transaction.
 */
public class QueryTransactionWrapper {
    private final boolean implicit;

    private final InternalTransaction transaction;

    /**
     * Creates a new transaction wrapper using an existing outer transaction or starting a new "implicit" transaction.
     *
     * @param queryType Query type.
     * @param transactions Transactions facade.
     * @param outerTx Outer transaction.
     * @return Wrapper for an active transaction.
     * @throws SqlException If an outer transaction was started for a {@link SqlQueryType#DDL DDL} query.
     */
    public static QueryTransactionWrapper beginImplicitTxIfNeeded(
            SqlQueryType queryType,
            IgniteTransactions transactions,
            @Nullable InternalTransaction outerTx
    ) {
        if (outerTx == null) {
            InternalTransaction tx = (InternalTransaction) transactions.begin(
                    new TransactionOptions().readOnly(queryType != SqlQueryType.DML));

            return new QueryTransactionWrapper(tx, true);
        }

        if (SqlQueryType.DDL == queryType) {
            throw new SqlException(STMT_VALIDATION_ERR, "DDL doesn't support transactions.");
        }

        return new QueryTransactionWrapper(outerTx, false);
    }

    /**
     * Unwrap transaction.
     */
    InternalTransaction unwrap() {
        return transaction;
    }

    /**
     * Commits an implicit transaction, if one has been started.
     */
    void commitImplicit() {
        if (implicit) {
            transaction.commit();
        }
    }

    /**
     * Rolls back an implicit transaction, if one has been started.
     */
    void rollbackImplicit() {
        if (implicit) {
            transaction.rollback();
        }
    }

    private QueryTransactionWrapper(InternalTransaction transaction, boolean implicit) {
        this.transaction = transaction;
        this.implicit = implicit;
    }
}
