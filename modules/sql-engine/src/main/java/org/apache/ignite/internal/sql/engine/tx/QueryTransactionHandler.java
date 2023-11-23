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

import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.sql.ParsedResult;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.tx.IgniteTransactions;
import org.jetbrains.annotations.Nullable;

/**
 * The query transaction handler is responsible for running implicit or script managed transactions during query execution.
 */
@SuppressWarnings("InterfaceMayBeAnnotatedFunctional")
public interface QueryTransactionHandler {
    /**
     * Starts a transaction if there is no external transaction.
     *
     * @param parsedResult Result of the parse.
     * @return Transaction wrapper.
     */
    QueryTransactionWrapper startTxIfNeeded(ParsedResult parsedResult);

    /**
     * Creates a new transaction handler that starts an implicit transaction if there is no external transaction.
     *
     * @param transactions Ignite transactions facade.
     * @param externalTransaction External transaction.
     * @return Transaction handler.
     */
    static QueryTransactionHandler forSingleStatement(IgniteTransactions transactions, @Nullable InternalTransaction externalTransaction) {
        return new QueryTransactionHandlerImpl(transactions, externalTransaction);
    }

    /**
     * Creates a new transaction handler that starts an implicit transaction if there is no external transaction and
     * supports script transaction management using {@link SqlQueryType#TX_CONTROL} statements.
     *
     * @param transactions Ignite transactions facade.
     * @param externalTransaction External transaction.
     * @return Transaction handler.
     */
    static QueryTransactionHandler forMultiStatement(IgniteTransactions transactions, @Nullable InternalTransaction externalTransaction) {
        return new ScriptTransactionHandler(transactions, externalTransaction);
    }
}
