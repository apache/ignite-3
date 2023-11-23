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

import org.apache.calcite.sql.SqlNode;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlCommitTransaction;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlStartTransaction;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlStartTransactionMode;
import org.apache.ignite.internal.sql.engine.sql.ParsedResult;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.sql.ExternalTransactionNotSupportedException;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.tx.IgniteTransactions;
import org.apache.ignite.tx.TransactionOptions;
import org.jetbrains.annotations.Nullable;

/**
 * Starts an implicit transaction if there is no external transaction. Supports script transaction management using
 * {@link SqlQueryType#TX_CONTROL} statements.
 */
public class ScriptTransactionHandler extends QueryTransactionHandlerImpl {
    /** Wrapper over transaction, which is managed by SQL engine. */
    private volatile @Nullable ScriptManagedTransactionWrapper managedTxWrapper;

    ScriptTransactionHandler(IgniteTransactions transactions, @Nullable InternalTransaction externalTransaction) {
        super(transactions, externalTransaction);
    }

    @Override
    protected @Nullable InternalTransaction activeTransaction() {
        return externalTransaction == null ? scriptTransaction() : externalTransaction;
    }

    private @Nullable InternalTransaction scriptTransaction() {
        ScriptManagedTransactionWrapper wrapper = managedTxWrapper;

        return wrapper != null ? wrapper.unwrap() : null;
    }

    @Override
    public QueryTransactionWrapper startTxIfNeeded(ParsedResult parsedResult) {
        try {
            SqlQueryType queryType = parsedResult.queryType();

            if (queryType == SqlQueryType.TX_CONTROL) {
                if (externalTransaction != null) {
                    throw new ExternalTransactionNotSupportedException();
                }

                return processTransactionManagementStatement(parsedResult);
            }

            ScriptManagedTransactionWrapper managedTxWrapper0 = managedTxWrapper;

            if (managedTxWrapper0 != null && managedTxWrapper0.trackStatementCursor(queryType)) {
                return managedTxWrapper0;
            }

            return super.startTxIfNeeded(parsedResult);
        } catch (SqlException e) {
            InternalTransaction scriptTx = scriptTransaction();

            if (scriptTx != null) {
                scriptTx.rollback();
            }

            throw e;
        }
    }

    private QueryTransactionWrapper processTransactionManagementStatement(ParsedResult parsedResult) {
        SqlNode node = parsedResult.parsedTree();

        ScriptManagedTransactionWrapper scriptTxWrapper = this.managedTxWrapper;

        if (node instanceof IgniteSqlCommitTransaction) {
            if (scriptTxWrapper == null) {
                return QueryTransactionWrapper.NOOP_TX_WRAPPER;
            }

            this.managedTxWrapper = null;

            return scriptTxWrapper.forCommit();
        }

        assert node instanceof IgniteSqlStartTransaction : node == null ? "null" : node.getClass().getName();

        if (scriptTxWrapper != null) {
            throw new SqlException(RUNTIME_ERR, "Nested transactions are not supported.");
        }

        IgniteSqlStartTransaction txStartNode = (IgniteSqlStartTransaction) node;

        TransactionOptions options =
                new TransactionOptions().readOnly(txStartNode.getMode() == IgniteSqlStartTransactionMode.READ_ONLY);

        scriptTxWrapper = new ScriptManagedTransactionWrapper((InternalTransaction) transactions.begin(options));

        this.managedTxWrapper = scriptTxWrapper;

        return scriptTxWrapper;
    }
}
