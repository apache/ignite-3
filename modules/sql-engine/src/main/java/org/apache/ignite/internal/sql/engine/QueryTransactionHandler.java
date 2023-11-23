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

import static org.apache.ignite.lang.ErrorGroups.Sql.RUNTIME_ERR;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.calcite.sql.SqlNode;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlCommitTransaction;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlStartTransaction;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlStartTransactionMode;
import org.apache.ignite.internal.sql.engine.sql.ParsedResult;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.sql.ExternalTransactionNotSupportedException;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.tx.IgniteTransactions;
import org.apache.ignite.tx.TransactionOptions;
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
        return new ControlStatementAwareTransactionHandler(transactions, externalTransaction);
    }

    /**
     * Starts an implicit transaction if there is no external transaction.
     */
    class QueryTransactionHandlerImpl implements QueryTransactionHandler {
        final IgniteTransactions transactions;
        final @Nullable InternalTransaction externalTransaction;

        QueryTransactionHandlerImpl(IgniteTransactions transactions, @Nullable InternalTransaction externalTransaction) {
            this.transactions = transactions;
            this.externalTransaction = externalTransaction;
        }

        protected @Nullable InternalTransaction activeTransaction() {
            return externalTransaction;
        }

        @Override
        public QueryTransactionWrapper startTxIfNeeded(ParsedResult parsedResult) {
            SqlQueryType queryType = parsedResult.queryType();
            InternalTransaction activeTx = activeTransaction();

            if (activeTx == null) {
                return new QueryTransactionWrapper((InternalTransaction) transactions.begin(
                        new TransactionOptions().readOnly(queryType != SqlQueryType.DML)), true);
            }

            if (SqlQueryType.DDL == queryType) {
                throw new SqlException(RUNTIME_ERR, "DDL doesn't support transactions.");
            }

            if (SqlQueryType.DML == queryType && activeTx.isReadOnly()) {
                throw new SqlException(RUNTIME_ERR, "DML query cannot be started by using read only transactions.");
            }

            return new QueryTransactionWrapper(activeTx, false);
        }
    }

    /**
     * Starts an implicit transaction if there is no external transaction.
     * Supports script transaction management using {@link SqlQueryType#TX_CONTROL} statements.
     */
    class ControlStatementAwareTransactionHandler extends QueryTransactionHandlerImpl {
        private static final NoopTransactionWrapper noopTxWrapper = new NoopTransactionWrapper();

        private volatile ScriptTxWrapper scriptTxWrapper;

        ControlStatementAwareTransactionHandler(IgniteTransactions transactions, @Nullable InternalTransaction externalTransaction) {
            super(transactions, externalTransaction);
        }

        @Override
        protected @Nullable InternalTransaction activeTransaction() {
            return externalTransaction == null ? scriptTransaction() : externalTransaction;
        }

        private @Nullable InternalTransaction scriptTransaction() {
            ScriptTxWrapper wrapper = scriptTxWrapper;

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

                ScriptTxWrapper scriptTxWrapper0 = scriptTxWrapper;

                if (scriptTxWrapper0 != null) {
                    if (queryType == SqlQueryType.QUERY) {
                        scriptTxWrapper0.waitingCursors.incrementAndGet();

                        return scriptTxWrapper0;
                    }
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

            ScriptTxWrapper scriptTxWrapper0 = scriptTxWrapper;

            if (node instanceof IgniteSqlCommitTransaction) {
                if (scriptTxWrapper0 == null) {
                    return noopTxWrapper;
                }

                // TODO Separate wrapper?
                scriptTxWrapper0.commit = true;
                scriptTxWrapper0.onCursorClose();

                scriptTxWrapper = null;

                return scriptTxWrapper0;
            }

            assert node instanceof IgniteSqlStartTransaction : node == null ? "null" : node.getClass().getName();

            if (scriptTxWrapper0 != null) {
                throw new SqlException(RUNTIME_ERR, "Nested transactions are not supported.");
            }

            IgniteSqlStartTransaction txStartNode = (IgniteSqlStartTransaction) node;

            TransactionOptions options =
                    new TransactionOptions().readOnly(txStartNode.getMode() == IgniteSqlStartTransactionMode.READ_ONLY);

            scriptTxWrapper0 = new ScriptTxWrapper((InternalTransaction) transactions.begin(options), false);

            scriptTxWrapper = scriptTxWrapper0;

            return scriptTxWrapper0;
        }

        static class NoopTransactionWrapper extends QueryTransactionWrapper {
            NoopTransactionWrapper() {
                super(null, false);
            }

            @Override
            CompletableFuture<Void> rollback() {
                return Commons.completedFuture();
            }
        }
    }

    /** TODO blah-blah. */
    class ScriptTxWrapper extends QueryTransactionWrapper {
        private final AtomicInteger waitingCursors = new AtomicInteger(1);

        private InternalTransaction transaction;

        private CompletableFuture<Void> completeFuture = new CompletableFuture<>();

        private volatile boolean rollback;

        private volatile boolean commit;

        ScriptTxWrapper(InternalTransaction transaction, boolean implicit) {
            super(transaction, implicit);

            this.transaction = transaction;
        }

        @Override
        CompletableFuture<Void> onCursorClose() {
            return checkCommit(false);
        }

        @Override
        CompletableFuture<Void> commitImplicit() {
            return commit ? completeFuture : Commons.completedFuture();
        }

        @Override
        CompletableFuture<Void> rollback() {
            return checkCommit(true);
        }

        CompletableFuture<Void> checkCommit(boolean rollback) {
            if (rollback) {
                this.rollback = true;
            }

            if (waitingCursors.decrementAndGet() == 0) {
                if (this.rollback) {
                    transaction.rollback();
                } else {
                    transaction.commit();
                }

                completeFuture.complete(null);
            }

            return Commons.completedFuture();
        }
    }
}
