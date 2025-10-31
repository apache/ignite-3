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

package org.apache.ignite.internal.sql.engine.exec.fsm;

import static org.apache.ignite.internal.sql.engine.exec.fsm.ValidationHelper.validateDynamicParameters;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.lang.ErrorGroups.Sql.EXECUTION_CANCELLED_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.RUNTIME_ERR;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.ignite.internal.sql.ResultSetMetadataImpl;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursorImpl;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.TxControlInsideExternalTxNotSupportedException;
import org.apache.ignite.internal.sql.engine.exec.TransactionalOperationTracker;
import org.apache.ignite.internal.sql.engine.exec.fsm.QueryExecutor.ParsedResultWithNextCursorFuture;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlStartTransaction;
import org.apache.ignite.internal.sql.engine.sql.ParsedResult;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionContext;
import org.apache.ignite.internal.sql.engine.tx.ScriptTransactionContext;
import org.apache.ignite.internal.sql.engine.util.IteratorToDataCursorAdapter;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.Nullable;

class MultiStatementHandler {
    private static final ResultSetMetadata EMPTY_RESULT_SET_METADATA =
            new ResultSetMetadataImpl(Collections.emptyList());

    private final Query query;
    private final Queue<ScriptStatement> statements;
    private final ScriptTransactionContext scriptTxContext;

    /**
     * Collection is used to track SELECT statements to postpone following DML operation.
     *
     * <p>We have no isolation within the same transaction. This implies, that any changes to the data
     * will be seen during next read. Considering lazy execution of read operations, DML started after SELECT operation may, in fact, outrun
     * actual read. To address this problem, let's collect all SELECT cursor in this collection and postpone following DML until at least
     * first page is ready for every collected so far cursor.
     */
    private final Queue<CompletableFuture<Void>> inFlightSelects = new ConcurrentLinkedQueue<>();

    private final Queue<CompletableFuture<Void>> dependentQueries = new ConcurrentLinkedQueue<>();

    MultiStatementHandler(
            TransactionalOperationTracker txTracker,
            Query query,
            QueryTransactionContext txContext,
            List<ParsedResult> parsedResults,
            Object[] params
    ) {
        this.query = query;
        this.statements = prepareStatementsQueue(parsedResults, params);
        this.scriptTxContext = new ScriptTransactionContext(txContext, txTracker);
    }

    /**
     * Returns a queue. each element of which represents parameters required to execute a single statement of the script.
     */
    private static Queue<ScriptStatement> prepareStatementsQueue(List<ParsedResult> parsedResults, Object[] params) {
        assert !parsedResults.isEmpty();

        int paramsCount = parsedResults.stream().mapToInt(ParsedResult::dynamicParamsCount).sum();

        validateDynamicParameters(paramsCount, params, true);

        ScriptStatement[] results = new ScriptStatement[parsedResults.size()];

        boolean txControlStatementFound = false;

        // We fill parameters in reverse order, because each script statement
        // requires a reference to the future of the next statement.
        CompletableFuture<AsyncSqlCursor<InternalSqlRow>> prevCursorFuture = null;
        for (int i = parsedResults.size() - 1; i >= 0; i--) {
            ParsedResult result = parsedResults.get(i);

            Object[] params0 = Arrays.copyOfRange(params, paramsCount - result.dynamicParamsCount(), paramsCount);
            paramsCount -= result.dynamicParamsCount();

            // Marks the beginning of a transaction block that does not end with a COMMIT.
            boolean unfinishedTxBlock = false;

            if (!txControlStatementFound && result.queryType() == SqlQueryType.TX_CONTROL) {
                unfinishedTxBlock = result.parsedTree() instanceof IgniteSqlStartTransaction;

                txControlStatementFound = true;
            }

            results[i] = new ScriptStatement(i, result, params0, unfinishedTxBlock, prevCursorFuture);
            prevCursorFuture = results[i].cursorFuture;
        }

        return new ArrayBlockingQueue<>(results.length, false, List.of(results));
    }

    CompletableFuture<AsyncSqlCursor<InternalSqlRow>> processNext() {
        ScriptStatement scriptStatement = statements.poll();

        assert scriptStatement != null;

        CompletableFuture<AsyncSqlCursor<InternalSqlRow>> cursorFuture = scriptStatement.cursorFuture;

        try {
            // future may be already completed by concurrent cancel of all statements due to error
            // during script execution
            if (cursorFuture.isDone()) {
                return cursorFuture;
            }

            int statementNum = scriptStatement.idx;
            ParsedResult parsedResult = scriptStatement.parsedResult;
            Object[] params = scriptStatement.dynamicParams;
            CompletableFuture<AsyncSqlCursor<InternalSqlRow>> nextCurFut = scriptStatement.nextStatementFuture;

            CompletableFuture<AsyncSqlCursor<InternalSqlRow>> fut;

            if (parsedResult.queryType() == SqlQueryType.TX_CONTROL) {
                // Ensure that TX_CONTROL statements are allowed.
                ValidationHelper.validateQueryType(query.properties.allowedQueryTypes(), SqlQueryType.TX_CONTROL);

                // start of a new transaction is possible only while there is no
                // other explicit transaction; commit of a transaction will wait
                // for related cursor to be closed. In other words, we have no
                // interest in in-flight selects anymore, so lets just clean this
                // collection
                if (!inFlightSelects.isEmpty()) {
                    inFlightSelects.clear();
                }

                if (scriptStatement.unfinishedTxBlock) {
                    // Stop script execution because the transaction block is not complete.
                    fut = CompletableFuture.failedFuture(new SqlException(RUNTIME_ERR,
                            "Transaction block doesn't have a COMMIT statement at the end."));
                } else {
                    // Return an empty cursor.
                    fut = scriptTxContext.handleControlStatement(parsedResult.parsedTree())
                            .thenApply(ignored -> new AsyncSqlCursorImpl<>(
                                    parsedResult.queryType(),
                                    EMPTY_RESULT_SET_METADATA,
                                    new IteratorToDataCursorAdapter<>(Collections.emptyIterator()),
                                    nextCurFut
                            ));
                }
            } else if (parsedResult.queryType() == SqlQueryType.DDL) {
                List<ParsedResultWithNextCursorFuture> ddlBatch = new ArrayList<>();

                do {
                    ddlBatch.add(new ParsedResultWithNextCursorFuture(scriptStatement.parsedResult, scriptStatement.nextStatementFuture));

                    ScriptStatement statement = statements.peek();
                    if (statement == null || statement.parsedResult.queryType() != SqlQueryType.DDL) {
                        break;
                    }

                    if (!DdlBatchingHelper.isCompatible(scriptStatement.parsedResult, statement.parsedResult)) {
                        break;
                    }

                    scriptStatement = statement;

                    statements.poll();
                } while (true);

                fut = query.executor.executeChildBatch(query, scriptTxContext, statementNum, ddlBatch);
            } else {
                scriptTxContext.registerCursorFuture(parsedResult.queryType(), cursorFuture);

                fut = query.executor.executeChildQuery(query, scriptTxContext, statementNum, parsedResult, params, nextCurFut);
            }

            boolean implicitTx = scriptTxContext.explicitTx() == null;
            boolean lastStatement = scriptStatement.isLastStatement();
            fut.whenComplete((cursor, ex) -> {
                if (ex != null) {
                    scriptTxContext.onError(ex);

                    cursorFuture.completeExceptionally(ex);

                    cancelAll(ex);

                    return;
                }

                cursorFuture.complete(cursor);

                if (!cursor.onClose().isDone()) {
                    dependentQueries.add(cursor.onClose());
                }

                if (lastStatement) {
                    // Main program is completed, therefore it's safe to schedule termination of a query
                    query.resultHolder
                            .thenRun(this::scheduleTermination);
                } else {
                    CompletableFuture<Void> triggerFuture;
                    ScriptStatement nextStatement = statements.peek();

                    if (implicitTx) {
                        if (cursor.queryType() != SqlQueryType.QUERY) {
                            triggerFuture = cursor.onFirstPageReady();
                        } else {
                            triggerFuture = nullCompletedFuture();
                        }
                    } else {
                        if (cursor.queryType() == SqlQueryType.QUERY) {
                            inFlightSelects.add(CompletableFuture.anyOf(
                                    cursor.onClose(), cursor.onFirstPageReady()
                            ).handle((r, e) -> null));

                            if (nextStatement != null && nextStatement.parsedResult.queryType() == SqlQueryType.DML) {
                                // we need to postpone DML until first page will be ready for every SELECT operation
                                // prior to that DML
                                triggerFuture = CompletableFuture.allOf(inFlightSelects.toArray(CompletableFuture[]::new));

                                inFlightSelects.clear();
                            } else {
                                triggerFuture = nullCompletedFuture();
                            }
                        } else {
                            CompletableFuture<Void> prefetchFuture = cursor.onFirstPageReady();

                            // for non query statements cursor should not be resolved until the very first page is ready.
                            // if prefetch was completed exceptionally, then cursor future is expected to be completed
                            // exceptionally as well, resulting in the early return in the very beginning of the `whenComplete`
                            assert prefetchFuture.isDone() && !prefetchFuture.isCompletedExceptionally()
                                    : "prefetch future is expected to be completed successfully, but was "
                                    + (prefetchFuture.isDone() ? "completed exceptionally" : "not completed");

                            triggerFuture = nullCompletedFuture();
                        }
                    }

                    triggerFuture.thenRunAsync(this::processNext, query.executor::execute)
                            .exceptionally(e -> {
                                cancelAll(e);

                                return null;
                            });
                }
            });
        } catch (TxControlInsideExternalTxNotSupportedException txEx) {
            scriptTxContext.onError(txEx);

            cursorFuture.completeExceptionally(txEx);
        } catch (Throwable e) {
            scriptTxContext.onError(e);

            cursorFuture.completeExceptionally(e);

            cancelAll(e);
        }

        return cursorFuture;
    }

    private void cancelAll(Throwable cause) {
        query.cancel.cancel();

        for (ScriptStatement scriptStatement : statements) {
            CompletableFuture<AsyncSqlCursor<InternalSqlRow>> fut = scriptStatement.cursorFuture;

            if (fut.isDone()) {
                continue;
            }

            fut.completeExceptionally(new SqlException(
                    EXECUTION_CANCELLED_ERR,
                    "The script execution was canceled due to an error in the previous statement.",
                    cause
            ));
        }

        scheduleTermination();
    }

    private void scheduleTermination() {
        CompletableFuture.allOf(dependentQueries.toArray(CompletableFuture[]::new))
                .whenComplete((ignored, ex) -> query.moveTo(ExecutionPhase.TERMINATED));
    }

    private static class ScriptStatement {
        private final CompletableFuture<AsyncSqlCursor<InternalSqlRow>> cursorFuture = new CompletableFuture<>();
        private final CompletableFuture<AsyncSqlCursor<InternalSqlRow>> nextStatementFuture;
        private final ParsedResult parsedResult;
        private final Object[] dynamicParams;
        private final int idx;
        private final boolean unfinishedTxBlock;

        private ScriptStatement(
                int idx,
                ParsedResult parsedResult,
                Object[] dynamicParams,
                boolean unfinishedTxBlock,
                @Nullable CompletableFuture<AsyncSqlCursor<InternalSqlRow>> nextStatementFuture
        ) {
            this.idx = idx;
            this.parsedResult = parsedResult;
            this.dynamicParams = dynamicParams;
            this.nextStatementFuture = nextStatementFuture;
            this.unfinishedTxBlock = unfinishedTxBlock;
        }

        boolean isLastStatement() {
            return nextStatementFuture == null;
        }
    }
}
