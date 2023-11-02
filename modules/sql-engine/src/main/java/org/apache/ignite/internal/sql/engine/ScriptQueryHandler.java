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

import static org.apache.ignite.internal.sql.engine.util.Commons.FRAMEWORK_CONFIG;
import static org.apache.ignite.lang.ErrorGroups.Sql.EXECUTION_CANCELLED_ERR;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.internal.sql.engine.exec.ExecutionService;
import org.apache.ignite.internal.sql.engine.exec.QueryTaskExecutor;
import org.apache.ignite.internal.sql.engine.prepare.PrepareService;
import org.apache.ignite.internal.sql.engine.schema.SqlSchemaManager;
import org.apache.ignite.internal.sql.engine.session.Session;
import org.apache.ignite.internal.sql.engine.sql.ParsedResult;
import org.apache.ignite.internal.sql.engine.sql.ParserService;
import org.apache.ignite.internal.sql.engine.util.BaseQueryContext;
import org.apache.ignite.internal.sql.engine.util.DynamicParametersValidator;
import org.apache.ignite.internal.table.distributed.schema.SchemaSyncService;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.tx.IgniteTransactions;

/**
 * TODO Blah blah blah.
 */
class ScriptQueryHandler extends QueryHandler<AsyncSqlCursorIterator<List<Object>>> {
    private final ParserService parserService;
    private final PrepareService prepareSvc;
    private final QueryTaskExecutor taskExecutor;

    ScriptQueryHandler(
            SchemaSyncService schemaSyncService,
            SqlSchemaManager sqlSchemaManager,
            ExecutionService executionSrvc,
            AtomicInteger numberOfOpenCursors,
            ParserService parserService,
            PrepareService prepareSvc,
            QueryTaskExecutor taskExecutor
    ) {
        super(schemaSyncService, sqlSchemaManager, executionSrvc, numberOfOpenCursors);
        this.parserService = parserService;
        this.prepareSvc = prepareSvc;
        this.taskExecutor = taskExecutor;
    }

    @Override
    CompletableFuture<AsyncSqlCursorIterator<List<Object>>> executeQuery(
            Session session,
            IgniteTransactions transactions,
            InternalTransaction outerTx,
            String sql,
            Set<SqlQueryType> allowedTypes,
            QueryCancel queryCancel,
            Object... params
    ) {
        CompletableFuture<AsyncSqlCursorIterator<List<Object>>> start = new CompletableFuture<>();

        // Parsing.
        CompletableFuture<List<SingleStatementParams>> parseFut = start.thenApply(ignored -> {
            List<ParsedResult> parsedResults = parserService.parseScript(sql);

            int totalDynamicParams = parsedResults.stream().mapToInt(ParsedResult::dynamicParamsCount).sum();

            List<SingleStatementParams> results = new ArrayList<>(parsedResults.size());

            DynamicParametersValidator.validate(totalDynamicParams, params);

            int pos = 0;

            for (ParsedResult result : parsedResults) {
                Object[] params0 = Arrays.copyOfRange(params, pos, pos + result.dynamicParamsCount());

                results.add(new SingleStatementParams(result, params0));

                pos += result.dynamicParamsCount();
            }

            return results;
        });

        start.completeAsync(() -> null, taskExecutor);

        parseFut.whenCompleteAsync((list, err) -> {
            if (err == null) {
                processNext(list.iterator(), session, transactions, outerTx, queryCancel);
            }
        }, taskExecutor);

        return parseFut.thenApply(list -> new AsyncSqlCursorIterator<>(list.stream()
                .filter(data -> data.result.queryType() != SqlQueryType.TX_CONTROL)
                .map(data -> data.resFut)
                .collect(Collectors.toList())
                .iterator()));
    }

    void processNext(
            Iterator<SingleStatementParams> resultItr,
            Session session,
            IgniteTransactions transactions,
            InternalTransaction outerTx,
            QueryCancel queryCancel
    ) {
        if (!resultItr.hasNext()) {
            System.out.println(">xxx> finish");

            return;
        }

        SingleStatementParams stmtParams = resultItr.next();

        try {
            ParsedResult result = stmtParams.result;
            CompletableFuture<AsyncSqlCursor<List<Object>>> resFut = stmtParams.resFut;
            Object[] params = stmtParams.dynamicParams;

            String schemaName = session.properties().get(QueryProperty.DEFAULT_SCHEMA);

            QueryTransactionWrapper txWrapper = SqlQueryProcessor.wrapTxOrStartImplicit(result.queryType(), transactions, outerTx);

            CompletableFuture<SchemaPlus> schemaFut = waitForActualSchema(schemaName, txWrapper.unwrap().startTimestamp());

            QueryCancel queryCancel0 = resultItr.hasNext() ? new QueryCancel() : new QueryCancel() {
                @Override
                public synchronized void cancel() {
                    super.cancel();

                    // TODO rework cancel handling.
                    System.out.println(">xxx> LAST CANCEL " + result.parsedTree());

                    queryCancel.cancel();
                }
            };

            schemaFut.thenComposeAsync(schema -> {
                CompletableFuture<Void> callbackFuture = new CompletableFuture<>();

                // TODO duplicated code
                BaseQueryContext ctx = BaseQueryContext.builder()
                        .frameworkConfig(Frameworks.newConfigBuilder(FRAMEWORK_CONFIG).defaultSchema(schema).build())
                        .queryId(UUID.randomUUID())
                        .prefetchCallback(ex -> {
                            if (ex != null) {
                                callbackFuture.completeExceptionally(ex);

                                completeRemainingExceptionally(resultItr, ex);
                            } else {
                                if (result.queryType() == SqlQueryType.DML) {
                                    txWrapper.commitImplicit();
                                }

                                callbackFuture.complete(null);

                                processNext(resultItr, session, transactions, outerTx, queryCancel);
                            }

                        })
                        .cancel(queryCancel0)
                        .parameters(params)
                        .build();

                return fetch(result, ctx, session, txWrapper, callbackFuture);
            }).whenComplete((res, ex) -> {
                if (ex != null) {
                    txWrapper.rollback();

                    resFut.completeExceptionally(ex);

                    completeRemainingExceptionally(resultItr, ex);

                    return;
                }

                resFut.complete(res);
            });
        } catch (Exception e) {
            stmtParams.resFut.completeExceptionally(e);

            completeRemainingExceptionally(resultItr, e);
        }
    }

    private CompletableFuture<AsyncSqlCursor<List<Object>>> fetch(
            ParsedResult result,
            BaseQueryContext ctx,
            Session session,
            QueryTransactionWrapper txWrapper,
            CompletableFuture<Void> fut
    ) {
        // TODO rework this part
        if (result.queryType() == SqlQueryType.TX_CONTROL) {
            fut.complete(null);

            taskExecutor.execute(() -> ctx.prefetchCallback().onPrefetchComplete(null));

            return CompletableFuture.completedFuture(null);
        }

        return prepareSvc.prepareAsync(result, ctx)
                .thenApply(plan -> executePlan(session, txWrapper, ctx, plan))
                .thenCompose(cur -> fut.thenApply(unused -> cur));
    }

    private void completeRemainingExceptionally(Iterator<SingleStatementParams> resultItr, Throwable ex) {
        while (resultItr.hasNext()) {
            resultItr.next().resFut.completeExceptionally(
                    new SqlException(EXECUTION_CANCELLED_ERR,
                            "The script statement execution was canceled due to an error in the previous statement.", ex)
            );
        }
    }

    static class SingleStatementParams {
        private final ParsedResult result;
        private final Object[] dynamicParams;
        private final CompletableFuture<AsyncSqlCursor<List<Object>>> resFut = new CompletableFuture<>();

        SingleStatementParams(ParsedResult result, Object[] dynamicParams) {
            this.result = result;
            this.dynamicParams = dynamicParams;
        }
    }
}
