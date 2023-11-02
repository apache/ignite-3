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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.util.Commons.FRAMEWORK_CONFIG;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
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
public class SingleQueryHandler extends QueryHandler<AsyncSqlCursor<List<Object>>> {
    private final ParserService parserService;
    private final PrepareService prepareSvc;
    private final QueryTaskExecutor taskExecutor;

    SingleQueryHandler(SchemaSyncService schemaSyncService,
            SqlSchemaManager sqlSchemaManager,
            ExecutionService executionSrvc,
            AtomicInteger numberOfOpenCursors,
            ParserService parserService,
            PrepareService prepareSvc,
            QueryTaskExecutor taskExecutor
    ) {
        super(schemaSyncService, sqlSchemaManager, executionSrvc, numberOfOpenCursors);

        this.prepareSvc = prepareSvc;
        this.taskExecutor = taskExecutor;
        this.parserService = parserService;
    }

    @Override
    public CompletableFuture<AsyncSqlCursor<List<Object>>> executeQuery(
            Session session,
            IgniteTransactions transactions,
            InternalTransaction outerTx,
            String sql,
            Set<SqlQueryType> allowedQueryTypes,
            QueryCancel queryCancel,
            Object... params
    ) {
        String schemaName = session.properties().get(QueryProperty.DEFAULT_SCHEMA);
        CompletableFuture<AsyncSqlCursor<List<Object>>> start = new CompletableFuture<>();

        CompletableFuture<AsyncSqlCursor<List<Object>>> stage = start.thenCompose(ignored -> {
            ParsedResult result = parserService.parse(sql);

            validateParsedStatement(allowedQueryTypes, result, params);

            QueryTransactionWrapper txWrapper = SqlQueryProcessor.wrapTxOrStartImplicit(result.queryType(), transactions, outerTx);

            return waitForActualSchema(schemaName, txWrapper.unwrap().startTimestamp())
                    .thenCompose(schema -> {
                        BaseQueryContext ctx = BaseQueryContext.builder()
                                .frameworkConfig(Frameworks.newConfigBuilder(FRAMEWORK_CONFIG).defaultSchema(schema).build())
                                .queryId(UUID.randomUUID())
                                .cancel(queryCancel)
                                .parameters(params)
                                .build();

                        return prepareSvc.prepareAsync(result, ctx).thenApply(plan -> executePlan(session, txWrapper, ctx, plan));
                    }).whenComplete((res, ex) -> {
                        if (ex != null) {
                            txWrapper.rollback();
                        }
                    });
        });

        // TODO IGNITE-20078 Improve (or remove) CancellationException handling.
        stage.whenComplete((cur, ex) -> {
            if (ex instanceof CancellationException) {
                queryCancel.cancel();
            }
        });

        start.completeAsync(() -> null, taskExecutor);

        return stage;
    }

    /** Performs additional validation of a parsed statement. **/
    private static void validateParsedStatement(
            Set<SqlQueryType> allowedTypes,
            ParsedResult parsedResult,
            Object[] params
    ) {
        SqlQueryType queryType = parsedResult.queryType();

        if (parsedResult.queryType() == SqlQueryType.TX_CONTROL) {
            String message = "Transaction control statement can not be executed as an independent statement";

            throw new SqlException(STMT_VALIDATION_ERR, message);
        }

        if (!allowedTypes.contains(queryType)) {
            String message = format("Invalid SQL statement type. Expected {} but got {}", allowedTypes, queryType);

            throw new SqlException(STMT_VALIDATION_ERR, message);
        }

        DynamicParametersValidator.validate(parsedResult.dynamicParamsCount(), params);
    }
}
