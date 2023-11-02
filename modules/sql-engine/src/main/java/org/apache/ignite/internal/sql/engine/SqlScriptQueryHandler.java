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
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
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
import org.apache.ignite.internal.util.AsyncCursor.BatchedResult;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.tx.IgniteTransactions;

public class SqlScriptQueryHandler extends QueryHandler<AsyncSqlCursorIterator<List<Object>>> {
    private static final IgniteLogger LOG = Loggers.forClass(SqlScriptQueryHandler.class);

    private final ParserService parserService;
    private final PrepareService prepareSvc;
    private final QueryTaskExecutor taskExecutor;

    SqlScriptQueryHandler(SchemaSyncService schemaSyncService, SqlSchemaManager sqlSchemaManager,
            ExecutionService executionSrvc, AtomicInteger numberOfOpenCursors, ParserService parserService, PrepareService prepareSvc, QueryTaskExecutor taskExecutor) {
        super(schemaSyncService, sqlSchemaManager, executionSrvc, numberOfOpenCursors);
        this.parserService = parserService;
        this.prepareSvc = prepareSvc;
        this.taskExecutor = taskExecutor;
    }

    @Override
    public CompletableFuture<AsyncSqlCursorIterator<List<Object>>> execQuery(
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
                processNext(list.iterator(), session, transactions, outerTx, queryCancel, null);
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
            QueryCancel queryCancel,
            ScriptExecutionOptions options
    ) {
        try {
            if (!resultItr.hasNext()) {
                System.out.println(">xxx> finish");
                return;
            }

            System.out.println(">xxx> start");

            SingleStatementParams stmtParams = resultItr.next();
            ParsedResult result = stmtParams.result;
            CompletableFuture<AsyncSqlCursor<List<Object>>> resFut = stmtParams.resFut;
            Object[] params = stmtParams.dynamicParams;

            String schemaName = session.properties().get(QueryProperty.DEFAULT_SCHEMA);

            QueryTransactionWrapper txWrapper = SqlQueryProcessor.wrapTxOrStartImplicit(result.queryType(), transactions, outerTx);

            System.out.println(">xxx> wait schema " + result.parsedTree());

            CompletableFuture<SchemaPlus> schemaFut = waitForActualSchema(schemaName, txWrapper.unwrap().startTimestamp());

            QueryCancel queryCancel0 = resultItr.hasNext() ? new QueryCancel() : new QueryCancel() {
                @Override
                public synchronized void cancel() {
                    super.cancel();

                    System.out.println(">xxx> LAST CANCEL " + result.parsedTree());

                    queryCancel.cancel();
                }
            };

            schemaFut.thenComposeAsync(schema -> {
                CompletableFuture<Void> callbackFuture = new CompletableFuture<>();

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

                                processNext(resultItr, session, transactions, outerTx, queryCancel, options);
                            }

                        })
                        .cancel(queryCancel0)
                        .parameters(params)
                        .build();

                System.out.println(">xxx> prepare " + result.parsedTree());

                return fetch(result, ctx, session, txWrapper, callbackFuture);
            }).whenComplete((res, ex) -> {
                if (ex != null) {
                    ex.printStackTrace();

                    txWrapper.rollback();

                    resFut.completeExceptionally(ex);

                    completeRemainingExceptionally(resultItr, ex);

                    return;
                }

                resFut.complete(res);
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private CompletableFuture<AsyncSqlCursor<List<Object>>> fetch(ParsedResult result, BaseQueryContext ctx, Session session, QueryTransactionWrapper txWrapper, CompletableFuture<Void> fut) {
        if (result.queryType() == SqlQueryType.TX_CONTROL) {
            // TODO
            fut.complete(null);

            taskExecutor.execute(() -> {
                ctx.prefetchCallback().onPrefetchComplete(null);
            });

            return CompletableFuture.completedFuture(new SingleResultCursor<>(new BatchedResult<>(List.of(), false), result.queryType()));
        }

        return prepareSvc.prepareAsync(result, ctx)
                .thenApply(plan -> executePlan(session, txWrapper, ctx, plan))
                .thenCompose(cur -> fut.thenApply(unused -> cur));
//                .whenComplete((cursor, err) -> {
//                    if (err != null)
//                        err.printStackTrace();
//                });
    }

    private void completeRemainingExceptionally(Iterator<SingleStatementParams> resultItr, Throwable ex) {
        while (resultItr.hasNext()) {
            resultItr.next().resFut.completeExceptionally(
                    new SqlException(EXECUTION_CANCELLED_ERR,
                            "The script statement execution was canceled due to an error in the previous statement.", ex)
            );
        }
    }

    private static class SingleResultCursor<T> implements AsyncSqlCursor<T> {
        // TODO
        private final AtomicReference<BatchedResult<T>> res;

        private final SqlQueryType type;


        SingleResultCursor(BatchedResult<T> res, SqlQueryType type) {
            this.res = new AtomicReference<>(res);
            this.type = type;
        }

        @Override
        public CompletableFuture<BatchedResult<T>> requestNextAsync(int rows) {
            return CompletableFuture.completedFuture(res.getAndSet(null));
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            res.set(null);

            return CompletableFuture.completedFuture(null);
        }

        @Override
        public SqlQueryType queryType() {
            return type;
        }

        @Override
        public ResultSetMetadata metadata() {
            return null;
        }
    }

    public static class ScriptExecutionOptions {
        private static final boolean IGNORE_SCRIPT_ERRORS = false;
        private static final boolean CLOSE_CURSOR_ON_ITERATION = false;

        private final boolean stopOnError;
        private final boolean closePreviousCursor;

        public ScriptExecutionOptions() {
            this(IGNORE_SCRIPT_ERRORS, CLOSE_CURSOR_ON_ITERATION);
        }

        public ScriptExecutionOptions(boolean stopOnError, boolean closePreviousCursor) {
            this.stopOnError = stopOnError;
            this.closePreviousCursor = closePreviousCursor;
        }

        public boolean stopOnError() {
            return stopOnError;
        }

        public boolean closePreviousCursor() {
            return closePreviousCursor;
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
