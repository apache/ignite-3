/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.client.handler;

import static org.apache.ignite.internal.jdbc.proto.IgniteQueryErrorCode.UNKNOWN;
import static org.apache.ignite.internal.util.ArrayUtils.OBJECT_EMPTY_ARRAY;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import org.apache.ignite.client.handler.requests.jdbc.JdbcQueryCursor;
import org.apache.ignite.internal.jdbc.proto.JdbcStatementType;
import org.apache.ignite.internal.jdbc.proto.event.BatchExecuteRequest;
import org.apache.ignite.internal.jdbc.proto.event.BatchExecuteResult;
import org.apache.ignite.internal.jdbc.proto.event.BatchPreparedStmntRequest;
import org.apache.ignite.internal.jdbc.proto.event.QueryExecuteRequest;
import org.apache.ignite.internal.jdbc.proto.event.QueryExecuteResult;
import org.apache.ignite.internal.jdbc.proto.event.QuerySingleResult;
import org.apache.ignite.internal.jdbc.proto.event.Response;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.QueryContext;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.QueryValidator;
import org.apache.ignite.internal.sql.engine.exec.QueryValidationException;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan.Type;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlColumnType;

/**
 * Jdbc query event handler implementation.
 */
public class JdbcQueryExecutionHandlerImpl implements JdbcQueryExecutionHandler {
    /** Sql query processor. */
    private final QueryProcessor processor;

    /** Client registry resources. */
    private final ClientResourceRegistry resources;

    /**
     * Constructor.
     *
     * @param processor Processor.
     */
    public JdbcQueryExecutionHandlerImpl(QueryProcessor processor, ClientResourceRegistry resources) {
        assert processor != null;
        assert resources != null;

        this.processor = processor;
        this.resources = resources;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<QueryExecuteResult> queryAsync(QueryExecuteRequest req) {
        if (req.pageSize() <= 0) {
            return CompletableFuture.failedFuture(new IgniteInternalException("Invalid fetch size : [fetchSize=" + req.pageSize() + ']'));
        }

        QueryContext context = createQueryContext(req.getStmtType());

        var results = new ArrayList<CompletableFuture<QuerySingleResult>>();
        for (var cursorFut : processor.queryAsync(context, req.schemaName(), req.sqlQuery(),
                req.arguments() == null ? OBJECT_EMPTY_ARRAY : req.arguments())) {
            results.add(
                    cursorFut.thenApply(cursor -> new JdbcQueryCursor<>(req.maxRows(), cursor))
                            .thenCompose(cursor -> createJdbcResult(cursor, req))
            );
        }

        if (results.isEmpty()) {
            return CompletableFuture.failedFuture(
                    new IgniteInternalException("At least one result is expected for query " + req.sqlQuery())
            );
        }

        return CompletableFuture.allOf(results.toArray(new CompletableFuture[0])).thenApply(none -> {
            var actualResults = results.stream().map(CompletableFuture::join).collect(Collectors.toList());

            return new QueryExecuteResult(actualResults);
        }).exceptionally(t -> {
            results.stream()
                    .filter(fut -> !fut.isCompletedExceptionally())
                    .map(CompletableFuture::join)
                    .map(res -> {
                        try {
                            return resources.remove(res.cursorId()).get(AsyncSqlCursor.class);
                        } catch (IgniteInternalCheckedException e) {
                            //we can do nothing about this.
                        }
                        return null;
                    }).filter(Objects::nonNull).forEach(AsyncSqlCursor::closeAsync);

            throw new IgniteInternalException("Exception while executing query " + req.sqlQuery() + ". Error message: " + t.getMessage());
        });
    }

    private QueryContext createQueryContext(JdbcStatementType stmtType) {
        if (stmtType == JdbcStatementType.ANY_STATEMENT_TYPE) {
            return QueryContext.of();
        }

        QueryValidator validator = (QueryPlan plan) -> {
            if (plan.type() == Type.QUERY || plan.type() == Type.EXPLAIN) {
                if (stmtType == JdbcStatementType.SELECT_STATEMENT_TYPE) {
                    return;
                }
                throw new QueryValidationException("Given statement type does not match that declared by JDBC driver.");
            }
            if (stmtType == JdbcStatementType.UPDATE_STATEMENT_TYPE) {
                return;
            }
            throw new QueryValidationException("Given statement type does not match that declared by JDBC driver.");
        };

        return QueryContext.of(validator);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<BatchExecuteResult> batchAsync(BatchExecuteRequest req) {
        List<String> queries = req.queries();

        var counters = new IntArrayList(req.queries().size());
        var tail = CompletableFuture.completedFuture(counters);

        for (String query : queries) {
            tail = tail.thenCompose(list -> executeAndCollectUpdateCount(req.schemaName(), query, OBJECT_EMPTY_ARRAY)
                    .thenApply(cnt -> {
                        list.add(cnt > Integer.MAX_VALUE ? Statement.SUCCESS_NO_INFO : cnt.intValue());

                        return list;
                    }));
        }

        return tail.handle((ignored, t) -> {
            if (t != null) {
                return handleBatchException(t, queries.get(counters.size()), counters.toIntArray());
            }

            return new BatchExecuteResult(counters.toIntArray());
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<BatchExecuteResult> batchPrepStatementAsync(BatchPreparedStmntRequest req) {
        var argList = req.getArgs();

        var counters = new IntArrayList(req.getArgs().size());
        var tail = CompletableFuture.completedFuture(counters);

        for (Object[] args : argList) {
            tail = tail.thenCompose(list -> executeAndCollectUpdateCount(req.schemaName(), req.getQuery(), args)
                    .thenApply(cnt -> {
                        list.add(cnt > Integer.MAX_VALUE ? Statement.SUCCESS_NO_INFO : cnt.intValue());

                        return list;
                    }));
        }

        return tail.handle((ignored, t) -> {
            if (t != null) {
                return handleBatchException(t, req.getQuery(), counters.toIntArray());
            }

            return new BatchExecuteResult(counters.toIntArray());
        });
    }

    private CompletableFuture<Long> executeAndCollectUpdateCount(String schema, String sql, Object[] arg) {
        var context = createQueryContext(JdbcStatementType.UPDATE_STATEMENT_TYPE);

        var cursors = processor.queryAsync(context, schema, sql, arg);

        if (cursors.size() != 1) {
            return CompletableFuture.failedFuture(new IgniteInternalException("Multi statement queries are not supported in batching"));
        }

        return cursors.get(0).thenCompose(cursor -> cursor.requestNextAsync(1).thenApply(batch -> (Long) batch.items().get(0).get(0)));
    }

    private BatchExecuteResult handleBatchException(Throwable e, String query, int[] counters) {
        String error;

        if (e instanceof ClassCastException) {
            error = "Unexpected result after query:" + query + ". Not an upsert statement? Error message: " + e.getMessage();
        } else {
            error = e.getMessage();
        }

        return new BatchExecuteResult(Response.STATUS_FAILED, UNKNOWN, error, counters);
    }

    /**
     * Serializes the stack trace of given exception for further sending to the client.
     *
     * @param t Throwable.
     * @return StringWriter filled with exception.
     */
    private StringWriter getWriterWithStackTrace(Throwable t) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);

        t.printStackTrace(pw);
        return sw;
    }

    /**
     * Creates jdbc result for the cursor.
     *
     * @param cur Sql cursor for query.
     * @param req Execution request.
     * @return JdbcQuerySingleResult filled with first batch of data.
     */
    private CompletionStage<QuerySingleResult> createJdbcResult(AsyncSqlCursor<List<Object>> cur, QueryExecuteRequest req) {
        long cursorId;
        try {
            cursorId = resources.put(new ClientResource(cur, cur::closeAsync));
        } catch (IgniteInternalCheckedException e) {
            return CompletableFuture.failedFuture(
                    new IgniteInternalException("Failed get next cursorId from resources holder. Node is stopping?")
            );
        }

        return cur.requestNextAsync(req.pageSize()).thenApply(batch -> {
            boolean hasNext = batch.hasMore();

            switch (cur.queryType()) {
                case EXPLAIN:
                case QUERY:
                    return new QuerySingleResult(cursorId, batch.items(), !hasNext);
                case DML:
                    if (!validateDmlResult(cur.metadata(), hasNext)) {
                        throw new IgniteInternalException("Unexpected result for DML query [" + req.sqlQuery() + "].");
                    }

                    return new QuerySingleResult(cursorId, (Long) batch.items().get(0).get(0));
                case DDL:
                    return new QuerySingleResult(cursorId, 0);
                default:
                    throw new IgniteInternalException("Query type [" + cur.queryType() + "] is not supported yet.");
            }
        });
    }

    /**
     * Validate dml result. Check if it stores only one value of Long type.
     *
     * @param meta Fetched data from cursor.
     * @param next  HasNext flag.
     * @return Boolean value indicates if data is valid or not.
     */
    private boolean validateDmlResult(ResultSetMetadata meta, boolean next) {
        if (next) {
            return false;
        }

        if (meta.columns().size() != 1) {
            return false;
        }

        return meta.columns().get(0).type() == SqlColumnType.INT64;
    }
}
