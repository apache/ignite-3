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
import static org.apache.ignite.internal.jdbc.proto.IgniteQueryErrorCode.UNSUPPORTED_OPERATION;
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
import org.apache.ignite.client.handler.requests.jdbc.JdbcMetadataCatalog;
import org.apache.ignite.client.handler.requests.jdbc.JdbcQueryCursor;
import org.apache.ignite.internal.jdbc.proto.JdbcQueryEventHandler;
import org.apache.ignite.internal.jdbc.proto.JdbcStatementType;
import org.apache.ignite.internal.jdbc.proto.event.JdbcBatchExecuteRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcBatchExecuteResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcBatchPreparedStmntRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaColumnsRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaColumnsResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaPrimaryKeysRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaPrimaryKeysResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaSchemasRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaSchemasResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaTablesRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaTablesResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQueryExecuteRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQueryExecuteResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQuerySingleResult;
import org.apache.ignite.internal.jdbc.proto.event.Response;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.QueryContext;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.QueryValidator;
import org.apache.ignite.internal.sql.engine.exec.QueryValidationException;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan.Type;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlColumnType;

/**
 * Jdbc query event handler implementation.
 */
public class JdbcQueryEventHandlerImpl implements JdbcQueryEventHandler {
    /** Sql query processor. */
    private final QueryProcessor processor;

    /** Jdbc metadata info. */
    private final JdbcMetadataCatalog meta;

    /** Current JDBC cursors. */
    private final ClientResourceRegistry resources;

    /**
     * Constructor.
     *
     * @param processor Processor.
     * @param meta JdbcMetadataInfo.
     * @param resources Client resources.
     */
    public JdbcQueryEventHandlerImpl(QueryProcessor processor, JdbcMetadataCatalog meta,
            ClientResourceRegistry resources) {
        this.processor = processor;
        this.meta = meta;
        this.resources = resources;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<? extends Response> queryAsync(JdbcQueryExecuteRequest req) {
        if (req.pageSize() <= 0) {
            return CompletableFuture.completedFuture(new JdbcQueryExecuteResult(Response.STATUS_FAILED,
                    "Invalid fetch size [fetchSize=" + req.pageSize() + ']'));
        }

        QueryContext context = createQueryContext(req.getStmtType());

        var results = new ArrayList<CompletableFuture<JdbcQuerySingleResult>>();
        for (var cursorFut : processor.queryAsync(context, req.schemaName(), req.sqlQuery(),
                req.arguments() == null ? OBJECT_EMPTY_ARRAY : req.arguments())) {
            results.add(
                    cursorFut.thenApply(cursor -> new JdbcQueryCursor<>(req.maxRows(), cursor))
                            .thenCompose(cursor -> createJdbcResult(cursor, req))
            );
        }

        if (results.isEmpty()) {
            return CompletableFuture.completedFuture(new JdbcQueryExecuteResult(Response.STATUS_FAILED,
                    "At least one cursor is expected for query [query=" + req.sqlQuery() + ']'));
        }

        return CompletableFuture.allOf(results.toArray(new CompletableFuture[0])).thenApply(none -> {
            var actualResults = results.stream().map(CompletableFuture::join).collect(Collectors.toList());

            return new JdbcQueryExecuteResult(actualResults);
        }).exceptionally(t -> {
            results.stream()
                    .filter(fut -> !fut.isCompletedExceptionally())
                    .map(CompletableFuture::join)
                    .filter(res -> res.cursorId() != null) //close only for QUERY cursors
                    .map(res -> {
                        try {
                            return resources.remove(res.cursorId()).get(AsyncSqlCursor.class);
                        } catch (IgniteInternalCheckedException e) {
                            //we can do nothing about this.
                        }
                        return null;
                    })
                    .filter(Objects::nonNull)
                    .forEach(AsyncSqlCursor::closeAsync);


            StringWriter sw = getWriterWithStackTrace(t);

            return new JdbcQueryExecuteResult(Response.STATUS_FAILED,
                    "Exception while executing query [query=" + req.sqlQuery() + "]. Error message:" + sw);
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
    public CompletableFuture<JdbcBatchExecuteResult> batchAsync(JdbcBatchExecuteRequest req) {
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

            return new JdbcBatchExecuteResult(counters.toIntArray());
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<JdbcBatchExecuteResult> batchPrepStatementAsync(
            JdbcBatchPreparedStmntRequest req) {
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

            return new JdbcBatchExecuteResult(counters.toIntArray());
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

    private JdbcBatchExecuteResult handleBatchException(Throwable e, String query, int[] counters) {
        StringWriter sw = getWriterWithStackTrace(e);

        String error;

        if (e instanceof ClassCastException) {
            error = "Unexpected result. Not an upsert statement? [query=" + query + "] Error message:" + sw;
        } else {
            error = sw.toString();
        }

        return new JdbcBatchExecuteResult(Response.STATUS_FAILED, UNKNOWN, error, counters);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<JdbcMetaTablesResult> tablesMetaAsync(JdbcMetaTablesRequest req) {
        return meta.getTablesMeta(req.schemaName(), req.tableName(), req.tableTypes()).thenApply(JdbcMetaTablesResult::new);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<JdbcMetaColumnsResult> columnsMetaAsync(JdbcMetaColumnsRequest req) {
        return meta.getColumnsMeta(req.schemaName(), req.tableName(), req.columnName()).thenApply(JdbcMetaColumnsResult::new);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<JdbcMetaSchemasResult> schemasMetaAsync(JdbcMetaSchemasRequest req) {
        return meta.getSchemasMeta(req.schemaName()).thenApply(JdbcMetaSchemasResult::new);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<JdbcMetaPrimaryKeysResult> primaryKeysMetaAsync(JdbcMetaPrimaryKeysRequest req) {
        return meta.getPrimaryKeys(req.schemaName(), req.tableName()).thenApply(JdbcMetaPrimaryKeysResult::new);
    }

    /**
     * Serializes the stack trace of given exception for further sending to the client.
     *
     * @param t Throwable.
     * @return StringWriter filled with exception.
     */
    private StringWriter getWriterWithStackTrace(Throwable t) {
        String message = ExceptionUtils.unwrapCause(t).getMessage();
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);

        pw.print(message);
        return sw;
    }

    /**
     * Creates jdbc result for the cursor.
     *
     * @param cur Sql cursor for query.
     * @param req Execution request.
     * @return JdbcQuerySingleResult filled with first batch of data.
     */
    private CompletionStage<JdbcQuerySingleResult> createJdbcResult(AsyncSqlCursor<List<Object>> cur, JdbcQueryExecuteRequest req) {
        return cur.requestNextAsync(req.pageSize()).thenApply(batch -> {
            boolean hasNext = batch.hasMore();

            switch (cur.queryType()) {
                case EXPLAIN:
                case QUERY: {
                    long cursorId;
                    try {
                        cursorId = resources.put(new ClientResource(cur, cur::closeAsync));
                    } catch (IgniteInternalCheckedException e) {
                        cur.closeAsync();

                        return new JdbcQuerySingleResult(Response.STATUS_FAILED,
                                "Unable to store query cursor.");
                    }
                    return new JdbcQuerySingleResult(cursorId, batch.items(), !hasNext);
                }
                case DML:
                    if (!validateDmlResult(cur.metadata(), hasNext)) {
                        return new JdbcQuerySingleResult(Response.STATUS_FAILED,
                                "Unexpected result for DML [query=" + req.sqlQuery() + ']');
                    }

                    return new JdbcQuerySingleResult((Long) batch.items().get(0).get(0));
                case DDL:
                    return new JdbcQuerySingleResult(0);
                default:
                    return new JdbcQuerySingleResult(UNSUPPORTED_OPERATION,
                            "Query type is not supported yet [queryType=" + cur.queryType() + ']');
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
