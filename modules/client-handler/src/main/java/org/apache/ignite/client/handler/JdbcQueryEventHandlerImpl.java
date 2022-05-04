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

import static org.apache.ignite.client.proto.query.IgniteQueryErrorCode.UNKNOWN;
import static org.apache.ignite.client.proto.query.IgniteQueryErrorCode.UNSUPPORTED_OPERATION;
import static org.apache.ignite.internal.util.ArrayUtils.OBJECT_EMPTY_ARRAY;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.ignite.client.handler.requests.sql.JdbcMetadataCatalog;
import org.apache.ignite.client.handler.requests.sql.JdbcQueryCursor;
import org.apache.ignite.client.proto.query.JdbcQueryEventHandler;
import org.apache.ignite.client.proto.query.JdbcStatementType;
import org.apache.ignite.client.proto.query.event.BatchExecuteRequest;
import org.apache.ignite.client.proto.query.event.BatchExecuteResult;
import org.apache.ignite.client.proto.query.event.BatchPreparedStmntRequest;
import org.apache.ignite.client.proto.query.event.JdbcColumnMeta;
import org.apache.ignite.client.proto.query.event.JdbcMetaColumnsRequest;
import org.apache.ignite.client.proto.query.event.JdbcMetaColumnsResult;
import org.apache.ignite.client.proto.query.event.JdbcMetaPrimaryKeysRequest;
import org.apache.ignite.client.proto.query.event.JdbcMetaPrimaryKeysResult;
import org.apache.ignite.client.proto.query.event.JdbcMetaSchemasRequest;
import org.apache.ignite.client.proto.query.event.JdbcMetaSchemasResult;
import org.apache.ignite.client.proto.query.event.JdbcMetaTablesRequest;
import org.apache.ignite.client.proto.query.event.JdbcMetaTablesResult;
import org.apache.ignite.client.proto.query.event.JdbcQueryMetadataRequest;
import org.apache.ignite.client.proto.query.event.QueryCloseRequest;
import org.apache.ignite.client.proto.query.event.QueryCloseResult;
import org.apache.ignite.client.proto.query.event.QueryExecuteRequest;
import org.apache.ignite.client.proto.query.event.QueryExecuteResult;
import org.apache.ignite.client.proto.query.event.QueryFetchRequest;
import org.apache.ignite.client.proto.query.event.QueryFetchResult;
import org.apache.ignite.client.proto.query.event.QuerySingleResult;
import org.apache.ignite.client.proto.query.event.Response;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.QueryContext;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.QueryValidator;
import org.apache.ignite.internal.sql.engine.ResultFieldMetadata;
import org.apache.ignite.internal.sql.engine.ResultSetMetadata;
import org.apache.ignite.internal.sql.engine.exec.QueryValidationException;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan.Type;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.lang.IgniteInternalException;

/**
 * Jdbc query event handler implementation.
 */
public class JdbcQueryEventHandlerImpl implements JdbcQueryEventHandler {
    /** Current JDBC cursors. */
    private final ConcurrentHashMap<Long, AsyncSqlCursor<List<Object>>> openCursors = new ConcurrentHashMap<>();

    /** Cursor Id generator. */
    private final AtomicLong cursorIdGenerator = new AtomicLong();

    /** Sql query processor. */
    private final QueryProcessor processor;

    /** Jdbc metadata info. */
    private final JdbcMetadataCatalog meta;

    /**
     * Constructor.
     *
     * @param processor Processor.
     * @param meta      JdbcMetadataInfo.
     */
    public JdbcQueryEventHandlerImpl(QueryProcessor processor, JdbcMetadataCatalog meta) {
        this.processor = processor;
        this.meta = meta;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<QueryExecuteResult> queryAsync(QueryExecuteRequest req) {
        if (req.pageSize() <= 0) {
            return CompletableFuture.completedFuture(new QueryExecuteResult(Response.STATUS_FAILED,
                    "Invalid fetch size : [fetchSize=" + req.pageSize() + ']'));
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
            return CompletableFuture.completedFuture(new QueryExecuteResult(Response.STATUS_FAILED,
                    "At least one cursor is expected for query " + req.sqlQuery()));
        }

        return CompletableFuture.allOf(results.toArray(new CompletableFuture[0])).thenApply(none -> {
            var actualResults = results.stream().map(CompletableFuture::join).collect(Collectors.toList());

            return new QueryExecuteResult(actualResults);
        }).exceptionally(t -> {
            results.stream()
                    .filter(fut -> !fut.isCompletedExceptionally())
                    .map(CompletableFuture::join)
                    .map(res -> openCursors.get(res.cursorId()))
                    .forEach(AsyncSqlCursor::closeAsync);

            StringWriter sw = getWriterWithStackTrace(t);

            return new QueryExecuteResult(Response.STATUS_FAILED,
                    "Exception while executing query " + req.sqlQuery() + ". Error message: " + sw);
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
    public CompletableFuture<QueryFetchResult> fetchAsync(QueryFetchRequest req) {
        var cur = openCursors.get(req.cursorId());

        if (cur == null) {
            return CompletableFuture.completedFuture(new QueryFetchResult(Response.STATUS_FAILED,
                    "Failed to find query cursor with ID: " + req.cursorId()));
        }

        if (req.pageSize() <= 0) {
            return CompletableFuture.completedFuture(new QueryFetchResult(Response.STATUS_FAILED,
                    "Invalid fetch size : [fetchSize=" + req.pageSize() + ']'));
        }

        return cur.requestNextAsync(req.pageSize()).handle((batch, t) -> {
            if (t != null) {
                StringWriter sw = getWriterWithStackTrace(t);

                return new QueryFetchResult(Response.STATUS_FAILED,
                        "Failed to fetch results for cursor id " + req.cursorId() + ". Error message: " + sw);
            }

            return new QueryFetchResult(batch.items(), batch.hasMore());
        }).toCompletableFuture();
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
        StringWriter sw = getWriterWithStackTrace(e);

        String error;

        if (e instanceof ClassCastException) {
            error = "Unexpected result after query:" + query + ". Not an upsert statement? " + sw;
        } else {
            error = sw.toString();
        }

        return new BatchExecuteResult(Response.STATUS_FAILED, UNKNOWN, error, counters);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<QueryCloseResult> closeAsync(QueryCloseRequest req) {
        var cur = openCursors.remove(req.cursorId());

        if (cur == null) {
            return CompletableFuture.completedFuture(new QueryCloseResult(Response.STATUS_FAILED,
                    "Failed to find query cursor with ID: " + req.cursorId()));
        }

        return cur.closeAsync().handle((none, t) -> {
            if (t != null) {
                StringWriter sw = getWriterWithStackTrace(t);

                return new QueryCloseResult(Response.STATUS_FAILED,
                        "Failed to close SQL query [curId=" + req.cursorId() + "]. Error message: " + sw);
            }

            return new QueryCloseResult();
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<JdbcMetaColumnsResult> queryMetadataAsync(JdbcQueryMetadataRequest req) {
        AsyncSqlCursor<?> cur = openCursors.get(req.cursorId());

        if (cur == null) {
            return CompletableFuture.completedFuture(new JdbcMetaColumnsResult(Response.STATUS_FAILED,
                    "Failed to find query cursor with ID: " + req.cursorId()));
        }

        ResultSetMetadata metadata = cur.metadata();

        if (metadata == null) {
            return CompletableFuture.completedFuture(new JdbcMetaColumnsResult(Response.STATUS_FAILED,
                    "Failed to get query metadata for cursor with ID : " + req.cursorId()));
        }

        List<JdbcColumnMeta> meta = metadata.fields().stream()
                .map(this::createColumnMetadata)
                .collect(Collectors.toList());

        return CompletableFuture.completedFuture(new JdbcMetaColumnsResult(meta));
    }

    /**
     * Create Jdbc representation of column metadata from given origin and RelDataTypeField field.
     *
     * @param fldMeta field metadata contains info about column.
     * @return JdbcColumnMeta object.
     */
    private JdbcColumnMeta createColumnMetadata(ResultFieldMetadata fldMeta) {
        List<String> origin = fldMeta.origin();

        String schemaName = origin == null ? null : origin.get(0);
        String tblName = origin == null ? null : origin.get(1);
        String colName = origin == null ? null : origin.get(2);

        return new JdbcColumnMeta(
                fldMeta.name(),
                schemaName,
                tblName,
                colName,
                Commons.nativeTypeToClass(fldMeta.type()),
                Commons.nativeTypePrecision(fldMeta.type()),
                Commons.nativeTypeScale(fldMeta.type()),
                fldMeta.isNullable()
        );
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
        long cursorId = cursorIdGenerator.getAndIncrement();

        openCursors.put(cursorId, cur);

        return cur.requestNextAsync(req.pageSize()).thenApply(batch -> {
            boolean hasNext = batch.hasMore();

            switch (cur.queryType()) {
                case EXPLAIN:
                case QUERY:
                    return new QuerySingleResult(cursorId, batch.items(), !hasNext);
                case DML:
                    if (!validateDmlResult(cur.metadata(), hasNext)) {
                        return new QuerySingleResult(Response.STATUS_FAILED,
                                "Unexpected result for DML query [" + req.sqlQuery() + "].");
                    }

                    return new QuerySingleResult(cursorId, (Long) batch.items().get(0).get(0));
                case DDL:
                    return new QuerySingleResult(cursorId, 0);
                default:
                    return new QuerySingleResult(UNSUPPORTED_OPERATION,
                            "Query type [" + cur.queryType() + "] is not supported yet.");
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

        if (meta.fields().size() != 1) {
            return false;
        }

        return meta.fields().get(0).type() == NativeTypes.INT64;
    }
}
