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

package org.apache.ignite.client.handler;

import static org.apache.ignite.internal.jdbc.proto.IgniteQueryErrorCode.UNKNOWN;
import static org.apache.ignite.internal.jdbc.proto.IgniteQueryErrorCode.UNSUPPORTED_OPERATION;
import static org.apache.ignite.internal.util.ArrayUtils.OBJECT_EMPTY_ARRAY;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Statement;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import org.apache.ignite.client.handler.requests.jdbc.JdbcMetadataCatalog;
import org.apache.ignite.client.handler.requests.jdbc.JdbcQueryCursor;
import org.apache.ignite.internal.jdbc.proto.JdbcQueryEventHandler;
import org.apache.ignite.internal.jdbc.proto.JdbcStatementType;
import org.apache.ignite.internal.jdbc.proto.event.JdbcBatchExecuteRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcBatchExecuteResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcBatchPreparedStmntRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcConnectResult;
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
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.exec.QueryValidationException;
import org.apache.ignite.internal.sql.engine.property.PropertiesHelper;
import org.apache.ignite.internal.sql.engine.property.PropertiesHolder;
import org.apache.ignite.internal.sql.engine.session.SessionId;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.internal.util.Pair;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.ResultSetMetadata;
import org.jetbrains.annotations.Nullable;

/**
 * Jdbc query event handler implementation.
 */
public class JdbcQueryEventHandlerImpl implements JdbcQueryEventHandler {

    /** {@link SqlQueryType}s allowed in JDBC select statements. **/
    private static final Set<SqlQueryType> SELECT_STATEMENT_QUERIES = Set.of(SqlQueryType.QUERY, SqlQueryType.EXPLAIN);

    /** {@link SqlQueryType}s allowed in JDBC update statements. **/
    private static final Set<SqlQueryType> UPDATE_STATEMENT_QUERIES = Set.of(SqlQueryType.DML, SqlQueryType.DDL);

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
    public JdbcQueryEventHandlerImpl(
            QueryProcessor processor,
            JdbcMetadataCatalog meta,
            ClientResourceRegistry resources
    ) {
        this.processor = processor;
        this.meta = meta;
        this.resources = resources;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<JdbcConnectResult> connect() {
        try {
            JdbcConnectionContext connectionContext = new JdbcConnectionContext(
                    processor::createSession,
                    processor::closeSession
            );

            long connectionId = resources.put(new ClientResource(
                    connectionContext,
                    connectionContext::close
            ));

            return CompletableFuture.completedFuture(new JdbcConnectResult(connectionId));
        } catch (IgniteInternalCheckedException exception) {
            StringWriter sw = getWriterWithStackTrace(exception);

            return CompletableFuture.completedFuture(new JdbcConnectResult(Response.STATUS_FAILED, "Unable to connect: " + sw));
        }
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<? extends Response> queryAsync(long connectionId, JdbcQueryExecuteRequest req) {
        if (req.pageSize() <= 0) {
            return CompletableFuture.completedFuture(new JdbcQueryExecuteResult(Response.STATUS_FAILED,
                    "Invalid fetch size [fetchSize=" + req.pageSize() + ']'));
        }

        JdbcConnectionContext connectionContext;
        try {
            connectionContext = resources.get(connectionId).get(JdbcConnectionContext.class);
        } catch (IgniteInternalCheckedException exception) {
            return CompletableFuture.completedFuture(new JdbcQueryExecuteResult(Response.STATUS_FAILED,
                    "Connection is broken"));
        }

        QueryContext context = createQueryContext(req.getStmtType());

        CompletableFuture<AsyncSqlCursor<List<Object>>> result = connectionContext.doInSession(sessionId -> processor.querySingleAsync(
                sessionId,
                context,
                req.sqlQuery(),
                req.arguments() == null ? OBJECT_EMPTY_ARRAY : req.arguments()
        ));

        return result.thenCompose(cursor -> createJdbcResult(new JdbcQueryCursor<>(req.maxRows(), cursor), req))
                .thenApply(jdbcResult -> new JdbcQueryExecuteResult(List.of(jdbcResult)))
                .exceptionally(t -> {
                    StringWriter sw = getWriterWithStackTrace(t);

                    return new JdbcQueryExecuteResult(Response.STATUS_FAILED,
                            "Exception while executing query [query=" + req.sqlQuery() + "]. Error message:" + sw);
                });
    }

    private QueryContext createQueryContext(JdbcStatementType stmtType) {
        switch (stmtType) {
            case ANY_STATEMENT_TYPE:
                return QueryContext.create(SqlQueryType.ALL);
            case SELECT_STATEMENT_TYPE:
                return QueryContext.create(SELECT_STATEMENT_QUERIES);
            case UPDATE_STATEMENT_TYPE:
                return QueryContext.create(UPDATE_STATEMENT_QUERIES);
            default:
                throw new AssertionError("Unexpected jdbc statement type: " + stmtType);
        }
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<JdbcBatchExecuteResult> batchAsync(long connectionId, JdbcBatchExecuteRequest req) {
        List<String> queries = req.queries();

        var counters = new IntArrayList(req.queries().size());
        var tail = CompletableFuture.completedFuture(counters);

        for (String query : queries) {
            tail = tail.thenCompose(list -> executeAndCollectUpdateCount(connectionId, query, OBJECT_EMPTY_ARRAY)
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
    public CompletableFuture<JdbcBatchExecuteResult> batchPrepStatementAsync(long connectionId, JdbcBatchPreparedStmntRequest req) {
        var argList = req.getArgs();

        var counters = new IntArrayList(req.getArgs().size());
        var tail = CompletableFuture.completedFuture(counters);

        for (Object[] args : argList) {
            tail = tail.thenCompose(list -> executeAndCollectUpdateCount(connectionId, req.getQuery(), args)
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

    private CompletableFuture<Long> executeAndCollectUpdateCount(long connectionId, String sql, Object[] arg) {
        var context = createQueryContext(JdbcStatementType.UPDATE_STATEMENT_TYPE);

        JdbcConnectionContext connectionContext;
        try {
            connectionContext = resources.get(connectionId).get(JdbcConnectionContext.class);
        } catch (IgniteInternalCheckedException exception) {
            return CompletableFuture.failedFuture(new IgniteInternalException("Connection is broken"));
        }

        CompletableFuture<AsyncSqlCursor<List<Object>>> result = connectionContext.doInSession(sessionId -> processor.querySingleAsync(
                sessionId,
                context,
                sql,
                arg == null ? OBJECT_EMPTY_ARRAY : arg
        ));

        return result.thenCompose(cursor -> cursor.requestNextAsync(1))
                .thenApply(batch -> (Long) batch.items().get(0).get(0));
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
        Throwable cause = ExceptionUtils.unwrapCause(t);
        StringWriter sw = new StringWriter();

        try (PrintWriter pw = new PrintWriter(sw)) {
            // We need to remap QueryValidationException into a jdbc error.
            if (cause instanceof QueryValidationException
                    || (cause instanceof IgniteException && cause.getCause() instanceof QueryValidationException)) {
                pw.print("Given statement type does not match that declared by JDBC driver.");
            } else {
                pw.print(cause.getMessage());
            }

            return sw;
        }
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

        return meta.columns().get(0).type() == ColumnType.INT64;
    }

    static class JdbcConnectionContext {
        private final Object mux = new Object();

        private final SessionFactory factory;
        private final SessionCleaner cleaner;
        private final PropertiesHolder properties = PropertiesHelper.emptyHolder();

        private volatile @Nullable SessionId sessionId;

        JdbcConnectionContext(
                SessionFactory factory,
                SessionCleaner cleaner
        ) {
            this.factory = factory;
            this.cleaner = cleaner;
        }

        void close() {
            synchronized (mux) {
                SessionId sessionId = this.sessionId;

                this.sessionId = null;

                cleaner.clean(sessionId);
            }
        }

        <T> CompletableFuture<T> doInSession(SessionAwareAction<T> action) {
            SessionId potentiallyNotCreatedSessionId = this.sessionId;

            if (potentiallyNotCreatedSessionId == null) {
                potentiallyNotCreatedSessionId = recreateSession(null);
            }

            SessionId finalSessionId = potentiallyNotCreatedSessionId;

            return action.perform(finalSessionId)
                    .handle((BiFunction<T, Throwable, Pair<T, Throwable>>) Pair::new)
                    .thenCompose(resAndError -> {
                        if (resAndError.getSecond() == null) {
                            return CompletableFuture.completedFuture(resAndError.getFirst());
                        }

                        Throwable error = resAndError.getSecond();

                        if (sessionExpiredError(error)) {
                            SessionId newSessionId = recreateSession(finalSessionId);

                            return action.perform(newSessionId);
                        }

                        return CompletableFuture.failedFuture(error);
                    });
        }

        private SessionId recreateSession(@Nullable SessionId expectedSessionId) {
            synchronized (mux) {
                SessionId actualSessionId = sessionId;

                // session was recreated by another thread
                if (actualSessionId != null && actualSessionId != expectedSessionId) {
                    return actualSessionId;
                }

                SessionId newSessionId = factory.create(properties);

                this.sessionId = newSessionId;

                return newSessionId;
            }
        }

        private static boolean sessionExpiredError(Throwable throwable) {
            if (!(throwable instanceof IgniteInternalException)) {
                return false;
            }

            IgniteInternalException internalException = (IgniteInternalException) throwable;

            // SESSION_EXPIRED_ERR is thrown when session has been expired but not yet been collected by cleaner thread
            // SESSION_NOT_FOUND_ERR is thrown when session has been expired AND collected by cleaner thread
            return internalException.code() == Sql.SESSION_EXPIRED_ERR
                    || internalException.code() == Sql.SESSION_NOT_FOUND_ERR;
        }
    }

    /** A factory to create a session. */
    @FunctionalInterface
    private static interface SessionFactory {
        SessionId create(PropertiesHolder properties);
    }

    /**
     * An interface describing an object to clean the session and release associated resources
     * when the session is no longer needed.
     */
    @FunctionalInterface
    private interface SessionCleaner {
        void clean(SessionId sessionId);
    }

    /** Interface describing an action that should be performed within the session. */
    @FunctionalInterface
    interface SessionAwareAction<T> {
        CompletableFuture<T> perform(SessionId sessionId);
    }
}
