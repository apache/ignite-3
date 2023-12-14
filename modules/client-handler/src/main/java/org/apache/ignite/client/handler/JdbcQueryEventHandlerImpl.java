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
import static org.apache.ignite.internal.sql.engine.SqlQueryType.DML;
import static org.apache.ignite.internal.util.ArrayUtils.OBJECT_EMPTY_ARRAY;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.lang.ErrorGroups.Client.CONNECTION_ERR;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.client.handler.requests.jdbc.JdbcMetadataCatalog;
import org.apache.ignite.client.handler.requests.jdbc.JdbcQueryCursor;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.jdbc.proto.JdbcQueryEventHandler;
import org.apache.ignite.internal.jdbc.proto.JdbcStatementType;
import org.apache.ignite.internal.jdbc.proto.event.JdbcBatchExecuteRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcBatchExecuteResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcBatchPreparedStmntRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcConnectResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcFinishTxResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaColumnsRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaColumnsResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaPrimaryKeysRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaPrimaryKeysResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaSchemasRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaSchemasResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaTablesRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaTablesResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQueryExecuteRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQuerySingleResult;
import org.apache.ignite.internal.jdbc.proto.event.Response;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.QueryProperty;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.property.SqlProperties;
import org.apache.ignite.internal.sql.engine.property.SqlPropertiesHelper;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.AsyncCursor.BatchedResult;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.tx.IgniteTransactions;
import org.jetbrains.annotations.Nullable;

/**
 * Jdbc query event handler implementation.
 */
public class JdbcQueryEventHandlerImpl implements JdbcQueryEventHandler {
    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(JdbcQueryEventHandlerImpl.class);

    /** {@link SqlQueryType}s allowed in JDBC select statements. **/
    private static final Set<SqlQueryType> SELECT_STATEMENT_QUERIES = Set.of(SqlQueryType.QUERY, SqlQueryType.EXPLAIN);

    /** {@link SqlQueryType}s allowed in JDBC update statements. **/
    private static final Set<SqlQueryType> UPDATE_STATEMENT_QUERIES = Set.of(DML, SqlQueryType.DDL);

    /** Sql query processor. */
    private final QueryProcessor processor;

    /** Jdbc metadata info. */
    private final JdbcMetadataCatalog meta;

    /** Current JDBC cursors. */
    private final ClientResourceRegistry resources;

    /** Ignite transactions API. */
    private final IgniteTransactions igniteTransactions;

    /**
     * Constructor.
     *
     * @param processor Processor.
     * @param meta JdbcMetadataInfo.
     * @param resources Client resources.
     * @param igniteTransactions Ignite transactions API.
     */
    public JdbcQueryEventHandlerImpl(
            QueryProcessor processor,
            JdbcMetadataCatalog meta,
            ClientResourceRegistry resources,
            IgniteTransactions igniteTransactions
    ) {
        this.processor = processor;
        this.meta = meta;
        this.resources = resources;
        this.igniteTransactions = igniteTransactions;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<JdbcConnectResult> connect() {
        try {
            JdbcConnectionContext connectionContext = new JdbcConnectionContext(
                    igniteTransactions
            );

            long connectionId = resources.put(new ClientResource(
                    connectionContext,
                    connectionContext::close
            ));

            return CompletableFuture.completedFuture(new JdbcConnectResult(connectionId));
        } catch (IgniteInternalCheckedException exception) {
            String msg = getErrorMessage(exception);

            return CompletableFuture.completedFuture(new JdbcConnectResult(Response.STATUS_FAILED, "Unable to connect: " + msg));
        }
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<? extends Response> queryAsync(long connectionId, JdbcQueryExecuteRequest req) {
        if (req.pageSize() <= 0) {
            return CompletableFuture.completedFuture(new JdbcQuerySingleResult(Response.STATUS_FAILED,
                    "Invalid fetch size [fetchSize=" + req.pageSize() + ']'));
        }

        JdbcConnectionContext connectionContext;
        try {
            connectionContext = resources.get(connectionId).get(JdbcConnectionContext.class);
        } catch (IgniteInternalCheckedException exception) {
            return CompletableFuture.completedFuture(new JdbcQuerySingleResult(Response.STATUS_FAILED,
                    "Connection is broken"));
        }

        InternalTransaction tx = req.autoCommit() ? null : connectionContext.getOrStartTransaction();
        SqlProperties properties = createProperties(req.getStmtType());

        CompletableFuture<AsyncSqlCursor<InternalSqlRow>> result;

        if (req.multiStatement()) {
            result = processor.queryScriptAsync(
                    properties,
                    igniteTransactions,
                    tx,
                    req.sqlQuery(),
                    req.arguments() == null ? OBJECT_EMPTY_ARRAY : req.arguments()
            );
        } else {
            result = processor.querySingleAsync(
                    properties,
                    igniteTransactions,
                    tx,
                    req.sqlQuery(),
                    req.arguments() == null ? OBJECT_EMPTY_ARRAY : req.arguments()
            );
        }

        return result.thenCompose(cursor -> createJdbcResult(new JdbcQueryCursor<>(req.maxRows(), cursor), req))
                .exceptionally(t -> {
                    LOG.info("Exception while executing query [query=" + req.sqlQuery() + "]", ExceptionUtils.unwrapCause(t));

                    String msg = getErrorMessage(t);

                    return new JdbcQuerySingleResult(Response.STATUS_FAILED, msg);
                });
    }

    private static SqlProperties createProperties(JdbcStatementType stmtType) {
        Set<SqlQueryType> allowedTypes;

        switch (stmtType) {
            case ANY_STATEMENT_TYPE:
                allowedTypes = SqlQueryType.SINGLE_STMT_TYPES;
                break;
            case SELECT_STATEMENT_TYPE:
                allowedTypes = SELECT_STATEMENT_QUERIES;
                break;
            case UPDATE_STATEMENT_TYPE:
                allowedTypes = UPDATE_STATEMENT_QUERIES;
                break;
            default:
                throw new AssertionError("Unexpected jdbc statement type: " + stmtType);
        }

        return SqlPropertiesHelper.newBuilder()
                .set(QueryProperty.ALLOWED_QUERY_TYPES, allowedTypes)
                .build();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<JdbcBatchExecuteResult> batchAsync(long connectionId, JdbcBatchExecuteRequest req) {
        JdbcConnectionContext connectionContext;
        try {
            connectionContext = resources.get(connectionId).get(JdbcConnectionContext.class);
        } catch (IgniteInternalCheckedException exception) {
            return CompletableFuture.completedFuture(new JdbcBatchExecuteResult(Response.STATUS_FAILED, "Connection is broken"));
        }

        InternalTransaction tx = req.autoCommit() ? null : connectionContext.getOrStartTransaction();
        var queries = req.queries();
        var counters = new IntArrayList(req.queries().size());
        var tail = CompletableFuture.completedFuture(counters);

        for (String query : queries) {
            tail = tail.thenCompose(list -> executeAndCollectUpdateCount(connectionContext, tx, query, OBJECT_EMPTY_ARRAY)
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
        JdbcConnectionContext connectionContext;
        try {
            connectionContext = resources.get(connectionId).get(JdbcConnectionContext.class);
        } catch (IgniteInternalCheckedException exception) {
            return CompletableFuture.completedFuture(new JdbcBatchExecuteResult(Response.STATUS_FAILED, "Connection is broken"));
        }

        InternalTransaction tx = req.autoCommit() ? null : connectionContext.getOrStartTransaction();
        var argList = req.getArgs();
        var counters = new IntArrayList(req.getArgs().size());
        var tail = CompletableFuture.completedFuture(counters);

        for (Object[] args : argList) {
            tail = tail.thenCompose(list -> executeAndCollectUpdateCount(connectionContext, tx, req.getQuery(), args)
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

    private CompletableFuture<Long> executeAndCollectUpdateCount(
            JdbcConnectionContext context,
            @Nullable InternalTransaction tx,
            String sql,
            Object[] arg
    ) {
        if (!context.valid()) {
            return CompletableFuture.failedFuture(new IgniteInternalException(CONNECTION_ERR, "Connection is closed"));
        }

        SqlProperties properties = createProperties(JdbcStatementType.UPDATE_STATEMENT_TYPE);

        CompletableFuture<AsyncSqlCursor<InternalSqlRow>> result = processor.querySingleAsync(
                properties,
                igniteTransactions,
                tx,
                sql,
                arg == null ? OBJECT_EMPTY_ARRAY : arg
        );

        return result.thenCompose(cursor -> cursor.requestNextAsync(1))
                .thenApply(batch -> (Long) batch.items().get(0).get(0));
    }

    private JdbcBatchExecuteResult handleBatchException(Throwable e, String query, int[] counters) {
        String msg = getErrorMessage(e);

        String error;

        if (e instanceof ClassCastException) {
            error = "Unexpected result. Not an upsert statement? [query=" + query + "] Error message:" + msg;
        } else {
            error = msg;
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

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<JdbcFinishTxResult> finishTxAsync(long connectionId, boolean commit) {
        JdbcConnectionContext connectionContext;

        try {
            connectionContext = resources.get(connectionId).get(JdbcConnectionContext.class);
        } catch (IgniteInternalCheckedException exception) {
            return CompletableFuture.completedFuture(new JdbcFinishTxResult(Response.STATUS_FAILED, "Connection is broken"));
        }

        return connectionContext.finishTransactionAsync(commit).handle((ignored, t) -> {
            if (t != null) {
                return new JdbcFinishTxResult(Response.STATUS_FAILED, t.getMessage());
            }

            return new JdbcFinishTxResult();
        });
    }

    /**
     * Get a message of given exception for further sending to the client.
     *
     * @param t Throwable.
     * @return String filled with exception message.
     */
    @Nullable private String getErrorMessage(Throwable t) {
        Throwable cause = ExceptionUtils.unwrapCause(t);
        return cause.getMessage();
    }

    /**
     * Creates jdbc result for the cursor.
     *
     * @param cur Sql cursor for query.
     * @param req Execution request.
     * @return JdbcQuerySingleResult filled with first batch of data.
     */
    private CompletionStage<JdbcQuerySingleResult> createJdbcResult(AsyncSqlCursor<InternalSqlRow> cur, JdbcQueryExecuteRequest req) {
        return cur.requestNextAsync(req.pageSize()).thenApply(batch -> {
            boolean hasNext = batch.hasMore();

            long cursorId;
            try {
                cursorId = resources.put(new ClientResource(cur, cur::closeAsync));
            } catch (IgniteInternalCheckedException e) {
                cur.closeAsync();

                return new JdbcQuerySingleResult(Response.STATUS_FAILED,
                        "Unable to store query cursor.");
            }

            switch (cur.queryType()) {
                case EXPLAIN:
                case QUERY:
                case DML: {
                    if (cur.queryType() == DML && !validateDmlResult(cur.metadata(), hasNext)) {
                        return new JdbcQuerySingleResult(Response.STATUS_FAILED,
                                "Unexpected result for DML [query=" + req.sqlQuery() + ']');
                    }

                    List<ColumnMetadata> columns = cur.metadata().columns();

                    return buildSingleRequest(batch, columns, cursorId, !hasNext, cur.queryType());
                }
                case DDL:
                case TX_CONTROL:
                    return new JdbcQuerySingleResult(cursorId, 0);
                default:
                    return new JdbcQuerySingleResult(UNSUPPORTED_OPERATION,
                            "Query type is not supported yet [queryType=" + cur.queryType() + ']');
            }
        });
    }

    static JdbcQuerySingleResult buildSingleRequest(
            BatchedResult<InternalSqlRow> batch,
            List<ColumnMetadata> columns,
            long cursorId,
            boolean hasNext,
            SqlQueryType queryType
    ) {
        List<BinaryTupleReader> rows = new ArrayList<>(batch.items().size());
        for (InternalSqlRow item : batch.items()) {
            rows.add(item.asBinaryTuple());
        }

        int[] decimalScales = new int[columns.size()];
        List<ColumnType> schema = new ArrayList<>(columns.size());

        int countOfDecimal = 0;
        for (ColumnMetadata column : columns) {
            schema.add(column.type());
            if (column.type() == ColumnType.DECIMAL) {
                decimalScales[countOfDecimal++] = column.scale();
            }
        }
        decimalScales = Arrays.copyOf(decimalScales, countOfDecimal);

        long updCount = 0;
        if (queryType == DML) {
            updCount = (long) batch.items().get(0).get(0);
            return new JdbcQuerySingleResult(cursorId, updCount);
        }

        boolean isQuery = queryType == SqlQueryType.QUERY || queryType == SqlQueryType.EXPLAIN;

        return new JdbcQuerySingleResult(cursorId, rows, schema, decimalScales, hasNext, isQuery, updCount);
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
        private final AtomicBoolean closed = new AtomicBoolean();

        private final Object mux = new Object();

        private final IgniteTransactions igniteTransactions;

        private @Nullable InternalTransaction tx;

        JdbcConnectionContext(
                IgniteTransactions igniteTransactions
        ) {
            this.igniteTransactions = igniteTransactions;
        }

        /**
         * Gets the transaction associated with the current connection, starts a new one if it doesn't already exist.
         *
         * <p>NOTE: this method is not thread-safe and should only be called by a single thread.
         *
         * @return Transaction associated with the current connection.
         */
        InternalTransaction getOrStartTransaction() {
            return tx == null ? tx = (InternalTransaction) igniteTransactions.begin() : tx;
        }

        /**
         * Finishes active transaction, if one exists.
         *
         * <p>NOTE: this method is not thread-safe and should only be called by a single thread.
         *
         * @param commit {@code True} to commit, {@code false} to rollback.
         * @return Future that represents the pending completion of the operation.
         */
        CompletableFuture<Void> finishTransactionAsync(boolean commit) {
            InternalTransaction tx0 = tx;

            tx = null;

            if (tx0 == null) {
                return nullCompletedFuture();
            }

            return commit ? tx0.commitAsync() : tx0.rollbackAsync();
        }

        boolean valid() {
            return !closed.get();
        }

        void close() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }

            synchronized (mux) {
                finishTransactionAsync(false);
            }
        }
    }
}
