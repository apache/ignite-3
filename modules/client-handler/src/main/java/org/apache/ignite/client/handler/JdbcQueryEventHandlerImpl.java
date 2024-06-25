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
import static org.apache.ignite.internal.sql.engine.SqlQueryType.DML;
import static org.apache.ignite.internal.util.ArrayUtils.OBJECT_EMPTY_ARRAY;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.lang.ErrorGroups.Client.CONNECTION_ERR;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.client.handler.requests.jdbc.JdbcMetadataCatalog;
import org.apache.ignite.client.handler.requests.jdbc.JdbcQueryCursor;
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
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.QueryProperty;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.property.SqlProperties;
import org.apache.ignite.internal.sql.engine.property.SqlPropertiesHelper;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.impl.IgniteTransactionsImpl;
import org.apache.ignite.tx.IgniteTransactions;
import org.jetbrains.annotations.Nullable;

/**
 * Jdbc query event handler implementation.
 */
public class JdbcQueryEventHandlerImpl extends JdbcHandlerBase implements JdbcQueryEventHandler {
    /** {@link SqlQueryType}s allowed in JDBC select statements. **/
    private static final Set<SqlQueryType> SELECT_STATEMENT_QUERIES = Set.of(SqlQueryType.QUERY, SqlQueryType.EXPLAIN);

    /** {@link SqlQueryType}s allowed in JDBC update statements. **/
    private static final Set<SqlQueryType> UPDATE_STATEMENT_QUERIES = Set.of(DML, SqlQueryType.DDL);

    /** Sql query processor. */
    private final QueryProcessor processor;

    /** Jdbc metadata info. */
    private final JdbcMetadataCatalog meta;

    /** Ignite transactions API. */
    private final IgniteTransactionsImpl igniteTransactions;

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
            IgniteTransactionsImpl igniteTransactions
    ) {
        super(resources);

        this.processor = processor;
        this.meta = meta;
        this.igniteTransactions = igniteTransactions;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<JdbcConnectResult> connect(ZoneId timeZoneId) {
        try {
            JdbcConnectionContext connectionContext = new JdbcConnectionContext(
                    igniteTransactions,
                    timeZoneId
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

        JdbcStatementType reqStmtType = req.getStmtType();
        boolean multiStatement = req.multiStatement();
        ZoneId timeZoneId = connectionContext.timeZoneId();
        long timeoutMillis = req.queryTimeoutMillis();

        SqlProperties properties = createProperties(reqStmtType, multiStatement, timeZoneId, timeoutMillis);

        CompletableFuture<AsyncSqlCursor<InternalSqlRow>> result = processor.queryAsync(
                properties,
                igniteTransactions.observableTimestampTracker(),
                tx,
                req.sqlQuery(),
                req.arguments() == null ? OBJECT_EMPTY_ARRAY : req.arguments()
        );

        return result.thenCompose(cursor -> createJdbcResult(new JdbcQueryCursor<>(req.maxRows(), cursor), req.pageSize()))
                .exceptionally(t -> createErrorResult("Exception while executing query [query=" + req.sqlQuery() + "]", t, null));
    }

    private static SqlProperties createProperties(
            JdbcStatementType stmtType, 
            boolean multiStatement, 
            ZoneId timeZoneId,
            long queryTimeoutMillis
    ) {
        Set<SqlQueryType> allowedTypes;

        switch (stmtType) {
            case ANY_STATEMENT_TYPE:
                allowedTypes = multiStatement ? SqlQueryType.ALL : SqlQueryType.SINGLE_STMT_TYPES;
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
                .set(QueryProperty.TIME_ZONE_ID, timeZoneId)
                .set(QueryProperty.QUERY_TIMEOUT, queryTimeoutMillis)
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
        long queryTimeoutMillis = req.queryTimeoutMillis();

        for (String query : queries) {
            tail = tail.thenCompose(list -> executeAndCollectUpdateCount(
                    connectionContext,
                    tx, query,
                    OBJECT_EMPTY_ARRAY,
                    queryTimeoutMillis
            ).thenApply(cnt -> {
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
        long timeoutMillis = req.queryTimeoutMillis();

        for (Object[] args : argList) {
            tail = tail.thenCompose(list -> executeAndCollectUpdateCount(connectionContext, tx, req.getQuery(), args, timeoutMillis)
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
            Object[] arg,
            long timeoutMillis) {

        if (!context.valid()) {
            return CompletableFuture.failedFuture(new IgniteInternalException(CONNECTION_ERR, "Connection is closed"));
        }

        SqlProperties properties = createProperties(JdbcStatementType.UPDATE_STATEMENT_TYPE, false, context.timeZoneId(), timeoutMillis);

        CompletableFuture<AsyncSqlCursor<InternalSqlRow>> result = processor.queryAsync(
                properties,
                igniteTransactions.observableTimestampTracker(),
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

    static class JdbcConnectionContext {
        private final AtomicBoolean closed = new AtomicBoolean();

        private final Object mux = new Object();

        private final IgniteTransactions igniteTransactions;

        private final ZoneId timeZoneId;

        private @Nullable InternalTransaction tx;

        JdbcConnectionContext(
                IgniteTransactions igniteTransactions,
                ZoneId timeZoneId
        ) {
            this.igniteTransactions = igniteTransactions;
            this.timeZoneId = timeZoneId;
        }

        ZoneId timeZoneId() {
            return timeZoneId;
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
