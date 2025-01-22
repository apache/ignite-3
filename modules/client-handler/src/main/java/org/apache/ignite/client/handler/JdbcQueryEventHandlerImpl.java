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
import static org.apache.ignite.internal.util.CompletableFutures.allOf;
import static org.apache.ignite.lang.ErrorGroups.Client.CONNECTION_ERR;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.client.handler.requests.jdbc.JdbcMetadataCatalog;
import org.apache.ignite.client.handler.requests.jdbc.JdbcQueryCursor;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
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
import org.apache.ignite.internal.jdbc.proto.event.JdbcQueryCancelResult;
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
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.util.AsyncCursor.BatchedResult;
import org.apache.ignite.lang.CancellationToken;
import org.apache.ignite.table.QualifiedName;
import org.jetbrains.annotations.Nullable;

/**
 * Jdbc query event handler implementation.
 */
public class JdbcQueryEventHandlerImpl extends JdbcHandlerBase implements JdbcQueryEventHandler {
    /** {@link SqlQueryType}s allowed in JDBC update statements. **/
    public static final Set<SqlQueryType> UPDATE_STATEMENT_QUERIES = Set.of(DML, SqlQueryType.DDL, SqlQueryType.KILL);

    /** Sql query processor. */
    private final QueryProcessor processor;

    /** Jdbc metadata info. */
    private final JdbcMetadataCatalog meta;

    /** Transaction manager. */
    private final TxManager txManager;

    /**
     * Constructor.
     *
     * @param processor Processor.
     * @param meta JdbcMetadataInfo.
     * @param resources Client resources.
     * @param txManager Transaction manager.
     */
    public JdbcQueryEventHandlerImpl(
            QueryProcessor processor,
            JdbcMetadataCatalog meta,
            ClientResourceRegistry resources,
            TxManager txManager
    ) {
        super(resources);

        this.processor = processor;
        this.meta = meta;
        this.txManager = txManager;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<JdbcConnectResult> connect(ZoneId timeZoneId) {
        try {
            JdbcConnectionContext connectionContext = new JdbcConnectionContext(
                    txManager,
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

        long correlationToken = req.correlationToken();
        CancellationToken token = connectionContext.registerExecution(correlationToken);

        HybridTimestampTracker timeTracker = Objects.requireNonNull(req.timestampTracker());
        JdbcStatementType reqStmtType = req.getStmtType();
        String defaultSchemaName = req.schemaName();
        boolean multiStatement = req.multiStatement();
        ZoneId timeZoneId = connectionContext.timeZoneId();
        long timeoutMillis = req.queryTimeoutMillis();

        InternalTransaction tx = req.autoCommit() ? null : connectionContext.getOrStartTransaction(timeTracker);
        SqlProperties properties = createProperties(reqStmtType, defaultSchemaName, multiStatement, timeZoneId, timeoutMillis);

        CompletableFuture<AsyncSqlCursor<InternalSqlRow>> result = processor.queryAsync(
                properties,
                timeTracker,
                tx,
                token,
                req.sqlQuery(),
                req.arguments() == null ? OBJECT_EMPTY_ARRAY : req.arguments()
        );

        doWhenAllCursorsComplete(result, () -> connectionContext.deregisterExecution(correlationToken));

        return result.thenCompose(cursor -> createJdbcResult(new JdbcQueryCursor<>(req.maxRows(), cursor), req.pageSize()))
                .exceptionally(t -> createErrorResult("Exception while executing query.", t, null));
    }

    private static SqlProperties createProperties(
            JdbcStatementType stmtType,
            String defaultSchemaName,
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

        // TODO: https://issues.apache.org/jira/browse/IGNITE-24021
        //  Replace this with using correct implementation of `IgniteNameUtils.parseSimpleName`, when it will be fixed.
        String schemaNameInCanonicalForm = QualifiedName.fromSimple(defaultSchemaName).objectName();

        return SqlPropertiesHelper.newBuilder()
                .set(QueryProperty.ALLOWED_QUERY_TYPES, allowedTypes)
                .set(QueryProperty.TIME_ZONE_ID, timeZoneId)
                .set(QueryProperty.DEFAULT_SCHEMA, schemaNameInCanonicalForm)
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

        HybridTimestampTracker timeTracker = Objects.requireNonNull(req.timestampTracker());
        InternalTransaction tx = req.autoCommit() ? null : connectionContext.getOrStartTransaction(timeTracker);

        long correlationToken = req.correlationToken();
        CancellationToken token = connectionContext.registerExecution(correlationToken);
        String defaultSchemaName = req.schemaName();
        var queries = req.queries();
        var counters = new IntArrayList(req.queries().size());
        var tail = CompletableFuture.completedFuture(counters);
        long queryTimeoutMillis = req.queryTimeoutMillis();

        for (String query : queries) {
            tail = tail.thenCompose(list -> executeAndCollectUpdateCount(
                    connectionContext,
                    defaultSchemaName,
                    timeTracker,
                    tx,
                    token,
                    query,
                    OBJECT_EMPTY_ARRAY,
                    queryTimeoutMillis,
                    list
            ));
        }

        return tail.handle((ignored, t) -> {
            connectionContext.deregisterExecution(correlationToken);

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

        HybridTimestampTracker timeTracker = Objects.requireNonNull(req.timestampTracker());
        InternalTransaction tx = req.autoCommit() ? null : connectionContext.getOrStartTransaction(timeTracker);

        assert req.autoCommit() || tx != null;

        long correlationToken = req.correlationToken();
        CancellationToken token = connectionContext.registerExecution(correlationToken);
        var argList = req.getArgs();
        String defaultSchemaName = req.schemaName();
        var counters = new IntArrayList(req.getArgs().size());
        var tail = CompletableFuture.completedFuture(counters);
        long timeoutMillis = req.queryTimeoutMillis();

        for (Object[] args : argList) {
            tail = tail.thenCompose(list -> executeAndCollectUpdateCount(
                    connectionContext, defaultSchemaName,  timeTracker, tx, token, req.getQuery(), args, timeoutMillis, list
            ));
        }

        return tail.handle((ignored, t) -> {
            connectionContext.deregisterExecution(correlationToken);

            if (t != null) {
                return handleBatchException(t, req.getQuery(), counters.toIntArray());
            }

            return new JdbcBatchExecuteResult(counters.toIntArray());
        });
    }

    private CompletableFuture<IntArrayList> executeAndCollectUpdateCount(
            JdbcConnectionContext context,
            String defaultSchemaName,
            HybridTimestampTracker tracker,
            @Nullable InternalTransaction tx,
            CancellationToken token,
            String sql,
            Object[] arg,
            long timeoutMillis,
            IntArrayList resultUpdateCounters
    ) {
        if (!context.valid()) {
            return CompletableFuture.failedFuture(new IgniteInternalException(CONNECTION_ERR, "Connection is closed"));
        }

        SqlProperties properties = createProperties(JdbcStatementType.UPDATE_STATEMENT_TYPE,
                defaultSchemaName,
                false,
                context.timeZoneId(),
                timeoutMillis
        );

        CompletableFuture<AsyncSqlCursor<InternalSqlRow>> result = processor.queryAsync(
                properties,
                tracker,
                tx,
                token,
                sql,
                arg == null ? OBJECT_EMPTY_ARRAY : arg
        );

        return result.thenCompose(cursor -> cursor.requestNextAsync(1)
                .thenApply(batch -> {
                    int updateCounter = handleBatchResult(cursor.queryType(), batch);

                    resultUpdateCounters.add(updateCounter);

                    return resultUpdateCounters;
                }));
    }

    private static int handleBatchResult(SqlQueryType type, BatchedResult<InternalSqlRow> result) {
        switch (type) {
            case DDL:
            case KILL:
                return Statement.SUCCESS_NO_INFO;
            case DML:
                Long updateCounts = (Long) result.items().get(0).get(0);

                assert updateCounts != null : "Invalid DML result";

                return updateCounts > Integer.MAX_VALUE ? Statement.SUCCESS_NO_INFO : updateCounts.intValue();
            default:
                throw new IllegalStateException("Unexpected query type: " + type);
        }
    }

    private static JdbcBatchExecuteResult handleBatchException(Throwable e, String query, int[] counters) {
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

        return connectionContext.finishTransactionAsync(commit).handle((observableTime, t) -> {
            if (t != null) {
                return new JdbcFinishTxResult(Response.STATUS_FAILED, t.getMessage());
            }

            return new JdbcFinishTxResult(observableTime);
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<JdbcQueryCancelResult> cancelAsync(long connectionId, long correlationToken) {
        JdbcConnectionContext connectionContext;

        try {
            connectionContext = resources.get(connectionId).get(JdbcConnectionContext.class);
        } catch (IgniteInternalCheckedException exception) {
            return CompletableFuture.completedFuture(new JdbcQueryCancelResult(Response.STATUS_FAILED, "Connection is broken"));
        }

        return connectionContext.cancelExecution(correlationToken).handle((ignored, t) -> {
            if (t != null) {
                return new JdbcQueryCancelResult(Response.STATUS_FAILED, t.getMessage());
            }

            return new JdbcQueryCancelResult();
        });
    }

    private void doWhenAllCursorsComplete(
            CompletableFuture<AsyncSqlCursor<InternalSqlRow>> cursorFuture, Runnable action
    ) {
        List<CompletableFuture<?>> dependency = new ArrayList<>();
        var cursorChainTraverser = new Function<AsyncSqlCursor<?>, CompletableFuture<AsyncSqlCursor<?>>>() {
            @Override
            public CompletableFuture<AsyncSqlCursor<?>> apply(AsyncSqlCursor<?> cursor) {
                dependency.add(cursor.onClose());

                if (cursor.hasNextResult()) {
                    return cursor.nextResult().thenCompose(this);
                }

                return allOf(dependency)
                        .thenRun(action)
                        .thenApply(ignored -> cursor);
            }
        };

        cursorFuture
                .thenCompose(cursorChainTraverser)
                .exceptionally(ex -> {
                    action.run();

                    return null;
                });
    }
}
