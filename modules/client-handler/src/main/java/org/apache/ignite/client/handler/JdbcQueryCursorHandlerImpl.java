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

import static org.apache.ignite.client.handler.JdbcQueryEventHandlerImpl.buildSingleRequest;
import static org.apache.ignite.internal.jdbc.proto.IgniteQueryErrorCode.UNSUPPORTED_OPERATION;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.internal.jdbc.JdbcConverterUtils;
import org.apache.ignite.internal.jdbc.proto.JdbcQueryCursorHandler;
import org.apache.ignite.internal.jdbc.proto.event.JdbcColumnMeta;
import org.apache.ignite.internal.jdbc.proto.event.JdbcFetchQueryResultsRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaColumnsResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQueryCloseRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQueryCloseResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQueryFetchResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQueryMetadataRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQuerySingleResult;
import org.apache.ignite.internal.jdbc.proto.event.Response;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.TxControlInsideExternalTxNotSupportedException;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ResultSetMetadata;

/**
 * Jdbc query event handler implementation.
 */
public class JdbcQueryCursorHandlerImpl implements JdbcQueryCursorHandler {
    /** Client registry resources. */
    private final ClientResourceRegistry resources;

    /**
     * Constructor.
     *
     * @param resources Client resources.
     */
    JdbcQueryCursorHandlerImpl(ClientResourceRegistry resources) {
        this.resources = resources;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<JdbcQueryFetchResult> fetchAsync(JdbcFetchQueryResultsRequest req) {
        AsyncSqlCursor<InternalSqlRow> asyncSqlCursor;
        try {
            asyncSqlCursor = resources.get(req.cursorId()).get(AsyncSqlCursor.class);
        } catch (IgniteInternalCheckedException e) {
            StringWriter sw = getWriterWithStackTrace(e);

            return CompletableFuture.completedFuture(new JdbcQueryFetchResult(Response.STATUS_FAILED,
                    "Failed to find query cursor [curId=" + req.cursorId() + "]. Error message:" + sw));
        }

        if (req.fetchSize() <= 0) {
            return CompletableFuture.completedFuture(new JdbcQueryFetchResult(Response.STATUS_FAILED,
                    "Invalid fetch size [fetchSize=" + req.fetchSize() + ']'));
        }

        return asyncSqlCursor.requestNextAsync(req.fetchSize()).handle((batch, t) -> {
            if (t != null) {
                StringWriter sw = getWriterWithStackTrace(t);

                return new JdbcQueryFetchResult(Response.STATUS_FAILED,
                    "Failed to fetch query results [curId=" + req.cursorId() + "]. Error message:" + sw);
            }

            List<ByteBuffer> rows = new ArrayList<>(batch.items().size());
            for (InternalSqlRow item : batch.items()) {
                rows.add(item.asBinaryTuple().byteBuffer());
            }

            return new JdbcQueryFetchResult(rows, !batch.hasMore());
        }).toCompletableFuture();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<JdbcQuerySingleResult> getMoreResultsAsync(JdbcFetchQueryResultsRequest req) {
        AsyncSqlCursor<InternalSqlRow> asyncSqlCursor;
        try {
            asyncSqlCursor = resources.get(req.cursorId()).get(AsyncSqlCursor.class);
        } catch (IgniteInternalCheckedException e) {
            StringWriter sw = getWriterWithStackTrace(e);

            return CompletableFuture.completedFuture(new JdbcQuerySingleResult(Response.STATUS_FAILED,
                    "Failed to find query cursor [curId=" + req.cursorId() + "]. Error message:" + sw));
        }

        if (!asyncSqlCursor.hasNextResult()) {
            return CompletableFuture.completedFuture(new JdbcQuerySingleResult());
        }

        return asyncSqlCursor.closeAsync().thenCompose(c -> asyncSqlCursor.nextResult())
                .thenCompose(cur -> cur.requestNextAsync(req.fetchSize())
                    .thenApply(batch -> {
                        try {
                            SqlQueryType queryType = cur.queryType();

                            long cursorId = resources.put(new ClientResource(cur, cur::closeAsync));

                            switch (queryType) {
                                case EXPLAIN:
                                case QUERY: {
                                    List<ColumnMetadata> columns = cur.metadata().columns();

                                    return buildSingleRequest(batch, columns, cursorId, !batch.hasMore());
                                }
                                case DML: {
                                    long updCount = (long) batch.items().get(0).get(0);

                                    return new JdbcQuerySingleResult(cursorId, updCount);
                                }
                                case DDL:
                                case TX_CONTROL:
                                    return new JdbcQuerySingleResult(cursorId, 0);
                                default:
                                    return new JdbcQuerySingleResult(UNSUPPORTED_OPERATION,
                                            "Query type is not supported yet [queryType=" + cur.queryType() + ']');
                            }
                        } catch (IgniteInternalCheckedException e) {
                            return new JdbcQuerySingleResult(Response.STATUS_FAILED,
                                    "Unable to store query cursor.");
                        }
                    })
                ).handle((res, t) -> {
                    if (t != null) {
                        iterateThroughResultsAndCloseThem(asyncSqlCursor);

                        String errMsg;

                        // TODO uniformly handle exceptions
                        if (ExceptionUtils.unwrapCause(t) instanceof TxControlInsideExternalTxNotSupportedException) {
                            errMsg = "Transaction control statements are not supported in non-autocommit mode";
                        } else {
                            errMsg = getWriterWithStackTrace(t).toString();
                        }

                        return new JdbcQuerySingleResult(Response.STATUS_FAILED,
                                "Failed to fetch query results [curId=" + req.cursorId() + "]. Error message:" + errMsg);
                    }

                    return res;
                });
    }

    static void iterateThroughResultsAndCloseThem(AsyncSqlCursor<InternalSqlRow> cursor) {
        Function<AsyncSqlCursor<InternalSqlRow>, CompletableFuture<AsyncSqlCursor<InternalSqlRow>>> traverser = new Function<>() {
            @Override
            public CompletableFuture<AsyncSqlCursor<InternalSqlRow>> apply(AsyncSqlCursor<InternalSqlRow> cur) {
                return cur.closeAsync()
                        .thenCompose(none -> {
                            if (cur.hasNextResult()) {
                                return cur.nextResult().thenCompose(this);
                            } else {
                                return CompletableFuture.completedFuture(cur);
                            }
                        });
            }
        };

        CompletableFuture.completedFuture(cursor).thenCompose(traverser);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<JdbcQueryCloseResult> closeAsync(JdbcQueryCloseRequest req) {
        AsyncSqlCursor<List<Object>> asyncSqlCursor;
        try {
            if (req.removeFromResources()) {
                asyncSqlCursor = resources.remove(req.cursorId()).get(AsyncSqlCursor.class);
            } else {
                asyncSqlCursor = resources.get(req.cursorId()).get(AsyncSqlCursor.class);
            }
        } catch (IgniteInternalCheckedException e) {
            StringWriter sw = getWriterWithStackTrace(e);

            return CompletableFuture.completedFuture(new JdbcQueryCloseResult(Response.STATUS_FAILED,
                    "Failed to find query cursor [curId=" + req.cursorId() + "]. Error message:" + sw));
        }

        return asyncSqlCursor.closeAsync().handle((none, t) -> {
            if (t != null) {
                StringWriter sw = getWriterWithStackTrace(t);

                return new JdbcQueryCloseResult(Response.STATUS_FAILED,
                        "Failed to close SQL query cursor [curId=" + req.cursorId() + "]. Error message: " + sw);
            }

            return new JdbcQueryCloseResult();
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<JdbcMetaColumnsResult> queryMetadataAsync(JdbcQueryMetadataRequest req) {
        AsyncSqlCursor<List<Object>> asyncSqlCursor;
        try {
            asyncSqlCursor = resources.get(req.cursorId()).get(AsyncSqlCursor.class);
        } catch (IgniteInternalCheckedException e) {
            StringWriter sw = getWriterWithStackTrace(e);

            return CompletableFuture.completedFuture(new JdbcMetaColumnsResult(Response.STATUS_FAILED,
                    "Failed to find query cursor [curId=" + req.cursorId() + "]. Error message:" + sw));
        }

        ResultSetMetadata metadata = asyncSqlCursor.metadata();

        if (metadata == null) {
            return CompletableFuture.completedFuture(new JdbcMetaColumnsResult(Response.STATUS_FAILED,
                    "Failed to get query metadata for cursor [curId=" + req.cursorId() + ']'));
        }

        List<JdbcColumnMeta> meta = metadata.columns().stream()
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
    private JdbcColumnMeta createColumnMetadata(ColumnMetadata fldMeta) {
        ColumnMetadata.ColumnOrigin origin = fldMeta.origin();

        String schemaName = null;
        String tblName = null;
        String colName = null;

        if (origin != null) {
            schemaName = origin.schemaName();
            tblName = origin.tableName();
            colName = origin.columnName();
        }

        return new JdbcColumnMeta(
            fldMeta.name(),
            schemaName,
            tblName,
            colName,
            JdbcConverterUtils.columnTypeToJdbcClass(fldMeta.type()),
            fldMeta.precision(),
            fldMeta.scale(),
            fldMeta.nullable()
        );
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
}
