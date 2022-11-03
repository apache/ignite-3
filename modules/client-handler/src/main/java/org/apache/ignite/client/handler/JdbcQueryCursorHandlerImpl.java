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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.internal.jdbc.proto.JdbcQueryCursorHandler;
import org.apache.ignite.internal.jdbc.proto.event.JdbcColumnMeta;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaColumnsResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQueryCloseRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQueryCloseResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQueryFetchRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQueryFetchResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQueryMetadataRequest;
import org.apache.ignite.internal.jdbc.proto.event.Response;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlColumnType;

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
    public JdbcQueryCursorHandlerImpl(ClientResourceRegistry resources) {
        this.resources = resources;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<JdbcQueryFetchResult> fetchAsync(JdbcQueryFetchRequest req) {
        AsyncSqlCursor<List<Object>> asyncSqlCursor = null;
        try {
            asyncSqlCursor = resources.get(req.cursorId()).get(AsyncSqlCursor.class);
        } catch (IgniteInternalCheckedException e) {
            StringWriter sw = getWriterWithStackTrace(e);

            return CompletableFuture.completedFuture(new JdbcQueryFetchResult(Response.STATUS_FAILED,
                    "Failed to find query cursor [curId=" + req.cursorId() + "]. Error message:" + sw));
        }

        if (req.pageSize() <= 0) {
            return CompletableFuture.completedFuture(new JdbcQueryFetchResult(Response.STATUS_FAILED,
                    "Invalid fetch size [fetchSize=" + req.pageSize() + ']'));
        }

        return asyncSqlCursor.requestNextAsync(req.pageSize()).handle((batch, t) -> {
            if (t != null) {
                StringWriter sw = getWriterWithStackTrace(t);

                return new JdbcQueryFetchResult(Response.STATUS_FAILED,
                    "Failed to fetch query results [curId=" + req.cursorId() + "]. Error message:" + sw);
            }

            return new JdbcQueryFetchResult(batch.items(), !batch.hasMore());
        }).toCompletableFuture();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<JdbcQueryCloseResult> closeAsync(JdbcQueryCloseRequest req) {
        AsyncSqlCursor<List<Object>> asyncSqlCursor = null;
        try {
            asyncSqlCursor = resources.remove(req.cursorId()).get(AsyncSqlCursor.class);
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
        AsyncSqlCursor<List<Object>> asyncSqlCursor = null;
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
            SqlColumnType.columnTypeToClass(fldMeta.type()),
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
