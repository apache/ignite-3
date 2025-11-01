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

package org.apache.ignite.client.handler.requests.sql;

import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;
import org.apache.ignite.client.handler.ClientHandlerMetricSource;
import org.apache.ignite.client.handler.ClientResource;
import org.apache.ignite.client.handler.ClientResourceRegistry;
import org.apache.ignite.client.handler.ResponseWriter;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.sql.QueryModifier;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.sql.api.AsyncResultSetImpl;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.prepare.partitionawareness.PartitionAwarenessMetadata;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ColumnMetadata.ColumnOrigin;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.jetbrains.annotations.Nullable;

/**
 * Common SQL request handling logic.
 */
class ClientSqlCommon {
    static void packCurrentPage(ClientMessagePacker out, AsyncResultSet<SqlRow> asyncResultSet) {
        ResultSetMetadata meta = asyncResultSet.metadata();
        assert meta != null : "Metadata can't be null when row set is present.";

        List<ColumnMetadata> cols = meta.columns();

        out.packInt(asyncResultSet.currentPageSize());

        for (SqlRow row : asyncResultSet.currentPage()) {
            // TODO IGNITE-18922 Avoid conversion, copy BinaryTuple from SQL to client.
            var builder = new BinaryTupleBuilder(row.columnCount());

            for (int i = 0; i < cols.size(); i++) {
                packValue(builder, cols.get(i), row, i);
            }

            out.packBinaryTuple(builder);
        }

        if (!asyncResultSet.hasMorePages()) {
            // Close in background.
            asyncResultSet.closeAsync();
        }
    }

    private static void packValue(BinaryTupleBuilder out, ColumnMetadata col, SqlRow row, int idx) {
        if (row.value(idx) == null) {
            out.appendNull();
            return;
        }

        switch (col.type()) {
            case BOOLEAN:
                out.appendByte((Boolean) row.value(idx) ? (byte) 1 : (byte) 0);
                break;

            case INT8:
                out.appendByte(row.byteValue(idx));
                break;

            case INT16:
                out.appendShort(row.shortValue(idx));
                break;

            case INT32:
                out.appendInt(row.intValue(idx));
                break;

            case INT64:
                out.appendLong(row.longValue(idx));
                break;

            case FLOAT:
                out.appendFloat(row.floatValue(idx));
                break;

            case DOUBLE:
                out.appendDouble(row.doubleValue(idx));
                break;

            case DECIMAL:
                out.appendDecimal(row.value(idx), col.scale());
                break;

            case DATE:
                out.appendDate(row.dateValue(idx));
                break;

            case TIME:
                out.appendTime(row.timeValue(idx));
                break;

            case DATETIME:
                out.appendDateTime(row.datetimeValue(idx));
                break;

            case TIMESTAMP:
                out.appendTimestamp(row.timestampValue(idx));
                break;

            case UUID:
                out.appendUuid(row.uuidValue(idx));
                break;

            case STRING:
                out.appendString(row.stringValue(idx));
                break;

            case BYTE_ARRAY:
                out.appendBytes(row.value(idx));
                break;

            case PERIOD:
                out.appendPeriod(row.value(idx));
                break;

            case DURATION:
                out.appendDuration(row.value(idx));
                break;
            default:
                throw new UnsupportedOperationException("Unsupported column type: " + col.type());
        }
    }

    /**
     * Pack columns metadata.
     *
     * @param out Message packer.
     * @param cols Columns.
     */
    static void packColumns(ClientMessagePacker out, List<ColumnMetadata> cols) {
        out.packInt(cols.size());

        // In many cases there are multiple columns from the same table.
        // Schema is the same for all columns in most cases.
        // When table or schema name was packed before, pack index instead of string.
        Map<String, Integer> schemas = new HashMap<>();
        Map<String, Integer> tables = new HashMap<>();

        for (int i = 0; i < cols.size(); i++) {
            ColumnMetadata col = cols.get(i);
            ColumnOrigin origin = col.origin();

            int fieldsNum = origin == null ? 6 : 9;
            out.packInt(fieldsNum);

            out.packString(col.name());
            out.packBoolean(col.nullable());
            out.packInt(col.type().id());
            out.packInt(col.scale());
            out.packInt(col.precision());

            if (origin == null) {
                out.packBoolean(false);
                continue;
            }

            out.packBoolean(true);

            if (col.name().equals(origin.columnName())) {
                out.packNil();
            } else {
                out.packString(origin.columnName());
            }

            Integer schemaIdx = schemas.get(origin.schemaName());

            if (schemaIdx == null) {
                schemas.put(origin.schemaName(), i);
                out.packString(origin.schemaName());
            } else {
                out.packInt(schemaIdx);
            }

            Integer tableIdx = tables.get(origin.tableName());

            if (tableIdx == null) {
                tables.put(origin.tableName(), i);
                out.packString(origin.tableName());
            } else {
                out.packInt(tableIdx);
            }
        }
    }

    static Set<SqlQueryType> convertQueryModifierToQueryType(Collection<QueryModifier> queryModifiers) {
        EnumSet<SqlQueryType> queryTypes = EnumSet.noneOf(SqlQueryType.class);

        for (QueryModifier queryModifier : queryModifiers) {
            switch (queryModifier) {
                case ALLOW_ROW_SET_RESULT:
                    queryTypes.addAll(SqlQueryType.HAS_ROW_SET_TYPES);
                    break;

                case ALLOW_AFFECTED_ROWS_RESULT:
                    queryTypes.addAll(SqlQueryType.RETURNS_AFFECTED_ROWS_TYPES);
                    break;

                case ALLOW_APPLIED_RESULT:
                    queryTypes.addAll(SqlQueryType.SUPPORT_WAS_APPLIED_TYPES);
                    break;

                case ALLOW_TX_CONTROL:
                    queryTypes.add(SqlQueryType.TX_CONTROL);
                    break;

                case ALLOW_MULTISTATEMENT:
                    break;

                default:
                    throw new IllegalArgumentException("Unexpected modifier " + queryModifier);
            }
        }

        return queryTypes;
    }

    static CompletableFuture<ResponseWriter> writeResultSetAsync(
            ClientResourceRegistry resources,
            AsyncResultSetImpl asyncResultSet,
            ClientHandlerMetricSource metrics,
            int pageSize,
            boolean includePartitionAwarenessMeta,
            boolean sqlDirectTxMappingSupported,
            boolean sqlMultiStatementSupported,
            Executor executor
    ) {
        try {
            Long nextResultResourceId = sqlMultiStatementSupported && asyncResultSet.cursor().hasNextResult()
                    ? saveNextResultResource(asyncResultSet.cursor().nextResult(), pageSize, resources, executor)
                    : null;

            if ((asyncResultSet.hasRowSet() && asyncResultSet.hasMorePages())) {
                metrics.cursorsActiveIncrement();

                var clientResultSet = new ClientSqlResultSet(asyncResultSet, metrics);

                ClientResource resource = new ClientResource(
                        clientResultSet,
                        clientResultSet::closeAsync);

                var resourceId = resources.put(resource);

                return CompletableFuture.completedFuture(out ->
                        writeResultSet(out, asyncResultSet, resourceId, includePartitionAwarenessMeta,
                                sqlDirectTxMappingSupported, sqlMultiStatementSupported, nextResultResourceId));
            }

            return asyncResultSet.closeAsync()
                    .thenApply(v -> (ResponseWriter) out ->
                            writeResultSet(out, asyncResultSet, null, includePartitionAwarenessMeta,
                                    sqlDirectTxMappingSupported, sqlMultiStatementSupported, nextResultResourceId));

        } catch (IgniteInternalCheckedException e) {
            // Resource registry was closed.
            return asyncResultSet
                    .closeAsync()
                    .thenRun(() -> {
                        throw new IgniteInternalException(e.getMessage(), e);
                    });
        }
    }

    private static Long saveNextResultResource(
            CompletableFuture<AsyncSqlCursor<InternalSqlRow>> nextResultFuture,
            int pageSize,
            ClientResourceRegistry resources,
            Executor executor
    ) throws IgniteInternalCheckedException {
        ClientResource resource = new ClientResource(
                new CursorWithPageSize(nextResultFuture, pageSize),
                () -> nextResultFuture.thenAccept(cur -> iterateThroughResultsAndCloseThem(cur, executor))
        );

        return resources.put(resource);
    }

    private static void iterateThroughResultsAndCloseThem(AsyncSqlCursor<InternalSqlRow> cursor, Executor executor) {
        Function<AsyncSqlCursor<InternalSqlRow>, CompletableFuture<AsyncSqlCursor<InternalSqlRow>>> traverser = new Function<>() {
            @Override
            public CompletableFuture<AsyncSqlCursor<InternalSqlRow>> apply(AsyncSqlCursor<InternalSqlRow> cur) {
                return cur.closeAsync()
                        .thenComposeAsync(none -> {
                            if (cur.hasNextResult()) {
                                return cur.nextResult().thenComposeAsync(this, executor);
                            } else {
                                return CompletableFuture.completedFuture(cur);
                            }
                        }, executor);
            }
        };

        CompletableFuture.completedFuture(cursor).thenCompose(traverser);
    }

    private static void writeResultSet(
            ClientMessagePacker out,
            AsyncResultSetImpl res,
            @Nullable Long resourceId,
            boolean includePartitionAwarenessMeta,
            boolean sqlDirectTxMappingSupported,
            boolean sqlMultiStatementsSupported,
            @Nullable Long nextResultResourceId
    ) {
        out.packLongNullable(resourceId);

        out.packBoolean(res.hasRowSet());
        out.packBoolean(res.hasMorePages());
        out.packBoolean(res.wasApplied());
        out.packLong(res.affectedRows());

        packMeta(out, res.metadata());

        if (includePartitionAwarenessMeta) {
            packPartitionAwarenessMeta(out, res.partitionAwarenessMetadata(), sqlDirectTxMappingSupported);
        }

        if (sqlMultiStatementsSupported) {
            out.packLongNullable(nextResultResourceId);
        }

        if (res.hasRowSet()) {
            packCurrentPage(out, res);
        }
    }

    private static void packMeta(ClientMessagePacker out, @Nullable ResultSetMetadata meta) {
        // TODO IGNITE-17179 metadata caching - avoid sending same meta over and over.
        if (meta == null || meta.columns() == null) {
            out.packInt(0);
            return;
        }

        packColumns(out, meta.columns());
    }

    private static void packPartitionAwarenessMeta(
            ClientMessagePacker out,
            @Nullable PartitionAwarenessMetadata meta,
            boolean sqlDirectTxMappingSupported
    ) {
        if (meta == null) {
            out.packNil();
            return;
        }

        out.packInt(meta.tableId());
        out.packIntArray(meta.indexes());
        out.packIntArray(meta.hash());

        if (sqlDirectTxMappingSupported) {
            out.packByte(meta.directTxMode().id);
        }
    }

    /** Holder of the cursor future and page size. */
    static class CursorWithPageSize {
        private final CompletableFuture<AsyncSqlCursor<InternalSqlRow>> cursorFuture;
        private final int pageSize;

        CursorWithPageSize(CompletableFuture<AsyncSqlCursor<InternalSqlRow>> cursorFuture, int pageSize) {
            this.cursorFuture = cursorFuture;
            this.pageSize = pageSize;
        }

        CompletableFuture<AsyncSqlCursor<InternalSqlRow>> cursorFuture() {
            return cursorFuture;
        }

        int pageSize() {
            return pageSize;
        }
    }
}
