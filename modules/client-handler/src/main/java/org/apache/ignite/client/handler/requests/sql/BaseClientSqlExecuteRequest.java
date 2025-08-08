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

import static org.apache.ignite.client.handler.requests.sql.ClientSqlCommon.packCurrentPage;
import static org.apache.ignite.internal.util.CompletableFutures.allOf;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.client.handler.ClientHandlerMetricSource;
import org.apache.ignite.client.handler.ClientResource;
import org.apache.ignite.client.handler.ClientResourceRegistry;
import org.apache.ignite.client.handler.ResponseWriter;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.sql.api.AsyncResultSetImpl;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.prepare.partitionawareness.PartitionAwarenessMetadata;
import org.apache.ignite.sql.ResultSetMetadata;
import org.jetbrains.annotations.Nullable;

/**
 * TODO Base client sql execute request.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
abstract class BaseClientSqlExecuteRequest {
    static CompletableFuture<ResponseWriter> writeResultSetAsync(
            ClientResourceRegistry resources,
            AsyncResultSetImpl asyncResultSet,
            ClientHandlerMetricSource metrics,
            boolean includePartitionAwarenessMeta,
            boolean sqlDirectTxMappingSupported,
            boolean sqlMultiStatementSupported
    ) {
        if ((asyncResultSet.hasRowSet() && asyncResultSet.hasMorePages()) || asyncResultSet.cursor().hasNextResult()) {
            try {
                metrics.cursorsActiveIncrement();

                var clientResultSet = new ClientSqlResultSet(asyncResultSet, asyncResultSet.cursor(), asyncResultSet.pageSize(), metrics);

                ClientResource resource = new ClientResource(
                        clientResultSet,
                        clientResultSet::closeAsync);

                var resourceId = resources.put(resource);

                return CompletableFuture.completedFuture(out ->
                        writeResultSet(out, asyncResultSet, resourceId, includePartitionAwarenessMeta, sqlDirectTxMappingSupported,
                                sqlMultiStatementSupported));
            } catch (IgniteInternalCheckedException e) {
                return asyncResultSet
                        .closeAsync()
                        .thenRun(() -> {
                            throw new IgniteInternalException(e.getMessage(), e);
                        });
            }
        }

        return asyncResultSet.closeAsync()
                .thenApply(v -> (ResponseWriter) out ->
                        writeResultSet(out, asyncResultSet, null, includePartitionAwarenessMeta, sqlDirectTxMappingSupported,
                                sqlMultiStatementSupported));
    }

    static void doWhenAllCursorsComplete(AsyncSqlCursor<InternalSqlRow> cursor, Runnable action) {
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

        cursorChainTraverser
                .apply(cursor)
                .exceptionally(ex -> {
                    action.run();

                    return null;
                });
    }

    private static void writeResultSet(
            ClientMessagePacker out,
            AsyncResultSetImpl res,
            @Nullable Long resourceId,
            boolean includePartitionAwarenessMeta,
            boolean sqlDirectTxMappingSupported,
            boolean sqlMultiStatementsSupported
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
            out.packBoolean(res.cursor().hasNextResult());
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

        ClientSqlCommon.packColumns(out, meta.columns());
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
}
