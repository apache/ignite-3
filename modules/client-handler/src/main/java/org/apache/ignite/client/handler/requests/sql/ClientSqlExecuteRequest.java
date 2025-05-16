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
import static org.apache.ignite.client.handler.requests.table.ClientTableCommon.readTx;
import static org.apache.ignite.internal.lang.SqlExceptionMapperUtil.mapToPublicSqlException;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import org.apache.ignite.client.handler.ClientHandlerMetricSource;
import org.apache.ignite.client.handler.ClientResource;
import org.apache.ignite.client.handler.ClientResourceRegistry;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.sql.api.AsyncResultSetImpl;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.SqlProperties;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.CancelHandle;
import org.apache.ignite.lang.CancellationToken;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Client SQL execute request.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class ClientSqlExecuteRequest {
    /**
     * Processes the request.
     *
     * @param operationExecutor Executor to submit execution of operation.
     * @param in Unpacker.
     * @param out Packer.
     * @param requestId Id of the request.
     * @param cancelHandles Registry of handlers. Request must register itself in this registry before switching to another thread.
     * @param sql SQL API.
     * @param resources Resources.
     * @param metrics Metrics.
     * @return Future representing result of operation.
     */
    public static CompletableFuture<Void> process(
            Executor operationExecutor,
            ClientMessageUnpacker in,
            ClientMessagePacker out,
            long requestId,
            Map<Long, CancelHandle> cancelHandles,
            QueryProcessor sql,
            ClientResourceRegistry resources,
            ClientHandlerMetricSource metrics
    ) {
        CancelHandle cancelHandle = CancelHandle.create();
        cancelHandles.put(requestId, cancelHandle);

        return nullCompletedFuture().thenComposeAsync(none -> {
            InternalTransaction tx = readTx(in, out, resources, null, null);
            ClientSqlProperties props = new ClientSqlProperties(in);
            String statement = in.unpackString();
            Object[] arguments = in.unpackObjectArrayFromBinaryTuple();

            if (arguments == null) {
                // SQL engine requires non-null arguments, but we don't want to complicate the protocol with this requirement.
                arguments = ArrayUtils.OBJECT_EMPTY_ARRAY;
            }

            HybridTimestamp clientTs = HybridTimestamp.nullableHybridTimestamp(in.unpackLong());

            HybridTimestampTracker tsUpdater = HybridTimestampTracker.atomicTracker(clientTs);

            return executeAsync(
                    tx,
                    sql,
                    tsUpdater,
                    statement,
                    cancelHandle.token(),
                    props.pageSize(),
                    props.toSqlProps(),
                    () -> cancelHandles.remove(requestId),
                    arguments
            ).thenCompose(asyncResultSet -> {
                out.meta(tsUpdater.get());

                return writeResultSetAsync(out, resources, asyncResultSet, metrics);
            });
        }, operationExecutor);
    }

    private static CompletionStage<Void> writeResultSetAsync(
            ClientMessagePacker out,
            ClientResourceRegistry resources,
            AsyncResultSet asyncResultSet,
            ClientHandlerMetricSource metrics) {
        boolean hasResource = asyncResultSet.hasRowSet() && asyncResultSet.hasMorePages();

        if (hasResource) {
            try {
                metrics.cursorsActiveIncrement();

                var clientResultSet = new ClientSqlResultSet(asyncResultSet, metrics);

                ClientResource resource = new ClientResource(
                        clientResultSet,
                        clientResultSet::closeAsync);

                out.packLong(resources.put(resource));
            } catch (IgniteInternalCheckedException e) {
                return asyncResultSet
                        .closeAsync()
                        .thenRun(() -> {
                            throw new IgniteInternalException(e.getMessage(), e);
                        });
            }
        } else {
            out.packNil(); // resourceId
        }

        out.packBoolean(asyncResultSet.hasRowSet());
        out.packBoolean(asyncResultSet.hasMorePages());
        out.packBoolean(asyncResultSet.wasApplied());
        out.packLong(asyncResultSet.affectedRows());

        packMeta(out, asyncResultSet.metadata());

        // Pack first page.
        if (asyncResultSet.hasRowSet()) {
            packCurrentPage(out, asyncResultSet);

            return hasResource
                    ? nullCompletedFuture()
                    : asyncResultSet.closeAsync();
        } else {
            return asyncResultSet.closeAsync();
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

    private static CompletableFuture<AsyncResultSet<SqlRow>> executeAsync(
            @Nullable Transaction transaction,
            QueryProcessor qryProc,
            HybridTimestampTracker timestampTracker,
            String query,
            CancellationToken token,
            int pageSize,
            SqlProperties props,
            Runnable onComplete,
            @Nullable Object... arguments
    ) {
        try {
            SqlProperties properties = new SqlProperties(props)
                    .allowedQueryTypes(SqlQueryType.SINGLE_STMT_TYPES);

            CompletableFuture<AsyncResultSet<SqlRow>> fut = qryProc.queryAsync(
                        properties,
                        timestampTracker,
                        (InternalTransaction) transaction,
                        token,
                        query,
                        arguments
                    )
                    .thenCompose(cur -> {
                                cur.onClose().whenComplete((none, ignore) -> onComplete.run());

                                return cur.requestNextAsync(pageSize)
                                        .thenApply(
                                                batchRes -> new AsyncResultSetImpl<>(
                                                        cur,
                                                        batchRes,
                                                        pageSize
                                                )
                                        );
                            }
                    );

            return fut.exceptionally((th) -> {
                onComplete.run();

                Throwable cause = ExceptionUtils.unwrapCause(th);

                throw new CompletionException(mapToPublicSqlException(cause));
            });
        } catch (Exception e) {
            return CompletableFuture.failedFuture(mapToPublicSqlException(e));
        }
    }
}
