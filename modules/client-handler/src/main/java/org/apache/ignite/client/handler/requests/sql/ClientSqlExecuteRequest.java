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
import static org.apache.ignite.client.handler.requests.table.ClientTableCommon.writeTxMeta;
import static org.apache.ignite.internal.lang.SqlExceptionMapperUtil.mapToPublicSqlException;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import org.apache.ignite.client.handler.ClientHandlerMetricSource;
import org.apache.ignite.client.handler.ClientResource;
import org.apache.ignite.client.handler.ClientResourceRegistry;
import org.apache.ignite.client.handler.NotificationSender;
import org.apache.ignite.client.handler.ResponseWriter;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.sql.api.AsyncResultSetImpl;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.SqlProperties;
import org.apache.ignite.internal.sql.engine.prepare.partitionawareness.PartitionAwarenessMetadata;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.CancelHandle;
import org.apache.ignite.lang.CancellationToken;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlRow;
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
     * @param requestId Id of the request.
     * @param cancelHandles Registry of handlers. Request must register itself in this registry before switching to another thread.
     * @param sql SQL API.
     * @param resources Resources.
     * @param metrics Metrics.
     * @param timestampTracker Server's view of latest seen by client time.
     * @param sqlPartitionAwarenessSupported Denotes whether client supports partition awareness for SQL or not.
     * @param sqlDirectTxMappingSupported Denotes whether client supports direct mapping of implicit transaction for SQL operations
     *         for SQL or not.
     * @param txManager Tx manager is used to start explicit transaction in case of transaction piggybacking, or to start remote
     *         transaction in case of direct mapping.
     * @param clockService Clock service is required to update observable time after execution of operation within a remote
     *         transaction.
     * @param notificationSender Notification sender is required to send acknowledge for underlying write operation within a remote
     *         transaction.
     * @param username Authenticated user name or {@code null} for unknown user.
     * @return Future representing result of operation.
     */
    public static CompletableFuture<ResponseWriter> process(
            Executor operationExecutor,
            ClientMessageUnpacker in,
            long requestId,
            Map<Long, CancelHandle> cancelHandles,
            QueryProcessor sql,
            ClientResourceRegistry resources,
            ClientHandlerMetricSource metrics,
            HybridTimestampTracker timestampTracker,
            boolean sqlPartitionAwarenessSupported,
            boolean sqlDirectTxMappingSupported,
            TxManager txManager,
            ClockService clockService,
            NotificationSender notificationSender,
            @Nullable String username,
            boolean sqlMultistatementsSupported
    ) {
        CancelHandle cancelHandle = CancelHandle.create();
        cancelHandles.put(requestId, cancelHandle);

        if (sqlDirectTxMappingSupported && !in.unpackBoolean()) {
            notificationSender = null;
        }

        long[] resIdHolder = {0};
        InternalTransaction tx = readTx(in, timestampTracker, resources, txManager, notificationSender, resIdHolder);
        ClientSqlProperties props = new ClientSqlProperties(in, sqlMultistatementsSupported);
        String statement = in.unpackString();
        Object[] arguments = readArgsNotNull(in);

        HybridTimestamp clientTs = HybridTimestamp.nullableHybridTimestamp(in.unpackLong());
        timestampTracker.update(clientTs);

        boolean includePartitionAwarenessMeta = sqlPartitionAwarenessSupported && in.unpackBoolean();

        return nullCompletedFuture().thenComposeAsync(none -> executeAsync(
                tx,
                sql,
                timestampTracker,
                statement,
                cancelHandle.token(),
                props.pageSize(),
                props.toSqlProps().userName(username),
                () -> cancelHandles.remove(requestId),
                arguments
        ).thenCompose(asyncResultSet ->
                        writeResultSetAsync(resources, asyncResultSet, metrics, includePartitionAwarenessMeta, sqlDirectTxMappingSupported))
                .thenApply(rsWriter -> out -> {
                    if (tx != null) {
                        writeTxMeta(out, timestampTracker, clockService, tx, resIdHolder[0]);
                    }

                    // write the rest of response
                    rsWriter.write(out);
                }), operationExecutor);
    }

    static Object[] readArgsNotNull(ClientMessageUnpacker in) {
        Object[] arguments = in.unpackObjectArrayFromBinaryTuple();

        // SQL engine requires non-null arguments, but we don't want to complicate the protocol with this requirement.
        return arguments == null ? ArrayUtils.OBJECT_EMPTY_ARRAY : arguments;
    }

    private static CompletableFuture<ResponseWriter> writeResultSetAsync(
            ClientResourceRegistry resources,
            AsyncResultSetImpl asyncResultSet,
            ClientHandlerMetricSource metrics,
            boolean includePartitionAwarenessMeta,
            boolean sqlDirectTxMappingSupported
    ) {
        if (asyncResultSet.hasRowSet() && asyncResultSet.hasMorePages()) {
            try {
                metrics.cursorsActiveIncrement();

                var clientResultSet = new ClientSqlResultSet(asyncResultSet, metrics);

                ClientResource resource = new ClientResource(
                        clientResultSet,
                        clientResultSet::closeAsync);

                var resourceId = resources.put(resource);

                return CompletableFuture.completedFuture(out ->
                        writeResultSet(out, asyncResultSet, resourceId, includePartitionAwarenessMeta, sqlDirectTxMappingSupported));
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
                        writeResultSet(out, asyncResultSet, null, includePartitionAwarenessMeta, sqlDirectTxMappingSupported));
    }

    private static void writeResultSet(
            ClientMessagePacker out,
            AsyncResultSetImpl res,
            @Nullable Long resourceId,
            boolean includePartitionAwarenessMeta,
            boolean sqlDirectTxMappingSupported
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

    private static CompletableFuture<AsyncResultSetImpl<SqlRow>> executeAsync(
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
            CompletableFuture<AsyncResultSetImpl<SqlRow>> fut = qryProc.queryAsync(
                        props,
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
