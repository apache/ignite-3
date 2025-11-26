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

import static org.apache.ignite.client.handler.requests.table.ClientTableCommon.readTx;
import static org.apache.ignite.client.handler.requests.table.ClientTableCommon.writeTxMeta;
import static org.apache.ignite.internal.lang.SqlExceptionMapperUtil.mapToPublicSqlException;
import static org.apache.ignite.internal.util.CompletableFutures.allOf;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.function.Function;
import org.apache.ignite.client.handler.ClientHandlerMetricSource;
import org.apache.ignite.client.handler.ClientResourceRegistry;
import org.apache.ignite.client.handler.NotificationSender;
import org.apache.ignite.client.handler.ResponseWriter;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.sql.api.AsyncResultSetImpl;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.SqlProperties;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.CancelHandle;
import org.apache.ignite.lang.CancellationToken;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.IgniteTables;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Client SQL execute request.
 */
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
            IgniteTables tables,
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
        InternalTransaction tx = readTx(in, timestampTracker, resources, txManager, tables, notificationSender, resIdHolder);
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
                        ClientSqlCommon.writeResultSetAsync(resources, asyncResultSet, metrics, props.pageSize(),
                                includePartitionAwarenessMeta, sqlDirectTxMappingSupported, sqlMultistatementsSupported, operationExecutor))
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
                                doWhenAllCursorsComplete(cur, onComplete);

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

    private static void doWhenAllCursorsComplete(AsyncSqlCursor<InternalSqlRow> cursor, Runnable action) {
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
}
