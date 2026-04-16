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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.ignite.client.handler.ClientHandlerMetricSource;
import org.apache.ignite.client.handler.ClientResource;
import org.apache.ignite.client.handler.ClientResourceRegistry;
import org.apache.ignite.client.handler.ResponseWriter;
import org.apache.ignite.client.handler.requests.sql.ClientSqlCommon.NextCursorContext;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.sql.api.AsyncResultSetImpl;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.sql.SqlRow;

/**
 * Client SQL cursor next result.
 */
public class ClientSqlCursorNextResultRequest {
    /**
     * Processes the request.
     *
     * @param operationExecutor Operation executor.
     * @param in Unpacker.
     * @param resources Resource bundle.
     * @param metrics Client metrics.
     * @param requestTsTracker TS tracker attached to current request processing.
     * @return Future representing result of operation.
     */
    public static CompletableFuture<ResponseWriter> process(
            Executor operationExecutor,
            ClientMessageUnpacker in,
            ClientResourceRegistry resources,
            ClientHandlerMetricSource metrics,
            HybridTimestampTracker requestTsTracker
    ) throws IgniteInternalCheckedException {
        long resourceId = in.unpackLong();
        ClientResource resource = resources.remove(resourceId);
        NextCursorContext nextCursorContext = resource.get(NextCursorContext.class);
        HybridTimestampTracker parentTsTracker = nextCursorContext.parentTsTracker();
        int pageSize = nextCursorContext.pageSize();

        CompletableFuture<ResponseWriter> f = nextCursorContext.cursorFuture()
                .thenComposeAsync(cur -> cur.requestNextAsync(pageSize)
                        .thenApply(batchRes -> new AsyncResultSetImpl<SqlRow>(
                                        cur,
                                        batchRes,
                                        pageSize
                                )
                        ).thenCompose(asyncResultSet -> {
                            // For multi-statement DML operations, this will help us keep the client's timestamp tracker up to date and
                            // ensure client reads are consistent with the latest updates.
                            requestTsTracker.update(parentTsTracker.get());

                            return ClientSqlCommon.writeResultSetAsync(
                                    resources,
                                    asyncResultSet,
                                    metrics,
                                    parentTsTracker,
                                    pageSize,
                                    false,
                                    false,
                                    true,
                                    false,
                                    operationExecutor);
                        }).thenApply(rsWriter -> rsWriter), operationExecutor);

        f.whenCompleteAsync((r, t) -> {
            if (t != null) {
                nextCursorContext.cursorFuture().thenAccept(cur -> closeRemainingCursors(cur, false, operationExecutor));
            }
        }, operationExecutor);

        return f;
    }

    private static void closeRemainingCursors(AsyncSqlCursor<?> cursor, boolean closeCursor, Executor operationExecutor) {
        if (cursor.hasNextResult()) {
            cursor.nextResult().whenCompleteAsync((c, err) -> {
                if (c != null) {
                    cursor.closeAsync();
                    closeRemainingCursors(c, true, operationExecutor);
                }
            }, operationExecutor);
        } else if (closeCursor) {
            cursor.closeAsync();
        }
    }
}
