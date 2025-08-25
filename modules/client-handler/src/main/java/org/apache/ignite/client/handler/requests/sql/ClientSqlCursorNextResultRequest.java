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
import org.apache.ignite.client.handler.requests.sql.ClientSqlCommon.CursorWithPageSize;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.sql.api.AsyncResultSetImpl;
import org.apache.ignite.sql.SqlRow;

/**
 * Client SQL cursor next result.
 */
public class ClientSqlCursorNextResultRequest {
    /**
     * Processes the request.
     *
     * @param in Unpacker.
     * @return Future representing result of operation.
     */
    public static CompletableFuture<ResponseWriter> process(
            ClientMessageUnpacker in,
            ClientResourceRegistry resources,
            Executor operationExecutor,
            ClientHandlerMetricSource metrics
    ) throws IgniteInternalCheckedException {
        long resourceId = in.unpackLong();
        ClientResource resource = resources.remove(resourceId);
        CursorWithPageSize cursorWithPageSize = resource.get(CursorWithPageSize.class);
        int pageSize = cursorWithPageSize.pageSize();

        return cursorWithPageSize.cursorFuture()
                .thenComposeAsync(cur -> cur.requestNextAsync(pageSize)
                        .thenApply(batchRes -> new AsyncResultSetImpl<SqlRow>(
                                        cur,
                                        batchRes,
                                        pageSize
                                )
                        ).thenCompose(asyncResultSet ->
                                ClientSqlCommon.writeResultSetAsync(resources, asyncResultSet, metrics, pageSize, false, false, true)
                        ).thenApply(rsWriter -> rsWriter), operationExecutor);
    }
}
