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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.client.handler.requests.sql.ClientSqlCommon.packCurrentPage;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.handler.ClientResourceRegistry;
import org.apache.ignite.client.handler.ResponseWriter;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.async.AsyncResultSet;

/**
 * Client SQL cursor next page request.
 */
public class ClientSqlCursorNextPageRequest {
    /**
     * Processes the request.
     *
     * @param in  Unpacker.
     * @return Future representing result of operation.
     */
    public static CompletableFuture<ResponseWriter> process(
            ClientMessageUnpacker in,
            ClientResourceRegistry resources
    ) throws IgniteInternalCheckedException {
        long resourceId = in.unpackLong();

        var resultSet = resources.get(resourceId).get(ClientSqlResultSet.class);

        return resultSet.resultSet().fetchNextPage()
                .thenCompose(r -> {
                    if (!r.hasMorePages()) {
                        try {
                            resources.remove(resourceId);
                        } catch (IgniteInternalCheckedException ignored) {
                            // Ignore: either resource already removed, or registry is closing.
                        }

                        return resultSet.closeAsync().thenApply(v -> getResponseWriter(r));
                    } else {
                        return completedFuture(getResponseWriter(r));
                    }
                })
                .toCompletableFuture();
    }

    private static ResponseWriter getResponseWriter(AsyncResultSet<SqlRow> r) {
        return out -> {
            packCurrentPage(out, r);
            out.packBoolean(r.hasMorePages());
        };
    }
}
