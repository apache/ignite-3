/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.client.handler.requests.jdbc;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.handler.ClientResourceRegistry;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.jdbc.proto.event.JdbcRequestStatus;
import org.apache.ignite.internal.jdbc.proto.event.QueryFetchResult;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.lang.IgniteInternalCheckedException;

/**
 * Client jdbc request handler.
 */
public class ClientJdbcFetchRequest {
    /**
     * Processes remote fetch next batch request.
     *
     * @param in      Client message unpacker.
     * @param out     Client message packer.
     * @return Operation future.
     */
    public static CompletableFuture<Object> process(
            ClientMessageUnpacker in,
            ClientMessagePacker out,
            ClientResourceRegistry resources
    ) throws IgniteInternalCheckedException {
        long cursorId = in.unpackLong();
        int fetchSize = in.unpackInt();

        if (fetchSize <= 0) {
            out.packByte(JdbcRequestStatus.FAILED.getStatus());
            out.packString("Invalid fetch size : [fetchSize=" + fetchSize + ']');
            //TODO:IGNITE-15247 A proper JDBC error code should be sent.

            return null;
        }

        AsyncSqlCursor<List<Object>> cur = resources.get(cursorId).get(AsyncSqlCursor.class);

        return cur.requestNextAsync(fetchSize).handle((batch, ex) -> {
            if (ex != null) {
                out.packByte(JdbcRequestStatus.FAILED.getStatus());
                out.packString("Failed to fetch results for cursor id " + cursorId + ", " + ex.getMessage());
                //TODO:IGNITE-15247 A proper JDBC error code should be sent.

                return null;
            }

            out.packByte(JdbcRequestStatus.SUCCESS.getStatus());
            new QueryFetchResult(batch.items(), !batch.hasMore()).writeBinary(out);

            return null;
        }).toCompletableFuture();
    }
}
