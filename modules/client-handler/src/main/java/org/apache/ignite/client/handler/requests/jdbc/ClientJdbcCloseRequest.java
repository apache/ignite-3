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

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.handler.ClientResourceRegistry;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.jdbc.proto.event.JdbcRequestStatus;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.lang.IgniteInternalCheckedException;

/**
 * Client jdbc close request handler.
 */
public class ClientJdbcCloseRequest {
    /**
     * Processes remote {@code JdbcQueryCloseRequest}.
     *
     * @param in      Client message unpacker.
     * @param out     Client message packer.
     * @return Operation future.
     */
    public static CompletableFuture<Void> process(
            ClientMessageUnpacker in,
            ClientMessagePacker out,
            ClientResourceRegistry resources) throws IgniteInternalCheckedException {
        long cursorId = in.unpackLong();

        return resources.remove(cursorId).get(AsyncSqlCursor.class).closeAsync()
                .handle((res, ex) -> {
                    if (ex != null) {
                        out.packByte(JdbcRequestStatus.FAILED.getStatus());
                        out.packString("Failed to close SQL query [curId=" + cursorId + "]");
                        //TODO:IGNITE-15247 A proper JDBC error code should be sent.
                    }

                    out.packByte(JdbcRequestStatus.SUCCESS.getStatus());

                    return null;
                });
    }
}
