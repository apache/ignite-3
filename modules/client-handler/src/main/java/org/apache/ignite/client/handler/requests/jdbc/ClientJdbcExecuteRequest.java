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

package org.apache.ignite.client.handler.requests.jdbc;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.handler.JdbcQueryEventHandlerImpl;
import org.apache.ignite.client.handler.ResponseWriter;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQueryExecuteRequest;

/**
 * Client jdbc request handler.
 */
public class ClientJdbcExecuteRequest {
    /**
     * Processes remote {@code JdbcQueryExecuteRequest}.
     *
     * @param in      Client message unpacker.
     * @param handler Query event handler.
     * @return Operation future.
     */
    public static CompletableFuture<ResponseWriter> execute(
            ClientMessageUnpacker in,
            JdbcQueryEventHandlerImpl handler,
            HybridTimestampTracker tsTracker
    ) {
        var req = new JdbcQueryExecuteRequest();
        long connectionId = in.unpackLong();

        req.readBinary(in);

        // Passing the tracker only to the server-side handler.
        tsTracker.update(req.observableTime());
        req.timestampTracker(tsTracker);

        return handler.queryAsync(connectionId, req).thenApply(res -> res::writeBinary);
    }
}
