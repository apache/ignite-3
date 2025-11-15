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

/**
 * Client jdbc cancel request handler.
 */
public class ClientJdbcCancelRequest {
    /**
     * Processes a remote JDBC request to cancel the current execution.
     *
     * @param in Client message unpacker.
     * @param handler Query event handler.
     * @return Operation future.
     */
    public static CompletableFuture<ResponseWriter> execute(
            ClientMessageUnpacker in,
            JdbcQueryEventHandlerImpl handler
    ) {
        long connectionId = in.unpackLong();
        long correlationToken = in.unpackLong();

        return handler.cancelAsync(connectionId, correlationToken).thenApply(res -> res::writeBinary);
    }
}
