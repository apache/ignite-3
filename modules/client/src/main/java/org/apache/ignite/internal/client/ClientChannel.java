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

package org.apache.ignite.internal.client;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.jetbrains.annotations.Nullable;

/**
 * Processing thin client requests and responses.
 */
public interface ClientChannel extends AutoCloseable {
    /**
     * Send request and handle response asynchronously for client operation.
     *
     * @param opCode              Operation code.
     * @param payloadWriter       Payload writer to stream or {@code null} if request has no payload.
     * @param payloadReader       Payload reader from stream or {@code null} if response has no payload.
     * @param expectNotifications Whether to expect notifications as a result of the operation.
     * @param <T>                 Response type.
     * @return Future for the operation.
     */
    <T> CompletableFuture<T> serviceAsync(
            int opCode,
            @Nullable PayloadWriter payloadWriter,
            @Nullable PayloadReader<T> payloadReader,
            boolean expectNotifications
    );

    /**
     * Send request and handle response asynchronously for client operation.
     *
     * @param opCode              Operation code.
     * @param payloadWriter       Payload writer to stream or {@code null} if request has no payload.
     * @param payloadReader       Payload reader from stream or {@code null} if response has no payload.
     * @param <T>                 Response type.
     * @return Future for the operation.
     */
    default <T> CompletableFuture<T> serviceAsync(
            int opCode,
            @Nullable PayloadWriter payloadWriter,
            @Nullable PayloadReader<T> payloadReader
    ) {
        return serviceAsync(opCode, payloadWriter, payloadReader, false);
    }

    /**
     * Returns {@code true} channel is closed.
     *
     * @return {@code True} channel is closed.
     */
    boolean closed();

    /**
     * Returns protocol context.
     *
     * @return Protocol context.
     */
    ProtocolContext protocolContext();

    /**
     * Returns endpoint.
     *
     * @return Endpoint.
     */
    String endpoint();

    /**
     * Send heartbeat request.
     *
     * @param payloadWriter Payload writer or {@code null} for no payload.
     *     Heartbeat request payload is ignored by the server, but can be used for benchmarking.
     * @return Future for the operation.
     */
    default CompletableFuture<Void> heartbeatAsync(@Nullable PayloadWriter payloadWriter) {
        return serviceAsync(ClientOp.HEARTBEAT, payloadWriter, null, false);
    }
}
