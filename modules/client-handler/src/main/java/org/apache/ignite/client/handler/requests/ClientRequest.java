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

package org.apache.ignite.client.handler.requests;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import org.apache.ignite.client.proto.ClientErrorCode;
import org.apache.ignite.client.proto.ClientMessagePacker;
import org.apache.ignite.client.proto.ClientMessageUnpacker;
import org.apache.ignite.client.proto.ServerMessageType;

/**
 * Basic thin client request.
 */
public class ClientRequest {
    /** Request id. */
    private final long reqId;

    /**
     * Constructor.
     *
     * @param in Message unpacker. Will be closed on constructor exit, all data must be read by that time.
     */
    public ClientRequest(ClientMessageUnpacker in) throws IOException {
        reqId = in.unpackLong();
    }

    /**
     * Gets the request id.
     *
     * @return Request id.
     */
    public long requestId() {
        return reqId;
    }

    /**
     * Processes the request and writes the response to the provided packer.
     *
     * @return Future for this operation, or null when completed synchronously.
     */
    public CompletableFuture<Object> process(ClientMessagePacker packer) throws IOException {
        packer.packInt(ServerMessageType.RESPONSE)
                .packLong(reqId)
                .packInt(ClientErrorCode.SUCCESS);

        return null;
    }
}
