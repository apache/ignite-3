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
import org.apache.ignite.client.handler.ClientResource;
import org.apache.ignite.client.handler.ClientResourceRegistry;
import org.apache.ignite.client.handler.ResponseWriter;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.util.CompletableFutures;

/**
 * Client SQL cursor close request.
 */
public class ClientSqlCursorCloseRequest {
    /**
     * Processes the request.
     *
     * @param in Unpacker.
     * @param resources Resources.
     * @return Future representing result of operation.
     */
    public static CompletableFuture<ResponseWriter> process(
            ClientMessageUnpacker in,
            ClientResourceRegistry resources
    ) throws IgniteInternalCheckedException {
        long resourceId = in.unpackLong();
        ClientResource resource;

        try {
            resource = resources.remove(resourceId);
        } catch (IgniteInternalCheckedException | IgniteInternalException ignored) {
            // TODO https://issues.apache.org/jira/browse/IGNITE-26927 Do not completely ignore "resource not found" exception
            // Ignore: either resource was already removed during asynchronous data fetch request, or registry is closing.
            return CompletableFutures.nullCompletedFuture();
        }

        ClientSqlResultSet asyncResultSet = resource.get(ClientSqlResultSet.class);

        return asyncResultSet.closeAsync().thenApply(v -> null);
    }
}
