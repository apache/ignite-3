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
import org.apache.ignite.client.handler.ClientResourceRegistry;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.tx.impl.IgniteTransactionsImpl;

/**
 * Client SQL cursor close request.
 */
public class ClientSqlCursorCloseRequest {
    /**
     * Processes the request.
     *
     * @param in        Unpacker.
     * @param resources Resources.
     */
    public static CompletableFuture<Void> process(
            ClientMessageUnpacker in,
            ClientMessagePacker out,
            ClientResourceRegistry resources,
            IgniteTransactionsImpl transactions
    ) throws IgniteInternalCheckedException {
        long resourceId = in.unpackLong();

        ClientSqlResultSet asyncResultSet = resources.remove(resourceId).get(ClientSqlResultSet.class);

        return asyncResultSet.closeAsync()
                .thenApply(ignored -> {
                    out.meta(transactions.observableTimestamp());

                    return null;
                });
    }
}
