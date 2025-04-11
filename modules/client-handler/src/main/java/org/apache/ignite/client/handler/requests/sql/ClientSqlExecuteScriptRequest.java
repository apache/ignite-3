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

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.sql.api.IgniteSqlImpl;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.lang.CancelHandle;

/**
 * Client SQL execute script request.
 */
public class ClientSqlExecuteScriptRequest {
    /**
     * Processes the request.
     *
     * @param operationExecutor Executor to submit execution of operation.
     * @param in Unpacker.
     * @param out Packer.
     * @param sql SQL API.
     * @param requestId Id of the request.
     * @param cancelHandleMap Registry of handlers. Request must register itself in this registry before switching to another thread.
     * @return Future representing result of operation.
     */
    public static CompletableFuture<Void> process(
            Executor operationExecutor,
            ClientMessageUnpacker in,
            ClientMessagePacker out,
            QueryProcessor sql,
            long requestId,
            Map<Long, CancelHandle> cancelHandleMap
    ) {
        CancelHandle cancelHandle = CancelHandle.create();
        cancelHandleMap.put(requestId, cancelHandle);

        return nullCompletedFuture().thenComposeAsync(none -> {
            ClientSqlProperties props = new ClientSqlProperties(in);
            String script = in.unpackString();
            Object[] arguments = in.unpackObjectArrayFromBinaryTuple();

            if (arguments == null) {
                // SQL engine requires non-null arguments, but we don't want to complicate the protocol with this requirement.
                arguments = ArrayUtils.OBJECT_EMPTY_ARRAY;
            }

            HybridTimestamp clientTs = HybridTimestamp.nullableHybridTimestamp(in.unpackLong());
            HybridTimestampTracker tsUpdater = HybridTimestampTracker.atomicTracker(clientTs);

            return IgniteSqlImpl.executeScriptCore(
                    sql,
                    tsUpdater,
                    () -> true,
                    () -> {},
                    script,
                    cancelHandle.token(),
                    arguments,
                    props.toSqlProps(),
                    operationExecutor
            ).whenComplete((none2, error) -> {
                cancelHandleMap.remove(requestId);

                // Unconditionally update observable time because script may be applied partially.
                out.meta(tsUpdater.get());
            });
        }, operationExecutor);
    }
}
