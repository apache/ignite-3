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

import static org.apache.ignite.client.handler.requests.table.ClientTableCommon.readTx;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.ignite.client.handler.ClientResourceRegistry;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.sql.api.IgniteSqlImpl;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.lang.CancelHandle;
import org.apache.ignite.sql.BatchedArguments;

/**
 * Client SQL execute batch request.
 */
public class ClientSqlExecuteBatchRequest {
    /**
     * Processes the request.
     *
     * @param operationExecutor Executor to submit execution of operation.
     * @param in Unpacker.
     * @param out Packer.
     * @param sql SQL API.
     * @param resources Resources.
     * @param requestId Id of the request.
     * @param cancelHandleMap Registry of handlers. Request must register itself in this registry before switching to another
     *         thread.
     * @return Future representing result of operation.
     */
    public static CompletableFuture<Void> process(
            Executor operationExecutor,
            ClientMessageUnpacker in,
            ClientMessagePacker out,
            QueryProcessor sql,
            ClientResourceRegistry resources,
            long requestId,
            Map<Long, CancelHandle> cancelHandleMap
    ) {
        return nullCompletedFuture().thenComposeAsync(none -> {
            CancelHandle cancelHandle = CancelHandle.create();
            cancelHandleMap.put(requestId, cancelHandle);

            InternalTransaction tx = readTx(in, out, resources, null, null);
            ClientSqlProperties props = new ClientSqlProperties(in);
            String statement = in.unpackString();
            BatchedArguments arguments = in.unpackBatchedArgumentsFromBinaryTupleArray();

            if (arguments == null) {
                // SQL engine requires non-null arguments, but we don't want to complicate the protocol with this requirement.
                arguments = BatchedArguments.of(ArrayUtils.OBJECT_EMPTY_ARRAY);
            }

            HybridTimestamp clientTs = HybridTimestamp.nullableHybridTimestamp(in.unpackLong());
            HybridTimestampTracker tsUpdater = HybridTimestampTracker.atomicTracker(clientTs);

            return IgniteSqlImpl.executeBatchCore(
                            sql,
                            tsUpdater,
                            tx,
                            cancelHandle.token(),
                            statement,
                            arguments,
                            props.toSqlProps(),
                            () -> true,
                            () -> {},
                            cursor -> 0,
                            cursorId -> {})
                    .whenComplete((none2, error) -> {
                        cancelHandleMap.remove(requestId);
                    })
                    .thenApply((affectedRows) -> {
                        out.meta(tsUpdater.get());

                        out.packNil(); // resourceId

                        out.packBoolean(false); // has row set
                        out.packBoolean(false); // has more pages
                        out.packBoolean(false); // was applied

                        out.packLongArray(affectedRows); // affected rows

                        return null;
                    });
        });
    }
}
