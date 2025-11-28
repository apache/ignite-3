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
import org.apache.ignite.client.handler.ResponseWriter;
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
     * @param sql SQL API.
     * @param resources Resources.
     * @param requestId Id of the request.
     * @param cancelHandleMap Registry of handlers. Request must register itself in this registry before switching to another
     *         thread.
     * @param username Authenticated user name.
     * @return Future representing result of operation.
     */
    public static CompletableFuture<ResponseWriter> process(
            Executor operationExecutor,
            ClientMessageUnpacker in,
            QueryProcessor sql,
            ClientResourceRegistry resources,
            long requestId,
            Map<Long, CancelHandle> cancelHandleMap,
            HybridTimestampTracker tsTracker,
            String username
    ) {
        CancelHandle cancelHandle = CancelHandle.create();
        cancelHandleMap.put(requestId, cancelHandle);

        InternalTransaction tx = readTx(in, tsTracker, resources, null, null, null, null);
        ClientSqlProperties props = new ClientSqlProperties(in, false);
        String statement = in.unpackString();
        BatchedArguments arguments = readArgs(in);

        HybridTimestamp clientTs = HybridTimestamp.nullableHybridTimestamp(in.unpackLong());
        tsTracker.update(clientTs);

        return nullCompletedFuture().thenComposeAsync(none -> {
            return IgniteSqlImpl.executeBatchCore(
                            sql,
                            tsTracker,
                            tx,
                            cancelHandle.token(),
                            statement,
                            arguments,
                            props.toSqlProps().userName(username),
                            () -> true,
                            () -> {},
                            cursor -> 0,
                            cursorId -> {})
                    .whenComplete((none2, error) -> {
                        cancelHandleMap.remove(requestId);
                    })
                    .thenApply((affectedRows) -> out -> {
                        out.packNil(); // resourceId

                        out.packBoolean(false); // has row set
                        out.packBoolean(false); // has more pages
                        out.packBoolean(false); // was applied

                        out.packLongArray(affectedRows); // affected rows
                    });
        });
    }

    private static BatchedArguments readArgs(ClientMessageUnpacker in) {
        BatchedArguments arguments = in.unpackBatchedArgumentsFromBinaryTupleArray();

        if (arguments == null) {
            // SQL engine requires non-null arguments, but we don't want to complicate the protocol with this requirement.
            arguments = BatchedArguments.of(ArrayUtils.OBJECT_EMPTY_ARRAY);
        }
        return arguments;
    }
}
