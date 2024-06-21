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

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.handler.ClientResourceRegistry;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.sql.api.IgniteSqlImpl;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.impl.IgniteTransactionsImpl;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.sql.BatchedArguments;
import org.apache.ignite.sql.SqlBatchException;

/**
 * Client SQL execute batch request.
 */
public class ClientSqlExecuteBatchRequest {
    /**
     * Processes the request.
     *
     * @param in Unpacker.
     * @param out Packer.
     * @param sql SQL API.
     * @param resources Resources.
     * @param transactions Transactional facade. Used to acquire last observed time to propagate to client in response.
     * @return Future representing result of operation.
     */
    public static CompletableFuture<Void> process(
            ClientMessageUnpacker in,
            ClientMessagePacker out,
            QueryProcessor sql,
            ClientResourceRegistry resources,
            IgniteTransactionsImpl transactions
    ) {
        InternalTransaction tx = readTx(in, out, resources);
        ClientSqlProperties props = new ClientSqlProperties(in);
        String statement = in.unpackString();
        BatchedArguments arguments = in.unpackBatchedArgumentsFromBinaryTupleArray();

        if (arguments == null) {
            // SQL engine requires non-null arguments, but we don't want to complicate the protocol with this requirement.
            arguments = BatchedArguments.of(ArrayUtils.OBJECT_EMPTY_ARRAY);
        }

        HybridTimestamp clientTs = HybridTimestamp.nullableHybridTimestamp(in.unpackLong());
        transactions.updateObservableTimestamp(clientTs);

        return IgniteSqlImpl.executeBatchCore(
                sql,
                        transactions.observableTimestampTracker(),
                        tx,
                        statement,
                        arguments,
                        props.toSqlProps(),
                        () -> true,
                        () -> {},
                        cursor -> 0,
                        cursorId -> {})
                .handle((affectedRows, ex) -> {
                    out.meta(transactions.observableTimestamp());

                    if (ex != null) {
                        var cause = ExceptionUtils.unwrapCause(ex.getCause());

                        if (cause instanceof SqlBatchException) {
                            var exBatch = ((SqlBatchException) cause);

                            writeBatchResult(out, exBatch.updateCounters(), exBatch.code(), exBatch.getMessage(), exBatch.traceId());
                            return null;
                        }

                        affectedRows = ArrayUtils.LONG_EMPTY_ARRAY;
                    }

                    writeBatchResult(out, affectedRows);
                    return null;
                });
    }

    private static void writeBatchResult(
            ClientMessagePacker out,
            long[] affectedRows,
            int errorCode,
            String errorMessage,
            UUID traceId) {
        out.packNil(); // resourceId

        out.packBoolean(false); // has row set
        out.packBoolean(false); // has more pages
        out.packBoolean(false); // was applied
        out.packLongArray(affectedRows); // affected rows
        out.packInt(errorCode); // error code
        out.packString(errorMessage); // error message
        out.packUuid(traceId);
    }

    private static void writeBatchResult(
            ClientMessagePacker out,
            long[] affectedRows) {
        out.packNil(); // resourceId

        out.packBoolean(false); // has row set
        out.packBoolean(false); // has more pages
        out.packBoolean(false); // was applied
        out.packLongArray(affectedRows); // affected rows
        out.packNil(); // error code
        out.packNil(); // error message
        out.packNil(); // trace id
    }
}
