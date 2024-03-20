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

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.handler.ClientHandlerMetricSource;
import org.apache.ignite.client.handler.ClientResourceRegistry;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.sql.api.SessionImpl;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.impl.IgniteTransactionsImpl;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.sql.BatchedArguments;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlBatchException;
import org.jetbrains.annotations.Nullable;

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
     * @param metrics Metrics.
     * @param transactions Transactional facade. Used to acquire last observed time to propagate to client in response.
     * @return Future representing result of operation.
     */
    public static CompletableFuture<Void> process(
            ClientMessageUnpacker in,
            ClientMessagePacker out,
            QueryProcessor sql,
            ClientResourceRegistry resources,
            ClientHandlerMetricSource metrics,
            IgniteTransactionsImpl transactions
    ) {
        InternalTransaction tx = readTx(in, out, resources);
        ClientSqlProperties props = new ClientSqlProperties(in);
        String statement = in.unpackString();
        BatchedArguments arguments = in.unpackObjectArrayFromBinaryTupleArray();

        HybridTimestamp clientTs = HybridTimestamp.nullableHybridTimestamp(in.unpackLong());
        transactions.updateObservableTimestamp(clientTs);

        return SessionImpl.executeBatchCore(
                sql,
                        transactions,
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

                            writeBatchResult(out, resources, exBatch.updateCounters(),
                                    exBatch.errorCode(), exBatch.getMessage(), metrics);

                            return null;
                        }

                        affectedRows = ArrayUtils.LONG_EMPTY_ARRAY;
                    }

                    writeBatchResult(out, resources, affectedRows, metrics);
                    return null;
                });
    }

    private static void writeBatchResult(
            ClientMessagePacker out,
            ClientResourceRegistry resources,
            long[] affectedRows,
            Short errorCode,
            String errorMessage,
            ClientHandlerMetricSource metrics) {
        out.packNil(); // resourceId

        out.packBoolean(false); // has row set
        out.packBoolean(false); // has more pages
        out.packBoolean(false); // was applied
        out.packLongArray(affectedRows); // affected rows
        out.packShort(errorCode); // error code
        out.packString(errorMessage); // error message
    }

    private static void writeBatchResult(
            ClientMessagePacker out,
            ClientResourceRegistry resources,
            long[] affectedRows,
            ClientHandlerMetricSource metrics) {
        out.packNil(); // resourceId

        out.packBoolean(false); // has row set
        out.packBoolean(false); // has more pages
        out.packBoolean(false); // was applied
        out.packLongArray(affectedRows); // affected rows
        out.packNil(); // error code
        out.packNil(); // error message
    }

    private static void packMeta(ClientMessagePacker out, @Nullable ResultSetMetadata meta) {
        // TODO IGNITE-17179 metadata caching - avoid sending same meta over and over.
        if (meta == null || meta.columns() == null) {
            out.packInt(0);
            return;
        }

        ClientSqlCommon.packColumns(out, meta.columns());
    }
}
