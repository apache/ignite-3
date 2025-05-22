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

package org.apache.ignite.client.handler.requests.table;

import static org.apache.ignite.client.handler.requests.table.ClientTableCommon.readOrStartImplicitTx;
import static org.apache.ignite.client.handler.requests.table.ClientTableCommon.readTableAsync;
import static org.apache.ignite.client.handler.requests.table.ClientTableCommon.readTuples;
import static org.apache.ignite.client.handler.requests.table.ClientTableCommon.writeTxMeta;

import java.util.BitSet;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.handler.ClientResourceRegistry;
import org.apache.ignite.client.handler.NotificationSender;
import org.apache.ignite.client.handler.ResponseWriter;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.table.IgniteTables;

/**
 * Client tuple upsert all request.
 */
public class ClientTupleUpsertAllRequest {
    /**
     * Processes the request.
     *
     * @param in Unpacker.
     * @param tables Ignite tables.
     * @param resources Resource registry.
     * @param txManager Ignite transactions.
     * @return Future.
     */
    public static CompletableFuture<ResponseWriter> process(
            ClientMessageUnpacker in,
            IgniteTables tables,
            ClientResourceRegistry resources,
            TxManager txManager,
            ClockService clockService,
            NotificationSender notificationSender,
            HybridTimestampTracker tsTracker
    ) {
        int tableId = in.unpackInt();
        int schemaId = in.unpackInt();

        var tx = readOrStartImplicitTx(in, tsTracker, resources, txManager, false, notificationSender);

        int count = in.unpackInt();

        BitSet[] noValueSet = new BitSet[count];
        byte[][] tupleBytes = new byte[count][];

        for (int i = 0; i < count; i++) {
            noValueSet[i] = in.unpackBitSet();
            tupleBytes[i] = in.readBinary();
        }

        return readTableAsync(tableId, tables).thenCompose(table -> {
            return readTuples(schemaId, noValueSet, tupleBytes, table, false).thenCompose(tuples -> {
                return table.recordView().upsertAllAsync(tx, tuples).thenApply(unused -> out -> {
                    writeTxMeta(out, tsTracker, clockService, tx);
                    out.packInt(table.schemaView().lastKnownSchemaVersion());
                });
            });
        });
    }
}
