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
import static org.apache.ignite.client.handler.requests.table.ClientTableCommon.readTuple;
import static org.apache.ignite.client.handler.requests.table.ClientTupleRequestBase.RequestOptions.KEY_ONLY;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.handler.ClientResourceRegistry;
import org.apache.ignite.client.handler.NotificationSender;
import org.apache.ignite.client.handler.requests.table.ClientTupleRequestBase.RequestOptions;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.table.IgniteTables;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.Nullable;

class ClientTuplesRequestBase {
    private final InternalTransaction tx;
    private final TableViewInternal table;
    private final List<Tuple> tuples;
    private final long resourceId;

    private ClientTuplesRequestBase(InternalTransaction tx, TableViewInternal table, List<Tuple> tuples, long resourceId) {
        this.tx = tx;
        this.table = table;
        this.tuples = tuples;
        this.resourceId = resourceId;
    }

    public InternalTransaction tx() {
        return tx;
    }

    public long resourceId() {
        return resourceId;
    }

    public TableViewInternal table() {
        return table;
    }

    public List<Tuple> tuples() {
        return tuples;
    }

    public static CompletableFuture<ClientTuplesRequestBase> readAsync(
            ClientMessageUnpacker in,
            IgniteTables tables,
            ClientResourceRegistry resources,
            TxManager txManager,
            @Nullable NotificationSender notificationSender,
            HybridTimestampTracker tsTracker,
            EnumSet<RequestOptions> options
    ) {
        int tableId = in.unpackInt();

        long[] resIdHolder = {0};

        InternalTransaction tx =
                readOrStartImplicitTx(in, tsTracker, resources, txManager, options, notificationSender, resIdHolder);

        int schemaId = in.unpackInt();

        int count = in.unpackInt();

        BitSet[] noValueSet = new BitSet[count];
        byte[][] tupleBytes = new byte[count][];

        for (int i = 0; i < count; i++) {
            noValueSet[i] = in.unpackBitSet();
            tupleBytes[i] = in.readBinary();
        }

        return readTableAsync(tableId, tables)
                .thenCompose(table -> ClientTableCommon.readSchema(schemaId, table)
                        .thenApply(schema -> {
                            var tuples = new ArrayList<Tuple>(count);

                            for (int i = 0; i < count; i++) {
                                tuples.add(readTuple(noValueSet[i], tupleBytes[i], options.contains(KEY_ONLY), schema));
                            }

                            return new ClientTuplesRequestBase(tx, table, tuples, resIdHolder[0]);
                        }));
    }
}
