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
import static org.apache.ignite.client.handler.requests.table.ClientTupleRequestBase.RequestOptions.READ_SECOND_TUPLE;

import java.util.BitSet;
import java.util.EnumSet;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.handler.ClientResourceRegistry;
import org.apache.ignite.client.handler.NotificationSender;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.table.IgniteTables;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.Nullable;

class ClientTupleRequestBase {
    private final @Nullable InternalTransaction tx;
    private final TableViewInternal table;
    private final Tuple tuple;
    private final @Nullable Tuple tuple2;
    private final long resourceId;

    private ClientTupleRequestBase(@Nullable InternalTransaction tx, TableViewInternal table, Tuple tuple, @Nullable Tuple tuple2,
            long resourceId) {
        this.tx = tx;
        this.table = table;
        this.tuple = tuple;
        this.tuple2 = tuple2;
        this.resourceId = resourceId;
    }

    public InternalTransaction tx() {
        assert tx != null : "tx is null";

        return tx;
    }

    public long resourceId() {
        return resourceId;
    }

    public TableViewInternal table() {
        return table;
    }

    public Tuple tuple() {
        return tuple;
    }

    public Tuple tuple2() {
        assert tuple2 != null : "tuple2 is null";

        return tuple2;
    }

    public enum RequestOptions {
        READ_ONLY,
        KEY_ONLY,
        READ_SECOND_TUPLE,
        NO_WRITES
    }

    public static CompletableFuture<ClientTupleRequestBase> readAsync(
            ClientMessageUnpacker in,
            IgniteTables tables,
            ClientResourceRegistry resources,
            @Nullable TxManager txManager,
            @Nullable NotificationSender notificationSender,
            @Nullable HybridTimestampTracker tsTracker,
            EnumSet<RequestOptions> options
    ) {
        assert (txManager != null) == (tsTracker != null) : "txManager and tsTracker must be both null or not null";

        int tableId = in.unpackInt();

        long[] resIdHolder = {0};

        InternalTransaction tx = txManager == null
                ? null
                : readOrStartImplicitTx(in, tsTracker, resources, txManager, options, notificationSender, resIdHolder);

        int schemaId = in.unpackInt();

        BitSet noValueSet = in.unpackBitSet();
        byte[] tupleBytes = in.readBinary();

        BitSet noValueSet2 = options.contains(READ_SECOND_TUPLE) ? in.unpackBitSet() : null;
        byte[] tupleBytes2 = options.contains(READ_SECOND_TUPLE) ? in.readBinary() : null;

        return readTableAsync(tableId, tables)
                .thenCompose(table -> ClientTableCommon.readSchema(schemaId, table)
                        .thenApply(schema -> {
                            var tuple = readTuple(noValueSet, tupleBytes, options.contains(KEY_ONLY), schema);
                            var tuple2 = options.contains(READ_SECOND_TUPLE) ?
                                    readTuple(noValueSet2, tupleBytes2, options.contains(KEY_ONLY), schema) : null;

                            return new ClientTupleRequestBase(tx, table, tuple, tuple2, resIdHolder[0]);
                        }));

    }
}
