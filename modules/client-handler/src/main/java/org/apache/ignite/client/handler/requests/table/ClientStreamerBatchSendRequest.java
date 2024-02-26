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

import static org.apache.ignite.client.handler.requests.table.ClientTableCommon.readSchema;
import static org.apache.ignite.client.handler.requests.table.ClientTableCommon.readTableAsync;
import static org.apache.ignite.client.handler.requests.table.ClientTableCommon.readTuple;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.table.RecordBinaryViewImpl;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.manager.IgniteTables;
import org.jetbrains.annotations.Nullable;

/**
 * Client streamer batch request.
 */
public class ClientStreamerBatchSendRequest {
    /**
     * Processes the request.
     *
     * @param in        Unpacker.
     * @param out       Packer.
     * @param tables    Ignite tables.
     * @return Future.
     */
    public static CompletableFuture<Void> process(
            ClientMessageUnpacker in,
            ClientMessagePacker out,
            IgniteTables tables
    ) {
        return readTableAsync(in, tables).thenCompose(table -> {
            int partition = in.unpackInt();
            BitSet deleted = in.unpackBitSetNullable();
            return readTuples(in, table, deleted).thenCompose(tuples -> {
                RecordBinaryViewImpl recordView = (RecordBinaryViewImpl) table.recordView();

                return recordView.updateAll(partition, tuples, deleted)
                        .thenAccept(unused -> out.packInt(table.schemaView().lastKnownSchemaVersion()));
            });
        });
    }

    private static CompletableFuture<List<Tuple>> readTuples(
            ClientMessageUnpacker unpacker,
            TableViewInternal table,
            @Nullable BitSet deleted) {
        return readSchema(unpacker, table).thenApply(schema -> {
            var rowCnt = unpacker.unpackInt();
            var res = new ArrayList<Tuple>(rowCnt);

            for (int i = 0; i < rowCnt; i++) {
                var keyOnly = deleted != null && deleted.get(i);
                res.add(readTuple(unpacker, keyOnly, schema));
            }

            return res;
        });
    }

}
