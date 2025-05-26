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
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.handler.ResponseWriter;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.table.RecordBinaryViewImpl;
import org.apache.ignite.table.IgniteTables;
import org.apache.ignite.table.Tuple;

/**
 * Client streamer batch request.
 */
public class ClientStreamerBatchSendRequest {
    /**
     * Processes the request.
     *
     * @param in        Unpacker.
     * @param tables    Ignite tables.
     * @return Future.
     */
    public static CompletableFuture<ResponseWriter> process(
            ClientMessageUnpacker in,
            IgniteTables tables
    ) {
        int tableId = in.unpackInt();
        int partition = in.unpackInt();
        BitSet deleted = in.unpackBitSetNullable();
        int schemaId = in.unpackInt();

        var count = in.unpackInt();

        BitSet[] noValueSet = new BitSet[count];
        byte[][] tupleBytes = new byte[count][];

        for (int i = 0; i < count; i++) {
            noValueSet[i] = in.unpackBitSet();
            tupleBytes[i] = in.readBinary();
        }

        return readTableAsync(tableId, tables)
                .thenCompose(table -> readSchema(schemaId, table)
                        .thenCompose(schema -> {
                            var tuples = new ArrayList<Tuple>(count);

                            for (int i = 0; i < count; i++) {
                                var keyOnly = deleted != null && deleted.get(i);
                                tuples.add(readTuple(noValueSet[i], tupleBytes[i], keyOnly, schema));
                            }

                            RecordBinaryViewImpl recordView = (RecordBinaryViewImpl) table.recordView();

                            return recordView.updateAll(partition, tuples, deleted)
                                    .thenApply(unused ->
                                            out -> out.packInt(table.schemaView().lastKnownSchemaVersion()));
                        }));
    }
}
