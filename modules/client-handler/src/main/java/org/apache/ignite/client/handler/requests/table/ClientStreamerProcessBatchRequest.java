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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.handler.ClientResourceRegistry;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.manager.IgniteTables;

/**
 * Client streamer batch request.
 */
public class ClientStreamerProcessBatchRequest {
    /**
     * Processes the request.
     *
     * @param in        Unpacker.
     * @param out       Packer.
     * @param tables    Ignite tables.
     * @param resources Resource registry.
     * @return Future.
     */
    public static CompletableFuture<Void> process(
            ClientMessageUnpacker in,
            ClientMessagePacker out,
            IgniteTables tables,
            ClientResourceRegistry resources
    ) {
        return readTableAsync(in, tables).thenCompose(table -> {
            return readSchema(in, table).thenCompose(schema -> {
                // TODO: Handle removals separately.
                // TODO: We must ensure ordering.
                // 1. Go on by one and batch the items. If there are removals, we should stop and process them separately.
                // 2. Ideally, we need an internal API to process additions and removals together.
                int size = in.unpackInt();
                int pos = 0;
                boolean currentRemove = false;
                List<Tuple> batch = new ArrayList<>(size);

                for (pos = 0; pos < size; pos++) {
                    int opCode = in.unpackInt();
                    boolean remove = opCode == 1;
                    boolean keyOnly = remove;

                    Tuple tuple = readTuple(in, keyOnly, schema);

                    if (remove == currentRemove) {
                        batch.add(tuple);
                    } else {
                        // Send current batch and start a new one.
                    }
                }

                return table.recordView().upsertAllAsync(null, null);
            });
        });
    }
}
