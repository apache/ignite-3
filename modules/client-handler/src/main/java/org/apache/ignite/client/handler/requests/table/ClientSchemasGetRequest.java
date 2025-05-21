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

import static java.util.concurrent.CompletableFuture.allOf;
import static org.apache.ignite.client.handler.requests.table.ClientTableCommon.readTableAsync;
import static org.apache.ignite.client.handler.requests.table.ClientTableCommon.writeSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.handler.ResponseWriter;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.table.distributed.schema.SchemaVersions;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.table.IgniteTables;
import org.jetbrains.annotations.Nullable;

/**
 * Client schemas retrieval request.
 */
public class ClientSchemasGetRequest {
    /**
     * Processes the request.
     *
     * @param in     Unpacker.
     * @param tables Ignite tables.
     * @return Future.
     * @throws IgniteException When schema registry is no initialized.
     */
    public static CompletableFuture<ResponseWriter> process(
            ClientMessageUnpacker in,
            IgniteTables tables,
            SchemaVersions schemaVersions
    ) {
        int tableId = in.unpackInt();
        List<Integer> schemaVers = readSchemaVersions(in);

        return readTableAsync(tableId, tables).thenCompose(table -> {
            if (schemaVers == null) {
                return schemaVersions.schemaVersionAtCurrentTime(table.tableId())
                        .thenApply(version -> {
                            SchemaDescriptor schema = table.schemaView().schema(version);

                            return out -> {
                                // Return the latest schema.
                                out.packInt(1);
                                writeSchema(out, schema.version(), schema);
                            };
                        });
            } else {
                //noinspection unchecked
                CompletableFuture<SchemaDescriptor>[] schemaFutures = new CompletableFuture[schemaVers.size()];
                for (int i = 0; i < schemaVers.size(); i++) {
                    var schemaVer = schemaVers.get(i);
                    // Use schemaAsync() as the schema version is coming from outside and we have no guarantees that this version is ready.
                    schemaFutures[i] = table.schemaView().schemaAsync(schemaVer);
                }

                return allOf(schemaFutures).thenApply(v -> out -> {
                    out.packInt(schemaFutures.length);

                    for (CompletableFuture<SchemaDescriptor> schemaFuture : schemaFutures) {
                        // join() is safe as all futures are already completed.
                        SchemaDescriptor schema = schemaFuture.join();
                        writeSchema(out, schema.version(), schema);
                    }
                });
            }
        });
    }

    private static @Nullable List<Integer> readSchemaVersions(ClientMessageUnpacker in) {
        if (in.tryUnpackNil()) {
            return null;
        }

        int cnt = in.unpackInt();
        List<Integer> res = new ArrayList<>(cnt);

        for (int i = 0; i < cnt; i++) {
            res.add(in.unpackInt());
        }

        return res;
    }
}
