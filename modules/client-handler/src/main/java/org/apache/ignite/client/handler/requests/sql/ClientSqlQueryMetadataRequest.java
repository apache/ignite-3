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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.ignite.client.handler.ClientResourceRegistry;
import org.apache.ignite.client.handler.ResponseWriter;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.SqlProperties;
import org.apache.ignite.internal.sql.engine.prepare.QueryMetadata;

/**
 * Client SQL request for the parameter metadata.
 */
public class ClientSqlQueryMetadataRequest {
    /**
     * Processes the request.
     *
     * @param operationExecutor Executor to submit execution of operation.
     * @param in Unpacker.
     * @param processor SQL API.
     * @param resources Resources.
     * @return Future representing result of operation.
     */
    public static CompletableFuture<ResponseWriter> process(
            Executor operationExecutor,
            ClientMessageUnpacker in,
            QueryProcessor processor,
            ClientResourceRegistry resources,
            HybridTimestampTracker tsTracker
    ) {
        var tx = readTx(in, tsTracker, resources, null, null, null);

        String schema = in.unpackString();
        String query = in.unpackString();

        SqlProperties properties = new SqlProperties().defaultSchema(schema);

        return nullCompletedFuture()
                .thenComposeAsync(none -> processor.prepareSingleAsync(properties, tx, query)
                .thenApply(meta -> out -> writeMeta(out, meta)), operationExecutor);
    }

    private static void writeMeta(ClientMessagePacker out, QueryMetadata meta) {
        var types = meta.parameterTypes();

        out.packInt(types.size());
        for (var param : types) {
            out.packBoolean(param.nullable());
            out.packInt(param.columnType().id());
            out.packInt(param.scale());
            out.packInt(param.precision());
        }

        ClientSqlCommon.packColumns(out, meta.columns());
    }
}
