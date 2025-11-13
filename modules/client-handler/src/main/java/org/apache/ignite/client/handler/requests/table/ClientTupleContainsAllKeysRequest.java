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

import static java.util.EnumSet.of;
import static org.apache.ignite.client.handler.requests.table.ClientTableCommon.writeTxMeta;
import static org.apache.ignite.client.handler.requests.table.ClientTupleRequestBase.RequestOptions.HAS_PRIORITY;
import static org.apache.ignite.client.handler.requests.table.ClientTupleRequestBase.RequestOptions.KEY_ONLY;

import java.util.EnumSet;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.handler.ClientResourceRegistry;
import org.apache.ignite.client.handler.ResponseWriter;
import org.apache.ignite.client.handler.requests.table.ClientTupleRequestBase.RequestOptions;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.table.IgniteTables;

/**
 * Client tuple contains all keys request.
 */
public class ClientTupleContainsAllKeysRequest {
    /**
     * Processes the request.
     *
     * @param in           Unpacker.
     * @param tables       Ignite tables.
     * @param resources    Resource registry.
     * @param txManager    Transaction manager.
     * @param clockService Clock service.
     * @param tsTracker    Tracker.
     * @param supportsPriority {@code True} if compatible with tx priority in request body.
     * @return Future.
     */
    public static CompletableFuture<ResponseWriter> process(
            ClientMessageUnpacker in,
            IgniteTables tables,
            ClientResourceRegistry resources,
            TxManager txManager,
            ClockService clockService,
            HybridTimestampTracker tsTracker,
            boolean supportsPriority
    ) {
        EnumSet<RequestOptions> options = supportsPriority ? of(KEY_ONLY, HAS_PRIORITY) : of(KEY_ONLY);

        return ClientTuplesRequestBase.readAsync(in, tables, resources, txManager, null, tsTracker, options)
                .thenCompose(req -> req.table().recordView().containsAllAsync(req.tx(), req.tuples())
                        .thenApply(containsAll -> out -> {
                            writeTxMeta(out, tsTracker, clockService, req);
                            out.packInt(req.table().schemaView().lastKnownSchemaVersion());
                            out.packBoolean(containsAll);
                        }));
    }
}
