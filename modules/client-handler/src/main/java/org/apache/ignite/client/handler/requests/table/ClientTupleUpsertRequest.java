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
import static org.apache.ignite.client.handler.requests.table.ClientTableCommon.writeTxMeta;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.handler.ClientResourceRegistry;
import org.apache.ignite.client.handler.NotificationSender;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.table.IgniteTables;

/**
 * Client tuple upsert request.
 */
public class ClientTupleUpsertRequest {
    /**
     * Processes the request.
     *
     * @param in Unpacker.
     * @param out Packer.
     * @param tables Ignite tables.
     * @param resources Resource registry.
     * @param txManager Ignite transactions.
     * @param clockService Clock service.
     * @param notificationSender Notification sender.
     * @return Future.
     */
    public static CompletableFuture<Void> process(
            ClientMessageUnpacker in,
            ClientMessagePacker out,
            IgniteTables tables,
            ClientResourceRegistry resources,
            TxManager txManager,
            ClockService clockService,
            NotificationSender notificationSender
    ) {
        return readTableAsync(in, tables).thenCompose(table -> {
            var tx = readOrStartImplicitTx(in, out, resources, txManager, false, notificationSender);
            return readTuple(in, table, false).thenCompose(tuple -> {
                return table.recordView().upsertAsync(tx, tuple).thenAccept(v -> {
                    writeTxMeta(out, clockService, tx);
                    out.packInt(table.schemaView().lastKnownSchemaVersion());
                });
            });
        });
    }
}
