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

package org.apache.ignite.client.handler.requests.tx;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.handler.ResponseWriter;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.tx.TxManager;

/**
 * Client transaction direct mapping discard request.
 */
public class ClientTransactionDiscardRequest {
    /**
     * Processes the request.
     *
     * @param in The unpacker.
     * @param igniteTables Tables facade.
     * @return The future.
     */
    public static CompletableFuture<ResponseWriter> process(
            ClientMessageUnpacker in,
            TxManager txManager,
            IgniteTablesInternal igniteTables
    ) throws IgniteInternalCheckedException {
        UUID txId = in.unpackUuid();
        int cnt = in.unpackInt(); // Number of direct enlistments.

        var cleaner = new ClientTxPartitionEnlistmentCleaner(txId, txManager, igniteTables);

        for (int i = 0; i < cnt; i++) {
            int tableId = in.unpackInt();
            int partId = in.unpackInt();
            cleaner.addEnlistment(tableId, partId);
        }

        return cleaner.clean().handle((res, err) -> null);
    }
}
