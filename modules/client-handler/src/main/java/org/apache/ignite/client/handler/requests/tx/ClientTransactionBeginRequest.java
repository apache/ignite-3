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

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.handler.ClientHandlerMetricSource;
import org.apache.ignite.client.handler.ClientResource;
import org.apache.ignite.client.handler.ClientResourceRegistry;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.tx.impl.IgniteTransactionsImpl;
import org.apache.ignite.tx.TransactionOptions;
import org.jetbrains.annotations.Nullable;

/**
 * Client transaction begin request.
 */
public class ClientTransactionBeginRequest {
    /**
     * Processes the request.
     *
     * @param in           Unpacker.
     * @param out          Packer.
     * @param transactions Transactions.
     * @param resources    Resources.
     * @param metrics      Metrics.
     * @return Future.
     */
    public static @Nullable CompletableFuture<Void> process(
            ClientMessageUnpacker in,
            ClientMessagePacker out,
            IgniteTransactionsImpl transactions,
            ClientResourceRegistry resources,
            ClientHandlerMetricSource metrics) throws IgniteInternalCheckedException {
        TransactionOptions options = null;
        HybridTimestamp observableTs = null;

        boolean readOnly = in.unpackBoolean();
        if (readOnly) {
            options = new TransactionOptions().readOnly(true);

            // Timestamp makes sense only for read-only transactions.
            observableTs = HybridTimestamp.nullableHybridTimestamp(in.unpackLong());
        }

        // NOTE: we don't use beginAsync here because it is synchronous anyway.
        var tx = transactions.begin(options, observableTs);

        if (readOnly) {
            // For read-only tx, override observable timestamp that we send to the client:
            // use readTimestamp() instead of now().
            out.meta(tx.readTimestamp());
        }

        try {
            long resourceId = resources.put(new ClientResource(tx, tx::rollbackAsync));
            out.packLong(resourceId);

            metrics.transactionsActiveIncrement();

            return null;
        } catch (IgniteInternalCheckedException e) {
            tx.rollback();
            throw e;
        }
    }
}
