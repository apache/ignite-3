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

import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_COMMIT_ERR;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.handler.ClientHandlerMetricSource;
import org.apache.ignite.client.handler.ClientResourceRegistry;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.impl.ReadWriteTransactionImpl;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionException;

/**
 * Client transaction commit request.
 */
public class ClientTransactionCommitRequest {
    /**
     * Processes the request.
     *
     * @param in Unpacker.
     * @param out Packer.
     * @param resources Resources.
     * @param metrics Metrics.
     * @param clockService Clock service.
     * @param igniteTables Tables.
     * @return Future.
     */
    public static CompletableFuture<Void> process(
            ClientMessageUnpacker in,
            ClientMessagePacker out,
            ClientResourceRegistry resources,
            ClientHandlerMetricSource metrics,
            ClockService clockService,
            IgniteTablesInternal igniteTables
    ) throws IgniteInternalCheckedException {
        // TODO adjust HLC
        long resourceId = in.unpackLong();
        InternalTransaction tx = resources.remove(resourceId).get(InternalTransaction.class);

        // Attempt to merge server and client transactions.
        if (!tx.isReadOnly()) {
            int cnt = in.unpackInt();
            for (int i = 0; i < cnt; i++) {
                int tableId = in.unpackInt();
                int partId = in.unpackInt();
                UUID nodeId = in.unpackUuid();
                long token = in.unpackLong();
                // TODO FIXME
                TableViewInternal table = igniteTables.cachedTable(tableId);
                if (table == null) {
                    return failedFuture(new TransactionException(TX_COMMIT_ERR, "Table not found [id=" + tableId + ']'));
                }

                if (!table.internalTable().validateEnlistment(partId, nodeId, token, tx)) {
                    return failedFuture(new TransactionException(TX_COMMIT_ERR, "Invalid enlistment token [id=" + tableId + ']'));
                }
            }
        }

        return tx.commitAsync().whenComplete((res, err) -> {
            if (!tx.isReadOnly()) {
                out.meta(clockService.current());
            }

            metrics.transactionsActiveDecrement();
        });
    }
}
