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

import static org.apache.ignite.client.handler.requests.tx.ClientTransactionCommitRequest.merge;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.handler.ClientHandlerMetricSource;
import org.apache.ignite.client.handler.ClientResourceRegistry;
import org.apache.ignite.client.handler.ResponseWriter;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.tx.InternalTransaction;

/**
 * Client transaction rollback request.
 */
public class ClientTransactionRollbackRequest {
    /**
     * Processes the request.
     *
     * @param in Unpacker.
     * @param resources Resources.
     * @param metrics Metrics.
     * @param igniteTables Tables facade.
     * @param enableDirectMapping Enable direct mapping.
     * @return Future.
     */
    public static CompletableFuture<ResponseWriter> process(
            ClientMessageUnpacker in,
            ClientResourceRegistry resources,
            ClientHandlerMetricSource metrics,
            IgniteTablesInternal igniteTables,
            boolean enableDirectMapping
    )
            throws IgniteInternalCheckedException {
        long resourceId = in.unpackLong();

        InternalTransaction tx = resources.remove(resourceId).get(InternalTransaction.class);

        if (enableDirectMapping && !tx.isReadOnly()) {
            // Attempt to merge server and client transactions.
            int cnt = in.unpackInt(); // Number of direct enlistments.
            for (int i = 0; i < cnt; i++) {
                int tableId = in.unpackInt();
                int partId = in.unpackInt();
                String consistentId = in.unpackString();
                long token = in.unpackLong();

                TableViewInternal table = igniteTables.cachedTable(tableId);

                if (table != null) {
                    merge(table.internalTable(), partId, consistentId, token, tx, false);
                }
            }
        }

        return tx.rollbackAsync().handle((res, err) -> {
            metrics.transactionsActiveDecrement();

            return null;
        });
    }
}
