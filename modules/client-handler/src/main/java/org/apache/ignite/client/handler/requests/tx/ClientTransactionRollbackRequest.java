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

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.handler.ClientHandlerMetricSource;
import org.apache.ignite.client.handler.ClientResourceRegistry;
import org.apache.ignite.client.handler.ResponseWriter;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.impl.ReadWriteTransactionImpl;
import org.apache.ignite.lang.ErrorGroups.Client;
import org.apache.ignite.lang.IgniteException;

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
     * @param reqToTxMap Tracker for first request of direct transactions.
     * @param enableDirectMapping Enable direct mapping.
     * @param sendRemoteWritesFlag Send remote writes flag.
     * @return Future.
     */
    public static CompletableFuture<ResponseWriter> process(
            ClientMessageUnpacker in,
            ClientResourceRegistry resources,
            ClientHandlerMetricSource metrics,
            IgniteTablesInternal igniteTables,
            Map<Long, Long> reqToTxMap,
            boolean enableDirectMapping,
            boolean sendRemoteWritesFlag
    )
            throws IgniteInternalCheckedException {
        long resourceId = in.unpackLong();

        InternalTransaction tx;

        if (!enableDirectMapping) {
            tx = resources.remove(resourceId).get(InternalTransaction.class);
        } else if (resourceId < 0) {
            // Direct mapping was enabled, but the user does not know the resourceId, so he sent the first req id.
            long reqId = -resourceId;
            var actualResourceId = reqToTxMap.get(reqId);

            // Is it ok to reuse this error??
            if (actualResourceId == null) {
                throw new IgniteException(Client.RESOURCE_NOT_FOUND_ERR, "Failed to find resource from requestId: " + reqId);
            }

            tx = resources.remove(actualResourceId).get(InternalTransaction.class);

            reqToTxMap.remove(reqId);
        } else {
            tx = resources.remove(resourceId).get(InternalTransaction.class);

            if (!tx.isReadOnly()) {
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

                if (cnt > 0) {
                    in.unpackLong(); // Unpack causality.

                    ReadWriteTransactionImpl tx0 = (ReadWriteTransactionImpl) tx;

                    // Enforce cleanup.
                    tx0.noRemoteWrites(sendRemoteWritesFlag && in.unpackBoolean());
                }
            }
        }

        return tx.rollbackAsync().handle((res, err) -> {
            metrics.transactionsActiveDecrement();

            return null;
        });
    }
}
