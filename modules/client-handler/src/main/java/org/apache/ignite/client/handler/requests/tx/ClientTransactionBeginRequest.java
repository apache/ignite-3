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

import static org.apache.ignite.client.handler.requests.table.ClientTableCommon.startExplicitTx;
import static org.apache.ignite.client.handler.requests.table.ClientTableCommon.tableIdNotFoundException;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.handler.ClientHandlerMetricSource;
import org.apache.ignite.client.handler.ClientResource;
import org.apache.ignite.client.handler.ClientResourceRegistry;
import org.apache.ignite.client.handler.ResponseWriter;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.lang.IgniteSystemProperties;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.tx.InternalTxOptions;
import org.apache.ignite.internal.tx.TxManager;

/**
 * Client transaction begin request.
 */
public class ClientTransactionBeginRequest {
    private static final boolean enabledColocation = IgniteSystemProperties.enabledColocation();

    /**
     * Processes the request.
     *
     * @param in Unpacker.
     * @param txManager Transactions.
     * @param resources Resources.
     * @param metrics Metrics.
     * @param igniteTables Tables facade.
     * @param enableDirectMapping Direct mapping feature.
     * @return Future.
     */
    public static CompletableFuture<ResponseWriter> process(
            ClientMessageUnpacker in,
            TxManager txManager,
            ClientResourceRegistry resources,
            ClientHandlerMetricSource metrics,
            IgniteTablesInternal igniteTables,
            boolean enableDirectMapping,
            HybridTimestampTracker tsTracker
    ) throws IgniteInternalCheckedException {
        boolean readOnly = in.unpackBoolean();
        long timeoutMillis = in.unpackLong();
        long observable = in.unpackLong();

        HybridTimestamp observableTs = null;
        int tableId = -1;
        int partition = -1;

        if (readOnly) {
            // Timestamp makes sense only for read-only transactions.
            observableTs = HybridTimestamp.nullableHybridTimestamp(observable);
        } else if (enableDirectMapping) {
            // Read commit partition info.
            // It may be not available if client has not yet loaded partition map.
            tableId = in.unpackInt();
            partition = in.unpackInt();
        }

        InternalTxOptions txOptions = InternalTxOptions.builder()
                .timeoutMillis(timeoutMillis)
                .build();

        var tx = startExplicitTx(tsTracker, txManager, observableTs, readOnly, txOptions);

        // Assign commit partition at the beginning to avoid races on a client.
        if (tableId != -1) {
            if (enabledColocation) {
                TableViewInternal tableView = igniteTables.cachedTable(tableId);

                if (tableView == null) {
                    tx.rollback();
                    throw tableIdNotFoundException(tableId);
                }

                tx.assignCommitPartition(new ZonePartitionId(tableView.zoneId(), partition));
            } else {
                tx.assignCommitPartition(new TablePartitionId(tableId, partition));
            }
        }

        if (readOnly) {
            // Propagate assigned read timestamp to client to enforce serializability on subsequent reads from another node.
            tsTracker.update(tx.readTimestamp());
        }

        try {
            long resourceId = resources.put(new ClientResource(tx, tx::rollbackAsync));
            metrics.transactionsActiveIncrement();

            return CompletableFuture.completedFuture(out -> {
                out.packLong(resourceId);

                if (enableDirectMapping && !readOnly) {
                    out.packUuid(tx.id());
                    out.packUuid(tx.coordinatorId());
                }
            });
        } catch (IgniteInternalCheckedException e) {
            tx.rollback();
            throw e;
        }
    }
}
