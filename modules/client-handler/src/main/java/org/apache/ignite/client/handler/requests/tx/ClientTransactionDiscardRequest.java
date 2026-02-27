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

import static java.util.stream.Collectors.toList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.handler.ResponseWriter;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.tx.PendingTxPartitionEnlistment;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.EnlistedPartitionGroup;

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
        Map<ZonePartitionId, PendingTxPartitionEnlistment> enlistedPartitions = new HashMap<>();

        UUID txId = in.unpackUuid();

        int cnt = in.unpackInt(); // Number of direct enlistments.
        for (int i = 0; i < cnt; i++) {
            int tableId = in.unpackInt();
            int partId = in.unpackInt();

            TableViewInternal table = igniteTables.cachedTable(tableId);

            if (table != null) {
                ZonePartitionId replicationGroupId = table.internalTable().targetReplicationGroupId(partId);
                enlistedPartitions.computeIfAbsent(replicationGroupId, k -> new PendingTxPartitionEnlistment(null, 0))
                        .addTableId(tableId);
            }
        }

        List<EnlistedPartitionGroup> enlistedPartitionGroups = enlistedPartitions.entrySet().stream()
                .map(entry -> new EnlistedPartitionGroup(entry.getKey(), entry.getValue().tableIds()))
                .collect(toList());

        return txManager.discardLocalWriteIntents(enlistedPartitionGroups, txId).handle((res, err) -> null);
    }
}
