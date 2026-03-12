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
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.tx.PendingTxPartitionEnlistment;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.EnlistedPartitionGroup;

/**
 * Helper class to clean up direct transaction enlistments on the client side.
 */
public class ClientTxPartitionEnlistmentCleaner {
    private final UUID txId;

    private final TxManager txManager;

    private final IgniteTablesInternal igniteTables;

    private final Map<ZonePartitionId, PendingTxPartitionEnlistment> enlistedPartitions = new HashMap<>();

    public ClientTxPartitionEnlistmentCleaner(UUID txId, TxManager txManager, IgniteTablesInternal igniteTables) {
        this.txId = txId;
        this.txManager = txManager;
        this.igniteTables = igniteTables;
    }

    public void addEnlistment(int tableId, int partId) {
        TableViewInternal table = igniteTables.cachedTable(tableId);

        if (table != null) {
            ZonePartitionId replicationGroupId = table.internalTable().targetReplicationGroupId(partId);
            enlistedPartitions.computeIfAbsent(replicationGroupId, k -> new PendingTxPartitionEnlistment(null, 0))
                    .addTableId(tableId);
        }
    }

    public CompletableFuture<Void> clean() {
        List<EnlistedPartitionGroup> enlistedPartitionGroups = enlistedPartitions.entrySet().stream()
                .map(entry -> new EnlistedPartitionGroup(entry.getKey(), entry.getValue().tableIds()))
                .collect(toList());

        return txManager.discardLocalWriteIntents(enlistedPartitionGroups, txId);
    }
}
