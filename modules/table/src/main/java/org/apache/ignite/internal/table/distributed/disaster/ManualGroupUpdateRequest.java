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

package org.apache.ignite.internal.table.distributed.disaster;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.util.IgniteUtils.copyStateTo;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.distributionzones.NodeWithAttributes;
import org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.tostring.S;

class ManualGroupUpdateRequest implements DisasterRecoveryRequest {
    /** Serial version UID. */
    private static final long serialVersionUID = 0L;

    private final UUID operationId;

    private final int zoneId;

    private final int tableId;

    ManualGroupUpdateRequest(UUID operationId, int zoneId, int tableId) {
        this.operationId = operationId;
        this.zoneId = zoneId;
        this.tableId = tableId;
    }

    @Override
    public UUID operationId() {
        return operationId;
    }

    @Override
    public int zoneId() {
        return zoneId;
    }

    public int tableId() {
        return tableId;
    }

    @Override
    public CompletableFuture<Void> handle(
            DisasterRecoveryManager disasterRecoveryManager,
            long msRevision,
            CompletableFuture<Void> operationFuture
    ) {
        HybridTimestamp msSafeTime = disasterRecoveryManager.metaStorageManager.timestampByRevision(msRevision);

        int catalogVersion = disasterRecoveryManager.catalogManager.activeCatalogVersion(msSafeTime.longValue());
        Catalog catalog = disasterRecoveryManager.catalogManager.catalog(catalogVersion);

        CatalogZoneDescriptor zoneDescriptor = catalog.zone(zoneId);
        CatalogTableDescriptor tableDescriptor = catalog.table(tableId);

        CompletableFuture<Set<String>> dataNodesFuture = disasterRecoveryManager.dzManager.dataNodes(msRevision, catalogVersion, zoneId);

        return dataNodesFuture.thenCompose(dataNodes -> {
            Set<String> nodeConsistentIds = disasterRecoveryManager.dzManager.logicalTopology()
                    .stream()
                    .map(NodeWithAttributes::nodeName)
                    .collect(toSet());

            CompletableFuture<?>[] futures = RebalanceUtil.forceAssignmentsUpdate(
                    tableDescriptor,
                    zoneDescriptor,
                    dataNodes,
                    nodeConsistentIds,
                    msRevision,
                    disasterRecoveryManager.metaStorageManager
            );

            allOf(futures).whenComplete(copyStateTo(operationFuture));

            return operationFuture;
        });
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
