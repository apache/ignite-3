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

package org.apache.ignite.internal.table.distributed.replicator;

import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.BUILDING;

import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadWriteReplicaRequest;
import org.apache.ignite.internal.tx.TransactionIds;
import org.jetbrains.annotations.Nullable;

/** Auxiliary class. */
class ReplicatorUtils {
    /**
     * Looks for the latest index with {@link CatalogIndexStatus#BUILDING} for the table, {@code null} if missing.
     *
     * <p>NOTE: It is expected that the method will be executed in the metastore thread so that the catalog does not change
     * concurrently.</p>
     *
     * @param catalogService Catalog service.
     * @param tableId Table ID.
     */
    static @Nullable CatalogIndexDescriptor latestIndexDescriptorInBuildingStatus(CatalogService catalogService, int tableId) {
        // Since we expect to be executed on the metastore thread, it is safe to use these versions.
        int latestCatalogVersion = catalogService.latestCatalogVersion();
        int earliestCatalogVersion = catalogService.earliestCatalogVersion();

        for (int catalogVersion = latestCatalogVersion; catalogVersion >= earliestCatalogVersion; catalogVersion--) {
            for (CatalogIndexDescriptor indexDescriptor : catalogService.indexes(catalogVersion, tableId)) {
                if (indexDescriptor.status() == BUILDING) {
                    return indexDescriptor;
                }
            }
        }

        return null;
    }

    /**
     * Extracts begin timestamp of a read-write transaction from a request.
     *
     * @param request Read-write replica request.
     */
    static HybridTimestamp beginRwTxTs(ReadWriteReplicaRequest request) {
        return TransactionIds.beginTimestamp(request.transactionId());
    }

    /**
     * Returns the active catalog version by begin timestamp of a read-write transaction from a request.
     *
     * @param catalogService Catalog service.
     * @param request Read-write replica request.
     */
    static int rwTxActiveCatalogVersion(CatalogService catalogService, ReadWriteReplicaRequest request) {
        HybridTimestamp beginRwTxTs = beginRwTxTs(request);

        return catalogService.activeCatalogVersion(beginRwTxTs.longValue());
    }
}
