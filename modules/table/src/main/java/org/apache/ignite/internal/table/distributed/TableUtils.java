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

package org.apache.ignite.internal.table.distributed;

import static java.util.Collections.unmodifiableSet;
import static org.apache.ignite.internal.util.CollectionUtils.view;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogNotFoundException;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogObjectDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TransactionIds;
import org.jetbrains.annotations.Nullable;

/** Contains common helper methods and fields for use within a module. */
public class TableUtils {
    /**
     * Returns index IDs for the table of interest from the catalog for the active catalog version at the beginning timestamp of read-write
     * transaction.
     *
     * <p>NOTE: To avoid races and errors, it is important to call this method after schema sync at beginTs or to be sure that the expected
     * catalog version at beginTs is already active.</p>
     *
     * @param catalogService Catalog service.
     * @param txId Read-write transaction ID for which indexes will be selected.
     * @param tableId Table ID for which indexes will be selected.
     * @return Ascending sorted list of index IDs.
     */
    // TODO: IGNITE-21476 Select indexes by operation timestamp of read-write transaction
    public static List<Integer> indexIdsAtRwTxBeginTs(CatalogService catalogService, UUID txId, int tableId) {
        HybridTimestamp beginTs = TransactionIds.beginTimestamp(txId);

        Catalog catalog = catalogService.activeCatalog(beginTs.longValue());

        List<CatalogIndexDescriptor> indexes = catalog.indexes(tableId);

        assert !indexes.isEmpty() : String.format("txId=%s, tableId=%s, catalogVersion=%s", txId, tableId, catalog.version());

        return view(indexes, CatalogObjectDescriptor::id);
    }

    /**
     * Returns index IDs for the table of interest from the catalog for the active catalog version at the beginning timestamp of read-write
     * transaction or {@code null} if catalog version was not found.
     *
     * @param catalogService Catalog service.
     * @param txId Read-write transaction ID for which indexes will be selected.
     * @param tableId Table ID for which indexes will be selected.
     * @return Ascending sorted list of index IDs or {@code null} if catalog version was not found.
     */
    // TODO IGNITE-24368 Use IndexMetaStorage to get the list of indices.
    public static @Nullable List<Integer> indexIdsAtRwTxBeginTsOrNull(CatalogService catalogService, UUID txId, int tableId) {
        try {
            return indexIdsAtRwTxBeginTs(catalogService, txId, tableId);
        } catch (CatalogNotFoundException e) {
            return null;
        }
    }

    /**
     * Collects all IDs of tables that were not dropped or were dropped, but should not have been destroyed yet due to a low watermark
     * (if the catalog version in which the table was removed were less than or equal to the active catalog version of the low watermark).
     *
     * <p>This method scans the Catalog, so it should be used keeping in mind potential concurrent modification to the Catalog.
     * For instance, it's safe to use it before the node has been started up (namely, before Metastorage watches deployment).
     *
     * @param catalogService Catalog service.
     * @param lowWatermark Low watermark, {@code null} if it has never been updated.
     */
    static Set<Integer> aliveTables(CatalogService catalogService, @Nullable HybridTimestamp lowWatermark) {
        int oldestAliveVersion;
        if (lowWatermark != null) {
            oldestAliveVersion = catalogService.activeCatalogVersion(lowWatermark.longValue());
        } else {
            oldestAliveVersion = catalogService.earliestCatalogVersion();
        }

        Set<Integer> ids = new HashSet<>();
        for (int version = oldestAliveVersion; version <= catalogService.latestCatalogVersion(); version++) {
            for (CatalogTableDescriptor table : catalogService.catalog(version).tables()) {
                ids.add(table.id());
            }
        }

        return unmodifiableSet(ids);
    }

    /**
     * Checks whether the transaction meets the requirements for a direct transaction or not.
     *
     * @param tx Transaction of {@code null}.
     * @return True of direct flow is applicable for the transaction.
     */
    public static boolean isDirectFlowApplicable(@Nullable InternalTransaction tx) {
        // TODO https://issues.apache.org/jira/browse/IGNITE-24218 Remove this method ot use tx == null || tx.direct() instead.
        return tx == null || (tx.implicit() && tx.isReadOnly());
    }
}
