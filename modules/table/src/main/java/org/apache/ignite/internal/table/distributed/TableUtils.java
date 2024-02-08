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

import static org.apache.ignite.internal.util.CollectionUtils.view;

import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogObjectDescriptor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.tx.TransactionIds;

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

        int catalogVersion = catalogService.activeCatalogVersion(beginTs.longValue());

        List<CatalogIndexDescriptor> indexes = catalogService.indexes(catalogVersion, tableId);

        assert !indexes.isEmpty() : String.format("txId=%s, tableId=%s, catalogVersion=%s", txId, tableId, catalogVersion);

        return view(indexes, CatalogObjectDescriptor::id);
    }
}
