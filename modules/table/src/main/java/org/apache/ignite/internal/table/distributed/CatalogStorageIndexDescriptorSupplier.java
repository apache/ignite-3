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

import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptorSupplier;
import org.jetbrains.annotations.Nullable;

/**
 * Index descriptor supplier from catalog.
 */
class CatalogStorageIndexDescriptorSupplier implements StorageIndexDescriptorSupplier {
    private final CatalogService catalogService;

    private final LowWatermark lowWatermark;

    CatalogStorageIndexDescriptorSupplier(CatalogService catalogService, LowWatermark lowWatermark) {
        this.catalogService = catalogService;
        this.lowWatermark = lowWatermark;
    }

    @Override
    public @Nullable StorageIndexDescriptor get(int indexId) {
        // Search for the index in the catalog history, which versions correspond to (lowWatermark, now] timestamp range.
        int latestCatalogVersion = catalogService.latestCatalogVersion();

        // Get the current Low Watermark value. Since this class is used only on recovery, we expect that this value will not change
        // concurrently.
        HybridTimestamp lowWatermarkTimestamp = lowWatermark.getLowWatermark();

        int earliestCatalogVersion = lowWatermarkTimestamp == null
                ? catalogService.earliestCatalogVersion()
                : catalogService.activeCatalogVersion(lowWatermarkTimestamp.longValue());

        for (int catalogVersion = latestCatalogVersion; catalogVersion >= earliestCatalogVersion; catalogVersion--) {
            Catalog catalog = catalogService.catalog(catalogVersion);

            CatalogIndexDescriptor index = catalog.index(indexId);

            if (index != null) {
                CatalogTableDescriptor table = catalog.table(index.tableId());

                assert table != null : "tableId=" + index.tableId() + ", indexId=" + index.id();

                return StorageIndexDescriptor.create(table, index);
            }
        }

        return null;
    }
}
