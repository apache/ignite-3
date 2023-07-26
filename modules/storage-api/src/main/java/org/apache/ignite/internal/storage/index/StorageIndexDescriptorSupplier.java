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

package org.apache.ignite.internal.storage.index;

import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.jetbrains.annotations.Nullable;

/**
 * Index descriptor supplier from catalog.
 */
// TODO: IGNITE-19717 Get rid of it
public class StorageIndexDescriptorSupplier {
    private final CatalogService catalogService;

    /**
     * Constructor.
     *
     * @param catalogService Catalog service.
     */
    public StorageIndexDescriptorSupplier(CatalogService catalogService) {
        this.catalogService = catalogService;
    }

    /**
     * Returns an index descriptor by its ID, {@code null} if absent.
     *
     * @param id Index ID.
     */
    public @Nullable StorageIndexDescriptor get(int id) {
        int catalogVersion = catalogService.latestCatalogVersion();

        CatalogIndexDescriptor index = catalogService.index(id, catalogVersion);

        if (index == null) {
            return null;
        }

        CatalogTableDescriptor table = catalogService.table(index.tableId(), catalogVersion);

        assert table != null : "tableId=" + index.tableId() + ", indexId=" + index.id();

        return StorageIndexDescriptor.create(table, index);
    }
}
