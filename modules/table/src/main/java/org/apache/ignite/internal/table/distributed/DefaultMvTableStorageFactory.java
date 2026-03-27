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

import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.table.distributed.storage.NullStorageEngine;

/**
 * Default implementation of {@link MvTableStorageFactory} that creates table storages via {@link DataStorageManager}.
 */
public final class DefaultMvTableStorageFactory implements MvTableStorageFactory {
    private final DataStorageManager dataStorageMgr;

    private final CatalogService catalogService;

    private final LowWatermark lowWatermark;

    /**
     * Constructor.
     *
     * @param dataStorageMgr Data storage manager.
     * @param catalogService Catalog service.
     * @param lowWatermark Low watermark.
     */
    public DefaultMvTableStorageFactory(
            DataStorageManager dataStorageMgr,
            CatalogService catalogService,
            LowWatermark lowWatermark
    ) {
        this.dataStorageMgr = dataStorageMgr;
        this.catalogService = catalogService;
        this.lowWatermark = lowWatermark;
    }

    @Override
    public MvTableStorage createMvTableStorage(CatalogTableDescriptor tableDescriptor, CatalogZoneDescriptor zoneDescriptor) {
        StorageEngine engine = dataStorageMgr.engineByStorageProfile(tableDescriptor.storageProfile());

        if (engine == null) {
            // Create a placeholder to allow Table object being created.
            engine = new NullStorageEngine();
        }

        return engine.createMvTable(
                new StorageTableDescriptor(tableDescriptor.id(), zoneDescriptor.partitions(), tableDescriptor.storageProfile()),
                new CatalogStorageIndexDescriptorSupplier(catalogService, lowWatermark)
        );
    }
}
