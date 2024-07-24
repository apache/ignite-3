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

package org.apache.ignite.internal.catalog.compaction;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManagerImpl;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.hlc.ClockService;
import org.jetbrains.annotations.Nullable;

/**
 * Class contains utility methods for interacting with the catalog manager.
 * These methods are only needed for catalog compaction routine.
 */
class CatalogManagerHelper {
    private final CatalogManagerImpl catalogManager;

    private final ClockService clockService;

    CatalogManagerHelper(CatalogManagerImpl catalogManager, ClockService clockService) {
        this.catalogManager = catalogManager;
        this.clockService = clockService;
    }

    Map<Integer, Integer> findAllTablesSince(long beginTs) {
        Map<Integer, Integer> tablesWithPartitions = new HashMap<>();
        int curVer = catalogManager.activeCatalogVersion(beginTs);
        int lastVer = catalogManager.activeCatalogVersion(clockService.nowLong());

        do {
            Catalog catalog = catalogManager.catalog(curVer);

            assert catalog != null : "ver=" + curVer + ", last=" + lastVer;

            for (CatalogTableDescriptor table : catalog.tables()) {
                CatalogZoneDescriptor zone = catalog.zone(table.zoneId());

                assert zone != null : table.zoneId();

                tablesWithPartitions.put(table.id(), zone.partitions());
            }
        } while (++curVer <= lastVer);

        return tablesWithPartitions;
    }

    @Nullable Catalog catalogByTsNullable(long ts) {
        try {
            int catalogVer = catalogManager.activeCatalogVersion(ts);

            return catalogManager.catalog(catalogVer - 1);
        } catch (IllegalStateException e) {
            return null;
        }
    }

    CompletableFuture<Boolean> compactCatalog(Catalog catalog) {
        return catalogManager.compactCatalog(catalog);
    }
}
