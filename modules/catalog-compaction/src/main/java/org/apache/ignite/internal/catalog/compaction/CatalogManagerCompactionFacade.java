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

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManagerImpl;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.jetbrains.annotations.Nullable;

/**
 * Class contains utility methods for interacting with the catalog manager.
 * These methods are only needed for catalog compaction routine.
 */
class CatalogManagerCompactionFacade {
    private final CatalogManagerImpl catalogManager;

    CatalogManagerCompactionFacade(CatalogManagerImpl catalogManager) {
        this.catalogManager = catalogManager;
    }

    /**
     * Scans catalog versions in a given time interval (including interval boundaries).
     * Extracts all tables contained in these catalog versions and creates a mapping
     * tableId -> number of partitions in this table.
     *
     * @param minTsInclusive Lower timestamp (inclusive).
     * @param maxTsInclusive Upper timestamp (inclusive).
     * @return Mapping tableId to number of partitions in this table.
     */
    Int2IntMap collectTablesWithPartitionsBetween(long minTsInclusive, long maxTsInclusive) {
        Int2IntMap tablesWithPartitions = new Int2IntOpenHashMap();
        int curVer = catalogManager.activeCatalogVersion(minTsInclusive);
        int lastVer = catalogManager.activeCatalogVersion(maxTsInclusive);

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

    /**
     * Returns the catalog version that is active at the given timestamp.
     *
     * @param timestamp Timestamp.
     * @return Catalog or {@code null} if such version of the catalog doesn't exist.
     */
    @Nullable Catalog catalogByTsNullable(long timestamp) {
        try {
            int catalogVer = catalogManager.activeCatalogVersion(timestamp);

            return catalogManager.catalog(catalogVer - 1);
        } catch (IllegalStateException ignore) {
            return null;
        }
    }

    CompletableFuture<Boolean> compactCatalog(int version) {
        return catalogManager.compactCatalog(version);
    }
}
