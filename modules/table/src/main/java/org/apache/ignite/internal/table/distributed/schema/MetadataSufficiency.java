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

package org.apache.ignite.internal.table.distributed.schema;

import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.SchemaSyncService;

/**
 * Logic that allows to determine whether the local schema metadata is sufficient.
 */
public class MetadataSufficiency {
    private MetadataSufficiency() {
        // Deny instantiation.
    }

    /**
     * Determines whether the local Catalog version is sufficient.
     *
     * @param requiredCatalogVersion Minimal catalog version that is required to present.
     * @param catalogService Catalog service.
     * @return {@code true} iff the local Catalog version is sufficient.
     */
    public static boolean isMetadataAvailableForCatalogVersion(int requiredCatalogVersion, CatalogService catalogService) {
        return catalogService.catalogReadyFuture(requiredCatalogVersion).isDone();
    }

    /**
     * Determines whether the local schema information is sufficient up to the given timestamp
     * (that is that schema sync on the timestamp will complete immediately without any waits).
     *
     * @param timestamp Minimal timestamp at which the metadata is required to present.
     * @param schemaSyncService Schema synchronization service.
     * @return {@code true} iff the local schema information is sufficient.
     */
    public static boolean isMetadataAvailableForTimestamp(HybridTimestamp timestamp, SchemaSyncService schemaSyncService) {
        return schemaSyncService.waitForMetadataCompleteness(timestamp).isDone();
    }
}
