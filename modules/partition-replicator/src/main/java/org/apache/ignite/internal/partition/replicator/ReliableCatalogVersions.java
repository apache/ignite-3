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

package org.apache.ignite.internal.partition.replicator;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogNotFoundException;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.SchemaSyncService;

/**
 * Logic to obtain catalog versions in a reliable (with respect to schema sync) way.
 */
public class ReliableCatalogVersions {
    private final SchemaSyncService schemaSyncService;
    private final CatalogService catalogService;

    /** Constructor. */
    public ReliableCatalogVersions(SchemaSyncService schemaSyncService, CatalogService catalogService) {
        this.schemaSyncService = schemaSyncService;
        this.catalogService = catalogService;
    }

    /**
     * Returns Catalog version corresponding to the given timestamp. Must only be called if there is a guarantee that
     * the corresponding version was not yet compacted.
     *
     * <p>This should only be used when the startup procedure is complete as it relies on the catalog to be started.
     *
     * @param ts Timestamp for which a Catalog version is to be obtained.
     */
    public CompletableFuture<Integer> reliableCatalogVersionFor(HybridTimestamp ts) {
        return schemaSyncService.waitForMetadataCompleteness(ts)
                .thenApply(unused -> catalogService.activeCatalogVersion(ts.longValue()));
    }

    /**
     * Returns Catalog version corresponding to the given timestamp, or a later one. A later version will be returned
     * if the exactly matching version is already compacted out.
     *
     * <p>This should only be used when the startup procedure is complete as it relies on the catalog to be started.
     *
     * @param ts Timestamp.
     */
    public CompletableFuture<Integer> safeReliableCatalogVersionFor(HybridTimestamp ts) {
        return safeReliableCatalogFor(ts).thenApply(Catalog::version);
    }

    /**
     * Returns Catalog corresponding to the given timestamp, or a later one. A later version will be returned
     * if the exactly matching version is already compacted out.
     *
     * <p>This should only be used when the startup procedure is complete as it relies on the catalog to be started.
     *
     * @param ts Timestamp.
     */
    public CompletableFuture<Catalog> safeReliableCatalogFor(HybridTimestamp ts) {
        return schemaSyncService.waitForMetadataCompleteness(ts)
                .thenApply(unused -> {
                    try {
                        return catalogService.activeCatalog(ts.longValue());
                    } catch (CatalogNotFoundException e) {
                        return catalogService.earliestCatalog();
                    }
                });
    }
}
