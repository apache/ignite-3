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

package org.apache.ignite.internal.catalog;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.event.EventProducer;
import org.jetbrains.annotations.Nullable;

/**
 * Catalog service provides methods to access catalog snapshots of specific version or point-in-time.
 *
 * <p>Catalog service listens distributed schema update event, stores/restores schema evolution history (schema versions) for time-travelled
 * queries purposes and for lazy data evolution purposes.
 *
 * <p>Notes:</p>
 * <ul>
 *     <li>Events are fired in the metastore thread.</li>
 * </ul>
 *
 * @see CatalogEvent Full list of events, which is fired by the catalog service.
 * @see CatalogManager The manager, which provides catalog manipulation methods and is responsible for managing distributed operations.
 */
public interface CatalogService extends EventProducer<CatalogEvent, CatalogEventParameters> {
    /** System schema name. */
    String SYSTEM_SCHEMA_NAME = "SYSTEM";

    /**
     * Information schema - a system schema defined by SQL standard.
     * The schema provides system-views, which describe Catalog objects, and can be read by a user.
     */
    String INFORMATION_SCHEMA = "INFORMATION_SCHEMA";

    /**
     * Definition schema - a system schema defined by SQL standard.
     * The schema provides tables/sources for Catalog object’s metadata and can’t be accessed by a user directly.
     */
    String DEFINITION_SCHEMA = "DEFINITION_SCHEMA";

    /** Default storage profile. */
    String DEFAULT_STORAGE_PROFILE = "default";

    /**
     * Retrieves the catalog for the specified version.
     *
     * @param catalogVersion The version of the catalog to retrieve.
     * @return The catalog for the specified version, or {@code null} if not found.
     */
    @Nullable Catalog catalog(int catalogVersion);

    /**
     * Retrieves the catalog, which was actual at the specified timestamp.
     *
     * @param timestamp The point-in-time to retrieve the catalog of actual version.
     * @return The active catalog at the specified timestamp.
     */
    Catalog activeCatalog(long timestamp);

    /**
     * Retrieves the actual catalog version at the specified timestamp.
     *
     * @param timestamp The point-in-time to retrieve the actual catalog version.
     * @return The active catalog version at the specified timestamp.
     */
    int activeCatalogVersion(long timestamp);

    /**
     * Returns the earliest registered version of the catalog.
     *
     * @return The earliest registered version of the catalog.
     */
    int earliestCatalogVersion();

    /**
     * Returns the latest registered version of the catalog.
     *
     * @return The latest registered version of the catalog.
     */
    int latestCatalogVersion();

    /**
     * Returns a future, which completes, when catalog of given version will be available.
     *
     * @param version The catalog version to wait for.
     * @return A future that completes when the catalog of the given version becomes available.
     */
    CompletableFuture<Void> catalogReadyFuture(int version);

    /**
     * Returns a future, which completes when empty catalog is initialised. Otherwise this future completes upon startup.
     */
    CompletableFuture<Void> catalogInitializationFuture();
}
