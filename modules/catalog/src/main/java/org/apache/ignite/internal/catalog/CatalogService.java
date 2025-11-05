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

/**
 * Catalog service provides methods to access catalog snapshots of specific version or point-in-time.
 *
 * <p>Catalog service listens distributed schema update event, stores/restores schema evolution history (schema versions) for time-travelled
 * queries purposes and for lazy data evolution purposes.
 *
 * <p>Notes:</p>
 * <ul>
 *     <li>Each catalog update applied and registers a new catalog snapshot first, then fire catalog events.</li>
 *     <li>Events are fired in the metastore thread in order they occurs.</li>
 *     <li>The order, which listeners are notified for the same event, is undefined. See {@link #catalogReadyFuture(int)}</li>
 *     <li>Catalog version readiness doesn't mean the version is active. Before getting active Catalog version by a timestamp, the user must
 *     take care of the CatalogService has seen actual metadata. See SchemaSyncService#waitForMetadataCompleteness(long) for details.</li>
 * </ul>
 *
 * @see CatalogEvent Full list of events, which is fired by the catalog service.
 * @see CatalogManager The manager, which provides catalog manipulation methods and is responsible for managing distributed operations.
 */
// TODO https://issues.apache.org/jira/browse/IGNITE-24322: Fix links to the SchemaSyncService class in javadocs.
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
     * Retrieves the catalog of the specified version.
     *
     * <p>Note: in general case, we expect the requested version is always exists. It's up to user to ensure the version is visible - the
     * requested version was already processed (see SchemaSyncService#waitForMetadataCompleteness(long)) and wasn't sunk under the garbage
     * collector watermark.
     *
     * <p>Note: the version may be not ready yet. If the version readiness is important use {@link #catalogReadyFuture(int)} method.
     *
     * <p>Note: the requested version can be not activated yet. To get the activated version, use {@link #activeCatalog(long)} method.
     *
     * @param catalogVersion The version of the catalog to retrieve.
     * @return The catalog for the specified version.
     * @throws CatalogNotFoundException If the catalog of requested version was not found.
     */
    Catalog catalog(int catalogVersion);

    /**
     * Retrieves the catalog, which was actual at the specified timestamp.
     *
     * <p>Note: It's up to user to ensure the timestamp is valid. It should respect schema-synchronization timeout and garbage collector.
     * See SchemaSyncService#waitForMetadataCompleteness(long) method for details.
     *
     * @param timestamp The point-in-time to retrieve the catalog of actual version.
     * @return The active catalog at the specified timestamp.
     * @throws CatalogNotFoundException If the catalog of requested version was not found.
     */
    Catalog activeCatalog(long timestamp);

    /**
     * Retrieves the actual catalog version at the specified timestamp.
     *
     * <p>Note: It's up to user to ensure the timestamp is valid. It should respect schema-synchronization timeout and garbage collector.
     * See SchemaSyncService#waitForMetadataCompleteness(long) method for details.
     *
     * @param timestamp The point-in-time to retrieve the actual catalog version.
     * @return The active catalog version at the specified timestamp.
     */
    int activeCatalogVersion(long timestamp);

    /**
     * Returns the earliest available version of the catalog.
     *
     * <p>Note: Garbage collector disposes earliest versions sporadically, when they sink under the low watermark that is become unavailable
     * for the historical queries.
     *
     * @return The earliest registered version of the catalog.
     */
    int earliestCatalogVersion();

    /**
     * Returns the earliest available catalog.
     *
     * @return The earliest registered catalog.
     */
    Catalog earliestCatalog();

    /**
     * Returns the latest registered version of the catalog.
     *
     * <p>Note: This version can be used to retrieve a latest Catalog snapshot, but gives no guarantees that all components have seen and
     * processed all events related to this version. If you need this guarantee, please, use {@link #catalogReadyFuture(int)} method.
     *
     * @return The latest registered version of the catalog.
     * @see #catalogReadyFuture(int)
     */
    int latestCatalogVersion();

    /**
     * Returns the latest registered catalog. Effectively returns a catalog with version from {@link #latestCatalogVersion}. See
     * the last method's note about it's guarantees.
     *
     * @return The latest registered catalog
     */
    default Catalog latestCatalog() {
        return catalog(latestCatalogVersion());
    }

    /**
     * Returns a future, which completes, when catalog of given version will be available.
     *
     * <p>Note: The future completeness guarantees all components have seen and processed the requested version. However, no guarantee
     * the version is activated.
     *
     * @param version The catalog version to wait for.
     * @return A future that completes when the catalog of the given version becomes available.
     */
    CompletableFuture<Void> catalogReadyFuture(int version);
}
