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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.event.EventProducer;
import org.jetbrains.annotations.Nullable;

/**
 * Catalog service provides methods to access schema object's descriptors of exact version and/or last actual version at given timestamp,
 * which is logical point-in-time.
 *
 * <p>Catalog service listens distributed schema update event, stores/restores schema evolution history (schema versions) for time-travelled
 * queries purposes and for lazy data evolution purposes.
 *
 * <p>Notes:</p>
 * <ul>
 *     <li>Events are fired in the metastore thread.</li>
 * </ul>
 *
 * <p>TBD: events
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

    @Nullable Catalog catalog(int catalogVersion);

    /**
     * Returns table descriptor by the given schema name and table name at given timestamp.
     *
     * @return Table descriptor or {@code null} if table not found.
     */
    @Nullable CatalogTableDescriptor table(String schemaName, String tableName, long timestamp);

    /**
     * Returns table descriptor by the given table ID and given timestamp.
     *
     * @return Table descriptor or {@code null} if table not found.
     */
    @Nullable CatalogTableDescriptor table(int tableId, long timestamp);

    /**
     * Returns table descriptor by the given table ID and catalog version.
     *
     * @return Table descriptor or {@code null} if table not found.
     */
    @Nullable CatalogTableDescriptor table(int tableId, int catalogVersion);

    Collection<CatalogTableDescriptor> tables(int catalogVersion);

    /**
     * Returns a descriptor for <em>alive</em> index by the given schema name and index name at given timestamp,
     * that is an index that has not been dropped yet at a given point in time.
     *
     * <p>This effectively means that the index must be present in the Catalog and not in the {@link CatalogIndexStatus#STOPPING}
     * state.
     */
    @Nullable CatalogIndexDescriptor aliveIndex(String schemaName, String indexName, long timestamp);

    @Nullable CatalogIndexDescriptor index(int indexId, long timestamp);

    /**
     * Returns index descriptor by the given index ID and catalog version.
     *
     * @return Index descriptor or {@code null} if index not found.
     */
    @Nullable CatalogIndexDescriptor index(int indexId, int catalogVersion);

    Collection<CatalogIndexDescriptor> indexes(int catalogVersion);

    List<CatalogIndexDescriptor> indexes(int catalogVersion, int tableId);

    @Nullable CatalogSchemaDescriptor schema(int catalogVersion);

    @Nullable CatalogSchemaDescriptor schema(@Nullable String schemaName, int catalogVersion);

    @Nullable CatalogSchemaDescriptor schema(int schemaId, int catalogVersion);

    @Nullable CatalogZoneDescriptor zone(String zoneName, long timestamp);

    @Nullable CatalogZoneDescriptor zone(int zoneId, long timestamp);

    @Nullable CatalogZoneDescriptor zone(int zoneId, int catalogVersion);

    Collection<CatalogZoneDescriptor> zones(int catalogVersion);

    @Nullable CatalogSchemaDescriptor activeSchema(long timestamp);

    @Nullable CatalogSchemaDescriptor activeSchema(@Nullable String schemaName, long timestamp);

    int activeCatalogVersion(long timestamp);

    /** Returns the earliest registered version of the catalog. */
    int earliestCatalogVersion();

    /** Returns the latest registered version of the catalog. */
    int latestCatalogVersion();

    /**
     * Returns a future, which completes, when catalog of given version will be available.
     *
     * @param version Catalog version to wait for.
     */
    CompletableFuture<Void> catalogReadyFuture(int version);

    /**
     * Returns a future, which completes when empty catalog is initialised. Otherwise this future completes upon startup.
     */
    CompletableFuture<Void> catalogInitializationFuture();
}
