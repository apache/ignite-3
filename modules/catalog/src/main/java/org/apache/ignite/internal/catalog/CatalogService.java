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
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.manager.EventListener;
import org.jetbrains.annotations.Nullable;

/**
 * Catalog service provides methods to access schema object's descriptors of exact version and/or last actual version at given timestamp,
 * which is logical point-in-time.
 *
 * <p>Catalog service listens distributed schema update event, stores/restores schema evolution history (schema versions) for time-travelled
 * queries purposes and for lazy data evolution purposes.
 *
 * <p>TBD: events
 */
public interface CatalogService {
    String DEFAULT_SCHEMA_NAME = "PUBLIC";

    String DEFAULT_ZONE_NAME = "Default";

    @Nullable CatalogTableDescriptor table(String tableName, long timestamp);

    @Nullable CatalogTableDescriptor table(int tableId, long timestamp);

    @Nullable CatalogTableDescriptor table(int tableId, int catalogVersion);

    Collection<CatalogTableDescriptor> tables(int catalogVersion);

    @Nullable CatalogIndexDescriptor index(String indexName, long timestamp);

    @Nullable CatalogIndexDescriptor index(int indexId, long timestamp);

    @Nullable CatalogIndexDescriptor index(int indexId, int catalogVersion);

    Collection<CatalogIndexDescriptor> indexes(int catalogVersion);

    CatalogSchemaDescriptor schema(int version);

    CatalogSchemaDescriptor schema(@Nullable String schemaName, int version);

    CatalogZoneDescriptor zone(String zoneName, long timestamp);

    CatalogZoneDescriptor zone(int zoneId, long timestamp);

    CatalogSchemaDescriptor activeSchema(long timestamp);

    CatalogSchemaDescriptor activeSchema(@Nullable String schemaName, long timestamp);

    int activeCatalogVersion(long timestamp);

    /**
     * Returns the latest registered version of the catalog.
     *
     * <p>NOTE: This method should only be used at the start of components that may be removed or moved in the future.
     */
    int latestCatalogVersion();

    void listen(CatalogEvent evt, EventListener<CatalogEventParameters> closure);

    /**
     * Returns a future, which completes, when catalog of given version will be available.
     */
    CompletableFuture<Void> catalogReadyFuture(int ver);
}
