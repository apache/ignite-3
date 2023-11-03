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

import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.SchemaManager;

/**
 * An implementation over {@link CatalogService}.
 */
public class CatalogSchemas implements Schemas {
    private final CatalogService catalogService;

    private final SchemaManager schemaManager;

    private final SchemaSyncService schemaSyncService;

    public CatalogSchemas(CatalogService catalogService, SchemaManager schemaManager, SchemaSyncService schemaSyncService) {
        this.catalogService = catalogService;
        this.schemaManager = schemaManager;
        this.schemaSyncService = schemaSyncService;
    }

    @Override
    public CompletableFuture<Void> waitForSchemasAvailability(HybridTimestamp ts) {
        return schemaSyncService.waitForMetadataCompleteness(ts);
    }

    @Override
    public CompletableFuture<Void> waitForSchemaAvailability(int tableId, int schemaVersion) {
        return schemaManager.schemaRegistry(tableId)
                .schemaAsync(schemaVersion)
                .thenApply(unused -> null);
    }

    @Override
    public List<FullTableSchema> tableSchemaVersionsBetween(int tableId, HybridTimestamp fromIncluding, HybridTimestamp toIncluding) {
        int fromCatalogVersion = catalogService.activeCatalogVersion(fromIncluding.longValue());
        int toCatalogVersion = catalogService.activeCatalogVersion(toIncluding.longValue());

        return catalogService.tableBetween(tableId, fromCatalogVersion, toCatalogVersion)
                .map(CatalogSchemas::fullSchemaFromTableDescriptor)
                .collect(toList());
    }

    private static FullTableSchema fullSchemaFromTableDescriptor(CatalogTableDescriptor tableDescriptor) {
        return new FullTableSchema(
                tableDescriptor.tableVersion(),
                tableDescriptor.id(),
                tableDescriptor.columns(),
                List.of()
        );
    }

    @Override
    public List<FullTableSchema> tableSchemaVersionsBetween(int tableId, HybridTimestamp fromIncluding, int toIncluding) {
        int fromCatalogVersion = catalogService.activeCatalogVersion(fromIncluding.longValue());

        return catalogService.tableBetween(tableId, fromCatalogVersion, Integer.MAX_VALUE)
                .takeWhile(tableDescriptor -> tableDescriptor.tableVersion() <= toIncluding)
                .map(CatalogSchemas::fullSchemaFromTableDescriptor)
                .collect(toList());
    }
}
