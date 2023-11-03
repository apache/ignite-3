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
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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

    private final ConcurrentMap<CatalogVersionsSpan, List<FullTableSchema>> catalogVersionSpansCache = new ConcurrentHashMap<>();

    // TODO: Remove entries from cache when compacting Catalog https://issues.apache.org/jira/browse/IGNITE-20790
    // TODO: Remove entries from cache when compacting schemas in SchemaManager https://issues.apache.org/jira/browse/IGNITE-20789
    private final ConcurrentMap<CatalogVersionToTableVersionSpan, List<FullTableSchema>> catalogVersionToTableVersionSpansCache
            = new ConcurrentHashMap<>();

    /** Constructor. */
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

    private List<FullTableSchema> tableSchemaVersionsBetweenCatalogVersions(int tableId, int fromCatalogVersion, int toCatalogVersion) {
        return catalogService.tableVersionsBetween(tableId, fromCatalogVersion, toCatalogVersion)
                .map(CatalogSchemas::fullSchemaFromTableDescriptor)
                .collect(toList());
    }

    @Override
    public List<FullTableSchema> tableSchemaVersionsBetween(int tableId, HybridTimestamp fromIncluding, HybridTimestamp toIncluding) {
        int fromCatalogVersion = catalogService.activeCatalogVersion(fromIncluding.longValue());
        int toCatalogVersion = catalogService.activeCatalogVersion(toIncluding.longValue());

        return catalogVersionSpansCache.computeIfAbsent(
                new CatalogVersionsSpan(tableId, fromCatalogVersion, toCatalogVersion),
                key -> tableSchemaVersionsBetweenCatalogVersions(tableId, fromCatalogVersion, toCatalogVersion)
        );
    }

    @Override
    public List<FullTableSchema> tableSchemaVersionsBetween(int tableId, HybridTimestamp fromIncluding, int toIncluding) {
        int fromCatalogVersion = catalogService.activeCatalogVersion(fromIncluding.longValue());

        return catalogVersionToTableVersionSpansCache.computeIfAbsent(
                new CatalogVersionToTableVersionSpan(tableId, fromCatalogVersion, toIncluding),
                key -> tableSchemaVersionsBetweenCatalogAndTableVersions(tableId, fromCatalogVersion, toIncluding)
        );
    }

    private List<FullTableSchema> tableSchemaVersionsBetweenCatalogAndTableVersions(
            int tableId,
            int fromCatalogVersion,
            int toTableVersion
    ) {
        return catalogService.tableVersionsBetween(tableId, fromCatalogVersion, Integer.MAX_VALUE)
                .takeWhile(tableDescriptor -> tableDescriptor.tableVersion() <= toTableVersion)
                .map(CatalogSchemas::fullSchemaFromTableDescriptor)
                .collect(toList());
    }

    private static FullTableSchema fullSchemaFromTableDescriptor(CatalogTableDescriptor tableDescriptor) {
        return new FullTableSchema(
                tableDescriptor.tableVersion(),
                tableDescriptor.id(),
                tableDescriptor.columns()
        );
    }

    private static class CatalogVersionsSpan {
        private final int tableId;
        private final int fromCatalogVersion;
        private final int toCatalogVersion;

        private CatalogVersionsSpan(int tableId, int fromCatalogVersion, int toCatalogVersion) {
            this.tableId = tableId;
            this.fromCatalogVersion = fromCatalogVersion;
            this.toCatalogVersion = toCatalogVersion;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CatalogVersionsSpan that = (CatalogVersionsSpan) o;
            return tableId == that.tableId && fromCatalogVersion == that.fromCatalogVersion && toCatalogVersion == that.toCatalogVersion;
        }

        @Override
        public int hashCode() {
            return Objects.hash(tableId, fromCatalogVersion, toCatalogVersion);
        }
    }

    private static class CatalogVersionToTableVersionSpan {
        private final int tableId;
        private final int fromCatalogVersion;
        private final int toTableVersion;

        private CatalogVersionToTableVersionSpan(int tableId, int fromCatalogVersion, int toTableVersion) {
            this.tableId = tableId;
            this.fromCatalogVersion = fromCatalogVersion;
            this.toTableVersion = toTableVersion;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CatalogVersionToTableVersionSpan that = (CatalogVersionToTableVersionSpan) o;
            return tableId == that.tableId && fromCatalogVersion == that.fromCatalogVersion && toTableVersion == that.toTableVersion;
        }

        @Override
        public int hashCode() {
            return Objects.hash(tableId, fromCatalogVersion, toTableVersion);
        }
    }
}
