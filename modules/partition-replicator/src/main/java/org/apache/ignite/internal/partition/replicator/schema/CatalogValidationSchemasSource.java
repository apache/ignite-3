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

package org.apache.ignite.internal.partition.replicator.schema;

import static java.util.stream.Collectors.toList;

import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.SchemaManager;

/**
 * An implementation over {@link CatalogService}.
 */
public class CatalogValidationSchemasSource implements ValidationSchemasSource {
    private final CatalogService catalogService;

    private final SchemaManager schemaManager;

    private final ConcurrentMap<CatalogVersionsSpan, List<FullTableSchema>> catalogVersionSpansCache = new ConcurrentHashMap<>();

    // TODO: Remove entries from cache when compacting schemas in SchemaManager https://issues.apache.org/jira/browse/IGNITE-20789
    private final ConcurrentMap<CatalogVersionToTableVersionSpan, List<FullTableSchema>> catalogVersionToTableVersionSpansCache
            = new ConcurrentHashMap<>();

    /** Constructor. */
    public CatalogValidationSchemasSource(CatalogService catalogService, SchemaManager schemaManager) {
        this.catalogService = catalogService;
        this.schemaManager = schemaManager;
    }

    @Override
    public CompletableFuture<Void> waitForSchemaAvailability(int tableId, int schemaVersion) {
        return schemaManager.schemaRegistry(tableId)
                .schemaAsync(schemaVersion)
                .thenApply(unused -> null);
    }

    @Override
    public List<FullTableSchema> tableSchemaVersionsBetween(int tableId, HybridTimestamp fromIncluding, HybridTimestamp toIncluding) {
        // It is safe to access the Catalog as the caller must have already waited till the Catalog is up-to-date with the timestamps.
        int fromCatalogVersion = catalogService.activeCatalogVersion(fromIncluding.longValue());
        int toCatalogVersion = catalogService.activeCatalogVersion(toIncluding.longValue());

        return catalogVersionSpansCache.computeIfAbsent(
                new CatalogVersionsSpan(tableId, fromCatalogVersion, toCatalogVersion),
                key -> tableSchemaVersionsBetweenCatalogVersions(tableId, fromCatalogVersion, toCatalogVersion)
        );
    }

    @Override
    public List<FullTableSchema> tableSchemaVersionsBetween(int tableId, HybridTimestamp fromIncluding, int toTableVersionIncluding) {
        // It is safe to access the Catalog as the caller must have already waited till the Catalog is up-to-date.
        int fromCatalogVersion = catalogService.activeCatalogVersion(fromIncluding.longValue());

        return catalogVersionToTableVersionSpansCache.computeIfAbsent(
                new CatalogVersionToTableVersionSpan(tableId, fromCatalogVersion, toTableVersionIncluding),
                key -> tableSchemaVersionsBetweenCatalogAndTableVersions(tableId, fromCatalogVersion, toTableVersionIncluding)
        );
    }

    private List<FullTableSchema> tableSchemaVersionsBetweenCatalogVersions(int tableId, int fromCatalogVersion, int toCatalogVersion) {
        return tableVersionsBetween(tableId, fromCatalogVersion, toCatalogVersion)
                .map(entry -> fullSchemaFromCatalog(entry.getKey(), entry.getValue()))
                .filter(new Predicate<>() {
                    FullTableSchema prevSchema = null;

                    @Override
                    public boolean test(FullTableSchema tableSchema) {
                        if (prevSchema != null && !tableSchema.hasValidatableChangeFrom(prevSchema)) {
                            return false;
                        }

                        prevSchema = tableSchema;

                        return true;
                    }
                })
                .collect(toList());
    }

    private List<FullTableSchema> tableSchemaVersionsBetweenCatalogAndTableVersions(
            int tableId,
            int fromCatalogVersion,
            int toTableVersion
    ) {
        Predicate<CatalogTableDescriptor> tableDescriptorFilter = new Predicate<>() {
            int prevVersion = Integer.MIN_VALUE;

            @Override
            public boolean test(CatalogTableDescriptor table) {
                if (table.latestSchemaVersion() == prevVersion) {
                    return false;
                }

                assert prevVersion == Integer.MIN_VALUE || table.latestSchemaVersion() == prevVersion + 1
                        : String.format("Table version is expected to be prevVersion+1, but version is %d and prevVersion is %d",
                        table.latestSchemaVersion(), prevVersion);

                prevVersion = table.latestSchemaVersion();

                return true;
            }
        };

        return tableVersionsBetween(tableId, fromCatalogVersion, catalogService.latestCatalogVersion())
                .filter(entry -> tableDescriptorFilter.test(entry.getValue()))
                .takeWhile(entry -> entry.getValue().latestSchemaVersion() <= toTableVersion)
                .map(entry -> fullSchemaFromCatalog(entry.getKey(), entry.getValue()))
                .collect(toList());
    }

    // It's ok to use Stream as the results of the methods that call this are cached.
    private Stream<SimpleEntry<Catalog, CatalogTableDescriptor>> tableVersionsBetween(
            int tableId,
            int fromCatalogVersionIncluding,
            int toCatalogVersionIncluding
    ) {
        return IntStream.rangeClosed(fromCatalogVersionIncluding, toCatalogVersionIncluding)
                .mapToObj(ver -> {
                    Catalog catalog = catalogService.catalog(ver);
                    CatalogTableDescriptor descriptor = catalog.table(tableId);

                    if (descriptor == null) {
                        return null;
                    }

                    return new SimpleEntry<>(catalog, descriptor);
                })
                .takeWhile(Objects::nonNull);
    }

    private static FullTableSchema fullSchemaFromCatalog(Catalog catalog, CatalogTableDescriptor tableDescriptor) {
        assert tableDescriptor != null;

        return new FullTableSchema(
                catalog.version(),
                tableDescriptor.latestSchemaVersion(),
                tableDescriptor.id(),
                tableDescriptor.name(),
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

            if (tableId != that.tableId) {
                return false;
            }
            if (fromCatalogVersion != that.fromCatalogVersion) {
                return false;
            }
            return toCatalogVersion == that.toCatalogVersion;
        }

        @Override
        public int hashCode() {
            int result = tableId;
            result = 31 * result + fromCatalogVersion;
            result = 31 * result + toCatalogVersion;
            return result;
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

            if (tableId != that.tableId) {
                return false;
            }
            if (fromCatalogVersion != that.fromCatalogVersion) {
                return false;
            }
            return toTableVersion == that.toTableVersion;
        }

        @Override
        public int hashCode() {
            int result = tableId;
            result = 31 * result + fromCatalogVersion;
            result = 31 * result + toTableVersion;
            return result;
        }
    }
}
