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

package org.apache.ignite.internal.schema.registry;

import static java.util.Collections.unmodifiableMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.ignite.internal.future.InFlightFutures;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.mapping.ColumnMapper;
import org.apache.ignite.internal.schema.mapping.ColumnMapping;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Caching registry of actual schema descriptors for a table.
 */
public class SchemaRegistryImpl implements SchemaRegistry {
    /** Cached schemas. */
    private final ConcurrentNavigableMap<Integer, SchemaDescriptor> schemaCache = new ConcurrentSkipListMap<>();

    /** Column mappers cache. */
    private final Map<Long, ColumnMapper> mappingCache = new ConcurrentHashMap<>();

    /**
     * Schema store. It's only safe to apply the function to version numbers for which there is guarantee that the schema was already saved
     * to the Metastore.
     */
    private final SchemaDescriptorLoader schemaDescriptorLoader;

    private final PendingComparableValuesTracker<Integer, Void> versionTracker = new PendingComparableValuesTracker<>(0);

    private final InFlightFutures inFlightTableSchemaFutures = new InFlightFutures();

    /**
     * Constructor.
     *
     * @param schemaDescriptorLoader Schema history.
     * @param initialSchema Initial schema.
     */
    public SchemaRegistryImpl(SchemaDescriptorLoader schemaDescriptorLoader, SchemaDescriptor initialSchema) {
        this.schemaDescriptorLoader = schemaDescriptorLoader;

        makeSchemaVersionAvailable(initialSchema);
    }

    private void makeSchemaVersionAvailable(SchemaDescriptor desc) {
        schemaCache.putIfAbsent(desc.version(), desc);

        versionTracker.update(desc.version(), null);
    }

    @Override
    public SchemaDescriptor lastKnownSchema() {
        return schema(lastKnownSchemaVersion());
    }

    @Override
    public SchemaDescriptor schema(int version) {
        int actualVersion = versionOrLatestForZero(version);

        SchemaDescriptor desc = getFromCacheOrLoad(actualVersion);

        if (desc != null) {
            return desc;
        }

        if (actualVersion <= 0 || actualVersion > schemaCache.lastKey()) {
            throw new SchemaRegistryException("Incorrect schema version requested: ver=" + actualVersion);
        } else {
            throw failedToFindSchemaException(actualVersion);
        }
    }

    private @Nullable SchemaDescriptor getFromCacheOrLoad(int version) {
        SchemaDescriptor desc = schemaCache.get(version);

        if (desc != null) {
            return desc;
        }

        desc = loadStoredSchemaByVersion(version);

        if (desc != null) {
            makeSchemaVersionAvailable(desc);
        }

        return desc;
    }

    private static SchemaRegistryException failedToFindSchemaException(int version) {
        return new SchemaRegistryException("Failed to find schema (was it compacted away?) [version=" + version + "]");
    }

    private int versionOrLatestForZero(int version) {
        if (version == 0) {
            // Use last version (any version may be used) for 0 version, that mean row doesn't contain value.
            return schemaCache.lastKey();
        } else {
            return version;
        }
    }

    @Override
    public CompletableFuture<SchemaDescriptor> schemaAsync(int version) {
        if (version <= 0) {
            return failedFuture(new SchemaRegistryException("Unsupported schema version [version=" + version + "]"));
        }

        SchemaDescriptor desc = getFromCacheOrLoad(version);

        if (desc != null) {
            return completedFuture(desc);
        }

        return tableSchemaAsync(version)
                .whenComplete((loadedDesc, ex) -> {
                    if (ex == null) {
                        if (loadedDesc == null) {
                            throw failedToFindSchemaException(version);
                        }

                        makeSchemaVersionAvailable(loadedDesc);
                    }
                });
    }

    @Override
    public int lastKnownSchemaVersion() {
        return schemaCache.lastKey();
    }

    @Override
    public Row resolve(BinaryRow row, int targetSchemaVersion) {
        SchemaDescriptor targetSchema = schema(targetSchemaVersion);

        throwIfNoSuchSchema(targetSchema, targetSchemaVersion);

        return resolveInternal(row, targetSchema, false);
    }

    @Override
    public Row resolve(BinaryRow row, SchemaDescriptor schemaDescriptor) {
        return resolveInternal(row, schemaDescriptor, false);
    }

    @Override
    public List<Row> resolve(Collection<BinaryRow> binaryRows, int targetSchemaVersion) {
        return resolveInternal(binaryRows, targetSchemaVersion, false);
    }

    private static void throwIfNoSuchSchema(SchemaDescriptor targetSchema, int targetSchemaVersion) {
        if (targetSchema == null) {
            throw new SchemaRegistryException("No schema found: schemaVersion=" + targetSchemaVersion);
        }
    }

    @Override
    public List<Row> resolveKeys(Collection<BinaryRow> keyOnlyRows, int targetSchemaVersion) {
        return resolveInternal(keyOnlyRows, targetSchemaVersion, true);
    }

    @Override
    public void close() {
        versionTracker.close();

        inFlightTableSchemaFutures.cancelInFlightFutures();
    }

    /**
     * Resolves a schema for row. The method is optimal when the latest schema is already got.
     *
     * @param binaryRow Binary row.
     * @param targetSchema The target schema.
     * @param keyOnly {@code true} if the given {@code binaryRow} only contains a key component, {@code false} otherwise.
     * @return Schema-aware row.
     * @throws SchemaRegistryException if no schema exists for the given row.
     */
    private Row resolveInternal(BinaryRow binaryRow, SchemaDescriptor targetSchema, boolean keyOnly) {
        if (binaryRow.schemaVersion() == 0 || targetSchema.version() == binaryRow.schemaVersion()) {
            return keyOnly ? Row.wrapKeyOnlyBinaryRow(targetSchema, binaryRow) : Row.wrapBinaryRow(targetSchema, binaryRow);
        }

        SchemaDescriptor rowSchema = schema(binaryRow.schemaVersion());

        ColumnMapper mapping = resolveMapping(targetSchema, rowSchema);

        if (keyOnly) {
            Row row = Row.wrapKeyOnlyBinaryRow(rowSchema, binaryRow);

            return UpgradingRowAdapter.upgradeKeyOnlyRow(targetSchema, mapping, row);
        } else {
            Row row = Row.wrapBinaryRow(rowSchema, binaryRow);

            return UpgradingRowAdapter.upgradeRow(targetSchema, mapping, row);
        }
    }

    private List<Row> resolveInternal(Collection<BinaryRow> binaryRows, int targetSchemaVersion, boolean keyOnly) {
        SchemaDescriptor targetSchema = schema(targetSchemaVersion);

        throwIfNoSuchSchema(targetSchema, targetSchemaVersion);

        var rows = new ArrayList<Row>(binaryRows.size());

        for (BinaryRow row : binaryRows) {
            rows.add(row == null ? null : resolveInternal(row, targetSchema, keyOnly));
        }

        return rows;
    }

    /**
     * Get cached or create a column mapper that maps column of current schema to the row schema as they may have different schema indices.
     *
     * @param curSchema Target schema.
     * @param rowSchema Row schema.
     * @return Column mapper for target schema.
     */
    ColumnMapper resolveMapping(SchemaDescriptor curSchema, SchemaDescriptor rowSchema) {
        assert curSchema.version() > rowSchema.version();

        if (curSchema.version() == rowSchema.version() + 1) {
            return curSchema.columnMapping();
        }

        long mappingKey = (((long) curSchema.version()) << 32) | (rowSchema.version());

        ColumnMapper mapping;

        if ((mapping = mappingCache.get(mappingKey)) != null) {
            return mapping;
        }

        mapping = schema(rowSchema.version() + 1).columnMapping();

        for (int i = rowSchema.version() + 2; i <= curSchema.version(); i++) {
            mapping = ColumnMapping.mergeMapping(mapping, schema(i));
        }

        mappingCache.putIfAbsent(mappingKey, mapping);

        return mapping;
    }

    /**
     * Registers new schema.
     *
     * @param desc Schema descriptor.
     * @throws SchemaRegistrationConflictException If schema of provided version was already registered.
     * @throws SchemaRegistryException If schema of incorrect version provided.
     */
    public void onSchemaRegistered(SchemaDescriptor desc) {
        int lastVer = schemaCache.lastKey();

        if (desc.version() != lastVer + 1) {
            if (desc.version() > 0 && desc.version() <= lastVer) {
                throw new SchemaRegistrationConflictException("Schema with given version has been already registered: " + desc.version());
            }

            throw new SchemaRegistryException("Try to register schema of wrong version: ver=" + desc.version() + ", lastVer=" + lastVer);
        }

        makeSchemaVersionAvailable(desc);
    }

    /**
     * Cleanup given schema version from history.
     *
     * @param ver Schema version to remove.
     * @throws SchemaRegistryException If incorrect schema version provided.
     */
    public void onSchemaDropped(int ver) {
        int lastVer = schemaCache.lastKey();

        if (ver <= 0 || ver >= lastVer || ver > schemaCache.keySet().first()) {
            throw new SchemaRegistryException("Incorrect schema version to clean up to: " + ver);
        }

        if (schemaCache.remove(ver) != null) {
            mappingCache.keySet().removeIf(k -> (k & 0xFFFF_FFFFL) == ver);
        }
    }

    /**
     * For test purposes only.
     *
     * @return ColumnMapping cache.
     */
    @TestOnly
    Map<Long, ColumnMapper> mappingCache() {
        return unmodifiableMap(mappingCache);
    }

    private CompletableFuture<SchemaDescriptor> tableSchemaAsync(int schemaVer) {
        if (schemaVer < lastKnownSchemaVersion()) {
            return completedFuture(loadStoredSchemaByVersion(schemaVer));
        }

        CompletableFuture<SchemaDescriptor> future = versionTracker.waitFor(schemaVer)
                .thenApply(unused -> schemaCache.get(schemaVer));

        inFlightTableSchemaFutures.registerFuture(future);

        return future;
    }

    private @Nullable SchemaDescriptor loadStoredSchemaByVersion(int schemaVer) {
        return schemaDescriptorLoader.load(schemaVer);
    }
}
