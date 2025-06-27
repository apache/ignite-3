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

import static it.unimi.dsi.fastutil.ints.Int2ObjectMaps.unmodifiable;
import static java.util.Comparator.comparingInt;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toUnmodifiableMap;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap.Entry;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collector;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.catalog.descriptors.CatalogObjectDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.tostring.IgniteToStringExclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.CollectionUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Catalog descriptor represents a snapshot of the database schema.
 *
 * <p>It contains information about schemas, tables, indexes, and zones available in the current version of the catalog.
 */
public class Catalog {
    private static <T extends CatalogObjectDescriptor> Collector<T, ?, Map<String, T>> toMapByName() {
        return toUnmodifiableMap(CatalogObjectDescriptor::name, identity());
    }

    private static <T extends CatalogObjectDescriptor> Collector<T, ?, Int2ObjectMap<T>> toMapById() {
        return collectingAndThen(
                CollectionUtils.toIntMapCollector(CatalogObjectDescriptor::id, identity()),
                Int2ObjectMaps::unmodifiable
        );
    }

    private final int version;
    private final int objectIdGen;
    private final long activationTimestamp;
    private final Map<String, CatalogSchemaDescriptor> schemasByName;
    private final Map<String, CatalogZoneDescriptor> zonesByName;
    private final @Nullable CatalogZoneDescriptor defaultZone;

    @IgniteToStringExclude
    private final Int2ObjectMap<CatalogSchemaDescriptor> schemasById;

    @IgniteToStringExclude
    private final Int2ObjectMap<CatalogTableDescriptor> tablesById;

    @IgniteToStringExclude
    private final Int2ObjectMap<CatalogIndexDescriptor> indexesById;

    @IgniteToStringExclude
    private final Int2ObjectMap<List<CatalogIndexDescriptor>> indexesByTableId;

    @IgniteToStringExclude
    private final Int2ObjectMap<CatalogZoneDescriptor> zonesById;

    @IgniteToStringExclude
    private final Int2ObjectMap<List<CatalogTableDescriptor>> tablesByZoneId;

    /**
     * Constructor.
     *
     * @param version A version of the catalog.
     * @param activationTimestamp A timestamp when this version becomes active (i.e. available for use).
     * @param objectIdGen Current state of identifier generator. This value should be used to assign an id to a new object in the
     *         next version of the catalog.
     * @param zones Distribution zones descriptors.
     * @param schemas Enumeration of schemas available in the current version of catalog.
     * @param defaultZoneId ID of the default distribution zone.
     */
    public Catalog(
            int version,
            long activationTimestamp,
            int objectIdGen,
            Collection<CatalogZoneDescriptor> zones,
            Collection<CatalogSchemaDescriptor> schemas,
            @Nullable Integer defaultZoneId
    ) {
        this.version = version;
        this.activationTimestamp = activationTimestamp;
        this.objectIdGen = objectIdGen;

        Objects.requireNonNull(schemas, "schemas");
        Objects.requireNonNull(zones, "zones");

        schemasByName = schemas.stream().collect(toMapByName());
        zonesByName = zones.stream().collect(toMapByName());

        schemasById = schemas.stream().collect(toMapById());
        tablesById = schemas.stream().flatMap(s -> Arrays.stream(s.tables())).collect(toMapById());
        indexesById = schemas.stream().flatMap(s -> Arrays.stream(s.indexes())).collect(toMapById());
        indexesByTableId = unmodifiable(toIndexesByTableId(schemas));
        zonesById = zones.stream().collect(toMapById());
        tablesByZoneId = unmodifiable(toTablesByZoneId(schemas));

        if (defaultZoneId != null) {
            defaultZone = zonesById.get((int) defaultZoneId);

            if (defaultZone == null) {
                throw new IllegalStateException("The default zone was not found among the provided zones [id=" + defaultZoneId + ']');
            }
        } else {
            defaultZone = null;
        }
    }

    /**
     * Returns the version of the catalog.
     *
     * @return The version of the catalog.
     */
    public int version() {
        return version;
    }

    /**
     * Returns the timestamp when this version becomes active (i.e., available for use).
     *
     * @return The activation timestamp.
     */
    public long time() {
        return activationTimestamp;
    }

    /**
     * Returns the current state of the identifier generator. This value is used to generate an unique id for a new object in the next
     * versions of the catalog.
     *
     * @return The current state of the identifier generator.
     */
    public int objectIdGenState() {
        return objectIdGen;
    }

    /**
     * Returns the schema descriptor by schema name.
     *
     * @param name The name of the schema.
     * @return The schema descriptor or {@code null} if the schema is not found.
     */
    public @Nullable CatalogSchemaDescriptor schema(String name) {
        return schemasByName.get(name);
    }

    /**
     * Returns the schema descriptor by schema ID.
     *
     * @param schemaId The ID of the schema.
     * @return The schema descriptor or {@code null} if the schema is not found.
     */
    public @Nullable CatalogSchemaDescriptor schema(int schemaId) {
        return schemasById.get(schemaId);
    }

    /**
     * Returns all schemas in the catalog.
     *
     * @return A collection of all schema descriptors.
     */
    public Collection<CatalogSchemaDescriptor> schemas() {
        return schemasByName.values();
    }

    /**
     * Returns the table descriptor by table ID.
     *
     * @param tableId The ID of the table.
     * @return The table descriptor or {@code null} if the table is not found.
     */
    public @Nullable CatalogTableDescriptor table(int tableId) {
        return tablesById.get(tableId);
    }

    /**
     * Returns the table descriptor by table name and schema name. Both names should be normalized.
     *
     * @param schemaName The name of the schema. Case-sensitive, without quotes.
     * @param tableName The name of the table without schema. Case-sensitive, without quotes.
     * @return The table descriptor or {@code null} if the schema or table is not found.
     */
    public @Nullable CatalogTableDescriptor table(String schemaName, String tableName) {
        CatalogSchemaDescriptor schema = schema(schemaName);
        return schema == null ? null : schema.table(tableName);
    }

    /**
     * Returns all tables in the catalog.
     *
     * @return A collection of all table descriptors.
     */
    public Collection<CatalogTableDescriptor> tables() {
        return tablesById.values();
    }

    /**
     * Returns all tables that belong to the specified zone.
     *
     * @return A collection of table descriptors.
     */
    public Collection<CatalogTableDescriptor> tables(int zoneId) {
        return tablesByZoneId.getOrDefault(zoneId, List.of());
    }

    /**
     * Returns an index descriptor by the given index name and schema name, that is an index that has not been dropped yet.
     *
     * <p>This effectively means that the index must be present in the Catalog and not in the {@link CatalogIndexStatus#STOPPING}
     * state.
     *
     * @param schemaName The name of the schema.
     * @param indexName The name of the index.
     * @return The index descriptor or {@code null} if the schema or index is not found.
     */
    public @Nullable CatalogIndexDescriptor aliveIndex(String schemaName, String indexName) {
        CatalogSchemaDescriptor schema = schema(schemaName);
        return schema == null ? null : schema.aliveIndex(indexName);
    }

    /**
     * Returns the index descriptor by index ID.
     *
     * @param indexId The ID of the index.
     * @return The index descriptor or {@code null} if the index is not found.
     */
    public @Nullable CatalogIndexDescriptor index(int indexId) {
        return indexesById.get(indexId);
    }

    /**
     * Returns all indexes in the catalog.
     *
     * @return A collection of all index descriptors.
     */
    public Collection<CatalogIndexDescriptor> indexes() {
        return indexesById.values();
    }

    /**
     * Returns a list of index descriptors for a given table ID.
     *
     * @param tableId The ID of the table.
     * @return A list of index descriptors or an empty list if no indexes are found.
     */
    public List<CatalogIndexDescriptor> indexes(int tableId) {
        return indexesByTableId.getOrDefault(tableId, List.of());
    }

    /**
     * Returns the zone descriptor by zone name.
     *
     * @param name The name of the zone.
     * @return The zone descriptor or {@code null} if the zone is not found.
     */
    public @Nullable CatalogZoneDescriptor zone(String name) {
        return zonesByName.get(name);
    }

    /**
     * Returns the zone descriptor by zone ID.
     *
     * @param zoneId The ID of the zone.
     * @return The zone descriptor or {@code null} if the zone is not found.
     */
    public @Nullable CatalogZoneDescriptor zone(int zoneId) {
        return zonesById.get(zoneId);
    }

    /**
     * Returns all zones in the catalog.
     *
     * @return A collection of all zone descriptors.
     */
    public Collection<CatalogZoneDescriptor> zones() {
        return zonesByName.values();
    }

    /**
     * Returns the default zone descriptor.
     *
     * @return The default zone descriptor or {@code null} if no default zone is set.
     */
    public @Nullable CatalogZoneDescriptor defaultZone() {
        return defaultZone;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(this);
    }

    private static Int2ObjectMap<List<CatalogIndexDescriptor>> toIndexesByTableId(Collection<CatalogSchemaDescriptor> schemas) {
        Int2ObjectMap<List<CatalogIndexDescriptor>> indexesByTableId = new Int2ObjectOpenHashMap<>();

        for (CatalogSchemaDescriptor schema : schemas) {
            for (CatalogIndexDescriptor index : schema.indexes()) {
                indexesByTableId.computeIfAbsent(index.tableId(), indexes -> new ArrayList<>()).add(index);
            }
        }

        for (List<CatalogIndexDescriptor> indexes : indexesByTableId.values()) {
            indexes.sort(comparingInt(CatalogIndexDescriptor::id));
        }

        for (Entry<List<CatalogIndexDescriptor>> entry : indexesByTableId.int2ObjectEntrySet()) {
            entry.setValue(List.copyOf(entry.getValue()));
        }

        return indexesByTableId;
    }

    private static Int2ObjectMap<List<CatalogTableDescriptor>> toTablesByZoneId(Collection<CatalogSchemaDescriptor> schemas) {
        var tablesByZoneId = new Int2ObjectOpenHashMap<List<CatalogTableDescriptor>>();

        for (CatalogSchemaDescriptor schema : schemas) {
            for (CatalogTableDescriptor table : schema.tables()) {
                tablesByZoneId.computeIfAbsent(table.zoneId(), tables -> new ArrayList<>()).add(table);
            }
        }

        for (Entry<List<CatalogTableDescriptor>> entry : tablesByZoneId.int2ObjectEntrySet()) {
            entry.setValue(List.copyOf(entry.getValue()));
        }

        return tablesByZoneId;
    }
}
