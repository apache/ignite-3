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

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogObjectDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.tostring.IgniteToStringExclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.CollectionUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Catalog descriptor represents database schema snapshot.
 */
public class Catalog {
    private static <T extends CatalogObjectDescriptor> Collector<T, ?, Map<String, T>> toMapByName() {
        return Collectors.toUnmodifiableMap(CatalogObjectDescriptor::name, Function.identity());
    }

    private static <T extends CatalogObjectDescriptor> Collector<T, ?, Int2ObjectMap<T>> toMapById() {
        return Collectors.collectingAndThen(
                CollectionUtils.toIntMapCollector(CatalogObjectDescriptor::id, Function.identity()),
                Int2ObjectMaps::unmodifiable
        );
    }

    private final int version;
    private final int objectIdGen;
    private final long activationTimestamp;
    private final Map<String, CatalogSchemaDescriptor> schemasByName;
    private final Map<String, CatalogZoneDescriptor> zonesByName;

    @IgniteToStringExclude
    private final Int2ObjectMap<CatalogTableDescriptor> tablesById;
    @IgniteToStringExclude
    private final Int2ObjectMap<CatalogIndexDescriptor> indexesById;
    @IgniteToStringExclude
    private final Int2ObjectMap<CatalogZoneDescriptor> zonesById;

    /**
     * Constructor.
     *
     * @param version A version of the catalog.
     * @param activationTimestamp A timestamp when this version becomes active (i.e. available for use).
     * @param objectIdGen Current state of identifier generator. This value should be used to assign an id to a new object in the
     *         next version of the catalog.
     * @param zones Distribution zones descriptors.
     * @param schemas Enumeration of schemas available in the current version of catalog.
     */
    public Catalog(
            int version,
            long activationTimestamp,
            int objectIdGen,
            Collection<CatalogZoneDescriptor> zones,
            Collection<CatalogSchemaDescriptor> schemas
    ) {
        this.version = version;
        this.activationTimestamp = activationTimestamp;
        this.objectIdGen = objectIdGen;

        Objects.requireNonNull(schemas, "schemas");
        Objects.requireNonNull(zones, "zones");

        this.schemasByName = schemas.stream().collect(toMapByName());
        this.zonesByName = zones.stream().collect(toMapByName());

        tablesById = schemas.stream().flatMap(s -> Arrays.stream(s.tables())).collect(toMapById());
        indexesById = schemas.stream().flatMap(s -> Arrays.stream(s.indexes())).collect(toMapById());
        zonesById = zones.stream().collect(toMapById());
    }

    public int version() {
        return version;
    }

    public long time() {
        return activationTimestamp;
    }

    public int objectIdGenState() {
        return objectIdGen;
    }

    public CatalogSchemaDescriptor schema(String name) {
        return schemasByName.get(name);
    }

    public Collection<CatalogSchemaDescriptor> schemas() {
        return schemasByName.values();
    }

    public @Nullable CatalogTableDescriptor table(int tableId) {
        return tablesById.get(tableId);
    }

    public Collection<CatalogTableDescriptor> tables() {
        return tablesById.values();
    }

    public @Nullable CatalogIndexDescriptor index(int indexId) {
        return indexesById.get(indexId);
    }

    public Collection<CatalogIndexDescriptor> indexes() {
        return indexesById.values();
    }

    public @Nullable CatalogZoneDescriptor zone(String name) {
        return zonesByName.get(name);
    }

    public @Nullable CatalogZoneDescriptor zone(int zoneId) {
        return zonesById.get(zoneId);
    }

    public Collection<CatalogZoneDescriptor> zones() {
        return zonesByName.values();
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
