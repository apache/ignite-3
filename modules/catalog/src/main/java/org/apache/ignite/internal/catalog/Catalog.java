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

import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
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

/**
 * Catalog descriptor represents database schema snapshot.
 */
public class Catalog {
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

        this.schemasByName = schemas.stream().collect(Collectors.toUnmodifiableMap(CatalogSchemaDescriptor::name, t -> t));
        this.zonesByName = zones.stream().collect(Collectors.toUnmodifiableMap(CatalogZoneDescriptor::name, t -> t));

        tablesById = schemas.stream().flatMap(s -> Arrays.stream(s.tables()))
                .collect(intMapCollector(CatalogObjectDescriptor::id, Function.identity()));
        indexesById = schemas.stream().flatMap(s -> Arrays.stream(s.indexes()))
                .collect(intMapCollector(CatalogObjectDescriptor::id, Function.identity()));
        zonesById = zones.stream()
                .collect(intMapCollector(CatalogObjectDescriptor::id, Function.identity()));
    }

    private static <T, V> Collector<T, ?, Int2ObjectArrayMap<V>> intMapCollector(
            Function<T, Integer> keyMapper, Function<T, V> valueMapper) {
        return Collectors.toMap(
                keyMapper,
                valueMapper,
                (oldVal, newVal) -> newVal,
                Int2ObjectArrayMap::new);
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

    public CatalogTableDescriptor table(int tableId) {
        return tablesById.get(tableId);
    }

    public CatalogIndexDescriptor index(int indexId) {
        return indexesById.get(indexId);
    }

    public CatalogZoneDescriptor zone(String name) {
        return zonesByName.get(name);
    }

    public CatalogZoneDescriptor zone(int zoneId) {
        return zonesById.get(zoneId);
    }

    public Collection<CatalogZoneDescriptor> zones() {
        return zonesByName.values();
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(this);
    }
}
