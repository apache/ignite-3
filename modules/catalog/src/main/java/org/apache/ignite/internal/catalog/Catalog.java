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

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
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
    private final Map<String, CatalogSchemaDescriptor> schemas;
    private final Map<String, CatalogZoneDescriptor> zones;

    @IgniteToStringExclude
    private final Map<Integer, CatalogTableDescriptor> tablesMap;
    @IgniteToStringExclude
    private final Map<Integer, CatalogIndexDescriptor> indexesMap;
    @IgniteToStringExclude
    private final Map<Integer, CatalogZoneDescriptor> zonesMap;

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
            CatalogSchemaDescriptor... schemas
    ) {
        this.version = version;
        this.activationTimestamp = activationTimestamp;
        this.objectIdGen = objectIdGen;

        Objects.requireNonNull(schemas, "schemas");

        assert schemas.length > 0 : "No schemas found";
        assert Arrays.stream(schemas).allMatch(t -> t.version() == version) : "Invalid schema version";

        this.schemas = Arrays.stream(schemas).collect(Collectors.toUnmodifiableMap(CatalogSchemaDescriptor::name, t -> t));
        this.zones = zones.stream().collect(Collectors.toUnmodifiableMap(CatalogZoneDescriptor::name, t -> t));

        tablesMap = Arrays.stream(schemas).flatMap(s -> Arrays.stream(s.tables()))
                .collect(Collectors.toUnmodifiableMap(CatalogObjectDescriptor::id, Function.identity()));
        indexesMap = Arrays.stream(schemas).flatMap(s -> Arrays.stream(s.indexes()))
                .collect(Collectors.toUnmodifiableMap(CatalogObjectDescriptor::id, Function.identity()));
        zonesMap = zones.stream().collect(Collectors.toUnmodifiableMap(CatalogZoneDescriptor::id, t -> t));
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
        return schemas.get(name);
    }

    public Collection<CatalogSchemaDescriptor> schemas() {
        return schemas.values();
    }

    public CatalogTableDescriptor table(int tableId) {
        return tablesMap.get(tableId);
    }

    public CatalogIndexDescriptor index(int indexId) {
        return indexesMap.get(indexId);
    }

    public CatalogZoneDescriptor zone(String name) {
        return zones.get(name);
    }

    public CatalogZoneDescriptor zone(int zoneId) {
        return zonesMap.get(zoneId);
    }

    public Collection<CatalogZoneDescriptor> zones() {
        return zones.values();
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(this);
    }
}
