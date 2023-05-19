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
import org.apache.ignite.internal.catalog.descriptors.DistributionZoneDescriptor;
import org.apache.ignite.internal.catalog.descriptors.IndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.ObjectDescriptor;
import org.apache.ignite.internal.catalog.descriptors.SchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.TableDescriptor;
import org.apache.ignite.internal.tostring.IgniteToStringExclude;
import org.apache.ignite.internal.tostring.S;

/**
 * Catalog descriptor represents database schema snapshot.
 */
public class Catalog {
    private final int version;
    private final int objectIdGen;
    private final long activationTimestamp;
    private final Map<String, SchemaDescriptor> schemas;
    private final Map<String, DistributionZoneDescriptor> zones;

    @IgniteToStringExclude
    private final Map<Integer, TableDescriptor> tablesMap;
    @IgniteToStringExclude
    private final Map<Integer, IndexDescriptor> indexesMap;
    @IgniteToStringExclude
    private final Map<Integer, DistributionZoneDescriptor> zonesMap;

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
            Collection<DistributionZoneDescriptor> zones,
            SchemaDescriptor... schemas
    ) {
        this.version = version;
        this.activationTimestamp = activationTimestamp;
        this.objectIdGen = objectIdGen;

        Objects.requireNonNull(schemas, "schemas");

        assert schemas.length > 0 : "No schemas found";
        assert Arrays.stream(schemas).allMatch(t -> t.version() == version) : "Invalid schema version";

        this.schemas = Arrays.stream(schemas).collect(Collectors.toUnmodifiableMap(SchemaDescriptor::name, t -> t));
        this.zones = zones.stream().collect(Collectors.toUnmodifiableMap(DistributionZoneDescriptor::name, t -> t));
        zonesMap = zones.stream().collect(Collectors.toUnmodifiableMap(DistributionZoneDescriptor::id, t -> t));

        tablesMap = this.schemas.values().stream().flatMap(s -> Arrays.stream(s.tables()))
                .collect(Collectors.toUnmodifiableMap(ObjectDescriptor::id, Function.identity()));
        indexesMap = this.schemas.values().stream().flatMap(s -> Arrays.stream(s.indexes()))
                .collect(Collectors.toUnmodifiableMap(ObjectDescriptor::id, Function.identity()));
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

    public SchemaDescriptor schema(String name) {
        return schemas.get(name);
    }

    public TableDescriptor table(int tableId) {
        return tablesMap.get(tableId);
    }

    public IndexDescriptor index(int indexId) {
        return indexesMap.get(indexId);
    }

    public DistributionZoneDescriptor zone(String name) {
        return zones.get(name);
    }

    public DistributionZoneDescriptor zone(int zoneId) {
        return zonesMap.get(zoneId);
    }

    public Collection<DistributionZoneDescriptor> zones() {
        return zones.values();
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(this);
    }
}
