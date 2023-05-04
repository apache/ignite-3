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

    @IgniteToStringExclude
    private final Map<Integer, TableDescriptor> tablesMap;
    @IgniteToStringExclude
    private final Map<Integer, IndexDescriptor> indexesMap;

    /**
     * Constructor.
     *
     * @param version Catalog version.
     * @param activationTimestamp Catalog activation timestamp.
     * @param descriptors Schema descriptors.
     */
    public Catalog(
            int version,
            long activationTimestamp,
            int objectIdGen,
            SchemaDescriptor... descriptors
    ) {
        this.version = version;
        this.activationTimestamp = activationTimestamp;
        this.objectIdGen = objectIdGen;

        Objects.requireNonNull(descriptors, "schemas");

        assert descriptors.length > 0 : "No schemas found";
        assert Arrays.stream(descriptors).allMatch(t -> t.version() == version) : "Invalid schema version";

        schemas = Arrays.stream(descriptors).collect(Collectors.toUnmodifiableMap(SchemaDescriptor::name, t -> t));

        tablesMap = schemas.values().stream().flatMap(s -> Arrays.stream(s.tables()))
                .collect(Collectors.toUnmodifiableMap(ObjectDescriptor::id, Function.identity()));
        indexesMap = schemas.values().stream().flatMap(s -> Arrays.stream(s.indexes()))
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

    public Collection<IndexDescriptor> tableIndexes(int tableId) {
        return indexesMap.values().stream().filter(desc -> desc.tableId() == tableId).collect(Collectors.toList());
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(this);
    }
}
