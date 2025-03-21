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

package org.apache.ignite.internal.catalog.descriptors;

import static java.util.stream.Collectors.toUnmodifiableMap;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntry;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.tostring.IgniteToStringExclude;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/** Schema definition contains database schema objects. */
public class CatalogSchemaDescriptor extends CatalogObjectDescriptor implements MarshallableEntry {
    private final CatalogTableDescriptor[] tables;
    private final CatalogIndexDescriptor[] indexes;
    private final CatalogSystemViewDescriptor[] systemViews;

    @IgniteToStringExclude
    private Map<String, CatalogTableDescriptor> tablesMap;
    @IgniteToStringExclude
    private Map<String, CatalogIndexDescriptor> indexesMap;
    @IgniteToStringExclude
    private Map<String, CatalogSystemViewDescriptor> systemViewsMap;

    /**
     * Constructor.
     *
     * @param id Schema id.
     * @param name Schema name.
     * @param tables Tables description.
     * @param indexes Indexes description.
     */
    public CatalogSchemaDescriptor(int id, String name,
            CatalogTableDescriptor[] tables,
            CatalogIndexDescriptor[] indexes,
            CatalogSystemViewDescriptor[] systemViews,
            HybridTimestamp timestamp
    ) {
        super(id, Type.SCHEMA, name, timestamp);
        this.tables = Objects.requireNonNull(tables, "tables");
        this.indexes = Objects.requireNonNull(indexes, "indexes");
        this.systemViews = Objects.requireNonNull(systemViews, "systemViews");

        rebuildMaps();
    }

    public CatalogTableDescriptor[] tables() {
        return tables;
    }

    public CatalogIndexDescriptor[] indexes() {
        return indexes;
    }

    public CatalogSystemViewDescriptor[] systemViews() {
        return systemViews;
    }

    public @Nullable CatalogTableDescriptor table(String name) {
        return tablesMap.get(name);
    }

    /**
     * Returns an <em>alive</em> index with the given name, that is an index that has not been dropped yet.
     *
     * <p>This effectively means that the index must be present in the schema and not in the {@link CatalogIndexStatus#STOPPING}
     * state.
     */
    public @Nullable CatalogIndexDescriptor aliveIndex(String name) {
        return indexesMap.get(name);
    }

    public @Nullable CatalogSystemViewDescriptor systemView(String name) {
        return systemViewsMap.get(name);
    }

    /** Returns {@code true} if the schema doesn't contain any objects, otherwise {@code false}. */
    public boolean isEmpty() {
        return tables.length == 0 && indexes.length == 0 && systemViews.length == 0;
    }

    private void rebuildMaps() {
        tablesMap = Arrays.stream(tables).collect(toUnmodifiableMap(CatalogObjectDescriptor::name, Function.identity()));
        indexesMap = Arrays.stream(indexes)
                .filter(index -> index.status().isAlive())
                .collect(toUnmodifiableMap(CatalogObjectDescriptor::name, Function.identity()));
        systemViewsMap = Arrays.stream(systemViews).collect(toUnmodifiableMap(CatalogObjectDescriptor::name, Function.identity()));
    }

    @Override
    public int typeId() {
        return MarshallableEntryType.DESCRIPTOR_SCHEMA.id();
    }

    @Override
    public String toString() {
        return S.toString(CatalogSchemaDescriptor.class, this, super.toString());
    }
}
