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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.internal.tostring.IgniteToStringExclude;
import org.apache.ignite.internal.tostring.S;

/**
 * Schema definition contains database schema objects.
 */
public class CatalogSchemaDescriptor extends CatalogObjectDescriptor {
    private static final long serialVersionUID = -233494425779955410L;

    private final int version;
    private final CatalogTableDescriptor[] tables;
    private final CatalogIndexDescriptor[] indexes;

    @IgniteToStringExclude
    private transient Map<String, CatalogTableDescriptor> tablesMap;
    @IgniteToStringExclude
    private transient Map<String, CatalogIndexDescriptor> indexesMap;

    /**
     * Constructor.
     *
     * @param id Schema id.
     * @param name Schema name.
     * @param version Catalog version.
     * @param tables Tables description.
     * @param indexes Indexes description.
     */
    public CatalogSchemaDescriptor(int id, String name, int version, CatalogTableDescriptor[] tables, CatalogIndexDescriptor[] indexes) {
        super(id, Type.SCHEMA, name);
        this.version = version;
        this.tables = Objects.requireNonNull(tables, "tables");
        this.indexes = Objects.requireNonNull(indexes, "indexes");

        rebuildMaps();
    }

    private CatalogSchemaDescriptor(int id, String name, int version, CatalogTableDescriptor[] tables, CatalogIndexDescriptor[] indexes,
            Map<String, CatalogTableDescriptor> tablesMap, Map<String, CatalogIndexDescriptor> indexesMap) {
        super(id, Type.SCHEMA, name);
        this.version = version;
        this.tables = Objects.requireNonNull(tables, "tables");
        this.indexes = Objects.requireNonNull(indexes, "indexes");
        this.tablesMap = tablesMap;
        this.indexesMap = indexesMap;
    }

    public int version() {
        return version;
    }

    public CatalogTableDescriptor[] tables() {
        return tables;
    }

    public CatalogIndexDescriptor[] indexes() {
        return indexes;
    }

    public CatalogTableDescriptor table(String name) {
        return tablesMap.get(name);
    }

    public CatalogIndexDescriptor index(String name) {
        return indexesMap.get(name);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        rebuildMaps();
    }

    private void rebuildMaps() {
        tablesMap = Arrays.stream(tables).collect(Collectors.toUnmodifiableMap(CatalogObjectDescriptor::name, Function.identity()));
        indexesMap = Arrays.stream(indexes).collect(Collectors.toUnmodifiableMap(CatalogObjectDescriptor::name, Function.identity()));
    }

    /** Creates new schema descriptor with new version. */
    public CatalogSchemaDescriptor copy(int version) {
        return new CatalogSchemaDescriptor(
                id(),
                name(),
                version,
                tables,
                indexes,
                tablesMap,
                indexesMap
        );
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(this);
    }
}
