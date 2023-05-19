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
public class SchemaDescriptor extends ObjectDescriptor {
    private static final long serialVersionUID = -233494425779955410L;

    private final int version;
    private final TableDescriptor[] tables;
    private final IndexDescriptor[] indexes;

    @IgniteToStringExclude
    private transient Map<String, TableDescriptor> tablesMap;
    @IgniteToStringExclude
    private transient Map<String, IndexDescriptor> indexesMap;

    /**
     * Constructor.
     *
     * @param id Schema id.
     * @param name Schema name.
     * @param version Catalog version.
     * @param tables Tables description.
     * @param indexes Indexes description.
     */
    public SchemaDescriptor(int id, String name, int version, TableDescriptor[] tables, IndexDescriptor[] indexes) {
        super(id, Type.SCHEMA, name);
        this.version = version;
        this.tables = Objects.requireNonNull(tables, "tables");
        this.indexes = Objects.requireNonNull(indexes, "indexes");

        rebuildMaps();
    }

    private SchemaDescriptor(int id, String name, int version, TableDescriptor[] tables, IndexDescriptor[] indexes,
            Map<String, TableDescriptor> tablesMap, Map<String, IndexDescriptor> indexesMap) {
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

    public TableDescriptor[] tables() {
        return tables;
    }

    public IndexDescriptor[] indexes() {
        return indexes;
    }

    public TableDescriptor table(String name) {
        return tablesMap.get(name);
    }

    public IndexDescriptor index(String name) {
        return indexesMap.get(name);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        rebuildMaps();
    }

    private void rebuildMaps() {
        tablesMap = Arrays.stream(tables).collect(Collectors.toUnmodifiableMap(ObjectDescriptor::name, Function.identity()));
        indexesMap = Arrays.stream(indexes).collect(Collectors.toUnmodifiableMap(ObjectDescriptor::name, Function.identity()));
    }

    /** Creates new schema descriptor with new version. */
    public SchemaDescriptor copy(int version) {
        return new SchemaDescriptor(
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
