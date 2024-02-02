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

import static org.apache.ignite.internal.catalog.serialization.CatalogSerializationUtils.readArray;
import static org.apache.ignite.internal.catalog.serialization.CatalogSerializationUtils.writeArray;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.internal.catalog.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.tostring.IgniteToStringExclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.jetbrains.annotations.Nullable;

/** Schema definition contains database schema objects. */
public class CatalogSchemaDescriptor extends CatalogObjectDescriptor {
    public static CatalogObjectSerializer<CatalogSchemaDescriptor> SERIALIZER = new SchemaDescriptorSerializer();

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
            long causalityToken) {
        super(id, Type.SCHEMA, name, causalityToken);
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

    public @Nullable CatalogIndexDescriptor index(String name) {
        return indexesMap.get(name);
    }

    public @Nullable CatalogSystemViewDescriptor systemView(String name) {
        return systemViewsMap.get(name);
    }

    private void rebuildMaps() {
        tablesMap = Arrays.stream(tables).collect(Collectors.toUnmodifiableMap(CatalogObjectDescriptor::name, Function.identity()));
        indexesMap = Arrays.stream(indexes).collect(Collectors.toUnmodifiableMap(CatalogObjectDescriptor::name, Function.identity()));
        systemViewsMap = Arrays.stream(systemViews)
                .collect(Collectors.toUnmodifiableMap(CatalogObjectDescriptor::name, Function.identity()));
    }

    @Override
    public String toString() {
        return S.toString(this);
    }

    /**
     * Serializer for {@link CatalogSchemaDescriptor}.
     */
    private static class SchemaDescriptorSerializer implements CatalogObjectSerializer<CatalogSchemaDescriptor> {
        @Override
        public CatalogSchemaDescriptor readFrom(IgniteDataInput input) throws IOException {
            int id = input.readInt();
            String name = input.readUTF();
            long updateToken = input.readLong();
            CatalogTableDescriptor[] tables = readArray(CatalogTableDescriptor.SERIALIZER, input, CatalogTableDescriptor.class);
            CatalogIndexDescriptor[] indexes = readArray(CatalogIndexDescriptor.SERIALIZER, input, CatalogIndexDescriptor.class);
            CatalogSystemViewDescriptor[] systemViews =
                    readArray(CatalogSystemViewDescriptor.SERIALIZER, input, CatalogSystemViewDescriptor.class);

            return new CatalogSchemaDescriptor(id, name, tables, indexes, systemViews, updateToken);
        }

        @Override
        public void writeTo(CatalogSchemaDescriptor descriptor, IgniteDataOutput output) throws IOException {
            output.writeInt(descriptor.id());
            output.writeUTF(descriptor.name());
            output.writeLong(descriptor.updateToken());
            writeArray(descriptor.tables(), CatalogTableDescriptor.SERIALIZER, output);
            writeArray(descriptor.indexes(), CatalogIndexDescriptor.SERIALIZER, output);
            writeArray(descriptor.systemViews(), CatalogSystemViewDescriptor.SERIALIZER, output);
        }
    }
}
