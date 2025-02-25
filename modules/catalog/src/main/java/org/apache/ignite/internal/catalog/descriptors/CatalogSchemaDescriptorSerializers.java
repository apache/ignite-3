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

import static org.apache.ignite.internal.catalog.storage.serialization.utils.CatalogSerializationUtils.readArray;
import static org.apache.ignite.internal.catalog.storage.serialization.utils.CatalogSerializationUtils.writeArray;

import java.io.IOException;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogEntrySerializerProvider;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;
import org.apache.ignite.internal.catalog.storage.serialization.utils.CatalogSerializationUtils.IndexDescriptorSerializerHelper;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;

/**
 * Serializers for {@link CatalogSchemaDescriptor}.
 */
public class CatalogSchemaDescriptorSerializers {
    /**
     * Serializer for {@link CatalogSchemaDescriptor}.
     */
    @CatalogSerializer(version = 1, type = MarshallableEntryType.DESCRIPTOR_SCHEMA, since = "3.0.0")
    static class SchemaDescriptorSerializerV1 implements CatalogObjectSerializer<CatalogSchemaDescriptor> {
        private final CatalogEntrySerializerProvider serializers;
        private final IndexDescriptorSerializerHelper indexSerializeHelper;

        public SchemaDescriptorSerializerV1(CatalogEntrySerializerProvider serializers) {
            this.serializers = serializers;
            this.indexSerializeHelper = new IndexDescriptorSerializerHelper(serializers);
        }

        @Override
        public CatalogSchemaDescriptor readFrom(IgniteDataInput input) throws IOException {
            CatalogObjectSerializer<CatalogTableDescriptor> tableDescriptorSerializer =
                    serializers.get(1, MarshallableEntryType.DESCRIPTOR_TABLE.id());
            CatalogObjectSerializer<CatalogSystemViewDescriptor> viewDescriptorSerializer =
                    serializers.get(1, MarshallableEntryType.DESCRIPTOR_SYSTEM_VIEW.id());

            int id = input.readVarIntAsInt();
            String name = input.readUTF();
            long updateToken = input.readVarInt();

            CatalogTableDescriptor[] tables = readArray(tableDescriptorSerializer, input, CatalogTableDescriptor.class);
            CatalogIndexDescriptor[] indexes = readArray(indexSerializeHelper, input, CatalogIndexDescriptor.class);
            CatalogSystemViewDescriptor[] systemViews = readArray(viewDescriptorSerializer, input, CatalogSystemViewDescriptor.class);

            return new CatalogSchemaDescriptor(id, name, tables, indexes, systemViews, updateToken);
        }

        @Override
        public void writeTo(CatalogSchemaDescriptor descriptor, IgniteDataOutput output) throws IOException {
            CatalogObjectSerializer<CatalogTableDescriptor> tableDescriptorSerializer =
                    serializers.get(1, MarshallableEntryType.DESCRIPTOR_TABLE.id());
            CatalogObjectSerializer<CatalogSystemViewDescriptor> viewDescriptorSerializer =
                    serializers.get(1, MarshallableEntryType.DESCRIPTOR_SYSTEM_VIEW.id());

            output.writeVarInt(descriptor.id());
            output.writeUTF(descriptor.name());
            output.writeVarInt(descriptor.updateToken());

            writeArray(descriptor.tables(), tableDescriptorSerializer, output);
            writeArray(descriptor.indexes(), indexSerializeHelper, output);
            writeArray(descriptor.systemViews(), viewDescriptorSerializer, output);
        }
    }
}
