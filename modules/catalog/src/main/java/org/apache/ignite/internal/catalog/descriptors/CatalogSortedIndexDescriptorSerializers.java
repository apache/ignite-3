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

import static org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializationUtils.readList;
import static org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializationUtils.writeList;

import java.io.IOException;
import java.util.List;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogEntrySerializerProvider;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializer;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;

/**
 * Serializers for {@link CatalogSortedIndexDescriptor}.
 */
public class CatalogSortedIndexDescriptorSerializers {
    /**
     * Serializer for {@link CatalogSortedIndexDescriptor}.
     */
    @CatalogSerializer(version = 1, since = "3.0.0")
    static class SortedIndexDescriptorSerializerV1 implements CatalogObjectSerializer<CatalogSortedIndexDescriptor> {
        private final CatalogEntrySerializerProvider serializers;

        private final IndexColumnDescriptorSerializerV1 indexColumnSerializer = new IndexColumnDescriptorSerializerV1();

        public SortedIndexDescriptorSerializerV1(CatalogEntrySerializerProvider serializers) {
            this.serializers = serializers;
        }

        @Override
        public CatalogSortedIndexDescriptor readFrom(IgniteDataInput input) throws IOException {
            int id = input.readVarIntAsInt();
            String name = input.readUTF();
            long updateToken = input.readVarInt();
            int tableId = input.readVarIntAsInt();
            boolean unique = input.readBoolean();
            CatalogIndexStatus status = CatalogIndexStatus.forId(input.readByte());
            boolean isCreatedWithTable = input.readBoolean();
            List<CatalogIndexColumnDescriptor> columns = readList(indexColumnSerializer, input);

            return new CatalogSortedIndexDescriptor(id, name, tableId, unique, status, columns, updateToken, isCreatedWithTable);
        }

        @Override
        public void writeTo(CatalogSortedIndexDescriptor descriptor, IgniteDataOutput output) throws IOException {
            output.writeVarInt(descriptor.id());
            output.writeUTF(descriptor.name());
            output.writeVarInt(descriptor.updateToken());
            output.writeVarInt(descriptor.tableId());
            output.writeBoolean(descriptor.unique());
            output.writeByte(descriptor.status().id());
            output.writeBoolean(descriptor.isCreatedWithTable());
            writeList(descriptor.columns(), indexColumnSerializer, output);
        }

        private static class IndexColumnDescriptorSerializerV1 implements CatalogObjectSerializer<CatalogIndexColumnDescriptor> {
            @Override
            public CatalogIndexColumnDescriptor readFrom(IgniteDataInput input) throws IOException {
                String name = input.readUTF();
                CatalogColumnCollation collation = CatalogColumnCollation.unpack(input.readByte());

                return new CatalogIndexColumnDescriptor(name, collation);
            }

            @Override
            public void writeTo(CatalogIndexColumnDescriptor descriptor, IgniteDataOutput output) throws IOException {
                output.writeUTF(descriptor.name());
                output.writeByte(CatalogColumnCollation.pack(descriptor.collation()));
            }
        }
    }
}
