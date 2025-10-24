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

import static org.apache.ignite.internal.catalog.CatalogManager.INITIAL_TIMESTAMP;
import static org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializationUtils.readList;
import static org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializationUtils.writeList;
import static org.apache.ignite.internal.hlc.HybridTimestamp.MIN_VALUE;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogEntrySerializerProvider;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectDataInput;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectDataOutput;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializer;
import org.apache.ignite.internal.hlc.HybridTimestamp;

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
        public CatalogSortedIndexDescriptor readFrom(CatalogObjectDataInput input) throws IOException {
            int id = input.readVarIntAsInt();
            String name = input.readUTF();

            // Read the update token.
            input.readVarInt();

            int tableId = input.readVarIntAsInt();
            boolean unique = input.readBoolean();
            CatalogIndexStatus status = CatalogIndexStatus.forId(input.readByte());
            boolean isCreatedWithTable = input.readBoolean();
            List<CatalogIndexColumnDescriptor> columns = readList(indexColumnSerializer, input);

            // Here we use the initial timestamp because it's old storage.
            return new CatalogSortedIndexDescriptor(id, name, tableId, unique, status, columns, INITIAL_TIMESTAMP, isCreatedWithTable);
        }

        @Override
        public void writeTo(CatalogSortedIndexDescriptor descriptor, CatalogObjectDataOutput output) throws IOException {
            output.writeVarInt(descriptor.id());
            output.writeUTF(descriptor.name());
            output.writeVarInt(descriptor.updateTimestamp().longValue());
            output.writeVarInt(descriptor.tableId());
            output.writeBoolean(descriptor.unique());
            output.writeByte(descriptor.status().id());
            output.writeBoolean(descriptor.isCreatedWithTable());
            writeList(descriptor.columns(), indexColumnSerializer, output);
        }

        private static class IndexColumnDescriptorSerializerV1 implements CatalogObjectSerializer<CatalogIndexColumnDescriptor> {
            @Override
            public CatalogIndexColumnDescriptor readFrom(CatalogObjectDataInput input) throws IOException {
                String name = input.readUTF();
                CatalogColumnCollation collation = CatalogColumnCollation.unpack(input.readByte());

                return new CatalogIndexColumnDescriptor(name, collation);
            }

            @SuppressWarnings("removal")
            @Override
            public void writeTo(CatalogIndexColumnDescriptor descriptor, CatalogObjectDataOutput output) throws IOException {
                output.writeUTF(descriptor.name());
                output.writeByte(CatalogColumnCollation.pack(descriptor.collation()));
            }
        }
    }

    /**
     * Serializer for {@link CatalogSortedIndexDescriptor}.
     */
    @CatalogSerializer(version = 2, since = "3.1.0")
    static class SortedIndexDescriptorSerializerV2 implements CatalogObjectSerializer<CatalogSortedIndexDescriptor> {
        @Override
        public CatalogSortedIndexDescriptor readFrom(CatalogObjectDataInput input) throws IOException {
            int id = input.readVarIntAsInt();
            String name = input.readUTF();
            long updateTimestampLong = input.readVarInt();
            HybridTimestamp updateTimestamp = updateTimestampLong == 0 ? MIN_VALUE : hybridTimestamp(updateTimestampLong);
            int tableId = input.readVarIntAsInt();
            boolean unique = input.readBoolean();
            CatalogIndexStatus status = CatalogIndexStatus.forId(input.readByte());
            boolean isCreatedWithTable = input.readBoolean();

            List<CatalogIndexColumnDescriptor> columns = input.readObjectCollection(in -> {
                String columnName = input.readUTF();
                CatalogColumnCollation collation = CatalogColumnCollation.unpack(input.readByte());
                return new CatalogIndexColumnDescriptor(columnName, collation);
            }, ArrayList::new);

            return new CatalogSortedIndexDescriptor(id, name, tableId, unique, status, columns, updateTimestamp, isCreatedWithTable);
        }

        @SuppressWarnings("removal")
        @Override
        public void writeTo(CatalogSortedIndexDescriptor descriptor, CatalogObjectDataOutput output) throws IOException {
            output.writeVarInt(descriptor.id());
            output.writeUTF(descriptor.name());
            output.writeVarInt(descriptor.updateTimestamp().longValue());
            output.writeVarInt(descriptor.tableId());
            output.writeBoolean(descriptor.unique());
            output.writeByte(descriptor.status().id());
            output.writeBoolean(descriptor.isCreatedWithTable());

            output.writeObjectCollection((out, elem) -> {
                output.writeUTF(Objects.requireNonNull(elem.name(), "column name"));
                output.writeByte(CatalogColumnCollation.pack(elem.collation()));
            }, descriptor.columns());
        }
    }

    @CatalogSerializer(version = 3, since = "3.2.0")
    static class SortedIndexDescriptorSerializerV3 implements CatalogObjectSerializer<CatalogSortedIndexDescriptor> {
        @Override
        public CatalogSortedIndexDescriptor readFrom(CatalogObjectDataInput input) throws IOException {
            int id = input.readVarIntAsInt();
            String name = input.readUTF();
            long updateTimestampLong = input.readVarInt();
            HybridTimestamp updateTimestamp = updateTimestampLong == 0 ? MIN_VALUE : hybridTimestamp(updateTimestampLong);
            int tableId = input.readVarIntAsInt();
            boolean unique = input.readBoolean();
            CatalogIndexStatus status = CatalogIndexStatus.forId(input.readByte());
            boolean isCreatedWithTable = input.readBoolean();

            List<CatalogIndexColumnDescriptor> columns = input.readObjectCollection(in -> {
                int columnId = input.readVarIntAsInt();
                CatalogColumnCollation collation = CatalogColumnCollation.unpack(input.readByte());
                return new CatalogIndexColumnDescriptor(columnId, collation);
            }, ArrayList::new);

            return new CatalogSortedIndexDescriptor(id, name, tableId, unique, status, columns, updateTimestamp, isCreatedWithTable);
        }

        @Override
        public void writeTo(CatalogSortedIndexDescriptor descriptor, CatalogObjectDataOutput output) throws IOException {
            output.writeVarInt(descriptor.id());
            output.writeUTF(descriptor.name());
            output.writeVarInt(descriptor.updateTimestamp().longValue());
            output.writeVarInt(descriptor.tableId());
            output.writeBoolean(descriptor.unique());
            output.writeByte(descriptor.status().id());
            output.writeBoolean(descriptor.isCreatedWithTable());

            output.writeObjectCollection((out, elem) -> {
                output.writeVarInt(elem.columnId());
                output.writeByte(CatalogColumnCollation.pack(elem.collation()));
            }, descriptor.columns());
        }
    }
}
