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
import java.util.List;
import org.apache.ignite.internal.catalog.descriptors.CatalogSystemViewDescriptor.SystemViewType;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogEntrySerializerProvider;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectDataInput;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectDataOutput;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;
import org.apache.ignite.internal.hlc.HybridTimestamp;

/**
 * Serializers for {@link CatalogSystemViewDescriptor}.
 */
public class CatalogSystemViewDescriptorSerializers {
    /**
     * Serializer for {@link CatalogSystemViewDescriptor}.
     */
    @CatalogSerializer(version = 1, since = "3.0.0")
    static class SystemViewDescriptorSerializerV1 implements CatalogObjectSerializer<CatalogSystemViewDescriptor> {
        private final CatalogEntrySerializerProvider serializers;

        public SystemViewDescriptorSerializerV1(CatalogEntrySerializerProvider serializers) {
            this.serializers = serializers;
        }

        @Override
        public CatalogSystemViewDescriptor readFrom(CatalogObjectDataInput input) throws IOException {
            CatalogObjectSerializer<CatalogTableColumnDescriptor> columnSerializer =
                    serializers.get(1, MarshallableEntryType.DESCRIPTOR_TABLE_COLUMN.id());

            int id = input.readVarIntAsInt();
            int schemaId = input.readVarIntAsInt();
            String name = input.readUTF();

            // Read the update token.
            input.readVarInt();

            List<CatalogTableColumnDescriptor> columns = readList(columnSerializer, input);

            byte sysViewTypeId = input.readByte();
            SystemViewType sysViewType = SystemViewType.forId(sysViewTypeId);

            // Here we use the initial timestamp because it's old storage.
            return new CatalogSystemViewDescriptor(id, schemaId, name, columns, sysViewType, INITIAL_TIMESTAMP);
        }

        @Override
        public void writeTo(CatalogSystemViewDescriptor descriptor, CatalogObjectDataOutput output) throws IOException {
            CatalogObjectSerializer<CatalogTableColumnDescriptor> columnSerializer =
                    serializers.get(1, MarshallableEntryType.DESCRIPTOR_TABLE_COLUMN.id());

            output.writeVarInt(descriptor.id());
            output.writeVarInt(descriptor.schemaId());
            output.writeUTF(descriptor.name());
            output.writeVarInt(descriptor.updateTimestamp().longValue());
            writeList(descriptor.columns(), columnSerializer, output);
            output.writeByte(descriptor.systemViewType().id());
        }
    }

    /**
     * Serializer for {@link CatalogSystemViewDescriptor}.
     */
    @CatalogSerializer(version = 2, since = "3.1.0")
    static class SystemViewDescriptorSerializerV2 implements CatalogObjectSerializer<CatalogSystemViewDescriptor> {

        @Override
        public CatalogSystemViewDescriptor readFrom(CatalogObjectDataInput input) throws IOException {
            int id = input.readVarIntAsInt();
            int schemaId = input.readVarIntAsInt();
            String name = input.readUTF();
            long updateTimestampLong = input.readVarInt();
            HybridTimestamp updateTimestamp = updateTimestampLong == 0 ? MIN_VALUE : hybridTimestamp(updateTimestampLong);

            List<CatalogTableColumnDescriptor> columns = input.readEntryList(CatalogTableColumnDescriptor.class);

            byte sysViewTypeId = input.readByte();
            SystemViewType sysViewType = SystemViewType.forId(sysViewTypeId);

            return new CatalogSystemViewDescriptor(id, schemaId, name, columns, sysViewType, updateTimestamp);
        }

        @Override
        public void writeTo(CatalogSystemViewDescriptor descriptor, CatalogObjectDataOutput output) throws IOException {
            output.writeVarInt(descriptor.id());
            output.writeVarInt(descriptor.schemaId());
            output.writeUTF(descriptor.name());
            output.writeVarInt(descriptor.updateTimestamp().longValue());
            output.writeEntryList(descriptor.columns());
            output.writeByte(descriptor.systemViewType().id());
        }
    }
}
