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

import static org.apache.ignite.internal.catalog.storage.serialization.utils.CatalogSerializationUtils.readList;
import static org.apache.ignite.internal.catalog.storage.serialization.utils.CatalogSerializationUtils.writeList;

import java.io.IOException;
import java.util.List;
import org.apache.ignite.internal.catalog.descriptors.CatalogSystemViewDescriptor.SystemViewType;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogEntrySerializerProvider;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;

/**
 * Serializers for {@link CatalogSystemViewDescriptor}.
 */
public class CatalogSystemViewDescriptorSerializers {
    /**
     * Serializer for {@link CatalogSystemViewDescriptor}.
     */
    @CatalogSerializer(version = 1, type = MarshallableEntryType.DESCRIPTOR_SYSTEM_VIEW, since = "3.0.0")
    static class SystemViewDescriptorSerializerV1 implements CatalogObjectSerializer<CatalogSystemViewDescriptor> {
        private final CatalogEntrySerializerProvider serializers;

        public SystemViewDescriptorSerializerV1(CatalogEntrySerializerProvider serializers) {
            this.serializers = serializers;
        }

        @Override
        public CatalogSystemViewDescriptor readFrom(IgniteDataInput input) throws IOException {
            CatalogObjectSerializer<CatalogTableColumnDescriptor> columnSerializer =
                    serializers.get(1, MarshallableEntryType.DESCRIPTOR_TABLE_COLUMN.id());

            int id = input.readVarIntAsInt();
            int schemaId = input.readVarIntAsInt();
            String name = input.readUTF();
            long updateToken = input.readVarInt();

            List<CatalogTableColumnDescriptor> columns = readList(columnSerializer, input);

            byte sysViewTypeId = input.readByte();
            SystemViewType sysViewType = SystemViewType.forId(sysViewTypeId);

            return new CatalogSystemViewDescriptor(id, schemaId, name, columns, sysViewType, updateToken);
        }

        @Override
        public void writeTo(CatalogSystemViewDescriptor descriptor, IgniteDataOutput output) throws IOException {
            CatalogObjectSerializer<CatalogTableColumnDescriptor> columnSerializer =
                    serializers.get(1, MarshallableEntryType.DESCRIPTOR_TABLE_COLUMN.id());

            output.writeVarInt(descriptor.id());
            output.writeVarInt(descriptor.schemaId());
            output.writeUTF(descriptor.name());
            output.writeVarInt(descriptor.updateToken());
            writeList(descriptor.columns(), columnSerializer, output);
            output.writeByte(descriptor.systemViewType().id());
        }
    }
}
