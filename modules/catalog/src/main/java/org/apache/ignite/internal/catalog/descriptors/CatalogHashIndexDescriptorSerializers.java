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

import static org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializationUtils.readStringCollection;
import static org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializationUtils.writeStringCollection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectDataInput;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectDataOutput;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializer;

/**
 * Serializers for {@link CatalogHashIndexDescriptor}.
 */
public class CatalogHashIndexDescriptorSerializers {
    /**
     * Serializer for {@link CatalogHashIndexDescriptor}.
     */
    @CatalogSerializer(version = 1, since = "3.0.0")
    static class HashIndexDescriptorSerializerV1 implements CatalogObjectSerializer<CatalogHashIndexDescriptor> {
        @Override
        public CatalogHashIndexDescriptor readFrom(CatalogObjectDataInput input) throws IOException {
            int id = input.readVarIntAsInt();
            String name = input.readUTF();
            long updateToken = input.readVarInt();
            int tableId = input.readVarIntAsInt();
            boolean unique = input.readBoolean();
            CatalogIndexStatus status = CatalogIndexStatus.forId(input.readByte());
            boolean isCreatedWithTable = input.readBoolean();
            List<String> columns = readStringCollection(input, ArrayList::new);

            return new CatalogHashIndexDescriptor(id, name, tableId, unique, status, columns, updateToken, isCreatedWithTable);
        }

        @Override
        public void writeTo(CatalogHashIndexDescriptor descriptor, CatalogObjectDataOutput output) throws IOException {
            output.writeVarInt(descriptor.id());
            output.writeUTF(descriptor.name());
            output.writeVarInt(descriptor.updateToken());
            output.writeVarInt(descriptor.tableId());
            output.writeBoolean(descriptor.unique());
            output.writeByte(descriptor.status().id());
            output.writeBoolean(descriptor.isCreatedWithTable());
            writeStringCollection(descriptor.columns(), output);
        }
    }

    @CatalogSerializer(version = 1, since = "3.1.0")
    static class HashIndexDescriptorSerializerV2 implements CatalogObjectSerializer<CatalogHashIndexDescriptor> {
        @Override
        public CatalogHashIndexDescriptor readFrom(CatalogObjectDataInput input) throws IOException {
            int id = input.readVarIntAsInt();
            String name = input.readUTF();
            long updateToken = input.readVarInt();
            int tableId = input.readVarIntAsInt();
            boolean unique = input.readBoolean();
            CatalogIndexStatus status = CatalogIndexStatus.forId(input.readByte());
            boolean isCreatedWithTable = input.readBoolean();
            List<String> columns = readStringCollection(input, ArrayList::new);

            return new CatalogHashIndexDescriptor(id, name, tableId, unique, status, columns, updateToken, isCreatedWithTable);
        }

        @Override
        public void writeTo(CatalogHashIndexDescriptor value, CatalogObjectDataOutput output) throws IOException {
            output.writeVarInt(value.id());
            output.writeUTF(value.name());
            output.writeVarInt(value.updateToken());
            output.writeVarInt(value.tableId());
            output.writeBoolean(value.unique());
            output.writeByte(value.status().id());
            output.writeBoolean(value.isCreatedWithTable());
            writeStringCollection(value.columns(), output);
        }
    }
}
