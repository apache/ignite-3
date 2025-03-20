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
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializer;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;

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
        public CatalogHashIndexDescriptor readFrom(IgniteDataInput input) throws IOException {
            int id = input.readVarIntAsInt();
            String name = input.readUTF();
            HybridTimestamp updateTimestamp = hybridTimestamp(input.readVarInt());
            int tableId = input.readVarIntAsInt();
            boolean unique = input.readBoolean();
            CatalogIndexStatus status = CatalogIndexStatus.forId(input.readByte());
            boolean isCreatedWithTable = input.readBoolean();
            List<String> columns = readStringCollection(input, ArrayList::new);

            return new CatalogHashIndexDescriptor(id, name, tableId, unique, status, columns, updateTimestamp, isCreatedWithTable);
        }

        @Override
        public void writeTo(CatalogHashIndexDescriptor descriptor, IgniteDataOutput output) throws IOException {
            output.writeVarInt(descriptor.id());
            output.writeUTF(descriptor.name());
            output.writeVarInt(descriptor.updateTimestamp().longValue());
            output.writeVarInt(descriptor.tableId());
            output.writeBoolean(descriptor.unique());
            output.writeByte(descriptor.status().id());
            output.writeBoolean(descriptor.isCreatedWithTable());
            writeStringCollection(descriptor.columns(), output);
        }
    }
}
