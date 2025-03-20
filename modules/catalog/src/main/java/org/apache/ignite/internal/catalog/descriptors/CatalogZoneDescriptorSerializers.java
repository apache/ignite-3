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

import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;

import java.io.IOException;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogEntrySerializerProvider;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;

/**
 * Serializers for {@link CatalogZoneDescriptor}.
 */
public class CatalogZoneDescriptorSerializers {
    /**
     * Serializer for {@link CatalogZoneDescriptor}.
     */
    @CatalogSerializer(version = 1, since = "3.0.0")
    static class ZoneDescriptorSerializerV1 implements CatalogObjectSerializer<CatalogZoneDescriptor> {
        private final CatalogEntrySerializerProvider serializers;

        public ZoneDescriptorSerializerV1(CatalogEntrySerializerProvider serializers) {
            this.serializers = serializers;
        }

        @Override
        public CatalogZoneDescriptor readFrom(IgniteDataInput input) throws IOException {
            int id = input.readVarIntAsInt();
            String name = input.readUTF();
            HybridTimestamp updateTimestamp = hybridTimestamp(input.readVarInt());

            CatalogObjectSerializer<CatalogStorageProfilesDescriptor> serializer =
                    serializers.get(1, MarshallableEntryType.DESCRIPTOR_STORAGE_PROFILES.id());

            CatalogStorageProfilesDescriptor catalogStorageProfilesDescriptor = serializer.readFrom(input);

            int partitions = input.readVarIntAsInt();
            int replicas = input.readVarIntAsInt();
            int dataNodesAutoAdjust = input.readVarIntAsInt();
            int dataNodesAutoAdjustScaleUp = input.readVarIntAsInt();
            int dataNodesAutoAdjustScaleDown = input.readVarIntAsInt();
            String filter = input.readUTF();
            ConsistencyMode consistencyMode = ConsistencyMode.forId(input.readByte());

            return new CatalogZoneDescriptor(
                    id,
                    name,
                    partitions,
                    replicas,
                    dataNodesAutoAdjust,
                    dataNodesAutoAdjustScaleUp,
                    dataNodesAutoAdjustScaleDown,
                    filter,
                    catalogStorageProfilesDescriptor,
                    updateTimestamp,
                    consistencyMode
            );
        }

        @Override
        public void writeTo(CatalogZoneDescriptor descriptor, IgniteDataOutput output) throws IOException {
            output.writeVarInt(descriptor.id());
            output.writeUTF(descriptor.name());
            output.writeVarInt(descriptor.updateTimestamp().longValue());

            CatalogStorageProfilesDescriptor storageProfilesDescriptor = descriptor.storageProfiles();

            serializers.get(1, storageProfilesDescriptor.typeId()).writeTo(storageProfilesDescriptor, output);

            output.writeVarInt(descriptor.partitions());
            output.writeVarInt(descriptor.replicas());
            output.writeVarInt(descriptor.dataNodesAutoAdjust());
            output.writeVarInt(descriptor.dataNodesAutoAdjustScaleUp());
            output.writeVarInt(descriptor.dataNodesAutoAdjustScaleDown());
            output.writeUTF(descriptor.filter());
            output.writeByte(descriptor.consistencyMode().id());
        }
    }
}
