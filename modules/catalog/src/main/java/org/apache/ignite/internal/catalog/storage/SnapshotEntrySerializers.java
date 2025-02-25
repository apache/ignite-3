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

package org.apache.ignite.internal.catalog.storage;

import java.io.IOException;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogEntrySerializerProvider;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;
import org.apache.ignite.internal.catalog.storage.serialization.utils.CatalogSerializationUtils;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;

/**
 * Serializers for {@link SnapshotEntry}.
 */
public class SnapshotEntrySerializers {
    /**
     * Serializer for {@link SnapshotEntry}.
     */
    @CatalogSerializer(version = 1, type = MarshallableEntryType.SNAPSHOT, since = "3.0.0")
    static class SnapshotEntrySerializerV1 implements CatalogObjectSerializer<SnapshotEntry> {
        private final CatalogEntrySerializerProvider serializers;

        public SnapshotEntrySerializerV1(CatalogEntrySerializerProvider serializers) {
            this.serializers = serializers;
        }

        @Override
        public SnapshotEntry readFrom(IgniteDataInput input) throws IOException {
            int catalogVersion = input.readVarIntAsInt();
            long activationTime = input.readLong();
            int objectIdGenState = input.readVarIntAsInt();

            CatalogObjectSerializer<CatalogZoneDescriptor> zoneSerializer =
                    serializers.get(1, MarshallableEntryType.DESCRIPTOR_ZONE.id());
            CatalogObjectSerializer<CatalogSchemaDescriptor> schemaSerializer =
                    serializers.get(1, MarshallableEntryType.DESCRIPTOR_SCHEMA.id());

            CatalogZoneDescriptor[] zones = CatalogSerializationUtils.readArray(zoneSerializer, input, CatalogZoneDescriptor.class);
            CatalogSchemaDescriptor[] schemas = CatalogSerializationUtils.readArray(schemaSerializer, input, CatalogSchemaDescriptor.class);

            Integer defaultZoneId = null;
            if (input.readBoolean()) {
                defaultZoneId = input.readVarIntAsInt();
            }

            return new SnapshotEntry(catalogVersion, activationTime, objectIdGenState, zones, schemas, defaultZoneId);
        }

        @Override
        public void writeTo(SnapshotEntry entry, IgniteDataOutput output) throws IOException {
            output.writeVarInt(entry.version());
            output.writeLong(entry.activationTime());
            output.writeVarInt(entry.objectIdGenState());

            CatalogObjectSerializer<CatalogZoneDescriptor> zoneDescSerializer =
                    serializers.get(1, MarshallableEntryType.DESCRIPTOR_ZONE.id());
            CatalogObjectSerializer<CatalogSchemaDescriptor> schemaDescSerializer =
                    serializers.get(1, MarshallableEntryType.DESCRIPTOR_SCHEMA.id());

            CatalogSerializationUtils.writeArray(entry.zones(), zoneDescSerializer, output);
            CatalogSerializationUtils.writeArray(entry.schemas(), schemaDescSerializer, output);

            Integer defaultZoneId = entry.defaultZoneId();
            output.writeBoolean(defaultZoneId != null);
            if (defaultZoneId != null) {
                output.writeVarInt(defaultZoneId);
            }
        }
    }
}
