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
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogEntrySerializerProvider;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectDataInput;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectDataOutput;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializationUtils;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;

/**
 * Serializers for {@link SnapshotEntry}.
 */
public class SnapshotEntrySerializers {
    /**
     * Serializer for {@link SnapshotEntry}.
     */
    @CatalogSerializer(version = 1, since = "3.0.0")
    static class SnapshotEntrySerializerV1 implements CatalogObjectSerializer<SnapshotEntry> {
        private final CatalogEntrySerializerProvider serializers;

        public SnapshotEntrySerializerV1(CatalogEntrySerializerProvider serializers) {
            this.serializers = serializers;
        }

        @Override
        public SnapshotEntry readFrom(CatalogObjectDataInput input)throws IOException {
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
        public void writeTo(SnapshotEntry entry, CatalogObjectDataOutput output) throws IOException {
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

    /**
     * Serializer for {@link SnapshotEntry}.
     */
    @CatalogSerializer(version = 2, since = "3.1.0")
    static class SnapshotEntrySerializerV2 implements CatalogObjectSerializer<SnapshotEntry> {
        @Override
        public SnapshotEntry readFrom(CatalogObjectDataInput input)throws IOException {
            int catalogVersion = input.readVarIntAsInt();
            long activationTime = input.readLong();
            int objectIdGenState = input.readVarIntAsInt();

            List<CatalogZoneDescriptor> zones = input.readCompactEntryList(CatalogZoneDescriptor.class);
            List<CatalogSchemaDescriptor> schemas = input.readCompactEntryList(CatalogSchemaDescriptor.class);

            Integer defaultZoneId = null;
            if (input.readBoolean()) {
                defaultZoneId = input.readVarIntAsInt();
            }

            return new SnapshotEntry(
                    catalogVersion,
                    activationTime,
                    objectIdGenState,
                    zones.toArray(new CatalogZoneDescriptor[0]),
                    schemas.toArray(new CatalogSchemaDescriptor[0]),
                    defaultZoneId
            );
        }

        @Override
        public void writeTo(SnapshotEntry entry, CatalogObjectDataOutput output) throws IOException {
            output.writeVarInt(entry.version());
            output.writeLong(entry.activationTime());
            output.writeVarInt(entry.objectIdGenState());

            output.writeCompactEntryList(Arrays.asList(entry.zones()));
            output.writeCompactEntryList(Arrays.asList(entry.schemas()));

            Integer defaultZoneId = entry.defaultZoneId();
            output.writeBoolean(defaultZoneId != null);
            if (defaultZoneId != null) {
                output.writeVarInt(defaultZoneId);
            }
        }
    }
}
