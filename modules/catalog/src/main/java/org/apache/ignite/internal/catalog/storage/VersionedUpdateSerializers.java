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
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogEntrySerializerProvider;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectDataInput;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectDataOutput;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntry;

/**
 * Serializers for {@link VersionedUpdate}.
 */
public class VersionedUpdateSerializers {
    /**
     * Serializer for {@link VersionedUpdate}.
     */
    @CatalogSerializer(version = 1, since = "3.0.0")
    public static class VersionedUpdateSerializerV1 implements CatalogObjectSerializer<VersionedUpdate> {
        private final CatalogEntrySerializerProvider serializers;

        public VersionedUpdateSerializerV1(CatalogEntrySerializerProvider serializers) {
            this.serializers = serializers;
        }

        @Override
        public VersionedUpdate readFrom(CatalogObjectDataInput input)throws IOException {
            int ver = input.readVarIntAsInt();
            long delayDurationMs = input.readVarInt();

            int size = input.readVarIntAsInt();
            List<UpdateEntry> entries = new ArrayList<>(size);

            for (int i = 0; i < size; i++) {
                short entryTypeId = (short) input.readVarIntAsInt();

                CatalogObjectSerializer<MarshallableEntry> serializer = serializers.get(1, entryTypeId);

                entries.add((UpdateEntry) serializer.readFrom(input));
            }

            return new VersionedUpdate(ver, delayDurationMs, entries);
        }

        @Override
        public void writeTo(VersionedUpdate update, CatalogObjectDataOutput output) throws IOException {
            output.writeVarInt(update.version());
            output.writeVarInt(update.delayDurationMs());

            output.writeVarInt(update.entries().size());
            for (UpdateEntry entry : update.entries()) {
                output.writeVarInt(entry.typeId());

                serializers.get(1, entry.typeId()).writeTo(entry, output);
            }
        }
    }

    /**
     * Serializer for {@link VersionedUpdate}.
     */
    @CatalogSerializer(version = 2, since = "3.1.0")
    public static class VersionedUpdateSerializerV2 implements CatalogObjectSerializer<VersionedUpdate> {
        @Override
        public VersionedUpdate readFrom(CatalogObjectDataInput input) throws IOException {
            int ver = input.readVarIntAsInt();
            long delayDurationMs = input.readVarInt();

            List<UpdateEntry> entries = input.readEntryList(UpdateEntry.class);

            return new VersionedUpdate(ver, delayDurationMs, entries);
        }

        @Override
        public void writeTo(VersionedUpdate value, CatalogObjectDataOutput output) throws IOException {
            output.writeVarInt(value.version());
            output.writeVarInt(value.delayDurationMs());
            output.writeEntryList(value.entries());
        }
    }
}
