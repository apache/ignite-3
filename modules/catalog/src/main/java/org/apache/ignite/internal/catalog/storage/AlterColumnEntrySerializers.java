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
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogEntrySerializerProvider;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectDataInput;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectDataOutput;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;

/**
 * Serializers for {@link AlterColumnEntry}.
 */
public class AlterColumnEntrySerializers {
    /**
     * Serializer for {@link AlterColumnEntry}.
     */
    @CatalogSerializer(version = 1, since = "3.0.0")
    static class AlterColumnEntrySerializerV1 implements CatalogObjectSerializer<AlterColumnEntry> {
        private final CatalogEntrySerializerProvider serializers;

        public AlterColumnEntrySerializerV1(CatalogEntrySerializerProvider serializers) {
            this.serializers = serializers;
        }

        @Override
        public AlterColumnEntry readFrom(CatalogObjectDataInput input) throws IOException {
            CatalogObjectSerializer<CatalogTableColumnDescriptor> serializer =
                    serializers.get(1, MarshallableEntryType.DESCRIPTOR_TABLE_COLUMN.id());

            CatalogTableColumnDescriptor descriptor = serializer.readFrom(input);
            int tableId = input.readVarIntAsInt();

            return new AlterColumnEntry(tableId, descriptor);
        }

        @Override
        public void writeTo(AlterColumnEntry value, CatalogObjectDataOutput output) throws IOException {
            serializers.get(1, value.descriptor().typeId()).writeTo(value.descriptor(), output);

            output.writeVarInt(value.tableId());
        }
    }

    /**
     * Serializer for {@link AlterColumnEntry}.
     */
    @CatalogSerializer(version = 2, since = "3.1.0")
    static class AlterColumnEntrySerializerV2 implements CatalogObjectSerializer<AlterColumnEntry> {
        @Override
        public AlterColumnEntry readFrom(CatalogObjectDataInput input) throws IOException {
            CatalogTableColumnDescriptor descriptor = input.readEntry(CatalogTableColumnDescriptor.class);
            int tableId = input.readVarIntAsInt();

            return new AlterColumnEntry(tableId, descriptor);
        }

        @Override
        public void writeTo(AlterColumnEntry value, CatalogObjectDataOutput output) throws IOException {
            output.writeEntry(value.descriptor());
            output.writeVarInt(value.tableId());
        }
    }
}
