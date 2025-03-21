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
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogEntrySerializerProvider;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectDataInput;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectDataOutput;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntry;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;

/**
 * Serializers for {@link AlterZoneEntry}.
 */
public class AlterZoneEntrySerializers {
    /**
     * Serializer for {@link AlterZoneEntry}.
     */
    @CatalogSerializer(version = 1, since = "3.0.0")
    static class AlterZoneEntrySerializerV1 implements CatalogObjectSerializer<AlterZoneEntry> {
        private final CatalogEntrySerializerProvider serializers;

        public AlterZoneEntrySerializerV1(CatalogEntrySerializerProvider serializers) {
            this.serializers = serializers;
        }

        @Override
        public AlterZoneEntry readFrom(CatalogObjectDataInput input)throws IOException {
            CatalogObjectSerializer<MarshallableEntry> serializer =
                    serializers.get(1, MarshallableEntryType.DESCRIPTOR_ZONE.id());

            CatalogZoneDescriptor descriptor = (CatalogZoneDescriptor) serializer.readFrom(input);

            return new AlterZoneEntry(descriptor);
        }

        @Override
        public void writeTo(AlterZoneEntry object, CatalogObjectDataOutput output) throws IOException {
            serializers.get(1, object.descriptor().typeId()).writeTo(object.descriptor(), output);
        }
    }

    /**
     * Serializer for {@link AlterZoneEntry}.
     */
    @CatalogSerializer(version = 2, since = "3.1.0")
    static class AlterZoneEntrySerializerV2 implements CatalogObjectSerializer<AlterZoneEntry> {
        @Override
        public AlterZoneEntry readFrom(CatalogObjectDataInput input) throws IOException {
            CatalogZoneDescriptor descriptor = input.readEntry(CatalogZoneDescriptor.class);

            return new AlterZoneEntry(descriptor);
        }

        @Override
        public void writeTo(AlterZoneEntry object, CatalogObjectDataOutput output) throws IOException {
            output.writeEntry(object.descriptor());
        }
    }
}
