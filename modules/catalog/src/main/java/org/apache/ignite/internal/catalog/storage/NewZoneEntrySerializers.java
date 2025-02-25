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
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;

/**
 * Serializers for {@link NewZoneEntry}.
 */
public class NewZoneEntrySerializers {
    /**
     * Serializer for {@link NewZoneEntry}.
     */
    @CatalogSerializer(version = 1, type = MarshallableEntryType.NEW_ZONE, since = "3.0.0")
    static class NewZoneEntrySerializerV1 implements CatalogObjectSerializer<NewZoneEntry> {
        private final CatalogEntrySerializerProvider serializers;

        public NewZoneEntrySerializerV1(CatalogEntrySerializerProvider serializers) {
            this.serializers = serializers;
        }

        @Override
        public NewZoneEntry readFrom(IgniteDataInput input) throws IOException {
            CatalogObjectSerializer<CatalogZoneDescriptor> serializer =
                    serializers.get(1, MarshallableEntryType.DESCRIPTOR_ZONE.id());

            CatalogZoneDescriptor descriptor = serializer.readFrom(input);

            return new NewZoneEntry(descriptor);
        }

        @Override
        public void writeTo(NewZoneEntry object, IgniteDataOutput output) throws IOException {
            serializers.get(1, object.descriptor().typeId()).writeTo(object.descriptor(), output);
        }
    }
}
