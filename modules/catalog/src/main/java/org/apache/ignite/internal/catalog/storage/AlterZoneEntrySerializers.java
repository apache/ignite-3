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
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntry;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;

/**
 * Serializers for {@link AlterZoneEntry}.
 */
public class AlterZoneEntrySerializers {
    /**
     * Serializer for {@link AlterZoneEntry}.
     */
    // TODO https://issues.apache.org/jira/browse/IGNITE-24170 This should be version 2, you need to add the original version 1.
    @CatalogSerializer(version = 1, type = MarshallableEntryType.ALTER_ZONE, since = "3.0.0")
    static class AlterZoneEntrySerializer implements CatalogObjectSerializer<AlterZoneEntry> {
        private final CatalogEntrySerializerProvider serializers;

        public AlterZoneEntrySerializer(CatalogEntrySerializerProvider serializers) {
            this.serializers = serializers;
        }

        @Override
        public AlterZoneEntry readFrom(IgniteDataInput input) throws IOException {
            CatalogObjectSerializer<MarshallableEntry> serializer =
                    serializers.get(1, MarshallableEntryType.DESCRIPTOR_ZONE.id());

            CatalogZoneDescriptor descriptor = (CatalogZoneDescriptor) serializer.readFrom(input);
            // TODO This was added in version 2.
            CatalogZoneDescriptor previousDescriptor = (CatalogZoneDescriptor) serializer.readFrom(input);

            return new AlterZoneEntry(descriptor, previousDescriptor);
        }

        @Override
        public void writeTo(AlterZoneEntry object, IgniteDataOutput output) throws IOException {
            serializers.get(1, object.descriptor().typeId()).writeTo(object.descriptor(), output);
            // TODO This was added in version 2.
            serializers.get(1, object.previousDescriptor().typeId()).writeTo(object.previousDescriptor(), output);
        }
    }
}
