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
import org.apache.ignite.internal.catalog.descriptors.CatalogSystemViewDescriptor;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogEntrySerializerProvider;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectDataInput;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectDataOutput;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;

/**
 * Serializers for {@link NewSystemViewEntry}.
 */
public class NewSystemViewEntrySerializers {
    /**
     * Serializer for {@link NewSystemViewEntry}.
     */
    @CatalogSerializer(version = 1, since = "3.0.0")
    static class NewSystemViewEntrySerializerV1 implements CatalogObjectSerializer<NewSystemViewEntry> {
        private final CatalogEntrySerializerProvider serializers;

        public NewSystemViewEntrySerializerV1(CatalogEntrySerializerProvider serializers) {
            this.serializers = serializers;
        }

        @Override
        public NewSystemViewEntry readFrom(CatalogObjectDataInput input)throws IOException {
            CatalogObjectSerializer<CatalogSystemViewDescriptor> serializer =
                    serializers.get(1, MarshallableEntryType.DESCRIPTOR_SYSTEM_VIEW.id());

            CatalogSystemViewDescriptor descriptor = serializer.readFrom(input);

            return new NewSystemViewEntry(descriptor);
        }

        @Override
        public void writeTo(NewSystemViewEntry entry, CatalogObjectDataOutput output) throws IOException {
            serializers.get(1, entry.descriptor().typeId()).writeTo(entry.descriptor(), output);
        }
    }

    /**
     * Serializer for {@link NewSystemViewEntry}.
     */
    @CatalogSerializer(version = 2, since = "3.1.0")
    static class NewSystemViewEntrySerializerV2 implements CatalogObjectSerializer<NewSystemViewEntry> {
        @Override
        public NewSystemViewEntry readFrom(CatalogObjectDataInput input) throws IOException {
            CatalogSystemViewDescriptor descriptor = input.readEntry(CatalogSystemViewDescriptor.class);

            return new NewSystemViewEntry(descriptor);
        }

        @Override
        public void writeTo(NewSystemViewEntry entry, CatalogObjectDataOutput output) throws IOException {
            output.writeEntry(entry.descriptor());
        }
    }
}
