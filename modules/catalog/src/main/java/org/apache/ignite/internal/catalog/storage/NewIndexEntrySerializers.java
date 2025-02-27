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
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogEntrySerializerProvider;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializationUtils.IndexDescriptorSerializerHelper;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;

/**
 * Serializers for {@link NewIndexEntry}.
 */
public class NewIndexEntrySerializers {
    /**
     * Serializer for {@link NewIndexEntry}.
     */
    @CatalogSerializer(version = 1, since = "3.0.0")
    static class NewIndexEntrySerializerV1 implements CatalogObjectSerializer<NewIndexEntry> {
        private final IndexDescriptorSerializerHelper serializerHelper;

        NewIndexEntrySerializerV1(CatalogEntrySerializerProvider serializers) {
            serializerHelper = new IndexDescriptorSerializerHelper(serializers);
        }

        @Override
        public NewIndexEntry readFrom(IgniteDataInput input) throws IOException {
            CatalogIndexDescriptor descriptor = serializerHelper.readFrom(input);

            return new NewIndexEntry(descriptor);
        }

        @Override
        public void writeTo(NewIndexEntry entry, IgniteDataOutput output) throws IOException {
            serializerHelper.writeTo(entry.descriptor(), output);
        }
    }
}
