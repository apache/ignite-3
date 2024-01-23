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

package org.apache.ignite.internal.catalog.serialization;

import java.io.IOException;
import org.apache.ignite.internal.catalog.descriptors.CatalogObjectDescriptor;
import org.apache.ignite.internal.catalog.serialization.DescriptorHeaderSerializer.CatalogDescriptorHeader;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;

/**
 * Catalog object descriptor header serializer.
 */
class DescriptorHeaderSerializer implements CatalogEntrySerializer<CatalogDescriptorHeader> {
    static DescriptorHeaderSerializer INSTANCE = new DescriptorHeaderSerializer();

    @Override
    public CatalogDescriptorHeader readFrom(int version, IgniteDataInput input) throws IOException {
        return new CatalogDescriptorHeader(version, input);
    }

    @Override
    public void writeTo(CatalogDescriptorHeader descriptor, int version, IgniteDataOutput output) throws IOException {
        output.writeInt(descriptor.id);
        output.writeUTF(descriptor.name);
        output.writeLong(descriptor.updateToken);
    }

    /**
     * Utility class to read catalog object descriptor common part.
     */
    static class CatalogDescriptorHeader {
        private final int id;
        private final String name;
        private final long updateToken;

        CatalogDescriptorHeader(CatalogObjectDescriptor desc) {
            this.id = desc.id();
            this.name = desc.name();
            this.updateToken = desc.updateToken();
        }

        CatalogDescriptorHeader(int version, IgniteDataInput input) throws IOException {
            id = input.readInt();
            name = input.readUTF();
            updateToken = input.readLong();
        }

        public int id() {
            return id;
        }

        public String name() {
            return name;
        }

        public long updateToken() {
            return updateToken;
        }
    }
}
