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

package org.apache.ignite.internal.catalog.descriptors;

import java.io.IOException;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;

/**
 * Storage profile descriptor.
 */
public class CatalogStorageProfileDescriptor {
    public static final CatalogObjectSerializer<CatalogStorageProfileDescriptor> SERIALIZER = new StorageProfileDescriptorSerializer();

    private final String storageProfile;

    /**
     * Constructor.
     *
     * @param storageProfile Storage profile name.
     */
    public CatalogStorageProfileDescriptor(String storageProfile) {
        this.storageProfile = storageProfile;
    }

    @Override
    public String toString() {
        return S.toString(this);
    }

    /**
     * Storage profile name.
     *
     * @return Name of the storage profile.
     */
    public String storageProfile() {
        return storageProfile;
    }

    /**
     * Serializer for {@link CatalogStorageProfilesDescriptor}.
     */
    private static class StorageProfileDescriptorSerializer implements CatalogObjectSerializer<CatalogStorageProfileDescriptor> {
        @Override
        public CatalogStorageProfileDescriptor readFrom(IgniteDataInput input) throws IOException {
            String storageProfileDescriptor = input.readUTF();

            return new CatalogStorageProfileDescriptor(storageProfileDescriptor);
        }

        @Override
        public void writeTo(CatalogStorageProfileDescriptor descriptor, IgniteDataOutput output) throws IOException {
            output.writeUTF(descriptor.storageProfile());
        }
    }
}
