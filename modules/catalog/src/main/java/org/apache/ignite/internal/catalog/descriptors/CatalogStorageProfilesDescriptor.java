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

import static org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializationUtils.readList;
import static org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializationUtils.writeList;

import java.io.IOException;
import java.util.List;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;

/**
 * Storage profiles descriptor.
 */
public class CatalogStorageProfilesDescriptor {
    public static final CatalogObjectSerializer<CatalogStorageProfilesDescriptor> SERIALIZER = new StorageProfilesDescriptorSerializer();

    private final List<CatalogStorageProfileDescriptor> storageProfiles;

    private final CatalogStorageProfileDescriptor defaultStorageProfile;

    /**
     * Constructor.
     *
     * @param storageProfiles List of storage profile descriptors.
     */
    public CatalogStorageProfilesDescriptor(List<CatalogStorageProfileDescriptor> storageProfiles) {
        this.storageProfiles = storageProfiles;
        this.defaultStorageProfile = storageProfiles.isEmpty() ? null : storageProfiles.get(0);
    }

    /**
     * List of storage profile descriptors.
     *
     * @return List of storage profile descriptors.
     */
    public List<CatalogStorageProfileDescriptor> profiles() {
        return storageProfiles;
    }

    /**
     * Default storage profile.
     *
     * @return Default storage profile.
     */
    public CatalogStorageProfileDescriptor defaultProfile() {
        return defaultStorageProfile;
    }

    @Override
    public String toString() {
        return S.toString(this);
    }

    /**
     * Serializer for {@link CatalogStorageProfilesDescriptor}.
     */
    private static class StorageProfilesDescriptorSerializer implements CatalogObjectSerializer<CatalogStorageProfilesDescriptor> {
        @Override
        public CatalogStorageProfilesDescriptor readFrom(IgniteDataInput input) throws IOException {
            List<CatalogStorageProfileDescriptor> storageProfileDescriptors =
                    readList(CatalogStorageProfileDescriptor.SERIALIZER, input);

            return new CatalogStorageProfilesDescriptor(storageProfileDescriptors);
        }

        @Override
        public void writeTo(CatalogStorageProfilesDescriptor descriptor, IgniteDataOutput output) throws IOException {
            writeList(descriptor.storageProfiles, CatalogStorageProfileDescriptor.SERIALIZER, output);
        }
    }
}
