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

import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntry;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;
import org.apache.ignite.internal.tostring.S;

/**
 * Storage profile descriptor.
 */
public class CatalogStorageProfileDescriptor implements MarshallableEntry {
    private final String storageProfile;

    /**
     * Constructor.
     *
     * @param storageProfile Storage profile name.
     */
    public CatalogStorageProfileDescriptor(String storageProfile) {
        this.storageProfile = storageProfile;
    }

    /**
     * Storage profile name.
     *
     * @return Name of the storage profile.
     */
    public String storageProfile() {
        return storageProfile;
    }

    @Override
    public int typeId() {
        return MarshallableEntryType.DESCRIPTOR_STORAGE_PROFILE.id();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CatalogStorageProfileDescriptor that = (CatalogStorageProfileDescriptor) o;
        return storageProfile.equals(that.storageProfile);
    }

    @Override
    public int hashCode() {
        return storageProfile.hashCode();
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
