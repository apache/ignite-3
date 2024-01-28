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

import java.io.Serializable;
import org.apache.ignite.internal.tostring.S;

/**
 * Storage profile descriptor.
 */
public class CatalogStorageProfileDescriptor implements Serializable {
    private static final long serialVersionUID = 1653344099975758919L;

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
}
