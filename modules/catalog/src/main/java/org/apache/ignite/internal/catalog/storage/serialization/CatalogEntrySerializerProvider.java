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

package org.apache.ignite.internal.catalog.storage.serialization;

import java.util.Arrays;

/**
 * Catalog entry serializer provider.
 */
public interface CatalogEntrySerializerProvider {
    /**
     * Gets a catalog entry serializer that supports serialization of the specified type.
     *
     * @param version Serializer version.
     * @param typeId Type id.
     * @return catalog update entry serializer.
     */
    <T extends MarshallableEntry> CatalogObjectSerializer<T> get(int version, int typeId);

    /**
     * Returns the latest available serializer version.
     *
     * @param typeId Type id.
     * @return Latest available serializer version.
     */
    int latestSerializerVersion(int typeId);

    /** Default implementation. */
    CatalogEntrySerializerProvider DEFAULT_PROVIDER =
            new CatalogEntrySerializerProviderImpl(Arrays.asList(MarshallableEntryType.values()));
}
