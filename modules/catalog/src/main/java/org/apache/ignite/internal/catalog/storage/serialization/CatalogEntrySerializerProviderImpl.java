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

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;

/**
 * Default serializer provider implementation.
 */
class CatalogEntrySerializerProviderImpl implements CatalogEntrySerializerProvider {
    private final Int2ObjectMap<VersionAwareSerializer<? extends MarshallableEntry>[]> serializers;

    {
        SerializerRegistryBuilder registryBuilder = new SerializerRegistryBuilder(this);

        try {
            serializers = registryBuilder.build();

            if (serializers.size() != MarshallableEntryType.values().length) {
                for (MarshallableEntryType type : MarshallableEntryType.values()) {
                    if (!serializers.containsKey(type.id())) {
                        throw new IllegalStateException("Serializer for type " + type + " not found.");
                    }
                }
            }
        } catch (Throwable t) {
            IgniteLogger logger = Loggers.forClass(CatalogEntrySerializerProviderImpl.class);

            logger.error("Failed to build serializer registry.", t);

            throw t;
        }
    }

    @Override
    public <T extends MarshallableEntry> VersionAwareSerializer<T> get(int version, int typeId) {
        VersionAwareSerializer<? extends MarshallableEntry>[] serializersArray = serializerOrThrow(typeId);

        return (VersionAwareSerializer<T>) serializersArray[0];
    }

    @Override
    public int latestSerializerVersion(int typeId) {
        return serializerOrThrow(typeId).length;
    }

    private VersionAwareSerializer<? extends MarshallableEntry>[] serializerOrThrow(int typeId) {
        VersionAwareSerializer<? extends MarshallableEntry>[] serializersArray = serializers.get(typeId);

        if (serializersArray == null) {
            throw new IllegalArgumentException("Unknown type ID: " + typeId);
        }

        return serializersArray;
    }
}
