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

package org.apache.ignite.internal.catalog.storage.serialization.utils;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogEntrySerializerProvider;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntry;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;
import org.apache.ignite.internal.catalog.storage.serialization.VersionAwareSerializer;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.jetbrains.annotations.Nullable;

/**
 * Serializers registry builder.
 */
public class SerializerRegistryBuilder {
    private final @Nullable CatalogEntrySerializerProvider provider;

    public SerializerRegistryBuilder(@Nullable CatalogEntrySerializerProvider provider) {
        this.provider = provider;
    }

    /**
     * Returns a registry (map) of available serializers.
     *
     * @return Registry of available serializers.
     */
    public Int2ObjectMap<VersionAwareSerializer<? extends MarshallableEntry>[]> build() {
        Map<Integer, List<VersionAwareSerializer<? extends MarshallableEntry>>> mapByType = mapSerializersByType();

        Int2ObjectMap<VersionAwareSerializer<? extends MarshallableEntry>[]> resultMap = remapToOrderedArray(mapByType);

        return Int2ObjectMaps.unmodifiable(resultMap);
    }

    private Map<Integer, List<VersionAwareSerializer<? extends MarshallableEntry>>> mapSerializersByType() {
        Map<Integer, List<VersionAwareSerializer<? extends MarshallableEntry>>> result = new HashMap<>();

        for (MarshallableEntryType type : MarshallableEntryType.values()) {
            Class<?> containerClass = type.serializersContainer();

            assert containerClass != null : type;

            for (Class<?> clazz : containerClass.getDeclaredClasses()) {
                CatalogSerializer ann = clazz.getAnnotation(CatalogSerializer.class);

                if (ann == null) {
                    continue;
                }

                List<VersionAwareSerializer<? extends MarshallableEntry>> serializers =
                        result.computeIfAbsent(type.id(), v -> new ArrayList<>());

                try {
                    CatalogObjectSerializer<? extends MarshallableEntry> serializer = instantiate(clazz);

                    if (ann.version() <= 0) {
                        throw new IllegalArgumentException("Serializer version must be positive [class=" + clazz.getCanonicalName() + "].");
                    }

                    serializers.add(new VersionAwareSerializer<>(serializer, ann.version()));
                } catch (InvocationTargetException | InstantiationException | IllegalAccessException e) {
                    throw new RuntimeException("Cannot instantiate serializer [class=" + clazz + "].", e);
                }
            }
        }

        return result;
    }

    private CatalogObjectSerializer<? extends MarshallableEntry> instantiate(Class<?> cls)
            throws InvocationTargetException, InstantiationException, IllegalAccessException {
        Constructor<?>[] constructors = cls.getDeclaredConstructors();

        for (Constructor<?> constructor : constructors) {
            constructor.setAccessible(true);

            if (constructor.getParameterCount() == 0) {
                return (CatalogObjectSerializer<? extends MarshallableEntry>) constructor.newInstance();
            }

            if (provider != null && constructor.getParameterCount() == 1 && CatalogEntrySerializerProvider.class.isAssignableFrom(
                    constructor.getParameterTypes()[0])) {
                return (CatalogObjectSerializer<? extends MarshallableEntry>) constructor.newInstance(provider);
            }
        }

        throw new IllegalStateException("Unable to create serializer, required constructor was not found [class=" + cls + "].");
    }

    private static Int2ObjectMap<VersionAwareSerializer<? extends MarshallableEntry>[]> remapToOrderedArray(
            Map<Integer, List<VersionAwareSerializer<? extends MarshallableEntry>>> mapByType) {
        Int2ObjectMap<VersionAwareSerializer<? extends MarshallableEntry>[]> result = new Int2ObjectOpenHashMap<>(mapByType.size());

        for (Map.Entry<Integer, List<VersionAwareSerializer<? extends MarshallableEntry>>> entry : mapByType.entrySet()) {
            List<VersionAwareSerializer<? extends MarshallableEntry>> serializers = entry.getValue();
            int typeId = entry.getKey();

            VersionAwareSerializer<? extends MarshallableEntry>[] orderedSerializers = new VersionAwareSerializer[serializers.size()];

            for (VersionAwareSerializer<? extends MarshallableEntry> serializer : serializers) {
                int versionIdx = serializer.version() - 1;

                if (versionIdx >= orderedSerializers.length) {
                    throw new IllegalArgumentException(IgniteStringFormatter.format(
                            "The serializer version must be incremented by one [type={}, ver={}, max={}, class={}].",
                            typeId, serializer.version(), orderedSerializers.length, serializer.delegate().getClass()));
                }

                if (orderedSerializers[versionIdx] != null) {
                    throw new IllegalArgumentException(IgniteStringFormatter.format(
                            "Duplicate serializer version [serializer1={}, serializer2={}].",
                            orderedSerializers[versionIdx].delegate().getClass(), serializer.delegate().getClass()));
                }

                orderedSerializers[versionIdx] = serializer;
            }

            result.put(typeId, orderedSerializers);
        }

        return result;
    }
}
