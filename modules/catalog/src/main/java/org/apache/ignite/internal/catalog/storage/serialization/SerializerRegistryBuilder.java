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
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.jetbrains.annotations.Nullable;

/**
 * Serializers registry builder.
 */
public class SerializerRegistryBuilder {
    private final @Nullable CatalogEntrySerializerProvider provider;

    private final List<CatalogSerializerTypeDefinition> serializerTypes;

    SerializerRegistryBuilder(List<CatalogSerializerTypeDefinition> serializerTypes, @Nullable CatalogEntrySerializerProvider provider) {
        this.serializerTypes = serializerTypes;
        this.provider = provider;
    }

    /**
     * Returns a registry (map) of available serializers.
     *
     * @return Registry of available serializers.
     */
    public Int2ObjectMap<VersionAwareSerializer<? extends MarshallableEntry>[]> build() {
        Int2ObjectMap<List<VersionAwareSerializer<? extends MarshallableEntry>>> mapByType = mapSerializersByType();
        Int2ObjectMap<VersionAwareSerializer<? extends MarshallableEntry>[]> resultMap = remapToOrderedArray(mapByType);

        return Int2ObjectMaps.unmodifiable(resultMap);
    }

    private Int2ObjectMap<List<VersionAwareSerializer<? extends MarshallableEntry>>> mapSerializersByType() {
        Int2ObjectMap<List<VersionAwareSerializer<? extends MarshallableEntry>>> result = new Int2ObjectOpenHashMap<>();

        for (CatalogSerializerTypeDefinition type : serializerTypes) {
            Class<?> containerClass = type.container();

            assert containerClass != null : type;

            boolean atLeastSingleSerializerExists = false;

            for (Class<?> clazz : containerClass.getDeclaredClasses()) {
                CatalogSerializer ann = clazz.getAnnotation(CatalogSerializer.class);

                if (ann == null) {
                    continue;
                }

                if (!CatalogObjectSerializer.class.isAssignableFrom(clazz)) {
                    throw new IllegalStateException(IgniteStringFormatter.format(
                            "The target class doesn't implement the required interface [class={}, interface={}].",
                            clazz.getCanonicalName(), CatalogObjectSerializer.class.getCanonicalName()));
                }

                List<VersionAwareSerializer<? extends MarshallableEntry>> serializers =
                        result.computeIfAbsent(type.id(), v -> new ArrayList<>());

                try {
                    CatalogObjectSerializer<? extends MarshallableEntry> serializer = instantiate(clazz);

                    if (ann.version() <= 0) {
                        throw new IllegalArgumentException("Serializer `version` attribute must be positive "
                                + "[version=" + ann.version() + ", class=" + clazz.getCanonicalName() + "].");
                    }

                    if (ann.since().isBlank()) {
                        throw new IllegalArgumentException("Serializer 'since' attribute can't be empty or blank "
                                + "[class=" + clazz.getCanonicalName() + "].");
                    }

                    serializers.add(new VersionAwareSerializer<>(serializer, ann.version()));
                } catch (InvocationTargetException | InstantiationException | IllegalAccessException e) {
                    throw new RuntimeException("Cannot instantiate serializer [class=" + clazz + "].", e);
                }

                atLeastSingleSerializerExists = true;
            }

            if (!atLeastSingleSerializerExists) {
                throw new IllegalStateException("At least one serializer must be implemented [type=" + type + "].");
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
                            "Serializer version must be incremented by one [typeId={}, version={}, expected={}, class={}].",
                            typeId, serializer.version(), orderedSerializers.length, serializer.delegate().getClass()));
                }

                if (orderedSerializers[versionIdx] != null) {
                    throw new IllegalArgumentException(IgniteStringFormatter.format(
                            "Duplicate serializer version [serializer1={}, serializer2={}].",
                            orderedSerializers[versionIdx].delegate().getClass().getCanonicalName(),
                            serializer.delegate().getClass().getCanonicalName()
                    ));
                }

                orderedSerializers[versionIdx] = serializer;
            }

            result.put(typeId, orderedSerializers);
        }

        return result;
    }
}
