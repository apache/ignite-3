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
import it.unimi.dsi.fastutil.ints.Int2ObjectMap.Entry;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;

/**
 * Default serializer provider implementation.
 */
class CatalogEntrySerializerProviderImpl implements CatalogEntrySerializerProvider {
    private final Int2ObjectMap<CatalogVersionAwareSerializer<? extends MarshallableEntry>[]> serializers;

    CatalogEntrySerializerProviderImpl(List<CatalogSerializerTypeDefinition> serializerTypes) {
        SerializerRegistryBuilder registryBuilder = new SerializerRegistryBuilder(serializerTypes, this);

        try {
            serializers = registryBuilder.build();
        } catch (Throwable t) {
            IgniteLogger logger = Loggers.forClass(CatalogEntrySerializerProviderImpl.class);

            logger.error("Failed to build serializer registry.", t);

            throw t;
        }
    }

    @Override
    public <T extends MarshallableEntry> CatalogVersionAwareSerializer<T> get(int version, int typeId) {
        CatalogVersionAwareSerializer<? extends MarshallableEntry>[] serializersArray = serializerOrThrow(typeId);

        if (version <= 0) {
            throw new IllegalArgumentException("Serializer version must be positive [version=" + version + "].");
        }

        if (version > serializersArray.length) {
            throw new IllegalArgumentException("Required serializer version not found [version=" + version + "].");
        }

        return (CatalogVersionAwareSerializer<T>) serializersArray[version - 1];
    }

    @Override
    public int latestSerializerVersion(int typeId) {
        return serializerOrThrow(typeId).length;
    }

    private CatalogVersionAwareSerializer<? extends MarshallableEntry>[] serializerOrThrow(int typeId) {
        CatalogVersionAwareSerializer<? extends MarshallableEntry>[] serializersArray = serializers.get(typeId);

        if (serializersArray == null) {
            throw new IllegalArgumentException("Unknown type ID: " + typeId);
        }

        return serializersArray;
    }

    /**
     * Serializers registry builder.
     */
    private static class SerializerRegistryBuilder {
        private final CatalogEntrySerializerProvider provider;

        private final List<CatalogSerializerTypeDefinition> serializerTypes;

        SerializerRegistryBuilder(List<CatalogSerializerTypeDefinition> serializerTypes, CatalogEntrySerializerProvider provider) {
            this.serializerTypes = serializerTypes;
            this.provider = provider;
        }

        /**
         * Returns a registry (map) of available serializers.
         *
         * @return Registry of available serializers.
         */
        Int2ObjectMap<CatalogVersionAwareSerializer<? extends MarshallableEntry>[]> build() {
            Int2ObjectMap<List<CatalogVersionAwareSerializer<? extends MarshallableEntry>>> mapByType = mapSerializersByType();
            Int2ObjectMap<CatalogVersionAwareSerializer<? extends MarshallableEntry>[]> resultMap = remapToOrderedArray(mapByType);

            return Int2ObjectMaps.unmodifiable(resultMap);
        }

        private Int2ObjectMap<List<CatalogVersionAwareSerializer<? extends MarshallableEntry>>> mapSerializersByType() {
            Int2ObjectMap<List<CatalogVersionAwareSerializer<? extends MarshallableEntry>>> result = new Int2ObjectOpenHashMap<>();

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

                    List<CatalogVersionAwareSerializer<? extends MarshallableEntry>> serializers =
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

                        serializers.add(new CatalogVersionAwareSerializer<>(serializer, ann.version()));
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

                // This constructor is only needed for serialization protocol V1,
                // because there is no object version in that protocol, so we assigned them version 1
                // to make CatalogEntrySerializerProvider API work for both protocol versions.
                if (constructor.getParameterCount() == 1 && CatalogEntrySerializerProvider.class.isAssignableFrom(
                        constructor.getParameterTypes()[0])) {
                    return (CatalogObjectSerializer<? extends MarshallableEntry>) constructor.newInstance(provider);
                }
            }

            throw new IllegalStateException("Unable to create serializer, required constructor was not found [class=" + cls + "].");
        }

        private static Int2ObjectMap<CatalogVersionAwareSerializer<? extends MarshallableEntry>[]> remapToOrderedArray(
                Int2ObjectMap<List<CatalogVersionAwareSerializer<? extends MarshallableEntry>>> mapByType) {
            Int2ObjectMap<CatalogVersionAwareSerializer<? extends MarshallableEntry>[]> result =
                    new Int2ObjectOpenHashMap<>(mapByType.size());

            for (Entry<List<CatalogVersionAwareSerializer<? extends MarshallableEntry>>> entry : mapByType.int2ObjectEntrySet()) {
                List<CatalogVersionAwareSerializer<? extends MarshallableEntry>> serializers = entry.getValue();
                int typeId = entry.getIntKey();

                CatalogVersionAwareSerializer<? extends MarshallableEntry>[] orderedSerializers =
                        new CatalogVersionAwareSerializer[serializers.size()];

                for (CatalogVersionAwareSerializer<? extends MarshallableEntry> serializer : serializers) {
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
}
