/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.network.serialization.marshal;

import static java.util.Collections.singletonList;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.ignite.internal.network.serialization.ClassDescriptor;

/**
 * Utility to (un)marshal built-in collections and maps.
 */
class BuiltInContainerMarshallers {
    /**
     * Map of all classes which are built-in collections AND may have different sizes AND are mutable. This makes
     * them eligible for a generic unmarshal algorithm: read length, create an empty collection, then read N elements
     * and add each of them into the collection.
     */
    private final Map<Class<?>, Supplier<? extends Collection<?>>> mutableBuiltInCollectionFactories = Map.of(
            ArrayList.class, ArrayList::new,
            LinkedList.class, LinkedList::new,
            HashSet.class, HashSet::new,
            LinkedHashSet.class, LinkedHashSet::new
    );

    /**
     * Map of all classes which are built-in maps AND may have different sizes AND are mutable. This makes
     * them eligible for a generic unmarshal algorithm: read length, create an empty map, then read N entries
     * and put each of them into the map.
     */
    private final Map<Class<?>, Supplier<? extends Map<?, ?>>> mutableBuiltInMapFactories = Map.of(
            HashMap.class, HashMap::new,
            LinkedHashMap.class, LinkedHashMap::new
    );

    /**
     * Used to write elements.
     */
    private final ValueWriter<?> elementWriter;

    BuiltInContainerMarshallers(ValueWriter<?> elementWriter) {
        this.elementWriter = elementWriter;
    }

    void writeGenericRefArray(Object[] array, ClassDescriptor arrayDescriptor, DataOutput output, MarshallingContext context)
            throws IOException, MarshalException {
        output.writeUTF(array.getClass().getComponentType().getName());
        writeCollection(Arrays.asList(array), arrayDescriptor, output, context);
    }

    <T> T[] preInstantiateGenericRefArray(DataInput input) throws IOException {
        return BuiltInMarshalling.preInstantiateGenericRefArray(input);
    }

    <T> void fillGenericRefArray(DataInput input, T[] array, ValueReader<T> elementReader, UnmarshallingContext context)
            throws IOException, UnmarshalException {
        BuiltInMarshalling.fillGenericRefArray(input, array, elementReader, context);
    }

    void writeBuiltInCollection(Collection<?> object, ClassDescriptor descriptor, DataOutput output, MarshallingContext context)
            throws IOException, MarshalException {
        if (supportsAsMutableBuiltInCollection(descriptor)) {
            writeCollection(object, descriptor, output, context);
        } else if (descriptor.isSingletonList()) {
            writeSingletonList((List<?>) object, descriptor, output, context);
        } else {
            throw new IllegalStateException("Marshalling of " + descriptor.clazz() + " is not supported, but it's marked as a built-in");
        }
    }

    /**
     * Returns {@code true} if the given descriptor is supported as a built-in mutable collection. Such types
     * are eligible for a generic unmarshal algorithm: read length, create an empty collection, then read N elements
     * and add each of them into the collection.
     *
     * @param descriptor the descriptor to check
     * @return {@code true} if the given descriptor is supported as a built-in mutable collection
     */
    private boolean supportsAsMutableBuiltInCollection(ClassDescriptor descriptor) {
        return mutableBuiltInCollectionFactories.containsKey(descriptor.clazz());
    }

    private void writeCollection(
            Collection<?> collection,
            ClassDescriptor collectionDescriptor,
            DataOutput output,
            MarshallingContext context
    ) throws IOException, MarshalException {
        context.addUsedDescriptor(collectionDescriptor);

        BuiltInMarshalling.writeCollection(collection, output, valueWriter(), context);
    }

    @SuppressWarnings("unchecked")
    private <T> ValueWriter<T> valueWriter() {
        return (ValueWriter<T>) elementWriter;
    }

    private void writeSingletonList(List<?> list, ClassDescriptor listDescriptor, DataOutput output, MarshallingContext context)
            throws MarshalException, IOException {
        assert list.size() == 1;

        Object element = list.get(0);

        context.addUsedDescriptor(listDescriptor);

        valueWriter().write(element, output, context);
    }

    @SuppressWarnings("unchecked")
    private <T, C extends Collection<T>> Supplier<C> requiredCollectionFactory(ClassDescriptor collectionDescriptor) {
        Supplier<C> collectionFactory = (Supplier<C>) mutableBuiltInCollectionFactories.get(collectionDescriptor.clazz());

        if (collectionFactory == null) {
            throw new IllegalStateException("Did not find a collection factory for " + collectionDescriptor.clazz()
                    + " even though it is marked as a built-in");
        }

        return collectionFactory;
    }

    Object preInstantiateBuiltInMutableCollection(ClassDescriptor collectionDescriptor) {
        // TODO: IGNITE-16229 - proper immutable collections unmarshalling?
        if (collectionDescriptor.isSingletonList()) {
            return singletonList(null);
        }

        return preInstantiateNonSingletonCollection(collectionDescriptor);
    }

    private <T, C extends Collection<T>> C preInstantiateNonSingletonCollection(ClassDescriptor collectionDescriptor) {
        Supplier<C> collectionFactory = requiredCollectionFactory(collectionDescriptor);

        return BuiltInMarshalling.preInstantiateCollection(collectionFactory);
    }

    <T, C extends Collection<T>> void fillBuiltInCollectionFrom(
            DataInput input,
            C collection,
            ClassDescriptor collectionDescriptor,
            ValueReader<T> elementReader,
            UnmarshallingContext context
    ) throws UnmarshalException, IOException {
        // TODO: IGNITE-16229 - proper immutable collections unmarshalling?
        if (collectionDescriptor.isSingletonList()) {
            BuiltInMarshalling.fillSingletonCollectionFrom(input, collection, elementReader, context);
            return;
        }

        BuiltInMarshalling.fillCollectionFrom(input, collection, elementReader, context);
    }

    void writeBuiltInMap(Map<?, ?> map, ClassDescriptor mapDescriptor, DataOutput output, MarshallingContext context)
            throws MarshalException, IOException {
        if (!supportsAsBuiltInMap(mapDescriptor)) {
            throw new IllegalStateException("Marshalling of " + mapDescriptor.clazz() + " is not supported, but it's marked as a built-in");
        }

        context.addUsedDescriptor(mapDescriptor);

        BuiltInMarshalling.writeMap(
                map,
                output,
                valueWriter(),
                valueWriter(),
                context
        );
    }

    private boolean supportsAsBuiltInMap(ClassDescriptor mapDescriptor) {
        return mutableBuiltInMapFactories.containsKey(mapDescriptor.clazz());
    }

    <K, V, M extends Map<K, V>> M preInstantiateBuiltInMutableMap(ClassDescriptor mapDescriptor) {
        Supplier<M> mapFactory = requiredMapFactory(mapDescriptor);
        return BuiltInMarshalling.preInstantiateMap(mapFactory);
    }

    private <K, V, M extends Map<K, V>> Supplier<M> requiredMapFactory(ClassDescriptor mapDescriptor) {
        @SuppressWarnings("unchecked")
        Supplier<M> mapFactory = (Supplier<M>) mutableBuiltInMapFactories.get(mapDescriptor.clazz());

        if (mapFactory == null) {
            throw new IllegalStateException("Did not find a map factory for " + mapDescriptor.clazz()
                    + " even though it is marked as a built-in");
        }
        return mapFactory;
    }

    <K, V, M extends Map<K, V>> void fillBuiltInMapFrom(
            DataInput input,
            M map,
            ValueReader<K> keyReader,
            ValueReader<V> valueReader,
            UnmarshallingContext context
    ) throws UnmarshalException, IOException {
        BuiltInMarshalling.fillMapFrom(input, map, keyReader, valueReader, context);
    }
}
