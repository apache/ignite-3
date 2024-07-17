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

package org.apache.ignite.internal.network.serialization.marshal;

import static it.unimi.dsi.fastutil.ints.Int2ObjectMaps.unmodifiable;
import static java.util.Collections.singletonList;
import static org.apache.ignite.internal.network.serialization.marshal.ProtocolMarshalling.writeLength;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;
import org.apache.ignite.internal.network.serialization.BuiltInType;
import org.apache.ignite.internal.network.serialization.ClassDescriptor;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;

/**
 * Utility to (un)marshal built-in collections and maps.
 */
class BuiltInContainerMarshallers {
    /**
     * Map of all classes which are built-in collections AND may have different sizes AND are mutable. This makes
     * them eligible for a generic unmarshal algorithm: read length, create an empty collection, then read N elements
     * and add each of them into the collection.
     */
    private final Int2ObjectMap<IntFunction<? extends Collection<?>>> mutableBuiltInCollectionFactories = unmodifiable(
            new Int2ObjectOpenHashMap<>(Map.of(
                    BuiltInType.ARRAY_LIST.descriptorId(), ArrayList::new,
                    BuiltInType.LINKED_LIST.descriptorId(), size -> new LinkedList<>(),
                    BuiltInType.HASH_SET.descriptorId(), HashSet::new,
                    BuiltInType.LINKED_HASH_SET.descriptorId(), LinkedHashSet::new
            ))
    );

    /**
     * Map of all classes which are built-in maps AND may have different sizes AND are mutable. This makes
     * them eligible for a generic unmarshal algorithm: read length, create an empty map, then read N entries
     * and put each of them into the map.
     */
    private final Int2ObjectMap<IntFunction<? extends Map<?, ?>>> mutableBuiltInMapFactories = unmodifiable(new Int2ObjectOpenHashMap<>(
            Map.of(
                    BuiltInType.HASH_MAP.descriptorId(), HashMap::new,
                    BuiltInType.LINKED_HASH_MAP.descriptorId(), LinkedHashMap::new
            )
    ));

    /**
     * Used to write elements.
     */
    private final TypedValueWriter typedWriter;
    private final ValueWriter<Object> untypedWriter;

    private final TypedValueReader typedReader;

    BuiltInContainerMarshallers(TypedValueWriter typedWriter, TypedValueReader typedReader) {
        this.typedWriter = typedWriter;
        untypedWriter = (obj, out, ctx) -> typedWriter.write(obj, null, out, ctx);
        this.typedReader = typedReader;
    }

    void writeGenericRefArray(Object[] array, ClassDescriptor arrayDescriptor, IgniteDataOutput output, MarshallingContext context)
            throws IOException, MarshalException {
        Class<?> componentType = array.getClass().getComponentType();

        BuiltInMarshalling.writeClass(componentType, output);
        writeLength(array.length, output);

        if (array.length > 0 && arrayDescriptor.isComponentSerializationTypeKnownUpfront()) {
            BitSet nullsBitSet = new BitSet(array.length);

            for (int i = 0; i < array.length; i++) {
                if (array[i] == null) {
                    nullsBitSet.set(i);
                }
            }

            BuiltInMarshalling.writeBitSet(nullsBitSet, output);
        }

        for (Object object : array) {
            typedWriter.write(object, arrayDescriptor.componentTypeDescriptor(), output, context);
        }

        context.addUsedDescriptor(arrayDescriptor);
    }

    <T> T[] preInstantiateGenericRefArray(DataInput input, UnmarshallingContext context) throws IOException, UnmarshalException {
        return BuiltInMarshalling.preInstantiateGenericRefArray(input, context);
    }

    @SuppressWarnings("unchecked")
    <T> void fillGenericRefArrayFrom(IgniteDataInput input, T[] array, ClassDescriptor arrayDescriptor, UnmarshallingContext context)
            throws IOException, UnmarshalException {
        if (array.length == 0) {
            return;
        }

        if (arrayDescriptor.isComponentSerializationTypeKnownUpfront()) {
            BitSet nullsBitSet = BuiltInMarshalling.readBitSet(input);

            for (int i = 0; i < array.length; i++) {
                if (!nullsBitSet.get(i)) {
                    array[i] = (T) typedReader.read(input, arrayDescriptor.componentTypeDescriptor(), context);
                }
            }
        } else {
            for (int i = 0; i < array.length; i++) {
                array[i] = (T) typedReader.read(input, arrayDescriptor.componentTypeDescriptor(), context);
            }
        }
    }

    boolean supportsCollection(ClassDescriptor descriptor) {
        return descriptor.isSingletonList() || supportsAsMutableBuiltInCollection(descriptor);
    }

    void writeBuiltInCollection(Collection<?> object, ClassDescriptor descriptor, IgniteDataOutput output, MarshallingContext context)
            throws IOException, MarshalException {
        if (supportsAsMutableBuiltInCollection(descriptor)) {
            writeCollection(object, descriptor, output, context);
        } else if (descriptor.isSingletonList()) {
            writeSingletonList((List<?>) object, descriptor, output, context);
        } else {
            throw new IllegalStateException("Marshalling of " + descriptor.className()
                    + " is not supported, but it's marked as a built-in");
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
        return mutableBuiltInCollectionFactories.containsKey(descriptor.descriptorId());
    }

    private void writeCollection(
            Collection<?> collection,
            ClassDescriptor collectionDescriptor,
            IgniteDataOutput output,
            MarshallingContext context
    ) throws IOException, MarshalException {
        BuiltInMarshalling.writeCollection(collection, output, untypedWriter(), context);

        context.addUsedDescriptor(collectionDescriptor);
    }

    @SuppressWarnings("unchecked")
    private <T> ValueWriter<T> untypedWriter() {
        return (ValueWriter<T>) untypedWriter;
    }

    private void writeSingletonList(List<?> list, ClassDescriptor listDescriptor, IgniteDataOutput output, MarshallingContext context)
            throws MarshalException, IOException {
        assert list.size() == 1;

        Object element = list.get(0);

        context.addUsedDescriptor(listDescriptor);

        typedWriter.write(element, null, output, context);
    }

    @SuppressWarnings("unchecked")
    private <T, C extends Collection<T>> IntFunction<C> requiredCollectionFactory(ClassDescriptor collectionDescriptor) {
        IntFunction<C> collectionFactory = (IntFunction<C>) mutableBuiltInCollectionFactories.get(collectionDescriptor.descriptorId());

        if (collectionFactory == null) {
            throw new IllegalStateException("Did not find a collection factory for " + collectionDescriptor.className()
                    + " even though it is marked as a built-in");
        }

        return collectionFactory;
    }

    Object preInstantiateBuiltInMutableCollection(ClassDescriptor collectionDescriptor, DataInput input, UnmarshallingContext context)
            throws IOException {
        if (collectionDescriptor.isSingletonList()) {
            return singletonList(null);
        }

        return preInstantiateNonSingletonCollection(collectionDescriptor, input, context);
    }

    private <T, C extends Collection<T>> C preInstantiateNonSingletonCollection(
            ClassDescriptor collectionDescriptor,
            DataInput input,
            UnmarshallingContext context
    ) throws IOException {
        IntFunction<C> collectionFactory = requiredCollectionFactory(collectionDescriptor);

        context.markSource(ProtocolMarshalling.MAX_LENGTH_BYTE_COUNT);

        C collection = BuiltInMarshalling.preInstantiateCollection(input, collectionFactory);

        context.resetSourceToMark();

        return collection;
    }

    <T, C extends Collection<T>> void fillBuiltInCollectionFrom(
            IgniteDataInput input,
            C collection,
            ClassDescriptor collectionDescriptor,
            ValueReader<T> elementReader,
            UnmarshallingContext context
    ) throws UnmarshalException, IOException {
        if (collectionDescriptor.isSingletonList()) {
            BuiltInMarshalling.fillSingletonCollectionFrom(input, collection, elementReader, context);
            return;
        }

        BuiltInMarshalling.fillCollectionFrom(input, collection, elementReader, context);
    }

    void writeBuiltInMap(Map<?, ?> map, ClassDescriptor mapDescriptor, IgniteDataOutput output, MarshallingContext context)
            throws MarshalException, IOException {
        if (!supportsAsBuiltInMap(mapDescriptor)) {
            throw new IllegalStateException("Marshalling of " + mapDescriptor.className()
                    + " is not supported, but it's marked as a built-in");
        }

        context.addUsedDescriptor(mapDescriptor);

        BuiltInMarshalling.writeMap(map, output, untypedWriter(), untypedWriter(), context);
    }

    boolean supportsAsBuiltInMap(ClassDescriptor mapDescriptor) {
        return mutableBuiltInMapFactories.containsKey(mapDescriptor.descriptorId());
    }

    <K, V, M extends Map<K, V>> M preInstantiateBuiltInMutableMap(
            ClassDescriptor mapDescriptor,
            DataInput input,
            UnmarshallingContext context
    ) throws IOException {
        IntFunction<M> mapFactory = requiredMapFactory(mapDescriptor);

        context.markSource(ProtocolMarshalling.MAX_LENGTH_BYTE_COUNT);

        M map = BuiltInMarshalling.preInstantiateMap(input, mapFactory);

        context.resetSourceToMark();

        return map;
    }

    private <K, V, M extends Map<K, V>> IntFunction<M> requiredMapFactory(ClassDescriptor mapDescriptor) {
        @SuppressWarnings("unchecked")
        IntFunction<M> mapFactory = (IntFunction<M>) mutableBuiltInMapFactories.get(mapDescriptor.descriptorId());

        if (mapFactory == null) {
            throw new IllegalStateException("Did not find a map factory for " + mapDescriptor.className()
                    + " even though it is marked as a built-in");
        }
        return mapFactory;
    }

    <K, V, M extends Map<K, V>> void fillBuiltInMapFrom(
            IgniteDataInput input,
            M map,
            ValueReader<K> keyReader,
            ValueReader<V> valueReader,
            UnmarshallingContext context
    ) throws UnmarshalException, IOException {
        BuiltInMarshalling.fillMapFrom(input, map, keyReader, valueReader, context);
    }
}
