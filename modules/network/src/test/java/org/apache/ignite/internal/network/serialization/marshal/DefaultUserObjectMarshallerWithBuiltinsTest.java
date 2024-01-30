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

import static java.util.Collections.singletonList;
import static org.apache.ignite.internal.network.serialization.marshal.TestDescriptors.MIN_CUSTOM_DESCRIPTOR_ID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.ignite.internal.lang.IgniteUuid;
import org.apache.ignite.internal.network.serialization.BuiltInType;
import org.apache.ignite.internal.network.serialization.ClassDescriptor;
import org.apache.ignite.internal.network.serialization.ClassDescriptorFactory;
import org.apache.ignite.internal.network.serialization.ClassDescriptorRegistry;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteUnsafeDataInput;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for how {@link DefaultUserObjectMarshaller} handles primitives.
 */
class DefaultUserObjectMarshallerWithBuiltinsTest {
    private final ClassDescriptorRegistry descriptorRegistry = new ClassDescriptorRegistry();
    private final ClassDescriptorFactory descriptorFactory = new ClassDescriptorFactory(descriptorRegistry);

    private final DefaultUserObjectMarshaller marshaller = new DefaultUserObjectMarshaller(descriptorRegistry, descriptorFactory);

    private final CleanSlateUnmarshaller unmarshaller = new CleanSlateUnmarshaller(marshaller, descriptorRegistry);

    @Test
    void marshalsAndUnmarshalsBareObject() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new Object());

        Object unmarshalled = unmarshaller.unmarshalNonNull(marshalled);

        assertThat(unmarshalled.getClass(), is(Object.class));
    }

    @Test
    void marshalsBareObjectUsingOnlyBareObjectDescriptor() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new Object());

        ClassDescriptor expectedDescriptor = descriptorRegistry.getRequiredDescriptor(Object.class);
        assertThat(marshalled.usedDescriptorIds(), equalTo(Set.of(expectedDescriptor.descriptorId())));
    }

    @Test
    void marshalsBareObjectWithCorrectDescriptorIdInMarshalledRepresentation() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new Object());

        assertThat(readDescriptorId(marshalled), is(BuiltInType.BARE_OBJECT.descriptorId()));
    }

    private int readDescriptorId(MarshalledObject marshalled) throws IOException {
        try (var dis = new DataInputStream(new ByteArrayInputStream(marshalled.bytes()))) {
            return ProtocolMarshalling.readDescriptorOrCommandId(dis);
        }
    }

    @Test
    void marshalsObjectArrayUsingExactlyDescriptorsOfObjectArrayAndComponents() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new Object[]{42, "abc"});

        assertThat(marshalled.usedDescriptorIds(), containsInAnyOrder(
                descriptorRegistry.getRequiredDescriptor(Object[].class).descriptorId(),
                descriptorRegistry.getRequiredDescriptor(Integer.class).descriptorId(),
                BuiltInType.STRING_LATIN1.descriptorId()
        ));
    }

    @Test
    void marshalsAndUnmarshalsSimpleEnums() throws Exception {
        SimpleEnum unmarshalled = marshalAndUnmarshal(SimpleEnum.FIRST);

        assertThat(unmarshalled, is(SimpleEnum.FIRST));
    }

    @Test
    void marshalsAndUnmarshalsEnumsWithAnonClassesForMembers() throws Exception {
        EnumWithAnonClassesForMembers unmarshalled = marshalAndUnmarshal(EnumWithAnonClassesForMembers.FIRST);

        assertThat(unmarshalled, is(EnumWithAnonClassesForMembers.FIRST));
    }

    @Test
    void marshalsSimpleEnumsUsingOnlyEnumClassDescriptor() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(SimpleEnum.FIRST);

        ClassDescriptor expectedDescriptor = descriptorRegistry.getRequiredDescriptor(SimpleEnum.class);
        assertThat(marshalled.usedDescriptorIds(), equalTo(Set.of(expectedDescriptor.descriptorId())));
    }

    @Test
    void marshalsEnumsWithAnonClassesForMembersUsingOnlyEnumClassDescriptor() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(EnumWithAnonClassesForMembers.FIRST);

        ClassDescriptor expectedDescriptor = descriptorRegistry.getRequiredDescriptor(EnumWithAnonClassesForMembers.class);
        assertThat(marshalled.usedDescriptorIds(), equalTo(Set.of(expectedDescriptor.descriptorId())));
    }

    @Test
    void marshalsSimpleEnumsUsingCorrectDescriptorIdInMarshalledRepresentation() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(SimpleEnum.FIRST);

        assertThat(readDescriptorId(marshalled), equalTo(descriptorRegistry.getRequiredDescriptor(SimpleEnum.class).descriptorId()));
    }

    @Test
    void marshalsEnumsWithAnonClassesForMembersUsingCorrectDescriptorIdInMarshalledRepresentation() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(EnumWithAnonClassesForMembers.FIRST);

        assertThat(readDescriptorId(marshalled),
                equalTo(descriptorRegistry.getRequiredDescriptor(EnumWithAnonClassesForMembers.class).descriptorId())
        );
    }

    @Test
    void marshalsSimpleEnumInCorrectFormat() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(SimpleEnum.FIRST);

        IgniteDataInput dis = openDataInput(marshalled);

        int descriptorId = ProtocolMarshalling.readDescriptorOrCommandId(dis);
        assertThat(descriptorRegistry.getRequiredDescriptor(descriptorId).className(), is(SimpleEnum.class.getName()));

        ProtocolMarshalling.readObjectId(dis);

        assertThat(dis.readUTF(), is("FIRST"));
        assertThat(dis.available(), is(0));
    }

    @Test
    void marshalsEnumWithAnonClassesForMembersInCorrectFormat() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(EnumWithAnonClassesForMembers.FIRST);

        IgniteDataInput dis = openDataInput(marshalled);

        int descriptorId = ProtocolMarshalling.readDescriptorOrCommandId(dis);
        assertThat(descriptorRegistry.getRequiredDescriptor(descriptorId).className(), is(EnumWithAnonClassesForMembers.class.getName()));

        ProtocolMarshalling.readObjectId(dis);

        assertThat(dis.readUTF(), is("FIRST"));
        assertThat(dis.available(), is(0));
    }

    @ParameterizedTest
    @MethodSource("builtInNonCollectionTypes")
    void marshalsAndUnmarshalsBuiltInNonCollectionTypes(BuiltInTypeValue typeValue) throws Exception {
        MarshalledObject marshalled = marshaller.marshal(typeValue.value);

        Object unmarshalled = marshaller.unmarshal(marshalled.bytes(), descriptorRegistry);

        assertThat(unmarshalled, is(equalTo(typeValue.value)));
        if (typeValue.builtinType != BuiltInType.NULL && typeValue.value.getClass().isArray()) {
            assertThat(unmarshalled, is(notNullValue()));
            assertThat(unmarshalled.getClass().getComponentType(), is(typeValue.value.getClass().getComponentType()));
        }
    }

    @ParameterizedTest
    @MethodSource("builtInNonCollectionTypes")
    void marshalsUsingOnlyCorrespondingDescriptorForBuiltInNonCollectionTypes(BuiltInTypeValue typeValue) throws Exception {
        MarshalledObject marshalled = marshaller.marshal(typeValue.value);

        ClassDescriptor expectedDescriptor = descriptorRegistry.getBuiltInDescriptor(typeValue.builtinType);
        assertThat(marshalled.usedDescriptorIds(), equalTo(Set.of(expectedDescriptor.descriptorId())));
    }

    static Stream<Arguments> builtInNonCollectionTypes() {
        return Stream.of(
                builtInTypeValue((byte) 42, BuiltInType.BYTE_BOXED),
                builtInTypeValue((short) 42, BuiltInType.SHORT_BOXED),
                builtInTypeValue(42, BuiltInType.INT_BOXED),
                builtInTypeValue(42.0f, BuiltInType.FLOAT_BOXED),
                builtInTypeValue((long) 42, BuiltInType.LONG_BOXED),
                builtInTypeValue(42.0, BuiltInType.DOUBLE_BOXED),
                builtInTypeValue(true, BuiltInType.BOOLEAN_BOXED),
                builtInTypeValue('a', BuiltInType.CHAR_BOXED),
                // BARE_OBJECT is handled separately
                builtInTypeValue("abc", BuiltInType.STRING_LATIN1),
                builtInTypeValue("Привет", BuiltInType.STRING),
                builtInTypeValue(UUID.fromString("c6f57d4a-619f-11ec-add6-73bc97c3c49e"), BuiltInType.UUID),
                builtInTypeValue(IgniteUuid.fromString("1234-c6f57d4a-619f-11ec-add6-73bc97c3c49e"),
                        BuiltInType.IGNITE_UUID),
                builtInTypeValue(new Date(42), BuiltInType.DATE),
                builtInTypeValue(new byte[]{1, 2, 3}, BuiltInType.BYTE_ARRAY),
                builtInTypeValue(new short[]{1, 2, 3}, BuiltInType.SHORT_ARRAY),
                builtInTypeValue(new int[]{1, 2, 3}, BuiltInType.INT_ARRAY),
                builtInTypeValue(new float[]{1.0f, 2.0f, 3.0f}, BuiltInType.FLOAT_ARRAY),
                builtInTypeValue(new long[]{1, 2, 3}, BuiltInType.LONG_ARRAY),
                builtInTypeValue(new double[]{1.0, 2.0, 3.0}, BuiltInType.DOUBLE_ARRAY),
                builtInTypeValue(new boolean[]{true, false, true}, BuiltInType.BOOLEAN_ARRAY),
                builtInTypeValue(new boolean[]{true, false, true, false, true, false, true, false, true}, BuiltInType.BOOLEAN_ARRAY),
                builtInTypeValue(new char[]{'a', 'b'}, BuiltInType.CHAR_ARRAY),
                builtInTypeValue(new BigDecimal(42), BuiltInType.DECIMAL),
                builtInTypeValue(BitSet.valueOf(new long[]{42, 43}), BuiltInType.BIT_SET),
                builtInTypeValue(null, BuiltInType.NULL),
                builtInTypeValue(IntHolder.class, BuiltInType.CLASS)
        ).map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("builtInCollectionTypes")
    void marshalsAndUnmarshalsBuiltInCollectionTypes(BuiltInTypeValue typeValue) throws Exception {
        MarshalledObject marshalled = marshaller.marshal(typeValue.value);

        Object unmarshalled = unmarshaller.unmarshalNonNull(marshalled);

        assertThat(unmarshalled, is(equalTo(typeValue.value)));
    }

    @ParameterizedTest
    @MethodSource("builtInCollectionTypes")
    void marshalsAndUnmarshalsBuiltInCollectionTypesToCollectionsOfOriginalTypes(BuiltInTypeValue typeValue) throws Exception {
        MarshalledObject marshalled = marshaller.marshal(typeValue.value);

        Object unmarshalled = unmarshaller.unmarshalNonNull(marshalled);

        assertThat(unmarshalled.getClass(), is(equalTo(typeValue.value.getClass())));
    }

    @ParameterizedTest
    @MethodSource("builtInCollectionTypes")
    void marshalsUsingOnlyCorrespondingDescriptorsForBuiltInCollectionTypes(BuiltInTypeValue typeValue) throws Exception {
        MarshalledObject marshalled = marshaller.marshal(typeValue.value);

        assertThat(marshalled.usedDescriptorIds(), containsInAnyOrder(
                typeValue.builtinType.descriptorId(),
                BuiltInType.INT_BOXED.descriptorId()
        ));
    }

    static Stream<Arguments> builtInCollectionTypes() {
        return Stream.of(
                builtInTypeValue(new ArrayList<>(List.of(42, 43)), BuiltInType.ARRAY_LIST),
                builtInTypeValue(new LinkedList<>(List.of(42, 43)), BuiltInType.LINKED_LIST),
                builtInTypeValue(new HashSet<>(Set.of(42, 43)), BuiltInType.HASH_SET),
                builtInTypeValue(new LinkedHashSet<>(Set.of(42, 43)), BuiltInType.LINKED_HASH_SET),
                builtInTypeValue(singletonList(42), BuiltInType.SINGLETON_LIST),
                builtInTypeValue(new HashMap<>(Map.of(42, 43)), BuiltInType.HASH_MAP),
                builtInTypeValue(new LinkedHashMap<>(Map.of(42, 43)), BuiltInType.LINKED_HASH_MAP)
        ).map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("builtInTypes")
    void marshalsBuiltInTypesWithCorrectDescriptorIdsInMarshalledRepresentation(BuiltInTypeValue typeValue) throws Exception {
        MarshalledObject marshalled = marshaller.marshal(typeValue.value);

        assertThat(readDescriptorId(marshalled), is(equalTo(typeValue.builtinType.descriptorId())));
    }

    static Stream<Arguments> builtInTypes() {
        return Stream.concat(builtInNonCollectionTypes(), builtInCollectionTypes());
    }

    private static BuiltInTypeValue builtInTypeValue(Object value, BuiltInType type) {
        return new BuiltInTypeValue(value, type);
    }

    @ParameterizedTest
    @MethodSource("objectArrays")
    void marshalsAndUnmarshalsObjectArrays(AtomicReference<Object[]> holder) throws Exception {
        Object[] array = holder.get();

        Object[] unmarshalled = marshalAndUnmarshal(array);

        assertThat(unmarshalled, is(equalTo(array)));
    }

    @ParameterizedTest
    @MethodSource("objectArrays")
    void marshalsObjectArraysUsingCorrectDescriptors(AtomicReference<Object[]> holder) throws Exception {
        Object[] array = holder.get();

        MarshalledObject marshalled = marshaller.marshal(array);

        ClassDescriptor descriptor = descriptorRegistry.getRequiredDescriptor(array.getClass());

        assertThat(marshalled.usedDescriptorIds(), hasItem(descriptor.descriptorId()));
    }

    @ParameterizedTest
    @MethodSource("objectArrays")
    void marshalsObjectArraysWithCorrectDescriptorIdsInMarshalledRepresentation(AtomicReference<Object[]> holder) throws Exception {
        Object[] array = holder.get();

        MarshalledObject marshalled = marshaller.marshal(array);

        int descriptorId = readDescriptorId(marshalled);
        assertThat(descriptorRegistry.getRequiredDescriptor(descriptorId).className(), is(array.getClass().getName()));
    }

    private static Stream<Arguments> objectArrays() {
        return Stream.of(
                new String[]{"Ignite", "rulez", null},
                new BigDecimal[]{new BigDecimal(42), new BigDecimal(43), null},
                new Object[]{42, "123", null},
                new BitSet[]{BitSet.valueOf(new long[]{42, 43}), BitSet.valueOf(new long[]{1, 2}), null},
                new Enum[]{SimpleEnum.FIRST, SimpleEnum.SECOND, null},
                new SimpleEnum[]{SimpleEnum.FIRST, SimpleEnum.SECOND, null},
                new Enum[]{EnumWithAnonClassesForMembers.FIRST, EnumWithAnonClassesForMembers.SECOND, null},
                new EnumWithAnonClassesForMembers[]{EnumWithAnonClassesForMembers.FIRST, EnumWithAnonClassesForMembers.SECOND, null},
                new Class<?>[]{String.class, null}
        ).map(AtomicReference::new).map(Arguments::of);
    }

    @Test
    void unmarshalsObjectGraphWithCycleStartingWithSingletonList() throws Exception {
        List<List<?>> mutableList = new ArrayList<>();
        List<List<?>> singletonList = singletonList(mutableList);
        mutableList.add(singletonList);

        List<List<?>> unmarshalled = marshalAndUnmarshal(singletonList);

        assertThat(unmarshalled.get(0).get(0), is(sameInstance(unmarshalled)));
    }

    private <T> T marshalAndUnmarshal(T object) throws MarshalException, UnmarshalException {
        MarshalledObject marshalled = marshaller.marshal(object);
        return unmarshaller.unmarshalNonNull(marshalled);
    }

    @Test
    void unmarshalsObjectGraphWithCycleContainingWithSingletonList() throws Exception {
        List<List<?>> mutableList = new ArrayList<>();
        List<List<?>> singletonList = singletonList(mutableList);
        mutableList.add(singletonList);

        List<List<?>> unmarshalled = marshalAndUnmarshal(mutableList);

        assertThat(unmarshalled.get(0).get(0), is(sameInstance(unmarshalled)));
    }

    @ParameterizedTest
    @MethodSource("mutableContainerSelfAssignments")
    <T> void unmarshalsObjectGraphWithSelfCycleViaMutableContainers(MutableContainerSelfAssignment<T> item) throws Exception {
        T container = item.factory.get();
        item.assignment.accept(container, container);

        T unmarshalled = marshalAndUnmarshal(container);
        T element = item.elementAccess.apply(unmarshalled);

        assertThat(element, is(sameInstance(unmarshalled)));
    }

    @SuppressWarnings("unchecked")
    private static Stream<Arguments> mutableContainerSelfAssignments() {
        return Stream.of(
                new MutableContainerSelfAssignment<>(Object[].class, () -> new Object[1], (a, b) -> a[0] = b, array -> (Object[]) array[0]),
                new MutableContainerSelfAssignment<>(ArrayList.class, ArrayList::new, ArrayList::add, list -> (ArrayList<?>) list.get(0)),
                new MutableContainerSelfAssignment<>(LinkedList.class, LinkedList::new, LinkedList::add,
                        list -> (LinkedList<?>) list.get(0)),
                new MutableContainerSelfAssignment<>(HashSet.class, HashSet::new, HashSet::add, set -> (HashSet<?>) set.iterator().next()),
                new MutableContainerSelfAssignment<>(LinkedHashSet.class, LinkedHashSet::new, LinkedHashSet::add,
                        set -> (LinkedHashSet<?>) set.iterator().next()),
                new MutableContainerSelfAssignment<>(HashMap.class, HashMap::new, (map, el) -> map.put(el, el),
                        map -> (HashMap<?, ?>) map.values().iterator().next()),
                new MutableContainerSelfAssignment<>(LinkedHashMap.class, LinkedHashMap::new, (map, el) -> map.put(el, el),
                        map -> (LinkedHashMap<?, ?>) map.values().iterator().next())
        ).map(Arguments::of);
    }

    @Test
    void marshalsSimpleEnumArrayCorrectly() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new SimpleEnum[]{SimpleEnum.FIRST});

        IgniteDataInput dis = openDataInput(marshalled);

        ProtocolMarshalling.readDescriptorOrCommandId(dis);
        ProtocolMarshalling.readObjectId(dis);

        assertThat(dis.readUTF(), is(SimpleEnum.class.getName()));
        assertThat(ProtocolMarshalling.readLength(dis), is(1));
        skipOneByteEmptyNullBitMask(dis);
        ProtocolMarshalling.readObjectId(dis);
        assertThat(dis.readUTF(), is(SimpleEnum.FIRST.name()));

        assertThat(dis.available(), is(0));
    }

    @Test
    void marshalsEnumArrayWithValuesOfSimpleEnumCorrectly() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new Enum[]{SimpleEnum.FIRST});

        IgniteDataInput dis = openDataInput(marshalled);

        ProtocolMarshalling.readDescriptorOrCommandId(dis);
        ProtocolMarshalling.readObjectId(dis);

        assertThat(dis.readUTF(), is(Enum.class.getName()));
        assertThat(ProtocolMarshalling.readLength(dis), is(1));
        assertThat(ProtocolMarshalling.readDescriptorOrCommandId(dis), greaterThanOrEqualTo(MIN_CUSTOM_DESCRIPTOR_ID));
        ProtocolMarshalling.readObjectId(dis);
        assertThat(dis.readUTF(), is(SimpleEnum.FIRST.name()));

        assertThat(dis.available(), is(0));
    }

    @Test
    void marshalsArrayOfEnumWithAnonClassesForMembersCorrectly() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new EnumWithAnonClassesForMembers[]{EnumWithAnonClassesForMembers.FIRST});

        IgniteDataInput dis = openDataInput(marshalled);

        ProtocolMarshalling.readDescriptorOrCommandId(dis);
        ProtocolMarshalling.readObjectId(dis);

        assertThat(dis.readUTF(), is(EnumWithAnonClassesForMembers.class.getName()));
        assertThat(ProtocolMarshalling.readLength(dis), is(1));
        skipOneByteEmptyNullBitMask(dis);
        ProtocolMarshalling.readObjectId(dis);
        assertThat(dis.readUTF(), is(EnumWithAnonClassesForMembers.FIRST.name()));

        assertThat(dis.available(), is(0));
    }

    private void skipOneByteEmptyNullBitMask(DataInput dis) throws IOException {
        assertThat(dis.readByte(), is((byte) 0));
    }

    @Test
    void marshalsEnumArrayWithValuesOfEnumWithAnonClassesForMembersValuesCorrectly() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new Enum[]{EnumWithAnonClassesForMembers.FIRST});

        IgniteDataInput dis = openDataInput(marshalled);

        ProtocolMarshalling.readDescriptorOrCommandId(dis);
        ProtocolMarshalling.readObjectId(dis);

        assertThat(dis.readUTF(), is(Enum.class.getName()));
        assertThat(ProtocolMarshalling.readLength(dis), is(1));
        assertThat(ProtocolMarshalling.readDescriptorOrCommandId(dis), greaterThanOrEqualTo(MIN_CUSTOM_DESCRIPTOR_ID));
        ProtocolMarshalling.readObjectId(dis);
        assertThat(dis.readUTF(), is(EnumWithAnonClassesForMembers.FIRST.name()));

        assertThat(dis.available(), is(0));
    }

    @Test
    void marshalsEmptyAbstractEnumArrayCorrectly() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new Enum[]{});

        IgniteDataInput dis = openDataInput(marshalled);

        ProtocolMarshalling.readDescriptorOrCommandId(dis);
        ProtocolMarshalling.readObjectId(dis);

        assertThat(dis.readUTF(), is(Enum.class.getName()));
        assertThat(ProtocolMarshalling.readLength(dis), is(0));

        assertThat(dis.available(), is(0));
    }

    private IgniteDataInput openDataInput(MarshalledObject marshalled) {
        return new IgniteUnsafeDataInput(marshalled.bytes());
    }

    @Test
    void marshalsEmptySimpleEnumArrayCorrectly() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new SimpleEnum[]{});

        IgniteDataInput dis = openDataInput(marshalled);

        ProtocolMarshalling.readDescriptorOrCommandId(dis);
        ProtocolMarshalling.readObjectId(dis);

        assertThat(dis.readUTF(), is(SimpleEnum.class.getName()));
        assertThat(ProtocolMarshalling.readLength(dis), is(0));

        assertThat(dis.available(), is(0));
    }

    @Test
    void marshalsEmptyArrayOfEnumWithAnonClassesForMembersCorrectly() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new EnumWithAnonClassesForMembers[]{});

        IgniteDataInput dis = openDataInput(marshalled);

        ProtocolMarshalling.readDescriptorOrCommandId(dis);
        ProtocolMarshalling.readObjectId(dis);

        assertThat(dis.readUTF(), is(EnumWithAnonClassesForMembers.class.getName()));
        assertThat(ProtocolMarshalling.readLength(dis), is(0));

        assertThat(dis.available(), is(0));
    }

    @ParameterizedTest
    @MethodSource("testStrings")
    void marshalsAndUnmarshalsStrings(String str) throws Exception {
        String unmarshalled = marshalAndUnmarshal(str);

        assertThat(unmarshalled, is(equalTo(str)));
    }

    private static Stream<Arguments> testStrings() {
        return Stream.of(
                "ASCII",
                "Not ASCII, but Latin-1: é",
                "Ютиэф-8"
        ).map(Arguments::of);
    }

    private static class BuiltInTypeValue {
        private final Object value;
        private final BuiltInType builtinType;

        private BuiltInTypeValue(Object value, BuiltInType builtinType) {
            this.value = value;
            this.builtinType = builtinType;
        }

        @Override
        public String toString() {
            return "BuiltInTypeValue{"
                    + "value=" + value
                    + ", builtinType=" + builtinType
                    + '}';
        }
    }

    private static class MutableContainerSelfAssignment<T> {
        private final Class<T> clazz;
        private final Supplier<T> factory;
        private final BiConsumer<T, T> assignment;
        private final Function<T, T> elementAccess;

        private MutableContainerSelfAssignment(
                Class<T> clazz,
                Supplier<T> factory,
                BiConsumer<T, T> assignment,
                Function<T, T> elementAccess
        ) {
            this.clazz = clazz;
            this.factory = factory;
            this.assignment = assignment;
            this.elementAccess = elementAccess;
        }

        @Override
        public String toString() {
            return "ContainerSelfCycle{"
                    + "clazz=" + clazz
                    + '}';
        }
    }
}
