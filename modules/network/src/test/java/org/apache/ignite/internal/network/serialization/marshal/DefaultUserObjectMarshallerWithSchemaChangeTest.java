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

import static java.util.stream.Collectors.toList;
import static net.bytebuddy.dynamic.loading.ClassLoadingStrategy.Default.CHILD_FIRST;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import java.io.Externalizable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectInputStream.GetField;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.implementation.StubMethod;
import org.apache.ignite.internal.network.serialization.ClassDescriptor;
import org.apache.ignite.internal.network.serialization.ClassDescriptorFactory;
import org.apache.ignite.internal.network.serialization.ClassDescriptorRegistry;
import org.apache.ignite.internal.network.serialization.ClassNameMapBackedClassIndexedDescriptors;
import org.apache.ignite.internal.network.serialization.CompositeDescriptorRegistry;
import org.apache.ignite.internal.network.serialization.FieldDescriptor;
import org.apache.ignite.internal.network.serialization.MapBackedIdIndexedDescriptors;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for how {@link DefaultUserObjectMarshaller} handles schema changes: that is, if an object was marshalled
 * with a class which structure differs from our local version of the class.
 */
@ExtendWith(MockitoExtension.class)
public class DefaultUserObjectMarshallerWithSchemaChangeTest extends BaseIgniteAbstractTest {
    private static final byte[] INT_42_BYTES_IN_LITTLE_ENDIAN = {42, 0, 0, 0};
    private static final String NON_LEAF_CLASS_NAME = "test.NonLeaf";
    private static final String LEAF_CLASS_NAME = "test.Leaf";

    private final ClassDescriptorRegistry localRegistry = new ClassDescriptorRegistry();
    private final ClassDescriptorFactory localFactory = new ClassDescriptorFactory(localRegistry);
    private final DefaultUserObjectMarshaller localMarshaller = new DefaultUserObjectMarshaller(localRegistry, localFactory);

    private final ClassDescriptorRegistry remoteRegistry = new ClassDescriptorRegistry();
    private final ClassDescriptorFactory remoteFactory = new ClassDescriptorFactory(remoteRegistry);
    private final DefaultUserObjectMarshaller remoteMarshaller = new DefaultUserObjectMarshaller(remoteRegistry, remoteFactory);

    @Mock
    private SchemaMismatchHandler<Object> schemaMismatchHandler;

    public static GetFieldReader getFieldReader;

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("extraFields")
    void remoteClassHasExtraField(ExtraField extraField) throws Exception {
        Class<?> localClass = Empty.class;

        localMarshaller.replaceSchemaMismatchHandler((Class<Object>) localClass, schemaMismatchHandler);

        Class<?> remoteClass = addFieldTo(localClass, "addedRemotely", extraField.type);
        Object remoteInstance = instantiate(remoteClass);
        IgniteTestUtils.setFieldValue(remoteInstance, "addedRemotely", extraField.value);

        Object unmarshalled = marshalRemotelyAndUnmarshalLocally(remoteInstance, localClass, remoteClass);

        verify(schemaMismatchHandler).onFieldIgnored(unmarshalled, "addedRemotely", extraField.value);
    }

    private Class<?> addFieldTo(Class<?> baseClass, String fieldName, Class<?> fieldType) {
        return new ByteBuddy()
                .redefine(baseClass)
                .defineField(fieldName, fieldType, Visibility.PRIVATE)
                .make()
                .load(getClass().getClassLoader(), CHILD_FIRST)
                .getLoaded();
    }

    private Object instantiate(Class<?> remoteClass) throws ReflectiveOperationException {
        Constructor<?> constructor = remoteClass.getDeclaredConstructor();
        constructor.setAccessible(true);
        return constructor.newInstance();
    }

    private static Stream<Arguments> extraFields() {
        return Stream.of(
                new ExtraField(byte.class, (byte) 0),
                new ExtraField(short.class, (short) 1),
                new ExtraField(int.class, 2),
                new ExtraField(long.class, (long) 3),
                new ExtraField(float.class, (float) 4),
                new ExtraField(double.class, (double) 5),
                new ExtraField(char.class, 'a'),
                new ExtraField(boolean.class, true),
                new ExtraField(String.class, "Hello")
        ).map(Arguments::of);
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("extraFields")
    void localClassHasExtraField(ExtraField extraField) throws Exception {
        Class<?> remoteClass = Empty.class;
        Class<?> localClass = addFieldTo(remoteClass, "addedLocally", extraField.type);

        localMarshaller.replaceSchemaMismatchHandler((Class<Object>) localClass, schemaMismatchHandler);

        Object remoteInstance = instantiate(remoteClass);

        Object unmarshalled = marshalRemotelyAndUnmarshalLocally(remoteInstance, localClass, remoteClass);

        verify(schemaMismatchHandler).onFieldMissed(unmarshalled, "addedLocally");
    }

    @SuppressWarnings("unchecked")
    @Test
    void primitiveFieldTypeChangedToNonPrimitive() throws Exception {
        Class<?> remoteClass = addFieldTo(Empty.class, "value", int.class);
        Class<?> localClass = addFieldTo(Empty.class, "value", String.class);

        localMarshaller.replaceSchemaMismatchHandler((Class<Object>) localClass, schemaMismatchHandler);

        Object remoteInstance = instantiate(remoteClass);
        IgniteTestUtils.setFieldValue(remoteInstance, "value", 42);

        Object unmarshalled = marshalRemotelyAndUnmarshalLocally(remoteInstance, localClass, remoteClass);

        verify(schemaMismatchHandler).onFieldTypeChanged(unmarshalled, "value", int.class, 42);
    }

    @SuppressWarnings("unchecked")
    @Test
    void nonPrimitiveFieldTypeChangedToPrimitive() throws Exception {
        Class<?> remoteClass = addFieldTo(Empty.class, "value", String.class);
        Class<?> localClass = addFieldTo(Empty.class, "value", int.class);

        localMarshaller.replaceSchemaMismatchHandler((Class<Object>) localClass, schemaMismatchHandler);

        Object remoteInstance = instantiate(remoteClass);
        IgniteTestUtils.setFieldValue(remoteInstance, "value", "forty two");

        Object unmarshalled = marshalRemotelyAndUnmarshalLocally(remoteInstance, localClass, remoteClass);

        verify(schemaMismatchHandler).onFieldTypeChanged(unmarshalled, "value", String.class, "forty two");
    }

    @Test
    void nonPrimitiveFieldTypeChangedToSuperClassIsCompatibleChange() throws Exception {
        Class<?> remoteClass = addFieldTo(Empty.class, "value", String.class);
        Class<?> localClass = addFieldTo(Empty.class, "value", CharSequence.class);

        //noinspection unchecked
        localMarshaller.replaceSchemaMismatchHandler((Class<Object>) localClass, schemaMismatchHandler);

        Object remoteInstance = instantiate(remoteClass);
        IgniteTestUtils.setFieldValue(remoteInstance, "value", "forty two");

        Object unmarshalled = marshalRemotelyAndUnmarshalLocally(remoteInstance, localClass, remoteClass);

        CharSequence localFieldValue = (CharSequence) IgniteTestUtils.getFieldValue(unmarshalled, localClass, "value");
        assertThat(localFieldValue.toString(), is("forty two"));

        verify(schemaMismatchHandler, never()).onFieldTypeChanged(any(), any(), any(), any());
    }

    private Object marshalRemotelyAndUnmarshalLocally(Object remoteInstance, Class<?> localClass, Class<?> remoteClass)
            throws MarshalException, UnmarshalException {
        MarshalledObject marshalled = remoteMarshaller.marshal(remoteInstance);
        return unmarshalNotNullLocally(marshalled, localClass, remoteClass);
    }

    private Object marshalRemotelyAndUnmarshalLocally(
            Object remoteInstance,
            Class<?> localClass,
            Class<?> remoteClass,
            Function<ClassDescriptor, ClassDescriptor> reconstructSuperDescriptor
    ) throws MarshalException, UnmarshalException {
        MarshalledObject marshalled = remoteMarshaller.marshal(remoteInstance);
        return unmarshalNotNullLocally(marshalled, localClass, remoteClass, reconstructSuperDescriptor);
    }

    private <T> T unmarshalNotNullLocally(MarshalledObject marshalled, Class<?> localClass, Class<?> remoteClass)
            throws UnmarshalException {
        T unmarshalled = unmarshalLocally(marshalled, localClass, remoteClass);
        assertThat(unmarshalled, is(notNullValue()));
        return unmarshalled;
    }

    private <T> T unmarshalNotNullLocally(
            MarshalledObject marshalled,
            Class<?> localClass,
            Class<?> remoteClass,
            Function<ClassDescriptor, ClassDescriptor> reconstructSuperDescriptor
    ) throws UnmarshalException {
        T unmarshalled = unmarshalLocally(marshalled, localClass, remoteClass, reconstructSuperDescriptor);
        assertThat(unmarshalled, is(notNullValue()));
        return unmarshalled;
    }

    @Nullable
    private <T> T unmarshalLocally(MarshalledObject marshalled, Class<?> localClass, Class<?> remoteClass)
            throws UnmarshalException {
        return unmarshalLocally(marshalled, localClass, remoteClass, Function.identity());
    }

    @Nullable
    private <T> T unmarshalLocally(
            MarshalledObject marshalled,
            Class<?> localClass,
            Class<?> remoteClass,
            Function<ClassDescriptor, ClassDescriptor> reconstructSuperDescriptor
    ) throws UnmarshalException {
        localFactory.create(localClass);
        ClassDescriptor localDescriptor = localRegistry.getRequiredDescriptor(localClass);

        ClassDescriptor remoteDescriptor = remoteRegistry.getRequiredDescriptor(remoteClass);

        ClassDescriptor reconstructedRemoteDescriptor = ClassDescriptor.forRemote(
                localDescriptor.localClass(),
                remoteDescriptor.descriptorId(),
                reconstructSuperDescriptor.apply(remoteDescriptor.superClassDescriptor()),
                remoteDescriptor.componentTypeDescriptor(),
                remoteDescriptor.isPrimitive(),
                remoteDescriptor.isArray(),
                remoteDescriptor.isRuntimeEnum(),
                remoteDescriptor.isSerializationTypeKnownUpfront(),
                reconstructFields(remoteDescriptor.fields(), localDescriptor.localClass()),
                remoteDescriptor.serialization(),
                localDescriptor
        );

        CompositeDescriptorRegistry compositeRegistry = new CompositeDescriptorRegistry(
                new MapBackedIdIndexedDescriptors(
                        Int2ObjectMaps.singleton(reconstructedRemoteDescriptor.descriptorId(), reconstructedRemoteDescriptor)
                ),
                new ClassNameMapBackedClassIndexedDescriptors(
                        Map.of(reconstructedRemoteDescriptor.localClass().getName(), reconstructedRemoteDescriptor)
                ),
                localRegistry
        );

        return localMarshaller.unmarshal(marshalled.bytes(), compositeRegistry);
    }

    private List<FieldDescriptor> reconstructFields(List<FieldDescriptor> remoteFields, Class<?> localDeclaringClass) {
        return remoteFields.stream()
                .map(remoteFieldDescriptor -> reconstructField(remoteFieldDescriptor, localDeclaringClass))
                .collect(toList());
    }

    private FieldDescriptor reconstructField(FieldDescriptor remoteField, Class<?> localDeclaringClass) {
        return FieldDescriptor.remote(
                remoteField.name(),
                reconstructClass(remoteField.localClass(), localDeclaringClass.getClassLoader()),
                remoteField.typeDescriptorId(),
                remoteField.isUnshared(),
                remoteField.isPrimitive(),
                remoteField.isSerializationTypeKnownUpfront(),
                localDeclaringClass
        );
    }

    private Class<?> reconstructClass(Class<?> remoteClass, ClassLoader localClassLoader) {
        if (remoteClass.isPrimitive()) {
            return remoteClass;
        }
        return classForName(remoteClass.getName(), localClassLoader);
    }

    private Class<?> classForName(String className, ClassLoader classLoader) {
        try {
            return Class.forName(className, true, classLoader);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void whenClassIsMergedIntoItsSubclassLocallyThenItsFieldsShouldNotBeFilledOnUnmarshalling() throws Exception {
        Object unmarshalled = marshalRemotelyAndUnmarshalWithSuperclassDisappearingLocally();

        assertThat(IgniteTestUtils.getFieldValue(unmarshalled, unmarshalled.getClass(), "value1"), is(0));
    }

    @SuppressWarnings("unchecked")
    @Test
    void whenClassIsMergedIntoItsSubclassLocallyThenItsFieldsShouldTriggerFieldMissedAndIgnoredEventsOnUnmarshalling() throws Exception {
        SchemaMismatchHandler<Object> nonLeafLayerHandler = mock(SchemaMismatchHandler.class);
        SchemaMismatchHandler<Object> leafLayerHandler = mock(SchemaMismatchHandler.class);

        localMarshaller.replaceSchemaMismatchHandler(LEAF_CLASS_NAME, leafLayerHandler);
        localMarshaller.replaceSchemaMismatchHandler(NON_LEAF_CLASS_NAME, nonLeafLayerHandler);

        Object unmarshalled = marshalRemotelyAndUnmarshalWithSuperclassDisappearingLocally();

        verify(leafLayerHandler).onFieldMissed(unmarshalled, "value1");
        verify(nonLeafLayerHandler).onFieldIgnored(unmarshalled, "value1", 1);
    }

    @SuppressWarnings("unchecked")
    @Test
    void whenClassWithWriteObjectMethodIsMergedIntoItsSubclassLocallyThenReadObjectIgnoredEventShouldBeTriggeredOnUnmarshalling()
            throws Exception {
        SchemaMismatchHandler<Object> nonLeafLayerHandler = mock(SchemaMismatchHandler.class);

        localMarshaller.replaceSchemaMismatchHandler(NON_LEAF_CLASS_NAME, nonLeafLayerHandler);

        Object unmarshalled = marshalRemotelyAndUnmarshalWithSuperclassDisappearingLocally(this::withEmptyWriteObjectMethod);

        verify(nonLeafLayerHandler).onReadObjectIgnored(eq(unmarshalled), any());
    }

    private DynamicType.Builder<Object> withEmptyWriteObjectMethod(DynamicType.Builder<Object> builder) {
        return builder
                .implement(Serializable.class)
                .defineMethod("writeObject", void.class, Visibility.PRIVATE)
                .withParameters(ObjectOutputStream.class)
                .intercept(StubMethod.INSTANCE);
    }

    private Object marshalRemotelyAndUnmarshalWithSuperclassDisappearingLocally()
            throws MarshalException, ReflectiveOperationException, UnmarshalException {
        return marshalRemotelyAndUnmarshalWithSuperclassDisappearingLocally(UnaryOperator.identity());
    }

    private Object marshalRemotelyAndUnmarshalWithSuperclassDisappearingLocally(
            UnaryOperator<DynamicType.Builder<Object>> remoteNonLeafClassCustomizer
    ) throws ReflectiveOperationException, MarshalException, UnmarshalException {

        DynamicType.Builder<Object> remoteNonLeafClassBuilder = new ByteBuddy()
                .subclass(Object.class)
                .name(NON_LEAF_CLASS_NAME)
                .defineField("value1", int.class, Visibility.PRIVATE);

        Class<?> remoteNonLeafClass = remoteNonLeafClassCustomizer.apply(remoteNonLeafClassBuilder)
                .make()
                .load(getClass().getClassLoader(), CHILD_FIRST)
                .getLoaded();
        Class<?> remoteLeafClass = new ByteBuddy()
                .subclass(remoteNonLeafClass)
                .name(LEAF_CLASS_NAME)
                .defineField("value2", int.class, Visibility.PRIVATE)
                .make()
                .load(remoteNonLeafClass.getClassLoader())
                .getLoaded();

        Class<?> localLeafClass = new ByteBuddy()
                .subclass(Object.class)
                .name(LEAF_CLASS_NAME)
                .defineField("value1", int.class, Visibility.PRIVATE)
                .defineField("value2", int.class, Visibility.PRIVATE)
                .make()
                .load(getClass().getClassLoader(), CHILD_FIRST)
                .getLoaded();

        Object remoteInstance = instantiate(remoteLeafClass);
        IgniteTestUtils.setFieldValue(remoteInstance, remoteInstance.getClass().getSuperclass(), "value1", 1);
        IgniteTestUtils.setFieldValue(remoteInstance, remoteLeafClass, "value2", 2);

        return marshalRemotelyAndUnmarshalLocally(
                remoteInstance,
                localLeafClass,
                remoteLeafClass,
                this::toRemoteDescriptorWithoutLocalClass
        );
    }

    private ClassDescriptor toRemoteDescriptorWithoutLocalClass(ClassDescriptor superDescriptor) {
        return ClassDescriptor.forRemote(
                superDescriptor.className(),
                superDescriptor.descriptorId(),
                superDescriptor.superClassDescriptor(),
                superDescriptor.componentTypeDescriptor(),
                superDescriptor.isPrimitive(),
                superDescriptor.isArray(),
                superDescriptor.isRuntimeEnum(),
                superDescriptor.isSerializationTypeKnownUpfront(),
                superDescriptor.fields(),
                superDescriptor.serialization()
        );
    }

    @Test
    void whenSuperclassIsSplitFromClassLocallyThenSuperclassFieldsShouldNotBeFilledOnUnmarshalling() throws Exception {
        Object unmarshalled = marshalRemotelyAndUnmarshalWithSuperclassAppearingLocally();

        assertThat(IgniteTestUtils.getFieldValue(unmarshalled, unmarshalled.getClass().getSuperclass(), "value1"), is(0));
    }

    @SuppressWarnings("unchecked")
    @Test
    void whenSuperclassIsSplitFromClassLocallyThenSuperclassFieldsShouldTriggerFieldMissedAndIgnoredEventsOnUnmarshalling()
            throws Exception {
        SchemaMismatchHandler<Object> nonLeafLayerHandler = mock(SchemaMismatchHandler.class);
        SchemaMismatchHandler<Object> leafLayerHandler = mock(SchemaMismatchHandler.class);

        localMarshaller.replaceSchemaMismatchHandler(LEAF_CLASS_NAME, leafLayerHandler);
        localMarshaller.replaceSchemaMismatchHandler(NON_LEAF_CLASS_NAME, nonLeafLayerHandler);

        Object unmarshalled = marshalRemotelyAndUnmarshalWithSuperclassAppearingLocally();

        verify(leafLayerHandler).onFieldIgnored(unmarshalled, "value1", 1);
        verify(nonLeafLayerHandler).onFieldMissed(unmarshalled, "value1");
    }

    @SuppressWarnings("unchecked")
    @Test
    void whenSuperclassWithReadObjectMethodIsSplitFromClassLocallyThenReadObjectMissedEventShouldBeTriggeredOnUnmarshalling()
            throws Exception {
        SchemaMismatchHandler<Object> nonLeafLayerHandler = mock(SchemaMismatchHandler.class);

        localMarshaller.replaceSchemaMismatchHandler(NON_LEAF_CLASS_NAME, nonLeafLayerHandler);

        Object unmarshalled = marshalRemotelyAndUnmarshalWithSuperclassAppearingLocally(this::withEmptyReadObjectMethod);

        verify(nonLeafLayerHandler).onReadObjectMissed(eq(unmarshalled));
    }

    private DynamicType.Builder<Object> withEmptyReadObjectMethod(DynamicType.Builder<Object> builder) {
        return builder
                .implement(Serializable.class)
                .defineMethod("readObject", void.class, Visibility.PRIVATE)
                .withParameters(ObjectInputStream.class)
                .intercept(StubMethod.INSTANCE);
    }

    private Object marshalRemotelyAndUnmarshalWithSuperclassAppearingLocally()
            throws ReflectiveOperationException, MarshalException, UnmarshalException {
        return marshalRemotelyAndUnmarshalWithSuperclassAppearingLocally(UnaryOperator.identity());
    }

    private Object marshalRemotelyAndUnmarshalWithSuperclassAppearingLocally(
            UnaryOperator<DynamicType.Builder<Object>> localNonLeafClassCustomizer
    ) throws ReflectiveOperationException, MarshalException, UnmarshalException {

        Class<?> remoteLeafClass = new ByteBuddy()
                .subclass(Object.class)
                .name(LEAF_CLASS_NAME)
                .defineField("value1", int.class, Visibility.PRIVATE)
                .defineField("value2", int.class, Visibility.PRIVATE)
                .make()
                .load(getClass().getClassLoader(), CHILD_FIRST)
                .getLoaded();

        DynamicType.Builder<Object> localNonLeafClassBuilder = new ByteBuddy()
                .subclass(Object.class)
                .name(NON_LEAF_CLASS_NAME)
                .defineField("value1", int.class, Visibility.PRIVATE);
        Class<?> localNonLeafClass = localNonLeafClassCustomizer.apply(localNonLeafClassBuilder)
                .make()
                .load(getClass().getClassLoader(), CHILD_FIRST)
                .getLoaded();
        Class<?> localLeafClass = new ByteBuddy()
                .subclass(localNonLeafClass)
                .name(LEAF_CLASS_NAME)
                .defineField("value2", int.class, Visibility.PRIVATE)
                .make()
                .load(localNonLeafClass.getClassLoader())
                .getLoaded();

        Object remoteInstance = instantiate(remoteLeafClass);
        IgniteTestUtils.setFieldValue(remoteInstance, remoteInstance.getClass(), "value1", 1);
        IgniteTestUtils.setFieldValue(remoteInstance, remoteInstance.getClass(), "value2", 2);

        return marshalRemotelyAndUnmarshalLocally(remoteInstance, localLeafClass, remoteLeafClass);
    }

    @ParameterizedTest
    @MethodSource("extraFieldsForGetField")
    void getFieldReturnsDefaultValueWhenRemoteClassHasExtraField(ExtraFieldForGetField extraField) throws Exception {
        Class<?> remoteClass = SerializableWithDefaultedGetField.class;
        Class<?> localClass = addFieldTo(remoteClass, "addedLocally", extraField.type);

        Object remoteInstance = instantiate(remoteClass);

        getFieldReader = extraField.reader;

        Object unmarshalled = marshalRemotelyAndUnmarshalLocally(remoteInstance, localClass, remoteClass);

        Object valueReadFromGetField = IgniteTestUtils.getFieldValue(unmarshalled, unmarshalled.getClass(), "readValue");
        assertThat(valueReadFromGetField, is(extraField.expectedValue));
    }

    private static Stream<Arguments> extraFieldsForGetField() {
        String fieldName = "addedLocally";
        return Stream.of(
                new ExtraFieldForGetField(byte.class, (byte) 10, field -> field.get(fieldName, (byte) 10)),
                new ExtraFieldForGetField(short.class, (short) 11, field -> field.get(fieldName, (short) 11)),
                new ExtraFieldForGetField(int.class, 12, field -> field.get(fieldName, 12)),
                new ExtraFieldForGetField(long.class, (long) 13, field -> field.get(fieldName, (long) 13)),
                new ExtraFieldForGetField(float.class, (float) 14, field -> field.get(fieldName, (float) 14)),
                new ExtraFieldForGetField(double.class, (double) 15, field -> field.get(fieldName, (double) 15)),
                new ExtraFieldForGetField(char.class, 'x', field -> field.get(fieldName, 'x')),
                new ExtraFieldForGetField(boolean.class, true, field -> field.get(fieldName, true)),
                new ExtraFieldForGetField(String.class, "Bye", field -> field.get(fieldName, "Bye"))
        ).map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("extraFields")
    void getFieldDefaultedReturnsTrueForFieldsAddedLocally(ExtraField extraField) throws Exception {
        Class<?> remoteClass = SerializableWithDefaultedGetField.class;
        Class<?> localClass = addFieldTo(remoteClass, "addedLocally", extraField.type);

        Object remoteInstance = instantiate(remoteClass);

        getFieldReader = getField -> getField.defaulted("addedLocally");

        Object unmarshalled = marshalRemotelyAndUnmarshalLocally(remoteInstance, localClass, remoteClass);

        Object valueReadFromGetField = IgniteTestUtils.getFieldValue(unmarshalled, unmarshalled.getClass(), "readValue");
        assertThat(valueReadFromGetField, is(true));
    }

    @Test
    void removalOfExternalizableInterfaceCausesUnfilledDeserializationResult() throws Exception {
        Object unmarshalled = marshalExternalizableUnmarshalNonExternalizableBasedOn(ExternalizationReady.class);

        int localFieldValue = (Integer) IgniteTestUtils.getFieldValue(unmarshalled, unmarshalled.getClass(), "value");
        assertThat(localFieldValue, is(0));
    }

    @Test
    void removalOfExternalizableInterfaceTriggersHandlerInvocation() throws Exception {
        Object unmarshalled = marshalExternalizableUnmarshalNonExternalizableBasedOn(ExternalizationReady.class);

        verify(schemaMismatchHandler).onExternalizableIgnored(eq(unmarshalled), any());
    }

    @Test
    void onExternalizableIgnoredReceivesStreamWithExactlyExternalizedDataAvailable() throws Exception {
        doAnswer(invocation -> {
            InputStream externalDataStream = invocation.getArgument(1);
            byte[] externalData = externalDataStream.readAllBytes();

            assertThat(externalData, is(INT_42_BYTES_IN_LITTLE_ENDIAN));

            return null;
        }).when(schemaMismatchHandler).onExternalizableIgnored(any(), any());

        marshalExternalizableUnmarshalNonExternalizableBasedOn(ExternalizationReady.class);
    }

    @Test
    void onExternalizableIgnoredSkipsExternalDataEvenIfHandlerDoesNotReadIt() throws Exception {
        doNothing().when(schemaMismatchHandler).onExternalizableIgnored(any(), any());

        assertDoesNotThrow(() -> marshalExternalizableUnmarshalNonExternalizableBasedOn(ExternalizationReady.class));
    }

    @SuppressWarnings("SameParameterValue")
    private Object marshalExternalizableUnmarshalNonExternalizableBasedOn(Class<?> baseClass)
            throws ReflectiveOperationException, MarshalException, UnmarshalException {
        return marshalWithInterfaceUnmarshalWithoutInterfaceBasedOn(baseClass, Externalizable.class);
    }

    private Object marshalWithInterfaceUnmarshalWithoutInterfaceBasedOn(Class<?> baseClass, Class<?> iface)
            throws ReflectiveOperationException, MarshalException, UnmarshalException {
        @SuppressWarnings("UnnecessaryLocalVariable")
        Class<?> localClass = baseClass;
        Class<?> remoteClass = addInterface(baseClass, iface);

        //noinspection unchecked
        localMarshaller.replaceSchemaMismatchHandler((Class<Object>) localClass, schemaMismatchHandler);

        Object remoteInstance = instantiate(remoteClass);
        IgniteTestUtils.setFieldValue(remoteInstance, "value", 42);

        return marshalRemotelyAndUnmarshalLocally(remoteInstance, localClass, remoteClass);
    }

    private Class<?> addInterface(Class<?> baseClass, Class<?> iface) {
        return new ByteBuddy()
                .redefine(baseClass)
                .implement(iface)
                .make()
                .load(getClass().getClassLoader(), CHILD_FIRST)
                .getLoaded();
    }

    @Test
    void additionOfExternalizableInterfaceCausesStandardDeserialization() throws Exception {
        Object unmarshalled = marshalNonExternalizableUnmarshalExternalizableBasedOn(ExternalizationReady.class);

        int localFieldValue = (Integer) IgniteTestUtils.getFieldValue(unmarshalled, unmarshalled.getClass(), "value");
        assertThat(localFieldValue, is(42));
    }

    @Test
    void additionOfExternalizableInterfaceTriggersHandlerInvocation() throws Exception {
        Object unmarshalled = marshalNonExternalizableUnmarshalExternalizableBasedOn(ExternalizationReady.class);

        verify(schemaMismatchHandler).onExternalizableMissed(unmarshalled);
    }

    @Test
    void onExternalizableMissedIsFiredAfterObjectIsFilledInStandardWay() throws Exception {
        doAnswer(invocation -> {
            Object object = invocation.getArgument(0);
            int fieldValue = (int) IgniteTestUtils.getFieldValue(object, object.getClass(), "value");

            assertThat(fieldValue, is(42));

            return null;
        }).when(schemaMismatchHandler).onExternalizableMissed(any());

        Object unmarshalled = marshalNonExternalizableUnmarshalExternalizableBasedOn(ExternalizationReady.class);

        verify(schemaMismatchHandler).onExternalizableMissed(unmarshalled);
    }

    @SuppressWarnings("SameParameterValue")
    private Object marshalNonExternalizableUnmarshalExternalizableBasedOn(Class<?> baseClass)
            throws ReflectiveOperationException, MarshalException, UnmarshalException {
        return marshalWithoutInterfaceUnmarshalWithInterfaceBasedOn(baseClass, Externalizable.class);
    }

    private Object marshalWithoutInterfaceUnmarshalWithInterfaceBasedOn(Class<?> baseClass, Class<?> iface)
            throws ReflectiveOperationException, MarshalException, UnmarshalException {
        @SuppressWarnings("UnnecessaryLocalVariable")
        Class<?> remoteClass = baseClass;
        Class<?> localClass = addInterface(baseClass, iface);

        //noinspection unchecked
        localMarshaller.replaceSchemaMismatchHandler((Class<Object>) localClass, schemaMismatchHandler);

        Object remoteInstance = instantiate(remoteClass);
        IgniteTestUtils.setFieldValue(remoteInstance, "value", 42);

        return marshalRemotelyAndUnmarshalLocally(remoteInstance, localClass, remoteClass);
    }

    @Test
    void apparitionOfReadResolveMethodTriggersHandlerInvocation() throws Exception {
        Object unmarshalled = marshalNonSerializableUnmarshalSerializableBasedOn(ReadResolveReady.class);

        verify(schemaMismatchHandler).onReadResolveAppeared(unmarshalled);
    }

    @Test
    void whenOnReadResolveAppearedReturnsFalseThenReadResolveIsApplied() throws Exception {
        when(schemaMismatchHandler.onReadResolveAppeared(any())).thenReturn(false);

        Object unmarshalled = marshalNonSerializableUnmarshalSerializableBasedOn(ReadResolveReady.class);

        int value = (int) IgniteTestUtils.getFieldValue(unmarshalled, unmarshalled.getClass(), "value");
        assertThat(value, is(42));
    }

    @Test
    void whenOnReadResolveAppearedReturnsTrueThenReadResolveIsApplied() throws Exception {
        when(schemaMismatchHandler.onReadResolveAppeared(any())).thenReturn(true);

        Object unmarshalled = marshalNonSerializableUnmarshalSerializableBasedOn(ReadResolveReady.class);

        int value = (int) IgniteTestUtils.getFieldValue(unmarshalled, unmarshalled.getClass(), "value");
        assertThat(value, equalTo(42 + ReadResolveReady.READ_RESOLVE_INCREMENT));
    }

    private Object marshalNonSerializableUnmarshalSerializableBasedOn(Class<?> baseClass)
            throws ReflectiveOperationException, MarshalException, UnmarshalException {
        return marshalWithoutInterfaceUnmarshalWithInterfaceBasedOn(baseClass, Serializable.class);
    }

    @Test
    void disappearanceOfReadResolveLeavesCoreFieldFillingUnchanged() throws Exception {
        Object unmarshalled = marshalSerializableUnmarshalNonSerializableBasedOn(ReadResolveReady.class);

        int localFieldValue = (Integer) IgniteTestUtils.getFieldValue(unmarshalled, unmarshalled.getClass(), "value");
        assertThat(localFieldValue, is(42));
    }

    @Test
    void disappearanceOfReadResolveTriggersHandlerInvocation() throws Exception {
        Object unmarshalled = marshalSerializableUnmarshalNonSerializableBasedOn(ReadResolveReady.class);

        verify(schemaMismatchHandler).onReadResolveDisappeared(unmarshalled);
    }

    private Object marshalSerializableUnmarshalNonSerializableBasedOn(Class<?> baseClass)
            throws ReflectiveOperationException, MarshalException, UnmarshalException {
        return marshalWithInterfaceUnmarshalWithoutInterfaceBasedOn(baseClass, Serializable.class);
    }

    @Test
    void disappearanceOfReadObjectCausesUnfilledDeserializationResult() throws Exception {
        Object unmarshalled = marshalSerializableUnmarshalNonSerializableBasedOn(WriteReadObjectReady.class);

        int localFieldValue = (Integer) IgniteTestUtils.getFieldValue(unmarshalled, unmarshalled.getClass(), "value");
        assertThat(localFieldValue, is(0));
    }

    @Test
    void disappearanceOfReadObjectTriggersHandlerInvocation() throws Exception {
        Object unmarshalled = marshalSerializableUnmarshalNonSerializableBasedOn(WriteReadObjectReady.class);

        verify(schemaMismatchHandler).onReadObjectIgnored(eq(unmarshalled), any());
    }

    @Test
    void onReadObjectIgnoredReceivesStreamWithExactlyWriteObjectDataAvailable() throws Exception {
        doAnswer(invocation -> {
            InputStream writeObjectDataStream = invocation.getArgument(1);
            byte[] writeObjectData = writeObjectDataStream.readAllBytes();

            assertThat(writeObjectData, is(INT_42_BYTES_IN_LITTLE_ENDIAN));

            return null;
        }).when(schemaMismatchHandler).onReadObjectIgnored(any(), any());

        marshalSerializableUnmarshalNonSerializableBasedOn(WriteReadObjectReady.class);
    }

    @Test
    void onReadObjectIgnoredSkipsWriteObjectDataEvenIfHandlerDoesNotReadIt() throws Exception {
        doNothing().when(schemaMismatchHandler).onReadObjectIgnored(any(), any());

        assertDoesNotThrow(() -> marshalSerializableUnmarshalNonSerializableBasedOn(WriteReadObjectReady.class));
    }

    @Test
    void additionOfReadObjectMethodCausesStandardDeserialization() throws Exception {
        Object unmarshalled = marshalNonSerializableUnmarshalSerializableBasedOn(WriteReadObjectReady.class);

        int localFieldValue = (Integer) IgniteTestUtils.getFieldValue(unmarshalled, unmarshalled.getClass(), "value");
        assertThat(localFieldValue, is(42));
    }

    @Test
    void additionOfReadObjectMethodTriggersHandlerInvocation() throws Exception {
        Object unmarshalled = marshalNonSerializableUnmarshalSerializableBasedOn(WriteReadObjectReady.class);

        verify(schemaMismatchHandler).onReadObjectMissed(unmarshalled);
    }

    @Test
    void onReadObjectMissedIsFiredAfterObjectIsFilledInStandardWay() throws Exception {
        doAnswer(invocation -> {
            Object object = invocation.getArgument(0);
            int fieldValue = (int) IgniteTestUtils.getFieldValue(object, object.getClass(), "value");

            assertThat(fieldValue, is(42));

            return null;
        }).when(schemaMismatchHandler).onReadObjectMissed(any());

        Object unmarshalled = marshalNonSerializableUnmarshalSerializableBasedOn(WriteReadObjectReady.class);

        verify(schemaMismatchHandler).onReadObjectMissed(unmarshalled);
    }

    private static class Empty {
    }

    private static class ExtraField {
        private final Class<?> type;
        private final Object value;

        private ExtraField(Class<?> type, Object value) {
            this.type = type;
            this.value = value;
        }

        /** {@inheritDoc} */
        @Override
        public String toString() {
            return "ExtraField{"
                    + "type=" + type
                    + ", value=" + value
                    + '}';
        }
    }

    private static class SerializableWithDefaultedGetField implements Serializable {
        Object readValue;

        private void writeObject(ObjectOutputStream stream) throws IOException {
            stream.putFields();
            stream.writeFields();
        }

        private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
            GetField getField = stream.readFields();
            readValue = getFieldReader.read(getField);
        }
    }

    /**
     * Reads a value from {@link GetField}.
     */
    public interface GetFieldReader {
        /**
         * Reads a value from the given {@link GetField}.
         *
         * @param getField {@link GetField} instance
         * @return a value extracted from GetField
         * @throws IOException if something goes wrong
         */
        Object read(GetField getField) throws IOException, ClassNotFoundException;
    }

    private static class ExtraFieldForGetField {
        private final Class<?> type;
        private final Object expectedValue;
        private final GetFieldReader reader;

        private ExtraFieldForGetField(Class<?> type, Object expectedValue, GetFieldReader reader) {
            this.type = type;
            this.expectedValue = expectedValue;
            this.reader = reader;
        }

        /** {@inheritDoc} */
        @Override
        public String toString() {
            return "ExtraFieldForGetField{"
                    + "type=" + type
                    + ", value=" + expectedValue
                    + '}';
        }
    }

    private static class ExternalizationReady {
        private int value;

        public ExternalizationReady() {
            // no-op
        }

        @SuppressWarnings("unused")
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(value);
        }

        @SuppressWarnings("unused")
        public void readExternal(ObjectInput in) throws IOException {
            value = in.readInt();
        }
    }

    private static class ReadResolveReady {
        private static final int READ_RESOLVE_INCREMENT = 100;

        private int value;

        @SuppressWarnings("unused")
        private ReadResolveReady() {
        }

        private ReadResolveReady(int value) {
            this.value = value;
        }

        @SuppressWarnings("unused")
        private Object readResolve() {
            return new ReadResolveReady(value + READ_RESOLVE_INCREMENT);
        }
    }

    private static class WriteReadObjectReady {
        private int value;

        @SuppressWarnings("unused")
        private void writeObject(ObjectOutputStream stream) throws IOException {
            stream.writeInt(value);
        }

        @SuppressWarnings("unused")
        private void readObject(ObjectInputStream stream) throws IOException {
            value = stream.readInt();
        }
    }
}
