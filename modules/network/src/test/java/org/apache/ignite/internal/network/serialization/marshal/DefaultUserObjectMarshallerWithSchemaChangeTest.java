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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import java.io.Externalizable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.modifier.Visibility;
import org.apache.ignite.internal.network.serialization.ClassDescriptor;
import org.apache.ignite.internal.network.serialization.ClassDescriptorFactory;
import org.apache.ignite.internal.network.serialization.ClassDescriptorRegistry;
import org.apache.ignite.internal.network.serialization.CompositeDescriptorRegistry;
import org.apache.ignite.internal.network.serialization.FieldDescriptor;
import org.apache.ignite.internal.network.serialization.MapBackedClassIndexedDescriptors;
import org.apache.ignite.internal.network.serialization.MapBackedIdIndexedDescriptors;
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
class DefaultUserObjectMarshallerWithSchemaChangeTest {
    private static final byte[] INT_42_BYTES_IN_LITTLE_ENDIAN = {42, 0, 0, 0};

    private final ClassDescriptorRegistry localRegistry = new ClassDescriptorRegistry();
    private final ClassDescriptorFactory localFactory = new ClassDescriptorFactory(localRegistry);
    private final DefaultUserObjectMarshaller localMarshaller = new DefaultUserObjectMarshaller(localRegistry, localFactory);

    private final ClassDescriptorRegistry remoteRegistry = new ClassDescriptorRegistry();
    private final ClassDescriptorFactory remoteFactory = new ClassDescriptorFactory(remoteRegistry);
    private final DefaultUserObjectMarshaller remoteMarshaller = new DefaultUserObjectMarshaller(remoteRegistry, remoteFactory);

    @Mock
    private SchemaMismatchHandler<Object> schemaMismatchHandler;

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

    private Class<?> addFieldTo(Class<?> localClass, String fieldName, Class<?> fieldType) {
        return new ByteBuddy()
                .redefine(localClass)
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

    private <T> T unmarshalNotNullLocally(MarshalledObject marshalled, Class<?> localClass, Class<?> remoteClass)
            throws UnmarshalException {
        T unmarshalled = unmarshalLocally(marshalled, localClass, remoteClass);
        assertThat(unmarshalled, is(notNullValue()));
        return unmarshalled;
    }

    @Nullable
    private <T> T unmarshalLocally(MarshalledObject marshalled, Class<?> localClass, Class<?> remoteClass)
            throws UnmarshalException {
        localFactory.create(localClass);
        ClassDescriptor localDescriptor = localRegistry.getRequiredDescriptor(localClass);

        ClassDescriptor remoteDescriptor = remoteRegistry.getRequiredDescriptor(remoteClass);

        ClassDescriptor reconstructedDescriptor = ClassDescriptor.forRemote(
                localDescriptor.localClass(),
                remoteDescriptor.descriptorId(),
                remoteDescriptor.superClassDescriptor(),
                remoteDescriptor.componentTypeDescriptor(),
                remoteDescriptor.isPrimitive(),
                remoteDescriptor.isArray(),
                remoteDescriptor.isRuntimeEnum(),
                remoteDescriptor.isRuntimeTypeKnownUpfront(),
                reconstructFields(remoteDescriptor.fields(), localDescriptor.localClass()),
                remoteDescriptor.serialization(),
                localDescriptor
        );

        CompositeDescriptorRegistry compositeRegistry = new CompositeDescriptorRegistry(
                new MapBackedIdIndexedDescriptors(
                        Int2ObjectMaps.singleton(reconstructedDescriptor.descriptorId(), reconstructedDescriptor)
                ),
                new MapBackedClassIndexedDescriptors(Map.of(reconstructedDescriptor.localClass(), reconstructedDescriptor)),
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
                remoteField.isRuntimeTypeKnownUpfront(),
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
