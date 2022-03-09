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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
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
        IgniteTestUtils.setFieldValue(remoteInstance, "value", "fourty two");

        Object unmarshalled = marshalRemotelyAndUnmarshalLocally(remoteInstance, localClass, remoteClass);

        verify(schemaMismatchHandler).onFieldTypeChanged(unmarshalled, "value", String.class, "fourty two");
    }

    @SuppressWarnings("unchecked")
    @Test
    void nonPrimitiveFieldTypeChangedToSuperClassIsCompatibleChange() throws Exception {
        Class<?> remoteClass = addFieldTo(Empty.class, "value", String.class);
        Class<?> localClass = addFieldTo(Empty.class, "value", CharSequence.class);

        localMarshaller.replaceSchemaMismatchHandler((Class<Object>) localClass, schemaMismatchHandler);

        Object remoteInstance = instantiate(remoteClass);
        IgniteTestUtils.setFieldValue(remoteInstance, "value", "fourty two");

        Object unmarshalled = marshalRemotelyAndUnmarshalLocally(remoteInstance, localClass, remoteClass);

        CharSequence localFieldValue = (CharSequence) IgniteTestUtils.getFieldValue(unmarshalled, localClass, "value");
        assertThat(localFieldValue.toString(), is("fourty two"));

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

        ClassDescriptor reconstructedDescriptor = ClassDescriptor.remote(
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
}
