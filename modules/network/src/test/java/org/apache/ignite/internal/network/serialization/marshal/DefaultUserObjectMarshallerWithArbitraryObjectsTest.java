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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import org.apache.ignite.internal.network.serialization.BuiltInType;
import org.apache.ignite.internal.network.serialization.ClassDescriptorFactory;
import org.apache.ignite.internal.network.serialization.ClassDescriptorRegistry;
import org.junit.jupiter.api.Test;

/**
 * Tests for how {@link DefaultUserObjectMarshaller} handles arbitrary objects.
 * An arbitrary object is an object of a class that is not built-in, not a {@link Serializable} and not an {@link java.io.Externalizable}.
 */
class DefaultUserObjectMarshallerWithArbitraryObjectsTest {
    private final ClassDescriptorRegistry descriptorRegistry = new ClassDescriptorRegistry();
    private final ClassDescriptorFactory descriptorFactory = new ClassDescriptorFactory(descriptorRegistry);

    private final DefaultUserObjectMarshaller marshaller = new DefaultUserObjectMarshaller(descriptorRegistry, descriptorFactory);

    private static boolean constructorCalled;
    private static boolean proxyRunCalled;

    private static final int INT_OUT_OF_INT_CACHE_RANGE = 1_000_000;

    @Test
    void marshalsAndUnmarshalsSimpleClassInstances() throws Exception {
        Simple unmarshalled = marshalAndUnmarshalNonNull(new Simple(42));

        assertThat(unmarshalled.value, is(42));
    }

    private <T> T marshalAndUnmarshalNonNull(Object object) throws MarshalException, UnmarshalException {
        MarshalledObject marshalled = marshaller.marshal(object);
        return unmarshalNonNull(marshalled);
    }

    private <T> T unmarshalNonNull(MarshalledObject marshalled) throws UnmarshalException {
        T unmarshalled = marshaller.unmarshal(marshalled.bytes(), descriptorRegistry);

        assertThat(unmarshalled, is(notNullValue()));

        return unmarshalled;
    }

    @Test
    void marshalsArbitraryObjectsUsingDescriptorsOfThemAndTheirContents() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new Simple(42));

        assertThat(marshalled.usedDescriptorIds(), equalTo(Set.of(
                descriptorRegistry.getRequiredDescriptor(Simple.class).descriptorId(),
                descriptorRegistry.getBuiltInDescriptor(BuiltInType.INT).descriptorId()
        )));
    }

    @Test
    void marshalsArbitraryObjectWithCorrectDescriptorIdInMarshalledRepresentation() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new Simple(42));

        assertThat(readType(marshalled), is(descriptorRegistry.getRequiredDescriptor(Simple.class).descriptorId()));
    }

    private int readType(MarshalledObject marshalled) throws IOException {
        try (var dis = new DataInputStream(new ByteArrayInputStream(marshalled.bytes()))) {
            return ProtocolMarshalling.readDescriptorOrCommandId(dis);
        }
    }

    @Test
    void marshalsAndUnmarshalsNullDeclaredInFieldOfTypeThrowable() throws Exception {
        WithThrowable unmarshalled = marshalAndUnmarshalNonNull(new WithThrowable());

        assertThat(unmarshalled.throwable, is(nullValue()));
    }

    @Test
    void marshalsAndUnmarshalsClassInstancesInvolvingSuperclasses() throws Exception {
        Child unmarshalled = marshalAndUnmarshalNonNull(new Child("answer", 42));

        assertThat(unmarshalled.parentValue(), is("answer"));
        assertThat(unmarshalled.childValue(), is(42));
    }

    @Test
    void usesDescriptorsOfAllAncestors() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new Child("answer", 42));

        assertThat(marshalled.usedDescriptorIds(), hasItems(
                descriptorRegistry.getRequiredDescriptor(Parent.class).descriptorId(),
                descriptorRegistry.getRequiredDescriptor(Child.class).descriptorId()
        ));
    }

    @Test
    void marshalsAndUnmarshalsClassInstancesHavingNestedArbitraryObjects() throws Exception {
        WithArbitraryClassField unmarshalled = marshalAndUnmarshalNonNull(new WithArbitraryClassField(new Simple(42)));

        assertThat(unmarshalled.nested, is(notNullValue()));
        assertThat(unmarshalled.nested.value, is(42));
    }

    @Test
    void marshalsAndUnmarshalsClassInstancesHavingCollectionsOfArbitraryObjects() throws Exception {
        WithArbitraryObjectInList unmarshalled = marshalAndUnmarshalNonNull(withArbitraryObjectInArrayList(new Simple(42)));

        assertThat(unmarshalled.list, hasSize(1));
        assertThat(unmarshalled.list.get(0).value, is(42));
    }

    private WithArbitraryObjectInList withArbitraryObjectInArrayList(Simple object) {
        List<Simple> list = new ArrayList<>(List.of(object));
        return new WithArbitraryObjectInList(list);
    }

    @Test
    void marshalsAndUnmarshalsClassInstancesHavingPolymorphicNestedArbitraryObjects() throws Exception {
        WithArbitraryClassField unmarshalled = marshalAndUnmarshalNonNull(new WithArbitraryClassField(new ChildOfSimple(42)));

        assertThat(unmarshalled.nested, is(instanceOf(ChildOfSimple.class)));
        assertThat(unmarshalled.nested.value, is(42));
    }

    @Test
    void marshalsAndUnmarshalsClassInstancesHavingCollectionsOfPolymorphicArbitraryObjects() throws Exception {
        WithArbitraryObjectInList object = withArbitraryObjectInArrayList(new ChildOfSimple(42));

        WithArbitraryObjectInList unmarshalled = marshalAndUnmarshalNonNull(object);

        assertThat(unmarshalled.list, hasSize(1));
        assertThat(unmarshalled.list.get(0), is(instanceOf(ChildOfSimple.class)));
        assertThat(unmarshalled.list.get(0).value, is(42));
    }

    @Test
    void restoresConcreteCollectionTypeCorrectlyWhenUnmarshalls() throws Exception {
        WithArbitraryObjectInList unmarshalled = marshalAndUnmarshalNonNull(withArbitraryObjectInArrayList(new Simple(42)));

        assertThat(unmarshalled.list, is(instanceOf(ArrayList.class)));
    }

    @Test
    void ignoresTransientFields() throws Exception {
        WithTransientFields unmarshalled = marshalAndUnmarshalNonNull(new WithTransientFields("Hi"));

        assertThat(unmarshalled.value, is(nullValue()));
    }

    @Test
    void supportsFinalFields() throws Exception {
        WithFinalFields unmarshalled = marshalAndUnmarshalNonNull(new WithFinalFields(42));

        assertThat(unmarshalled.value, is(42));
    }

    @Test
    void doesNotSupportInnerClassInstances() {
        assertThrows(MarshallingNotSupportedException.class, () -> marshaller.marshal(new Inner()));
    }

    @Test
    void doesNotSupportInnerClassInstancesInsideContainers() {
        List<Inner> list = singletonList(new Inner());

        assertThrows(MarshallingNotSupportedException.class, () -> marshaller.marshal(list));
    }

    @Test
    void supportsNonCapturingAnonymousClassInstances() throws Exception {
        Callable<String> unmarshalled = marshalAndUnmarshalNonNull(nonCapturingAnonymousInstance());

        assertThat(unmarshalled.call(), is("Hi!"));
    }

    private static Callable<String> nonCapturingAnonymousInstance() {
        return new Callable<>() {
            /** {@inheritDoc} */
            @Override
            public String call() {
                return "Hi!";
            }
        };
    }

    @Test
    void doesNotSupportCapturingAnonymousClassInstances() {
        Runnable capturingClosure = capturingAnonymousInstance();

        assertThrows(MarshallingNotSupportedException.class, () -> marshaller.marshal(capturingClosure));
    }

    private Runnable capturingAnonymousInstance() {
        return new Runnable() {
            /** {@inheritDoc} */
            @Override
            public void run() {
                System.out.println(DefaultUserObjectMarshallerWithArbitraryObjectsTest.this);
            }
        };
    }

    @Test
    void doesNotSupportCapturingAnonymousClassInstancesInsideContainers() {
        Runnable capturingAnonymousInstance = capturingAnonymousInstance();
        List<Runnable> list = singletonList(capturingAnonymousInstance);

        assertThrows(MarshallingNotSupportedException.class, () -> marshaller.marshal(list));
    }

    /**
     * We should not support non-capturing non-serializable Lambdas. Even though it's possible to marshal and unmarshal
     * such lambda inside the same JVM, it's impossible to load its class by name (even when it exists in the JVM with
     * that same name!), so such lambdas will be impossible to unmarshal at another JVM.
     *
     */
    @Test
    void doesNotSupportNonCapturingNonSerializableLambdas() {
        assertThrows(MarshallingNotSupportedException.class, () -> marshalAndUnmarshalNonNull(nonCapturingLambda()));
    }

    private static Callable<String> nonCapturingLambda() {
        return () -> "Hi!";
    }

    @Test
    void supportsNonCapturingSerializableLambdas() throws Exception {
        Callable<String> unmarshalled = marshalAndUnmarshalNonNull(nonCapturingSerializableLambda());

        assertThat(unmarshalled.call(), is("Hi!"));
    }

    private static Callable<String> nonCapturingSerializableLambda() {
        return (Callable<String> & Serializable) () -> "Hi!";
    }

    @Test
    void doesNotSupportCapturingNonSerializableLambdas() {
        Runnable capturingClosure = capturingNonSerializableLambda();

        assertThrows(MarshallingNotSupportedException.class, () -> marshaller.marshal(capturingClosure));
    }

    private Runnable capturingNonSerializableLambda() {
        return () -> System.out.println(DefaultUserObjectMarshallerWithArbitraryObjectsTest.this);
    }

    @Test
    void doesNotSupportCapturingSerializableLambdas() {
        Runnable capturingClosure = capturingSerializableLambda();

        assertThrows(MarshallingNotSupportedException.class, () -> marshaller.marshal(capturingClosure));
    }

    private Runnable capturingSerializableLambda() {
        return (Runnable & Serializable) () -> System.out.println(DefaultUserObjectMarshallerWithArbitraryObjectsTest.this);
    }

    @Test
    void doesNotSupportCapturingLambdasInsideContainers() {
        Runnable capturingLambda = capturingNonSerializableLambda();
        List<Runnable> list = singletonList(capturingLambda);

        assertThrows(MarshallingNotSupportedException.class, () -> marshaller.marshal(list));
    }

    @Test
    void supportsNonCapturingLocalClassInstances() throws Exception {
        Callable<String> unmarshalled = marshalAndUnmarshalNonNull(nonCapturingLocalClassInstance());

        assertThat(unmarshalled.call(), is("Hi!"));
    }

    private static Object nonCapturingLocalClassInstance() {
        class Local implements Callable<String> {
            /** {@inheritDoc} */
            @Override
            public String call() {
                return "Hi!";
            }
        }

        return new Local();
    }

    @Test
    void doesNotSupportCapturingLocalClassInstances() {
        Object instance = capturingLocalClassInstance();

        assertThrows(MarshallingNotSupportedException.class, () -> marshaller.marshal(instance));
    }

    private Object capturingLocalClassInstance() {
        class Local {
        }

        return new Local();
    }

    @Test
    void supportsNonSerializableClassesWithoutNoArgConstructor() throws Exception {
        NonSerializableWithoutNoArgConstructor unmarshalled = marshalAndUnmarshalNonNull(new NonSerializableWithoutNoArgConstructor(42));

        assertThat(unmarshalled.value, is(42));
    }

    @Test
    void supportsInstancesDirectlyContainingThemselvesInFields() throws Exception {
        WithInfiniteCycleViaField unmarshalled = marshalAndUnmarshalNonNull(new WithInfiniteCycleViaField(42));

        assertThat(unmarshalled.value, is(42));
        assertThat(unmarshalled.myself, is(sameInstance(unmarshalled)));
    }

    @Test
    void supportsInstancesParticipatingInIndirectInfiniteCyclesViaArbitraryObjects() throws Exception {
        WithFirstCyclePart first = new WithFirstCyclePart();
        WithSecondCyclePart second = new WithSecondCyclePart();
        first.part = second;
        second.part = first;

        WithFirstCyclePart unmarshalled = marshalAndUnmarshalNonNull(first);

        assertThat(unmarshalled.part.part, is(sameInstance(unmarshalled)));
    }

    @Test
    void supportsInstancesParticipatingInIndirectInfiniteCyclesViaMutableContainers() throws Exception {
        WithObjectList object = new WithObjectList();
        List<Object> container = new ArrayList<>();
        object.contents = container;
        container.add(object);

        WithObjectList unmarshalled = marshalAndUnmarshalNonNull(object);

        assertThat(unmarshalled.contents.get(0), is(sameInstance(unmarshalled)));
    }

    @Test
    void doesNotInvokeNoArgConstructorOfArbitraryClassOnUnmarshalling() throws Exception {
        WithSideEffectInConstructor object = new WithSideEffectInConstructor();
        constructorCalled = false;

        marshalAndUnmarshalNonNull(object);

        assertFalse(constructorCalled);
    }

    @Test
    void supportsListOf() throws Exception {
        List<Integer> list = marshalAndUnmarshalNonNull(List.of(1, 2, 3));

        assertThat(list, contains(1, 2, 3));
    }

    @Test
    void supportsSetOf() throws Exception {
        Set<Integer> set = marshalAndUnmarshalNonNull(Set.of(1, 2, 3));

        assertThat(set, containsInAnyOrder(1, 2, 3));
    }

    @Test
    void supportsMapOf() throws Exception {
        Map<?, ?> map = marshalAndUnmarshalNonNull(Map.of(1, 2, 3, 4));

        assertThat(map, is(equalTo(Map.of(1, 2, 3, 4))));
    }

    @Test
    void supportsEnumsInFields() throws Exception {
        WithSimpleEnumField unmarshalled = marshalAndUnmarshalNonNull(new WithSimpleEnumField(SimpleEnum.FIRST));

        assertThat(unmarshalled.value, is(SimpleEnum.FIRST));
    }

    @Test
    void supportsEnumsWithAnonClassesForMembersInFields() throws Exception {
        WithEnumWithAnonClassesForMembersField object = new WithEnumWithAnonClassesForMembersField(EnumWithAnonClassesForMembers.FIRST);

        WithEnumWithAnonClassesForMembersField unmarshalled = marshalAndUnmarshalNonNull(object);

        assertThat(unmarshalled.value, is(EnumWithAnonClassesForMembers.FIRST));
    }

    @Test
    void marshalsAndUnmarshalsPrimitivesInFieldsCorrectly() throws Exception {
        WithPrimitives unmarshalled = marshalAndUnmarshalNonNull(new WithPrimitives());

        assertThat(unmarshalled.byteVal, is((byte) 1));
        assertThat(unmarshalled.shortVal, is((short) 2));
        assertThat(unmarshalled.intVal, is(3));
        assertThat(unmarshalled.longVal, is(4L));
        assertThat(unmarshalled.floatVal, is(5.0f));
        assertThat(unmarshalled.doubleVal, is(6.0));
        assertThat(unmarshalled.charVal, is('a'));
        assertThat(unmarshalled.booleanVal, is(true));
    }

    @Test
    void marshalsAndUnmarshalsPrimitiveWrappersInFieldsCorrectly() throws Exception {
        WithPrimitiveWrappers unmarshalled = marshalAndUnmarshalNonNull(new WithPrimitiveWrappers());

        assertThat(unmarshalled.byteVal, is((byte) 1));
        assertThat(unmarshalled.shortVal, is((short) 2));
        assertThat(unmarshalled.integerVal, is(3));
        assertThat(unmarshalled.longVal, is(4L));
        assertThat(unmarshalled.floatVal, is(5.0f));
        assertThat(unmarshalled.doubleVal, is(6.0));
        assertThat(unmarshalled.characterVal, is('a'));
        assertThat(unmarshalled.booleanVal, is(true));
    }

    @Test
    void unmarshalsReferencesToSameObjectOfNonBuiltInTypeToSameObject() throws Exception {
        Simple obj = new Simple(42);
        List<?> list = new ArrayList<>(Arrays.asList(obj, obj));

        List<?> unmarshalled = marshalAndUnmarshalNonNull(list);

        assertThat(unmarshalled.get(0), sameInstance(unmarshalled.get(1)));
    }

    @Test
    void unmarshalsSamePrimitiveWrapperReferencesToSameInstances() throws Exception {
        Integer obj = INT_OUT_OF_INT_CACHE_RANGE;
        List<?> list = new ArrayList<>(Arrays.asList(obj, obj));

        List<?> unmarshalled = marshalAndUnmarshalNonNull(list);

        assertThat(unmarshalled.get(0), sameInstance(unmarshalled.get(1)));
    }

    @Test
    void unmarshalsDifferentButEqualObjectsToDifferentObjects() throws Exception {
        int intValue = INT_OUT_OF_INT_CACHE_RANGE;
        String obj1 = String.valueOf(intValue);
        String obj2 = String.valueOf(intValue);
        List<?> list = new ArrayList<>(Arrays.asList(obj1, obj2));

        List<?> unmarshalled = marshalAndUnmarshalNonNull(list);

        assertThat(unmarshalled.get(0), not(sameInstance(unmarshalled.get(1))));
    }

    @SuppressWarnings("unchecked")
    @Test
    void marshalsAndUnmarshalsJavaProxies() throws Exception {
        proxyRunCalled = false;

        Object proxy = testProxyInstance();

        Object unmarshalled = marshalAndUnmarshalNonNull(proxy);

        assertThat(unmarshalled, is(instanceOf(Runnable.class)));
        Runnable runnable = (Runnable) unmarshalled;
        runnable.run();
        assertTrue(proxyRunCalled);

        assertThat(unmarshalled, is(instanceOf(Callable.class)));
        Callable<String> callable = (Callable<String>) unmarshalled;
        assertThat(callable.call(), is("Hi!"));
    }

    private Object testProxyInstance() {
        InvocationHandler handler = new TestInvocationHandler();
        return Proxy.newProxyInstance(
                contextClassLoader(),
                new Class[]{Runnable.class, Callable.class},
                handler
        );
    }

    @Test
    void cycleViaProxyIsSupported() throws Exception {
        InvocationHandlerWithRefToProxy originalHandler = new InvocationHandlerWithRefToProxy();
        Object originalProxy = Proxy.newProxyInstance(contextClassLoader(), new Class[]{Runnable.class}, originalHandler);
        originalHandler.ref = originalProxy;

        Object unmarshalledProxy = marshalAndUnmarshalNonNull(originalProxy);
        var unmarshalledHandler = (InvocationHandlerWithRefToProxy) Proxy.getInvocationHandler(unmarshalledProxy);

        assertThat(unmarshalledHandler.ref, is(sameInstance(unmarshalledProxy)));
    }

    private ClassLoader contextClassLoader() {
        return Thread.currentThread().getContextClassLoader();
    }

    @Test
    void asciiInStringFieldIsSupported() throws Exception {
        WithStringField unmarshalled = marshalAndUnmarshalNonNull(new WithStringField("a"));

        assertThat(unmarshalled.value, is("a"));
    }

    @Test
    void nonAsciiLatin1InStringFieldIsSupported() throws Exception {
        WithStringField unmarshalled = marshalAndUnmarshalNonNull(new WithStringField("é"));

        assertThat(unmarshalled.value, is("é"));
    }

    @Test
    void nonLatin1InStringFieldIsSupported() throws Exception {
        WithStringField unmarshalled = marshalAndUnmarshalNonNull(new WithStringField("щ"));

        assertThat(unmarshalled.value, is("щ"));
    }

    @Test
    void bigIntegerIsSupported() throws Exception {
        BigInteger result = marshalAndUnmarshalNonNull(BigInteger.TEN);

        assertThat(result, is(BigInteger.TEN));
    }

    private static boolean noArgs(Method method) {
        return method.getParameterTypes().length == 0;
    }

    private static class Simple {
        private int value;

        @SuppressWarnings("unused") // needed for instantiation
        public Simple() {
        }

        public Simple(int value) {
            this.value = value;
        }
    }

    private abstract static class Parent {
        private String value;

        public Parent() {
        }

        public Parent(String value) {
            this.value = value;
        }

        String parentValue() {
            return value;
        }
    }

    private static class WithThrowable {
        @SuppressWarnings("unused")
        private Throwable throwable;
    }

    private static class Child extends Parent {
        private int value;

        @SuppressWarnings("unused") // needed for instantiation
        public Child() {
        }

        public Child(String parentValue, int childValue) {
            super(parentValue);
            this.value = childValue;
        }

        int childValue() {
            return value;
        }
    }

    private static class WithArbitraryClassField {
        private Simple nested;

        @SuppressWarnings("unused") // used for instantiation
        public WithArbitraryClassField() {
        }

        public WithArbitraryClassField(Simple nested) {
            this.nested = nested;
        }
    }

    private static class WithArbitraryObjectInList {
        private List<Simple> list;

        @SuppressWarnings("unused") // needed for instantiation
        public WithArbitraryObjectInList() {
        }

        public WithArbitraryObjectInList(List<Simple> list) {
            this.list = list;
        }
    }

    private static class ChildOfSimple extends Simple {
        @SuppressWarnings("unused") // needed for instantiation
        public ChildOfSimple() {
        }

        public ChildOfSimple(int value) {
            super(value);
        }
    }

    private static class WithTransientFields {
        private transient String value;

        @SuppressWarnings("unused") // needed for instantiation
        public WithTransientFields() {
        }

        public WithTransientFields(String value) {
            this.value = value;
        }
    }

    private static class WithFinalFields {
        private final int value;

        @SuppressWarnings("unused") // needed for instantiation
        public WithFinalFields() {
            this(0);
        }

        private WithFinalFields(int value) {
            this.value = value;
        }
    }

    @SuppressWarnings("InnerClassMayBeStatic")
    private class Inner {
    }

    private static class WithInfiniteCycleViaField {
        private int value;
        @SuppressWarnings({"FieldCanBeLocal", "unused"})
        private WithInfiniteCycleViaField myself;

        @SuppressWarnings("unused")
        public WithInfiniteCycleViaField() {
        }

        public WithInfiniteCycleViaField(int value) {
            this.value = value;

            this.myself = this;
        }
    }

    private static class WithFirstCyclePart {
        private WithSecondCyclePart part;
    }

    private static class WithSecondCyclePart {
        private WithFirstCyclePart part;
    }

    private static class WithObjectList {
        private List<Object> contents;
    }

    private static class WithSideEffectInConstructor {
        public WithSideEffectInConstructor() {
            constructorCalled = true;
        }
    }

    private static class WithSimpleEnumField {
        private final SimpleEnum value;

        private WithSimpleEnumField(SimpleEnum value) {
            this.value = value;
        }
    }

    private static class WithEnumWithAnonClassesForMembersField {
        private final EnumWithAnonClassesForMembers value;

        private WithEnumWithAnonClassesForMembersField(EnumWithAnonClassesForMembers value) {
            this.value = value;
        }
    }

    private static class WithPrimitives {
        private final byte byteVal = 1;
        private final short shortVal = 2;
        private final int intVal = 3;
        private final long longVal = 4L;
        private final float floatVal = 5.0f;
        private final double doubleVal = 6.0;
        private final char charVal = 'a';
        private final boolean booleanVal = true;
    }

    private static class WithPrimitiveWrappers {
        private final Byte byteVal = 1;
        private final Short shortVal = 2;
        private final Integer integerVal = 3;
        private final Long longVal = 4L;
        private final Float floatVal = 5.0f;
        private final Double doubleVal = 6.0;
        private final Character characterVal = 'a';
        private final Boolean booleanVal = true;
    }

    private static class TestInvocationHandler implements InvocationHandler {
        /** {@inheritDoc} */
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if ("run".equals(method.getName()) && noArgs(method)) {
                proxyRunCalled = true;
                return null;
            }
            if ("call".equals(method.getName()) && noArgs(method)) {
                return "Hi!";
            }
            if ("hashCode".equals(method.getName()) && noArgs(method)) {
                return hashCode();
            }

            throw new RuntimeException("Don't know how to handle " + method + " with args" + Arrays.toString(args));
        }
    }

    private static class InvocationHandlerWithRefToProxy implements InvocationHandler, Serializable {
        private Object ref;

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if ("equals".equals(method.getName()) && method.getParameterTypes().length == 1
                    && method.getParameterTypes()[0] == Object.class) {
                return proxy == args[0];
            }
            if ("hashCode".equals(method.getName()) && noArgs(method)) {
                return hashCode();
            }
            if ("toString".equals(method.getName()) && noArgs(method)) {
                return "Proxy with placeholder";
            }

            throw new RuntimeException("Don't know how to handle " + method + " with args" + Arrays.toString(args));
        }
    }

    private static class WithStringField {
        private final String value;

        private WithStringField(String value) {
            this.value = value;
        }
    }
}
