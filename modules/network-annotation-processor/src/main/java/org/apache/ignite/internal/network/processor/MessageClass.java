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

package org.apache.ignite.internal.network.processor;

import com.squareup.javapoet.ClassName;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.annotations.Transferable;

/**
 * A wrapper around a {@link TypeElement} and the corresponding {@link ClassName} of an annotated Network Message.
 */
public class MessageClass {
    /** Annotated element. */
    private final TypeElement element;

    /** Class name of the {@code element}. */
    private final ClassName className;

    /** Annotation present on the {@code element}. */
    private final Transferable annotation;

    /**
     * Getter methods declared in the annotated interface.
     *
     * @see Transferable
     */
    private final List<ExecutableElement> getters;

    /**
     * Constructor.
     *
     * @param processingEnv Processing environment.
     * @param messageElement Element marked with the {@link Transferable} annotation.
     */
    MessageClass(ProcessingEnvironment processingEnv, TypeElement messageElement) {
        element = messageElement;
        className = ClassName.get(messageElement);
        annotation = messageElement.getAnnotation(Transferable.class);
        getters = extractGetters(processingEnv, messageElement);

        if (annotation.value() < 0) {
            throw new ProcessingException("Message type must not be negative", null, element);
        }
    }

    /**
     * Finds all getters in the given element and all of its superinterfaces.
     */
    private static List<ExecutableElement> extractGetters(ProcessingEnvironment processingEnv, TypeElement element) {
        var typeUtils = new TypeUtils(processingEnv);

        Map<String, ExecutableElement> gettersByName = typeUtils.allInterfaces(element)
                // this algorithm is suboptimal, since we have to scan over the same interfaces over and over again,
                // but this shouldn't be an issue, because it is not expected to have deep inheritance hierarchies
                .filter(e -> typeUtils.hasSuperInterface(e, NetworkMessage.class))
                // remove the NetworkMessage interface itself
                .filter(e -> !typeUtils.isSameType(e.asType(), NetworkMessage.class))
                .flatMap(e -> e.getEnclosedElements().stream())
                .filter(e -> e.getKind() == ElementKind.METHOD)
                .map(ExecutableElement.class::cast)
                .filter(e -> !e.isDefault())
                .filter(e -> !e.getModifiers().contains(Modifier.STATIC))
                // use a tree map to sort getters by name and remove duplicates
                .collect(Collectors.toMap(
                        e -> e.getSimpleName().toString(),
                        Function.identity(),
                        (e1, e2) -> e1,
                        TreeMap::new
                ));

        return List.copyOf(gettersByName.values());
    }

    /**
     * Returns annotated element.
     *
     * @return Annotated element.
     */
    public TypeElement element() {
        return element;
    }

    /**
     * Returns class name of the {@link #element()}.
     *
     * @return Class name of the {@link #element()}.
     */
    public ClassName className() {
        return className;
    }

    /**
     * Returns package name of the {@link #element()}.
     *
     * @return Package name of the {@link #element()}.
     */
    public String packageName() {
        return className.packageName();
    }

    /**
     * Returns simple name of the {@link #element()}.
     *
     * @return Simple name of the {@link #element()}.
     */
    public String simpleName() {
        return className.simpleName();
    }

    /**
     * Returns getter methods declared in the annotated interface.
     *
     * @return Getter methods declared in the annotated interface.
     */
    public List<ExecutableElement> getters() {
        return getters;
    }

    /**
     * Returns class name that the generated SerializationFactory should have.
     *
     * @return Class name that the generated SerializationFactory should have.
     */
    public ClassName serializationFactoryName() {
        return ClassName.get(packageName(), simpleName() + "SerializationFactory");
    }

    /**
     * Returns class name that the generated Network Message implementation should have.
     *
     * @return Class name that the generated Network Message implementation should have.
     */
    public ClassName implClassName() {
        return ClassName.get(packageName(), simpleName() + "Impl");
    }

    /**
     * Returns class name that the generated Builder interface should have.
     *
     * @return Class name that the generated Builder interface should have.
     */
    public ClassName builderClassName() {
        return ClassName.get(packageName(), simpleName() + "Builder");
    }

    /**
     * Returns class name that the generated Serializer should have.
     */
    public ClassName serializerClassName() {
        return ClassName.get(packageName(), simpleName() + "Serializer");
    }

    /**
     * Returns name of the factory method that should be used by the message factories.
     *
     * @return name of the factory method that should be used by the message factories
     */
    public String asMethodName() {
        return decapitalize(simpleName());
    }

    /**
     * Returns {@link Transferable#value()}.
     *
     * @return {@link Transferable#value()}.
     */
    public short messageType() {
        return annotation.value();
    }

    /**
     * Returns {@link Transferable#autoSerializable()}.
     *
     * @return {@link Transferable#autoSerializable()}.
     */
    public boolean isAutoSerializable() {
        return annotation.autoSerializable();
    }

    /**
     * Returns a copy of the given string with the first character converted to lower case.
     *
     * @return A copy of the given string with the first character converted to lower case.
     */
    private static String decapitalize(String str) {
        return Character.toLowerCase(str.charAt(0)) + str.substring(1);
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MessageClass clazz = (MessageClass) o;
        return element.equals(clazz.element);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return Objects.hash(element);
    }
}
