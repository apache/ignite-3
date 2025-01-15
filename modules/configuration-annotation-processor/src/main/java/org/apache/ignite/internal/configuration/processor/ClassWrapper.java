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

package org.apache.ignite.internal.configuration.processor;

import static java.util.stream.Collectors.toList;
import static javax.lang.model.element.Modifier.STATIC;
import static org.apache.ignite.internal.configuration.processor.ConfigurationProcessorUtils.sameType;

import java.lang.annotation.Annotation;
import java.util.List;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeMirror;
import org.apache.ignite.internal.util.Lazy;
import org.jetbrains.annotations.Nullable;

/**
 * A wrapper around a class (represented by a {@link TypeElement}) with some helper methods.
 */
public class ClassWrapper {
    private final ProcessingEnvironment processingEnvironment;

    private final TypeElement clazz;

    private final List<VariableElement> fields;

    private final Lazy<ClassWrapper> superClass = new Lazy<>(this::computeSuperClass);

    /** Constructor. */
    public ClassWrapper(ProcessingEnvironment processingEnvironment, TypeElement clazz) {
        this.processingEnvironment = processingEnvironment;
        this.clazz = clazz;
        this.fields = fields(clazz);
    }

    public TypeElement clazz() {
        return clazz;
    }

    public <A extends Annotation> A getAnnotation(Class<A> annotationType) {
        return clazz.getAnnotation(annotationType);
    }

    /**
     * Returns the superclass of the class, or {@code null} if the superclass is {@link Object}.
     */
    public @Nullable ClassWrapper superClass() {
        return superClass.get();
    }

    /**
     * Returns the superclass of the class, or throws {@link NullPointerException} if the superclass is {@link Object}.
     */
    public ClassWrapper requiredSuperClass() {
        ClassWrapper superClass = superClass();

        if (superClass == null) {
            throw new NullPointerException(String.format("Class %s does not have a superclass", this));
        }

        return superClass;
    }

    private @Nullable ClassWrapper computeSuperClass() {
        TypeMirror superClassType = clazz.getSuperclass();

        if (sameType(processingEnvironment, superClassType, Object.class)) {
            return null;
        }

        var superType = (TypeElement) processingEnvironment.getTypeUtils().asElement(superClassType);

        return new ClassWrapper(processingEnvironment, superType);
    }

    private static List<VariableElement> fields(TypeElement type) {
        return type.getEnclosedElements().stream()
                .filter(el -> el.getKind() == ElementKind.FIELD)
                .filter(el -> !el.getModifiers().contains(STATIC)) // ignore static members
                .map(VariableElement.class::cast)
                .collect(toList());
    }

    public List<VariableElement> fields() {
        return fields;
    }

    public List<VariableElement> fieldsAnnotatedWith(Class<? extends Annotation> annotationClass) {
        return fields.stream().filter(f -> f.getAnnotation(annotationClass) != null).collect(toList());
    }

    @Override
    public String toString() {
        return clazz.getQualifiedName().toString();
    }
}
