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

import static java.util.stream.Collectors.joining;

import com.squareup.javapoet.ClassName;
import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.Element;
import javax.lang.model.type.ArrayType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import org.apache.ignite.configuration.annotation.Value;

/**
 * Annotation processing utilities.
 */
public class ConfigurationProcessorUtils {
    /**
     * Returns {@link ClassName} for configuration class public interface.
     *
     * @param schemaClassName Configuration schema ClassName.
     */
    public static ClassName getConfigurationInterfaceName(ClassName schemaClassName) {
        return ClassName.get(
                schemaClassName.packageName(),
                schemaClassName.simpleName().replaceAll("Schema$", "")
        );
    }

    /**
     * Returns {@link ClassName} for configuration VIEW object class.
     *
     * @param schemaClassName Configuration schema ClassName.
     */
    public static ClassName getViewName(ClassName schemaClassName) {
        return ClassName.get(
                schemaClassName.packageName(),
                schemaClassName.simpleName().replace("ConfigurationSchema", "View")
        );
    }

    /**
     * Returns {@link ClassName} for configuration CHANGE object class.
     *
     * @param schemaClassName Configuration schema ClassName.
     */
    public static ClassName getChangeName(ClassName schemaClassName) {
        return ClassName.get(
                schemaClassName.packageName(),
                schemaClassName.simpleName().replace("ConfigurationSchema", "Change")
        );
    }

    /**
     * Returns the simple name of the annotation as: {@code @Config}.
     *
     * @param annotationClass Annotation class.
     */
    public static String simpleName(Class<? extends Annotation> annotationClass) {
        return '@' + annotationClass.getSimpleName();
    }

    /**
     * Creates a string with simple annotation names like: {@code @Config} and {@code @PolymorphicConfig}.
     *
     * @param delimiter Delimiter between elements.
     * @param annotations Annotations.
     */
    @SafeVarargs
    public static String joinSimpleName(String delimiter, Class<? extends Annotation>... annotations) {
        return Stream.of(annotations).map(ConfigurationProcessorUtils::simpleName).collect(joining(delimiter));
    }

    /**
     * Returns {@code true} if any of the given annotations are present on any of the given elements.
     */
    @SafeVarargs
    public static boolean containsAnyAnnotation(List<? extends Element> elements, Class<? extends Annotation>... annotations) {
        return elements.stream().anyMatch(e -> containsAnyAnnotation(e, annotations));
    }

    /**
     * Returns {@code true} if any of the given annotations are present on the given element.
     */
    @SafeVarargs
    public static boolean containsAnyAnnotation(Element element, Class<? extends Annotation>... annotations) {
        return Arrays.stream(annotations).anyMatch(a -> element.getAnnotation(a) != null);
    }

    /**
     * Checks that the type of the field with {@link Value} is valid: primitive, {@link String}, or {@link UUID} (or an array of one of
     * these).
     *
     * @param type Field type with {@link Value}.
     * @return {@code True} if the field type is valid.
     */
    public static boolean isValidValueAnnotationFieldType(ProcessingEnvironment processingEnvironment, TypeMirror type) {
        if (type.getKind() == TypeKind.ARRAY) {
            type = ((ArrayType) type).getComponentType();
        }

        if (type.getKind().isPrimitive()) {
            return true;
        }

        return sameType(processingEnvironment, type, String.class) || sameType(processingEnvironment, type, UUID.class);
    }

    /**
     * Returns {@code true} if the given types are the same type.
     *
     * @param type1 first type (represented by a mirror)
     * @param type2 second type (represented by a {@code Class})
     * @return {@code true} if both types represent the same type, {@code false} otherwise.
     */
    public static boolean sameType(ProcessingEnvironment processingEnvironment, TypeMirror type1, Class<?> type2) {
        TypeMirror classType = processingEnvironment
                .getElementUtils()
                .getTypeElement(type2.getCanonicalName())
                .asType();

        return processingEnvironment.getTypeUtils().isSameType(classType, type1);
    }
}
