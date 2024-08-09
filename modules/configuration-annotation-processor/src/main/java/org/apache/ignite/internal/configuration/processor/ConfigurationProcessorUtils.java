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
import static java.util.stream.Collectors.toList;
import static javax.lang.model.element.Modifier.STATIC;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.TypeSpec;
import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;

/**
 * Annotation processing utilities.
 */
class ConfigurationProcessorUtils {
    /** Java file padding. */
    static final String INDENT = "    ";

    /** Postfix with which any configuration schema class name must end. */
    static final String CONFIGURATION_SCHEMA_POSTFIX = "ConfigurationSchema";

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
                schemaClassName.simpleName().replace(CONFIGURATION_SCHEMA_POSTFIX, "View")
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
                schemaClassName.simpleName().replace(CONFIGURATION_SCHEMA_POSTFIX, "Change")
        );
    }

    /**
     * Returns {@link ClassName} for configuration BUILDER object interface.
     *
     * @param schemaClassName Configuration schema ClassName.
     */
    static ClassName getBuilderName(ClassName schemaClassName) {
        // Builder interface is in public package
        String packageName = schemaClassName.packageName().replace(".internal", "");
        return ClassName.get(
                packageName,
                schemaClassName.simpleName().replace(CONFIGURATION_SCHEMA_POSTFIX, "Builder")
        );
    }

    /**
     * Returns {@link ClassName} for configuration BUILDER object class.
     *
     * @param schemaClassName Configuration schema ClassName.
     */
    static ClassName getBuilderImplName(ClassName schemaClassName) {
        return ClassName.get(
                schemaClassName.packageName(),
                schemaClassName.simpleName().replace(CONFIGURATION_SCHEMA_POSTFIX, "BuilderImpl")
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
     * Returns the first annotation found for the class.
     *
     * @param clazz Class type.
     * @param annotationClasses Annotation classes that will be searched for the class.
     */
    @SafeVarargs
    public static Optional<? extends Annotation> findFirstPresentAnnotation(
            TypeElement clazz,
            Class<? extends Annotation>... annotationClasses
    ) {
        return Stream.of(annotationClasses).map(clazz::getAnnotation).filter(Objects::nonNull).findFirst();
    }

    /**
     * Collect fields with annotation.
     *
     * @param fields Fields.
     * @param annotationClass Annotation class.
     */
    public static List<VariableElement> collectFieldsWithAnnotation(
            Collection<VariableElement> fields,
            Class<? extends Annotation> annotationClass
    ) {
        return fields.stream().filter(f -> f.getAnnotation(annotationClass) != null).collect(toList());
    }

    static String capitalize(String name) {
        return name.substring(0, 1).toUpperCase() + name.substring(1);
    }

    /**
     * Get class fields.
     *
     * @param type Class type.
     * @return Class fields.
     */
    static List<VariableElement> fields(TypeElement type) {
        return type.getEnclosedElements().stream()
                .filter(el -> el.getKind() == ElementKind.FIELD)
                .filter(el -> !el.getModifiers().contains(STATIC)) // ignore static members
                .map(VariableElement.class::cast)
                .collect(toList());
    }

    static void buildClass(ProcessingEnvironment processingEnv, String packageName, TypeSpec cls) {
        try {
            JavaFile.builder(packageName, cls)
                    .indent(INDENT)
                    .build()
                    .writeTo(processingEnv.getFiler());
        } catch (Throwable throwable) {
            throw new ConfigurationProcessorException("Failed to generate class " + packageName + "." + cls.name, throwable);
        }
    }
}
