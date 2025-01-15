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

package org.apache.ignite.internal.configuration.processor.validation;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.configuration.processor.ConfigurationProcessor.TOP_LEVEL_ANNOTATIONS;
import static org.apache.ignite.internal.configuration.processor.ConfigurationProcessorUtils.containsAnyAnnotation;
import static org.apache.ignite.internal.configuration.processor.ConfigurationProcessorUtils.isValidValueAnnotationFieldType;
import static org.apache.ignite.internal.configuration.processor.ConfigurationProcessorUtils.joinSimpleName;
import static org.apache.ignite.internal.configuration.processor.ConfigurationProcessorUtils.sameType;
import static org.apache.ignite.internal.configuration.processor.ConfigurationProcessorUtils.simpleName;

import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.Name;
import javax.lang.model.element.VariableElement;
import org.apache.ignite.internal.configuration.processor.ClassWrapper;
import org.apache.ignite.internal.configuration.processor.ConfigurationValidationException;

/**
 * Base class for configuration validators.
 */
public abstract class Validator {
    final ProcessingEnvironment processingEnvironment;

    Validator(ProcessingEnvironment processingEnvironment) {
        this.processingEnvironment = processingEnvironment;
    }

    public abstract void validate(ClassWrapper classWrapper) throws ConfigurationValidationException;

    @SafeVarargs
    static void assertNotContainsFieldAnnotatedWith(ClassWrapper classWrapper, Class<? extends Annotation>... annotations) {
        if (containsAnyAnnotation(classWrapper.fields(), annotations)) {
            throw new ConfigurationValidationException(classWrapper, String.format(
                    "Must not have fields annotated with %s.", joinSimpleName(" or ", annotations)
            ));
        }
    }

    @SafeVarargs
    static void assertSuperClassNotContainsFieldAnnotatedWith(ClassWrapper classWrapper, Class<? extends Annotation>... annotations) {
        if (containsAnyAnnotation(classWrapper.requiredSuperClass().fields(), annotations)) {
            throw new ConfigurationValidationException(classWrapper, String.format(
                    "Superclass %s must not have fields annotated with %s.",
                    classWrapper.requiredSuperClass(), joinSimpleName(" or ", annotations)
            ));
        }
    }

    static void assertHasSuperClass(ClassWrapper classWrapper) {
        if (classWrapper.superClass() == null) {
            throw new ConfigurationValidationException(classWrapper, "Must have a superclass.");
        }
    }

    static void assertHasNoSuperClass(ClassWrapper classWrapper) {
        if (classWrapper.superClass() != null) {
            throw new ConfigurationValidationException(classWrapper, "Must not have a superclass.");
        }
    }

    @SafeVarargs
    static void assertSuperclassHasAnnotations(ClassWrapper classWrapper, Class<? extends Annotation>... annotations) {
        if (!containsAnyAnnotation(classWrapper.requiredSuperClass().clazz(), annotations)) {
            throw new ConfigurationValidationException(
                    classWrapper,
                    String.format("Superclass must be annotated with %s.", joinSimpleName(" or ", annotations))
            );
        }
    }

    static void assertNoFieldNameConflictsWithSuperClass(ClassWrapper classWrapper) {
        Collection<Name> duplicateFieldNames = findDuplicates(classWrapper.fields(), classWrapper.requiredSuperClass().fields());

        if (!duplicateFieldNames.isEmpty()) {
            throw new ConfigurationValidationException(classWrapper, String.format(
                    "Duplicate field names detected with the superclass %s: %s.",
                    classWrapper.requiredSuperClass(), duplicateFieldNames
            ));
        }
    }

    private static Collection<Name> findDuplicates(Collection<VariableElement> fields1, Collection<VariableElement> fields2) {
        if (fields1.isEmpty() || fields2.isEmpty()) {
            return List.of();
        }

        Set<Name> fieldNames1 = fields1.stream()
                .map(VariableElement::getSimpleName)
                .collect(toSet());

        return fields2.stream()
                .map(VariableElement::getSimpleName)
                .filter(fieldNames1::contains)
                .collect(toList());
    }

    void assertFieldType(ClassWrapper classWrapper, VariableElement field, Class<?> expectedType) {
        if (!sameType(processingEnvironment, field.asType(), expectedType)) {
            throw new ConfigurationValidationException(classWrapper, String.format(
                    "Field '%s' must be a %s.",
                    field.getSimpleName(),
                    expectedType.getSimpleName()
            ));
        }
    }

    void assertValidValueFieldType(ClassWrapper classWrapper, VariableElement field) {
        if (!isValidValueAnnotationFieldType(processingEnvironment, field.asType())) {
            throw new ConfigurationValidationException(classWrapper, String.format(
                    "Field '%s' must have one of the following types: "
                            + "boolean, int, long, double, String, UUID or an array of aforementioned types.",
                    field.getSimpleName()
            ));
        }
    }

    @SafeVarargs
    static void assertHasCompatibleTopLevelAnnotation(
            ClassWrapper classWrapper,
            Class<? extends Annotation> currentAnnotation,
            Class<? extends Annotation>... compatibleAnnotations
    ) {
        var allCompatibleAnnotations = new HashSet<Class<? extends Annotation>>();

        allCompatibleAnnotations.add(currentAnnotation);
        allCompatibleAnnotations.addAll(Arrays.asList(compatibleAnnotations));

        assert TOP_LEVEL_ANNOTATIONS.containsAll(allCompatibleAnnotations) : allCompatibleAnnotations;

        @SuppressWarnings("unchecked")
        Class<? extends Annotation>[] conflictingAnnotations = TOP_LEVEL_ANNOTATIONS.stream()
                .filter(annotation -> classWrapper.getAnnotation(annotation) != null && !allCompatibleAnnotations.contains(annotation))
                .toArray(Class[]::new);

        if (conflictingAnnotations.length > 0) {
            throw new ConfigurationValidationException(classWrapper, String.format(
                    "Has incompatible set of annotations: %s and %s.",
                    simpleName(currentAnnotation),
                    joinSimpleName(" and ", conflictingAnnotations)
            ));
        }
    }
}
