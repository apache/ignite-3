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
import static org.apache.ignite.internal.configuration.processor.ConfigurationProcessorUtils.containsAnyAnnotation;
import static org.apache.ignite.internal.configuration.processor.ConfigurationProcessorUtils.joinSimpleName;
import static org.apache.ignite.internal.configuration.processor.ConfigurationProcessorUtils.simpleName;

import java.lang.annotation.Annotation;
import java.util.List;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.VariableElement;
import org.apache.ignite.configuration.annotation.AbstractConfiguration;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.InjectedName;
import org.apache.ignite.configuration.annotation.InjectedValue;
import org.apache.ignite.configuration.annotation.InternalId;
import org.apache.ignite.configuration.annotation.PolymorphicConfig;
import org.apache.ignite.configuration.annotation.PolymorphicId;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.configuration.processor.ClassWrapper;
import org.apache.ignite.internal.configuration.processor.ConfigurationValidationException;

/**
 * Validator for the {@link InjectedName} annotation.
 */
public class InjectedNameValidator extends Validator {
    private static final List<Class<? extends Annotation>> INCOMPATIBLE_ANNOTATIONS = List.of(
            ConfigValue.class,
            Value.class,
            InjectedValue.class,
            PolymorphicId.class,
            InternalId.class
    );

    public InjectedNameValidator(ProcessingEnvironment processingEnvironment) {
        super(processingEnvironment);
    }

    @Override
    public void validate(ClassWrapper classWrapper) {
        List<VariableElement> injectedNameFields = classWrapper.fieldsAnnotatedWith(InjectedName.class);

        if (injectedNameFields.isEmpty()) {
            return;
        }

        if (injectedNameFields.size() > 1) {
            throw new ConfigurationValidationException(classWrapper, String.format(
                    "Contains more than one field with %s: %s.",
                    simpleName(InjectedName.class),
                    injectedNameFields.stream().map(VariableElement::getSimpleName).collect(toList())
            ));
        }

        if (!containsAnyAnnotation(classWrapper.clazz(), Config.class, PolymorphicConfig.class, AbstractConfiguration.class)) {
            throw new ConfigurationValidationException(classWrapper, String.format(
                    "%s can only be present in a class annotated with %s.",
                    simpleName(InjectedName.class),
                    joinSimpleName(" or ", Config.class, PolymorphicConfig.class, AbstractConfiguration.class)
            ));
        }

        VariableElement injectedNameField = injectedNameFields.get(0);

        assertFieldType(classWrapper, injectedNameField, String.class);

        @SuppressWarnings("unchecked")
        Class<? extends Annotation>[] incompatibleAnnotations = INCOMPATIBLE_ANNOTATIONS.stream()
                .filter(annotation -> injectedNameField.getAnnotation(annotation) != null)
                .toArray(Class[]::new);

        if (incompatibleAnnotations.length > 0) {
            throw new ConfigurationValidationException(classWrapper, String.format(
                    "Field '%s' contains annotations conflicting with %s: %s.",
                    injectedNameField.getSimpleName(),
                    simpleName(InjectedName.class),
                    joinSimpleName(" and ", incompatibleAnnotations)
            ));
        }
    }
}
