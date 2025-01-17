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

import static javax.lang.model.element.Modifier.PUBLIC;
import static org.apache.ignite.internal.configuration.processor.ConfigurationProcessor.CONFIGURATION_SCHEMA_POSTFIX;
import static org.apache.ignite.internal.configuration.processor.ConfigurationProcessorUtils.containsAnyAnnotation;
import static org.apache.ignite.internal.configuration.processor.ConfigurationProcessorUtils.simpleName;

import java.util.UUID;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.VariableElement;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.InjectedName;
import org.apache.ignite.configuration.annotation.InjectedValue;
import org.apache.ignite.configuration.annotation.InternalId;
import org.apache.ignite.configuration.annotation.Name;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.PolymorphicId;
import org.apache.ignite.configuration.annotation.Secret;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.configuration.processor.ClassWrapper;
import org.apache.ignite.internal.configuration.processor.ConfigurationValidationException;

/**
 * Validator for miscellaneous issues that are too small to have their own validators.
 */
public class MiscellaneousIssuesValidator extends Validator {
    public MiscellaneousIssuesValidator(ProcessingEnvironment processingEnvironment) {
        super(processingEnvironment);
    }

    @Override
    public void validate(ClassWrapper classWrapper) {
        if (!classWrapper.clazz().getSimpleName().toString().endsWith(CONFIGURATION_SCHEMA_POSTFIX)) {
            throw new ConfigurationValidationException(classWrapper, String.format(
                    "Class name must end with '%s'.", CONFIGURATION_SCHEMA_POSTFIX
            ));
        }

        for (VariableElement field : classWrapper.fields()) {
            if (!field.getModifiers().contains(PUBLIC)) {
                throw new ConfigurationValidationException(classWrapper, String.format(
                        "Field %s must be public.", field.getSimpleName()
                ));
            }

            boolean containsAnySupportedAnnotation = containsAnyAnnotation(
                    field,
                    Value.class,
                    ConfigValue.class,
                    NamedConfigValue.class,
                    PolymorphicId.class,
                    InjectedName.class,
                    InternalId.class,
                    InjectedValue.class,
                    Secret.class
            );

            if (!containsAnySupportedAnnotation) {
                throw new ConfigurationValidationException(classWrapper, String.format(
                        "Field '%s' does not contain any supported annotations", field.getSimpleName()
                ));
            }

            if (field.getAnnotation(Value.class) != null) {
                // Must be a primitive or an array of the primitives (including java.lang.String, java.util.UUID).
                assertValidValueFieldType(classWrapper, field);
            }

            if (field.getAnnotation(PolymorphicId.class) != null) {
                assertFieldType(classWrapper, field, String.class);
            }

            if (field.getAnnotation(InternalId.class) != null) {
                assertFieldType(classWrapper, field, UUID.class);
            }

            if (field.getAnnotation(Secret.class) != null) {
                assertFieldType(classWrapper, field, String.class);
            }

            if (field.getAnnotation(Name.class) != null && field.getAnnotation(ConfigValue.class) == null) {
                throw new ConfigurationValidationException(classWrapper, String.format(
                        "Field '%s' annotated with %s can only be used together with %s.",
                        field.getSimpleName(),
                        simpleName(Name.class),
                        simpleName(ConfigValue.class)
                ));
            }
        }
    }
}
