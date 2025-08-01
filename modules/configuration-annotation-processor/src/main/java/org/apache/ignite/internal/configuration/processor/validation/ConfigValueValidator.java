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

import static org.apache.ignite.internal.configuration.processor.ConfigurationProcessorUtils.containsAnyAnnotation;
import static org.apache.ignite.internal.configuration.processor.ConfigurationProcessorUtils.joinSimpleName;
import static org.apache.ignite.internal.configuration.processor.ConfigurationProcessorUtils.simpleName;

import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.InjectedName;
import org.apache.ignite.configuration.annotation.Name;
import org.apache.ignite.configuration.annotation.PolymorphicConfig;
import org.apache.ignite.internal.configuration.processor.ClassWrapper;
import org.apache.ignite.internal.configuration.processor.ConfigurationValidationException;

/**
 * Validator for the {@link ConfigValue} annotation.
 */
public class ConfigValueValidator extends Validator {
    public ConfigValueValidator(ProcessingEnvironment processingEnvironment) {
        super(processingEnvironment);
    }

    @Override
    public void validate(ClassWrapper classWrapper) {
        for (VariableElement field : classWrapper.fields()) {
            if (field.getAnnotation(ConfigValue.class) == null) {
                continue;
            }

            TypeElement fieldType = (TypeElement) processingEnvironment.getTypeUtils().asElement(field.asType());

            if (!containsAnyAnnotation(fieldType, Config.class, PolymorphicConfig.class)) {
                throw new ConfigurationValidationException(classWrapper, String.format(
                        "Class for %s field must be defined as %s",
                        simpleName(ConfigValue.class),
                        joinSimpleName(" or ", Config.class, PolymorphicConfig.class)
                ));
            }

            if (field.getAnnotation(Name.class) != null) {
                return;
            }

            var fieldClass = new ClassWrapper(processingEnvironment, fieldType);

            if (!fieldClass.fieldsAnnotatedWith(InjectedName.class).isEmpty()) {
                throw new ConfigurationValidationException(classWrapper, String.format(
                        "Missing %s annotation for field '%s'.",
                        simpleName(Name.class),
                        field.getSimpleName()
                ));
            }

            ClassWrapper fieldSuperClass = fieldClass.superClass();

            if (fieldSuperClass == null) {
                return;
            }

            if (!fieldSuperClass.fieldsAnnotatedWith(InjectedName.class).isEmpty()) {
                throw new ConfigurationValidationException(classWrapper, String.format(
                        "Missing %s annotation for field in superclass: %s.%s.",
                        simpleName(Name.class),
                        field.getEnclosingElement(),
                        field.getSimpleName()
                ));
            }
        }
    }
}
