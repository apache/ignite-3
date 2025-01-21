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

import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.PolymorphicConfig;
import org.apache.ignite.internal.configuration.processor.ClassWrapper;
import org.apache.ignite.internal.configuration.processor.ConfigurationValidationException;

/**
 * Validator for the {@link NamedConfigValue} annotation.
 */
public class NamedConfigValueValidator extends Validator {
    public NamedConfigValueValidator(ProcessingEnvironment processingEnvironment) {
        super(processingEnvironment);
    }

    @Override
    public void validate(ClassWrapper classWrapper) {
        for (VariableElement field : classWrapper.fields()) {
            if (field.getAnnotation(NamedConfigValue.class) == null) {
                continue;
            }

            TypeElement fieldType = (TypeElement) processingEnvironment.getTypeUtils().asElement(field.asType());

            if (!containsAnyAnnotation(fieldType, Config.class, PolymorphicConfig.class)) {
                throw new ConfigurationValidationException(classWrapper, String.format(
                        "Class %s for field %s must be annotated with %s.",
                        fieldType.getQualifiedName(),
                        field.getSimpleName(),
                        joinSimpleName(" or ", Config.class, PolymorphicConfig.class)
                ));
            }
        }
    }
}
