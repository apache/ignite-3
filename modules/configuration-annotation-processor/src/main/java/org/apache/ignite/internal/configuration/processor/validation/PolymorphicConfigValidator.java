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

import static org.apache.ignite.internal.configuration.processor.ConfigurationProcessorUtils.simpleName;

import java.util.List;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.VariableElement;
import org.apache.ignite.configuration.annotation.PolymorphicConfig;
import org.apache.ignite.configuration.annotation.PolymorphicId;
import org.apache.ignite.internal.configuration.processor.ClassWrapper;
import org.apache.ignite.internal.configuration.processor.ConfigurationValidationException;

/**
 * Validator for the {@link PolymorphicConfig} annotation.
 */
public class PolymorphicConfigValidator extends Validator {
    public PolymorphicConfigValidator(ProcessingEnvironment processingEnvironment) {
        super(processingEnvironment);
    }

    @Override
    public void validate(ClassWrapper classWrapper) {
        if (classWrapper.clazz().getAnnotation(PolymorphicConfig.class) == null) {
            return;
        }

        assertHasCompatibleTopLevelAnnotation(classWrapper, PolymorphicConfig.class);

        assertHasNoSuperClass(classWrapper);

        List<VariableElement> annotatedFields = classWrapper.fieldsAnnotatedWith(PolymorphicId.class);

        if (annotatedFields.size() != 1 || classWrapper.fields().indexOf(annotatedFields.get(0)) != 0) {
            throw new ConfigurationValidationException(classWrapper, String.format(
                    "Must contain one field with %s and it must be the first in the schema.",
                    simpleName(PolymorphicConfig.class),
                    simpleName(PolymorphicId.class)
            ));
        }
    }
}
