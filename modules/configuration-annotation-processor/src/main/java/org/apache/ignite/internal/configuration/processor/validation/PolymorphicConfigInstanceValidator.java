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

import javax.annotation.processing.ProcessingEnvironment;
import org.apache.ignite.configuration.annotation.PolymorphicConfig;
import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.PolymorphicId;
import org.apache.ignite.internal.configuration.processor.ClassWrapper;
import org.apache.ignite.internal.configuration.processor.ConfigurationValidationException;

/**
 * Validator for the {@link PolymorphicConfigInstance} annotation.
 */
public class PolymorphicConfigInstanceValidator extends Validator {
    public PolymorphicConfigInstanceValidator(ProcessingEnvironment processingEnvironment) {
        super(processingEnvironment);
    }

    @Override
    public void validate(ClassWrapper classWrapper) {
        if (classWrapper.clazz().getAnnotation(PolymorphicConfigInstance.class) == null) {
            return;
        }

        assertHasCompatibleTopLevelAnnotation(classWrapper, PolymorphicConfigInstance.class);

        assertNotContainsFieldAnnotatedWith(classWrapper, PolymorphicId.class);

        String id = classWrapper.getAnnotation(PolymorphicConfigInstance.class).value();

        if (id == null || id.isBlank()) {
            throw new ConfigurationValidationException(classWrapper, String.format(
                    "%s value cannot be empty.",
                    simpleName(PolymorphicConfigInstance.class) + ".id()"
            ));
        }

        assertHasSuperClass(classWrapper);

        assertSuperclassHasAnnotations(classWrapper, PolymorphicConfig.class);

        assertNoFieldNameConflictsWithSuperClass(classWrapper);
    }
}
