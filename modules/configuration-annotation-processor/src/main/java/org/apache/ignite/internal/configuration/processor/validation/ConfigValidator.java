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
import static org.apache.ignite.internal.configuration.processor.ConfigurationProcessorUtils.simpleName;

import java.lang.annotation.Annotation;
import javax.annotation.processing.ProcessingEnvironment;
import org.apache.ignite.configuration.annotation.AbstractConfiguration;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigurationExtension;
import org.apache.ignite.configuration.annotation.InjectedName;
import org.apache.ignite.configuration.annotation.InternalId;
import org.apache.ignite.configuration.annotation.PolymorphicId;
import org.apache.ignite.internal.configuration.processor.ClassWrapper;
import org.apache.ignite.internal.configuration.processor.ConfigurationValidationException;

/**
 * Validator for the {@link Config} annotation.
 */
public class ConfigValidator extends Validator {
    public ConfigValidator(ProcessingEnvironment processingEnvironment) {
        super(processingEnvironment);
    }

    @Override
    public void validate(ClassWrapper classWrapper) {
        if (classWrapper.getAnnotation(Config.class) == null) {
            return;
        }

        assertHasCompatibleTopLevelAnnotation(classWrapper, Config.class, ConfigurationExtension.class);

        assertNotContainsFieldAnnotatedWith(classWrapper, PolymorphicId.class);

        ClassWrapper superClass = classWrapper.superClass();

        if (superClass == null) {
            return;
        }

        assertSuperclassHasAnnotations(classWrapper, AbstractConfiguration.class);

        assertNoFieldNameConflictsWithSuperClass(classWrapper);

        checkDuplicateAnnotationsWithSuperClass(classWrapper, InjectedName.class);
        checkDuplicateAnnotationsWithSuperClass(classWrapper, InternalId.class);
    }

    private static void checkDuplicateAnnotationsWithSuperClass(ClassWrapper classWrapper, Class<? extends Annotation> annotationClass) {
        if (containsAnyAnnotation(classWrapper.fields(), annotationClass)
                && containsAnyAnnotation(classWrapper.requiredSuperClass().fields(), annotationClass)) {
            throw new ConfigurationValidationException(classWrapper, String.format(
                    "Field with %s is already present in the superclass %s.",
                    simpleName(annotationClass),
                    classWrapper.requiredSuperClass()
            ));
        }
    }
}
