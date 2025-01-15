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

import javax.annotation.processing.ProcessingEnvironment;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigurationExtension;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.PolymorphicId;
import org.apache.ignite.internal.configuration.processor.ClassWrapper;

/**
 * Validator for the {@link ConfigurationExtension} annotation.
 */
public class ConfigurationExtentionValidator extends Validator {
    public ConfigurationExtentionValidator(ProcessingEnvironment processingEnvironment) {
        super(processingEnvironment);
    }

    @Override
    public void validate(ClassWrapper classWrapper) {
        if (classWrapper.getAnnotation(ConfigurationExtension.class) == null) {
            return;
        }

        assertHasCompatibleTopLevelAnnotation(classWrapper, ConfigurationExtension.class, ConfigurationRoot.class, Config.class);

        assertNotContainsFieldAnnotatedWith(classWrapper, PolymorphicId.class);

        if (classWrapper.getAnnotation(ConfigurationRoot.class) == null) {
            assertHasSuperClass(classWrapper);

            assertSuperclassHasAnnotations(classWrapper, ConfigurationRoot.class, Config.class);

            assertNoFieldNameConflictsWithSuperClass(classWrapper);
        } else {
            assertHasNoSuperClass(classWrapper);
        }
    }
}
