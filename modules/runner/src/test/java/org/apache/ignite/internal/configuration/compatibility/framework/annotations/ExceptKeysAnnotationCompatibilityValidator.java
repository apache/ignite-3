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

package org.apache.ignite.internal.configuration.compatibility.framework.annotations;

import java.util.List;
import org.apache.ignite.configuration.validation.ExceptKeys;
import org.apache.ignite.internal.configuration.compatibility.framework.AnnotationCompatibilityValidator;
import org.apache.ignite.internal.configuration.compatibility.framework.ConfigAnnotation;

/**
 * Validator for the {@link ExceptKeys} annotation.
 */
public class ExceptKeysAnnotationCompatibilityValidator implements AnnotationCompatibilityValidator {
    /** {@inheritDoc} */
    @Override
    public void validate(ConfigAnnotation candidate, ConfigAnnotation current, List<String> errors) {
        List<String> candidateKeys = AnnotationCompatibilityValidator.getValue(candidate, "value", (v) -> (List<String>) v.value());
        List<String> currentKeys = AnnotationCompatibilityValidator.getValue(current, "value", (v) -> (List<String>) v.value());

        if (!candidateKeys.containsAll(currentKeys)) {
            errors.add("ExceptKeys: changed keys from " + candidateKeys + " to " + currentKeys);
        }
    }
}
