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
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import org.apache.ignite.internal.configuration.compatibility.framework.AnnotationCompatibilityValidator;
import org.apache.ignite.internal.configuration.compatibility.framework.ConfigAnnotation;
import org.apache.ignite.internal.configuration.compatibility.framework.ConfigAnnotationValue;

/**
 * Default validator. All changes made to an annotation are incompatible.
 */
public class DefaultAnnotationCompatibilityValidator implements AnnotationCompatibilityValidator {
    @Override
    public void validate(ConfigAnnotation candidate, ConfigAnnotation current, List<String> errors) {
        Map<String, ConfigAnnotationValue> candidateValues = candidate.properties();
        Map<String, ConfigAnnotationValue> currentValues = current.properties();

        Set<String> changedValues = new TreeSet<>();

        for (Map.Entry<String, ConfigAnnotationValue> e : candidateValues.entrySet()) {
            ConfigAnnotationValue value = currentValues.get(e.getKey());
            if (value == null) {
                // Handled by base validator.
                continue;
            }

            if (!Objects.equals(e.getValue(), value)) {
                changedValues.add(e.getKey());
            }
        }

        if (!changedValues.isEmpty()) {
            errors.add(candidate.name() + " changed values " + changedValues);
        }
    }
}
