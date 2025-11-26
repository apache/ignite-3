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
import org.apache.ignite.configuration.validation.Range;
import org.apache.ignite.internal.configuration.compatibility.framework.AnnotationCompatibilityValidator;
import org.apache.ignite.internal.configuration.compatibility.framework.ConfigAnnotation;

/**
 * Validator for the {@link Range} annotation.
 */
public class RangeAnnotationCompatibilityValidator implements AnnotationCompatibilityValidator {
    /** {@inheritDoc} */
    @Override
    public void validate(ConfigAnnotation candidate, ConfigAnnotation current, List<String> errors) {
        long candidateMin = AnnotationCompatibilityValidator.getValue(candidate, "min", (v) -> (Long) v.value());
        long candidateMax = AnnotationCompatibilityValidator.getValue(candidate, "max", (v) -> (Long) v.value());

        long currentMin = AnnotationCompatibilityValidator.getValue(current, "min", (v) -> (Long) v.value());
        long currentMax = AnnotationCompatibilityValidator.getValue(current, "max", (v) -> (Long) v.value());

        if (currentMin > candidateMin) {
            errors.add("Range: min changed from " + candidateMin + " to " + currentMin);
        }

        if (currentMax < candidateMax) {
            errors.add("Range: max changed from " + candidateMax + " to " + currentMax);
        }
    }
}
