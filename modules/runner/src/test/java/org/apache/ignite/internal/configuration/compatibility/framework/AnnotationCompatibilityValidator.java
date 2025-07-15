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

package org.apache.ignite.internal.configuration.compatibility.framework;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.util.List;
import java.util.function.Function;

/**
 * Annotation compatibility validator.
 */
@FunctionalInterface
public interface AnnotationCompatibilityValidator {
    /** Validates compatibility between annotation revisions. */
    void validate(ConfigAnnotation candidate, ConfigAnnotation current, List<String> errors);

    /** 
     * Reads a value of the given annotation property. Note that all int values are stored as {@code long} values.
     */
    static <T> T getValue(ConfigAnnotation annotation, String name, Function<ConfigAnnotationValue, T> parse) {
        ConfigAnnotationValue value = annotation.properties().get(name);
        try {
            return parse.apply(value);
        } catch (Exception e) {
            String errorMessage = format("Unable to read annotation property. Property: {}, annotation: {}", name, annotation.name());
            throw new IllegalStateException(errorMessage, e);
        }
    }
}
