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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.validation.ExceptKeys;
import org.apache.ignite.configuration.validation.OneOf;
import org.apache.ignite.configuration.validation.Range;
import org.apache.ignite.internal.configuration.compatibility.framework.annotations.DefaultAnnotationCompatibilityValidator;
import org.apache.ignite.internal.configuration.compatibility.framework.annotations.ExceptKeysAnnotationCompatibilityValidator;
import org.apache.ignite.internal.configuration.compatibility.framework.annotations.OneOfAnnotationCompatibilityValidator;
import org.apache.ignite.internal.configuration.compatibility.framework.annotations.RangeAnnotationCompatibilityValidator;

/**
 * Configuration annotation validator.
 */
final class ConfigAnnotationsValidator {

    private static final DefaultAnnotationCompatibilityValidator DEFAULT_VALIDATOR = new DefaultAnnotationCompatibilityValidator();

    private final Map<String, AnnotationCompatibilityValidator> validators;

    /** Constructor. */
    ConfigAnnotationsValidator() {
        this(Map.of(
                Range.class.getName(), new RangeAnnotationCompatibilityValidator(),
                ExceptKeys.class.getName(), new ExceptKeysAnnotationCompatibilityValidator(),
                OneOf.class.getName(), new OneOfAnnotationCompatibilityValidator()
        ));
    }

    /** Constructor. */
    ConfigAnnotationsValidator(Map<String, AnnotationCompatibilityValidator> validators) {
        this.validators = Map.copyOf(validators);
    }

    /**
     * Validates annotations.
     */
    void validate(ConfigNode candidate, ConfigNode node, List<String> errors) {
        List<ConfigAnnotation> currentAnnotations = node.annotations();
        List<ConfigAnnotation> candidateAnnotations = candidate.annotations();

        Map<String, ConfigAnnotation> currentMap = new HashMap<>();
        for (ConfigAnnotation annotation : currentAnnotations) {
            currentMap.put(annotation.name(), annotation);
        }

        Map<String, ConfigAnnotation> candidateMap = new HashMap<>();
        for (ConfigAnnotation annotation : candidateAnnotations) {
            candidateMap.put(annotation.name(), annotation);
        }

        Set<String> newAnnotations = new TreeSet<>();

        for (String currentKey : currentMap.keySet()) {
            if (!candidateMap.containsKey(currentKey)) {
                newAnnotations.add(currentKey);
            }
        }

        if (!newAnnotations.isEmpty()) {
            errors.add("Adding annotations is not allowed. New annotations: " + newAnnotations);
        }

        Set<String> removedAnnotations = new TreeSet<>();

        for (Map.Entry<String, ConfigAnnotation> entry : candidateMap.entrySet()) {
            ConfigAnnotation candidateAnnotation = entry.getValue();
            ConfigAnnotation currentAnnotation = currentMap.get(entry.getKey());

            if (currentAnnotation == null) {
                removedAnnotations.add(candidateAnnotation.name());
                continue;
            }

            validateStructure(candidateAnnotation, currentAnnotation, errors);
        }

        if (!removedAnnotations.isEmpty()) {
            errors.add("Removing annotations is not allowed. Removed annotations: " + removedAnnotations);
        }
    }

    private void validateStructure(ConfigAnnotation candidate, ConfigAnnotation current, List<String> errors) {
        assert candidate.name().equals(current.name()) : "Annotation name mismatch";

        Set<String> currentProperties = current.properties().keySet();
        Set<String> candidateProperties = candidate.properties().keySet();

        Set<String> removed = candidateProperties.stream()
                .filter(c -> !currentProperties.contains(c))
                .collect(Collectors.toSet());

        Set<String> added = currentProperties.stream()
                .filter(c -> !candidateProperties.contains(c))
                .collect(Collectors.toSet());

        if (!removed.isEmpty()) {
            errors.add(candidate.name() + " removed properties " + new TreeSet<>(removed));
        }

        if (!added.isEmpty()) {
            errors.add(candidate.name() + " added properties " + new TreeSet<>(added));
        }

        Set<String> changedTypes = new TreeSet<>();

        for (Map.Entry<String, ConfigAnnotationValue> entry : candidate.properties().entrySet()) {
            ConfigAnnotationValue currentValue = current.properties().get(entry.getKey());
            if (currentValue == null) {
                // Already an error, just skip it.
                continue;
            }

            ConfigAnnotationValue candidateValue = entry.getValue();

            if (!Objects.equals(candidateValue.typeName(), currentValue.typeName())) {
                changedTypes.add(entry.getKey());
            }
        }

        if (!changedTypes.isEmpty()) {
            errors.add(candidate.name() + " properties with changed types " + changedTypes);
        }

        validateSpecificAnnotation(candidate, current, errors);
    }

    private void validateSpecificAnnotation(ConfigAnnotation candidate, ConfigAnnotation current, List<String> errors) {
        AnnotationCompatibilityValidator validator = validators.getOrDefault(candidate.name(), DEFAULT_VALIDATOR);

        List<String> newErrors = new ArrayList<>();
        validator.validate(candidate, current, newErrors);

        errors.addAll(newErrors);

        // Report additional error iff the annotation has properties and there is no custom validator.
        if (!newErrors.isEmpty()) {
            boolean hasProperties = !candidate.properties().isEmpty() || !current.properties().isEmpty();
            if (hasProperties && validator == DEFAULT_VALIDATOR) {
                String error = format("Annotation requires a custom compatibility validator: {}. "
                                + "Consider using {} if every change should be treated as incompatible "
                                + "or implement an {} for this annotation", 
                        candidate.name(),
                        DefaultAnnotationCompatibilityValidator.class.getName(),
                        AnnotationCompatibilityValidator.class.getName()
                );

                errors.add(error);
            }
        }
    }
}
