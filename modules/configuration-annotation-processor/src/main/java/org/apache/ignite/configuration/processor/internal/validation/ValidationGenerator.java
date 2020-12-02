/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.configuration.processor.internal.validation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.MirroredTypesException;
import javax.lang.model.type.TypeMirror;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import com.squareup.javapoet.CodeBlock;
import org.apache.ignite.configuration.internal.DynamicConfiguration;
import org.apache.ignite.configuration.internal.annotation.Validate;
import org.apache.ignite.configuration.internal.validation.MaxValidator;
import org.apache.ignite.configuration.internal.validation.MinValidator;
import org.apache.ignite.configuration.internal.validation.NotNullValidator;

/**
 * Class that handles validation generation.
 */
public class ValidationGenerator {
    /** Private constructor. */
    private ValidationGenerator() {
    }

    /**
     * Generate validation block.
     *
     * @param variableElement Configuration field.
     * @return Code block for field validation.
     */
    public static CodeBlock generateValidators(VariableElement variableElement) {
        List<CodeBlock> validators = new ArrayList<>();
        final Min minAnnotation = variableElement.getAnnotation(Min.class);
        if (minAnnotation != null) {
            final long minValue = minAnnotation.value();
            final String message = minAnnotation.message();
            final CodeBlock build = CodeBlock.builder().add("new $T<$T<?, ?, ?>>($L, $S)", MinValidator.class, DynamicConfiguration.class, minValue, message).build();
            validators.add(build);
        }

        final Max maxAnnotation = variableElement.getAnnotation(Max.class);
        if (maxAnnotation != null) {
            final long maxValue = maxAnnotation.value();
            final String message = maxAnnotation.message();
            final CodeBlock build = CodeBlock.builder().add("new $T<$T<?, ?, ?>>($L, $S)", MaxValidator.class, DynamicConfiguration.class, maxValue, message).build();
            validators.add(build);
        }

        final NotNull notNull = variableElement.getAnnotation(NotNull.class);
        if (notNull != null) {
            final String message = notNull.message();
            final CodeBlock build = CodeBlock.builder().add("new $T<$T<?, ?, ?>>($S)", NotNullValidator.class, DynamicConfiguration.class, message).build();
            validators.add(build);
        }

        List<Validate> validateAnnotations = new ArrayList<>();

        final Validate.List validateAnnotationsList = variableElement.getAnnotation(Validate.List.class);

        if (validateAnnotationsList != null) {
            validateAnnotations.addAll(Arrays.asList(validateAnnotationsList.value()));
        }

        final Validate validateAnnotationSingle = variableElement.getAnnotation(Validate.class);

        if (validateAnnotationSingle != null) {
            validateAnnotations.add(validateAnnotationSingle);
        }

        validateAnnotations.forEach(validateAnnotation -> {
            List<? extends TypeMirror> values = null;
            try {
                validateAnnotation.value();
            } catch (MirroredTypesException e) {
                values = e.getTypeMirrors();
            }
            values.forEach(value -> {
                final String message = validateAnnotation.message();
                final CodeBlock build = CodeBlock.builder().add("new $T($S)", value, message).build();
                validators.add(build);
            });
        });

        String text = validators.stream().map(v -> "$L").collect(Collectors.joining(","));

        final CodeBlock validatorsArguments = CodeBlock.builder().add(text, validators.toArray()).build();

        return CodeBlock.builder().add("$T.asList($L)", Arrays.class, validatorsArguments).build();
    }

}
