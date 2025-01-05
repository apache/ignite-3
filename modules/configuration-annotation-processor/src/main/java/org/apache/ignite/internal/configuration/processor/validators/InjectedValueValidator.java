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

package org.apache.ignite.internal.configuration.processor.validators;

import static org.apache.ignite.internal.configuration.processor.ConfigurationProcessorUtils.collectFieldsWithAnnotation;
import static org.apache.ignite.internal.configuration.processor.ConfigurationProcessorUtils.simpleName;
import static org.apache.ignite.internal.util.CollectionUtils.concat;

import java.util.List;
import java.util.UUID;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.ArrayType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import org.apache.ignite.configuration.annotation.InjectedValue;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.configuration.processor.ConfigurationProcessorException;

/**
 * Validator class for the {@link InjectedValue} annotation.
 */
public class InjectedValueValidator {
    private final ProcessingEnvironment processingEnv;

    public InjectedValueValidator(ProcessingEnvironment processingEnv) {
        this.processingEnv = processingEnv;
    }

    /**
     * Validates invariants of the {@link InjectedValue} annotation. This includes:
     *
     * <ol>
     *     <li>Type of InjectedValue field is either a primitive, or a String, or a UUID;</li>
     *     <li>There is only a single InjectedValue field in the schema (including {@link Value} fields).</li>
     * </ol>
     */
    public void validate(TypeElement clazz, List<VariableElement> fields) {
        List<VariableElement> injectedValueFields = collectFieldsWithAnnotation(fields, InjectedValue.class);

        if (injectedValueFields.isEmpty()) {
            return;
        }

        List<VariableElement> valueFields = collectFieldsWithAnnotation(fields, Value.class);

        if (injectedValueFields.size() > 1 || !valueFields.isEmpty()) {
            throw new ConfigurationProcessorException(String.format(
                    "Field marked as %s must be the only \"value\" field in the schema %s, found: %s",
                    simpleName(InjectedValue.class),
                    clazz.getQualifiedName(),
                    concat(injectedValueFields, valueFields)
            ));
        }

        VariableElement injectedValueField = injectedValueFields.get(0);

        // Must be a primitive or an array of the primitives (including java.lang.String, java.util.UUID).
        if (!isValidValueAnnotationFieldType(injectedValueField.asType())) {
            throw new ConfigurationProcessorException(String.format(
                    "%s.%s field must have one of the following types: "
                            + "boolean, int, long, double, String, UUID or an array of aforementioned type.",
                    clazz.getQualifiedName(),
                    injectedValueField.getSimpleName()
            ));
        }
    }

    private boolean isValidValueAnnotationFieldType(TypeMirror type) {
        if (type.getKind() == TypeKind.ARRAY) {
            type = ((ArrayType) type).getComponentType();
        }

        return type.getKind().isPrimitive() || isClass(type, String.class) || isClass(type, UUID.class);

    }

    private boolean isClass(TypeMirror type, Class<?> clazz) {
        TypeMirror classType = processingEnv
                .getElementUtils()
                .getTypeElement(clazz.getCanonicalName())
                .asType();

        return classType.equals(type);
    }
}
