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
import static org.apache.ignite.internal.util.CollectionUtils.concat;

import java.util.List;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.VariableElement;
import org.apache.ignite.configuration.annotation.InjectedValue;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.configuration.processor.ClassWrapper;
import org.apache.ignite.internal.configuration.processor.ConfigurationValidationException;

/**
 * Validator class for the {@link InjectedValue} annotation.
 */
public class InjectedValueValidator extends Validator {
    public InjectedValueValidator(ProcessingEnvironment processingEnv) {
        super(processingEnv);
    }

    /**
     * Validates invariants of the {@link InjectedValue} annotation. This includes:
     *
     * <ol>
     *     <li>Type of InjectedValue field is either a primitive, or a String, or a UUID;</li>
     *     <li>There is only a single InjectedValue field in the schema (including {@link Value} fields).</li>
     * </ol>
     */
    @Override
    public void validate(ClassWrapper classWrapper) {
        List<VariableElement> injectedValueFields = classWrapper.fieldsAnnotatedWith(InjectedValue.class);

        if (injectedValueFields.isEmpty()) {
            return;
        }

        List<VariableElement> valueFields = classWrapper.fieldsAnnotatedWith(Value.class);

        if (injectedValueFields.size() > 1 || !valueFields.isEmpty()) {
            throw new ConfigurationValidationException(classWrapper, String.format(
                    "Field marked as %s must be the only \"value\" field in the schema, found: %s",
                    simpleName(InjectedValue.class),
                    concat(injectedValueFields, valueFields)
            ));
        }

        VariableElement injectedValueField = injectedValueFields.get(0);

        assertValidValueFieldType(classWrapper, injectedValueField);
    }
}
