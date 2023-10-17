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

package org.apache.ignite.internal.configuration.validation;

import org.apache.ignite.configuration.validation.NotBlank;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;
import org.apache.ignite.internal.util.StringUtils;

/**
 * {@link Validator} implementation for the {@link NotBlank} annotation.
 */
public class NotBlankValidator implements Validator<NotBlank, String> {
    /** {@inheritDoc} */
    @Override
    public void validate(NotBlank annotation, ValidationContext<String> ctx) {
        String value = ctx.getNewValue();

        if (StringUtils.nullOrBlank(value)) {
            String message = "'" + ctx.currentKey() + "' configuration value must not be blank.";

            ctx.addIssue(new ValidationIssue(ctx.currentKey(), message));
        }
    }
}
