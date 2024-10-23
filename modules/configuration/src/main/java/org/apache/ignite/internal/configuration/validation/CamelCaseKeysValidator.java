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

import java.util.regex.Pattern;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.validation.CamelCaseKeys;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;

/** {@link Validator} implementation for the {@link CamelCaseKeys} annotation. */
public class CamelCaseKeysValidator implements Validator<CamelCaseKeys, NamedListView<?>> {
    private static final Pattern CAMEL_CASE_PATTERN = Pattern.compile("^[a-z]+([A-Z][a-z]+)*$");

    @Override
    public void validate(CamelCaseKeys annotation, ValidationContext<NamedListView<?>> ctx) {
        for (String namedListKey : ctx.getNewValue().namedListKeys()) {
            if (!CAMEL_CASE_PATTERN.matcher(namedListKey).matches()) {
                String message = String.format(
                        "'%s' configuration key must be in lower camel case '%s', for example 'v', 'value' and 'valueOneAndTwo'",
                        ctx.currentKey(), namedListKey
                );

                ctx.addIssue(new ValidationIssue(ctx.currentKey(), message));
            }
        }
    }
}
