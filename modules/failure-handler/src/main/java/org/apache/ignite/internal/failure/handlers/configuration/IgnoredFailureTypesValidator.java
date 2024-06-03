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

package org.apache.ignite.internal.failure.handlers.configuration;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;
import org.apache.ignite.internal.failure.FailureType;

/**
 * Validates IgnoredFailureTypes configuration.
 */
public class IgnoredFailureTypesValidator implements Validator<IgnoredFailureTypes, String[]> {
    public static final IgnoredFailureTypesValidator INSTANCE = new IgnoredFailureTypesValidator();

    @Override
    public void validate(IgnoredFailureTypes annotation, ValidationContext<String[]> ctx) {
        Set<String> possibleFailureTypes = Arrays.stream(FailureType.values())
                .map(Enum::name)
                .collect(Collectors.toSet());

        String[] ignoredFailureTypes = ctx.getNewValue();

        for (String ignoredFailureType : ignoredFailureTypes) {
            if (!possibleFailureTypes.contains(ignoredFailureType)) {
                ctx.addIssue(new ValidationIssue(
                        ctx.currentKey(),
                        String.format(
                                "Unknown failure type '%s'. The possible values are %s.",
                                ignoredFailureType,
                                possibleFailureTypes
                        )));
            }
        }
    }
}
