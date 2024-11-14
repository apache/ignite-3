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

import java.util.Set;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;
import org.apache.ignite.internal.configuration.SystemPropertyView;

/** Validator for system property values that are expected to be non-negative {@code int} number. */
public class NonNegativeIntegerNumberSystemPropertyValueValidator implements
        Validator<NamedConfigValue, NamedListView<SystemPropertyView>> {
    /** Returns the names of the properties that need to be checked. */
    private final Set<String> propertyNames;

    /**
     * Constructor.
     *
     * @param propertyNames Names of system properties to check.
     */
    public NonNegativeIntegerNumberSystemPropertyValueValidator(String... propertyNames) {
        this.propertyNames = Set.of(propertyNames);
    }

    @Override
    public void validate(NamedConfigValue annotation, ValidationContext<NamedListView<SystemPropertyView>> ctx) {
        for (String propertyName : propertyNames) {
            SystemPropertyView systemPropertyView = ctx.getNewValue().get(propertyName);

            if (systemPropertyView != null && !isNonNegativeIntegerValue(systemPropertyView.propertyValue())) {
                String message = String.format(
                        "'%s' system property value must be a non-negative long number '%s'",
                        ctx.currentKey(), propertyName
                );

                ctx.addIssue(new ValidationIssue(ctx.currentKey(), message));
            }
        }
    }

    private static boolean isNonNegativeIntegerValue(String propertyValue) {
        try {
            return Integer.parseInt(propertyValue) >= 0;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
