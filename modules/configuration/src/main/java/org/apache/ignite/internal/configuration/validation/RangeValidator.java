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

import org.apache.ignite.configuration.validation.Range;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;

/**
 * Implementing a validator for {@link Range}.
 */
public class RangeValidator implements Validator<Range, Number> {
    /** {@inheritDoc} */
    @Override
    public void validate(Range annotation, ValidationContext<Number> ctx) {
        long longValue = ctx.getNewValue().longValue();

        if (longValue < annotation.min()) {
            ctx.addIssue(new ValidationIssue(
                    ctx.currentKey(),
                    "Configuration value '" + ctx.currentKey() + "' must not be less than " + annotation.min()
            ));
        }

        if (longValue > annotation.max()) {
            ctx.addIssue(new ValidationIssue(
                    ctx.currentKey(),
                    "Configuration value '" + ctx.currentKey() + "' must not be greater than " + annotation.max()
            ));
        }
    }
}
