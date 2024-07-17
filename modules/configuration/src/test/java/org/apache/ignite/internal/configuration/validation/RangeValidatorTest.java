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

import static org.apache.ignite.internal.configuration.validation.TestValidationUtil.mockValidationContext;
import static org.apache.ignite.internal.configuration.validation.TestValidationUtil.validate;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.ignite.configuration.validation.Range;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/**
 * For {@link RangeValidator} testing.
 */
public class RangeValidatorTest extends BaseIgniteAbstractTest {
    @Test
    void testValidationSuccess() {
        Range range0 = createRange(0L, 100L);

        RangeValidator validator = new RangeValidator();

        validate(validator, range0, mockValidationContext(null, 0));
        validate(validator, range0, mockValidationContext(null, 50));
        validate(validator, range0, mockValidationContext(null, 100));

        Range range1 = createRange(0L, null);

        validate(validator, range1, mockValidationContext(null, 0));
        validate(validator, range1, mockValidationContext(null, 50));
        validate(validator, range1, mockValidationContext(null, 100));
        validate(validator, range1, mockValidationContext(null, Long.MAX_VALUE));

        Range range2 = createRange(null, 100L);

        validate(validator, range2, mockValidationContext(null, 0));
        validate(validator, range2, mockValidationContext(null, 50));
        validate(validator, range2, mockValidationContext(null, 100));
        validate(validator, range2, mockValidationContext(null, Long.MIN_VALUE));
    }

    @Test
    void testValidationFail() {
        RangeValidator validator = new RangeValidator();

        String lessThanErrorPrefix = "Configuration value 'null' must not be less than";
        String greaterThanErrorPrefix = "Configuration value 'null' must not be greater than";

        Range range0 = createRange(0L, 100L);

        validate(validator, range0, mockValidationContext(null, -1), lessThanErrorPrefix);
        validate(validator, range0, mockValidationContext(null, 101), greaterThanErrorPrefix);

        Range range1 = createRange(0L, null);

        validate(validator, range1, mockValidationContext(null, -1), lessThanErrorPrefix);
        validate(validator, range1, mockValidationContext(null, Long.MIN_VALUE), lessThanErrorPrefix);

        Range range2 = createRange(null, 100L);

        validate(validator, range2, mockValidationContext(null, 101), greaterThanErrorPrefix);
        validate(validator, range2, mockValidationContext(null, Long.MAX_VALUE), greaterThanErrorPrefix);
    }

    private Range createRange(@Nullable Long min, @Nullable Long max) {
        Range range = mock(Range.class);

        when(range.min()).then(answer -> min == null ? answer.getMethod().getDefaultValue() : min);

        when(range.max()).then(answer -> max == null ? answer.getMethod().getDefaultValue() : max);

        return range;
    }
}
