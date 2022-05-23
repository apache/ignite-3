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

package org.apache.ignite.internal.configuration.validation;

import static org.apache.ignite.internal.configuration.validation.TestValidationUtil.mockValidationContext;
import static org.apache.ignite.internal.configuration.validation.TestValidationUtil.validate;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.ignite.configuration.validation.Range;
import org.junit.jupiter.api.Test;

/**
 * For {@link RangeValidator} testing.
 */
public class RangeValidatorTest {
    @Test
    void testValidationSuccess() {
        Range range = createRange(0, 100);

        RangeValidator validator = new RangeValidator();

        validate(validator, range, mockValidationContext(null, 0), null);
        validate(validator, range, mockValidationContext(null, 50), null);
        validate(validator, range, mockValidationContext(null, 100), null);
    }

    @Test
    void testValidationFail() {
        Range range = createRange(0, 100);

        RangeValidator validator = new RangeValidator();

        String errorMessagePrefix = "Configuration value 'null' must not be power of two";

        validate(
                validator,
                range,
                mockValidationContext(null, -1),
                "Configuration value 'null' must not be less than"
        );

        validate(
                validator,
                range,
                mockValidationContext(null, 101),
                "Configuration value 'null' must not be greater than"
        );
    }

    private Range createRange(long min, long max) {
        Range range = mock(Range.class);

        when(range.min()).thenReturn(min);

        when(range.max()).thenReturn(max);

        return range;
    }
}
