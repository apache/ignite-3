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

import org.apache.ignite.configuration.validation.PowerOfTwo;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;

/**
 * For {@link PowerOfTwoValidator} testing.
 */
public class PowerOfTwoValidatorTest extends BaseIgniteAbstractTest {
    @Test
    void testValidationSuccess() {
        PowerOfTwo powerOfTwo = mock(PowerOfTwo.class);

        PowerOfTwoValidator validator = new PowerOfTwoValidator();

        validate(validator, powerOfTwo, mockValidationContext(null, 1));
        validate(validator, powerOfTwo, mockValidationContext(null, 1L));
    }

    @Test
    void testValidationFail() {
        PowerOfTwo powerOfTwo = mock(PowerOfTwo.class);

        PowerOfTwoValidator validator = new PowerOfTwoValidator();

        String errorMessagePrefix = "Configuration value 'null' must not be power of two";

        validate(validator, powerOfTwo, mockValidationContext(null, 3), errorMessagePrefix);
        validate(validator, powerOfTwo, mockValidationContext(null, 3L), errorMessagePrefix);
    }
}
