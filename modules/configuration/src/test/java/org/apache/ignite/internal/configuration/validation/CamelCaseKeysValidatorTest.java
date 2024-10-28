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

import static org.apache.ignite.internal.configuration.validation.TestValidationUtil.mockNamedListView;
import static org.apache.ignite.internal.configuration.validation.TestValidationUtil.mockValidationContext;
import static org.apache.ignite.internal.configuration.validation.TestValidationUtil.validate;
import static org.mockito.Mockito.mock;

import java.util.List;
import org.apache.ignite.configuration.validation.CamelCaseKeys;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** For {@link CamelCaseKeysValidator} testing. */
public class CamelCaseKeysValidatorTest extends BaseIgniteAbstractTest {
    private final CamelCaseKeysValidator validator = new CamelCaseKeysValidator();

    @ParameterizedTest
    @ValueSource(strings = {
            "0", "A", " ", "_", "-", "?", ",", ".",
            "Value", "valueONe",
            "0Value", "007", "0value", "0Value",
            "value_one", "value-one",
            "valueOneAnd2", "value0", "value1AndTwo",
            "valuE"
    })
    void testWrongKey(String key) {
        validate(
                validator,
                mock(CamelCaseKeys.class),
                mockValidationContext(null, mockNamedListView(List.of(key))),
                String.format(
                        "'%s' configuration key must be in lower camel case '%s', for example 'v', 'value' and 'valueOneAndTwo'",
                        null, key
                )
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"v", "value", "valueOne", "valueOneAndTwo"})
    void testCorrectKey(String key) {
        validate(validator, mock(CamelCaseKeys.class), mockValidationContext(null, mockNamedListView(List.of(key))));
    }
}
