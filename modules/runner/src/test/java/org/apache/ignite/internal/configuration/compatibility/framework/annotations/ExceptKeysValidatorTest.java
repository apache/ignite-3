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

package org.apache.ignite.internal.configuration.compatibility.framework.annotations;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.ignite.configuration.validation.ExceptKeys;
import org.apache.ignite.internal.configuration.compatibility.framework.AnnotationCompatibilityValidator;
import org.apache.ignite.internal.configuration.compatibility.framework.ConfigAnnotation;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for {@link ExceptKeysAnnotationCompatibilityValidator}.
 */
public class ExceptKeysValidatorTest extends BaseAnnotationCompatibilityValidatorSelfTest {

    @ParameterizedTest
    @MethodSource("exceptKeysRules")
    public void exceptKeysAnnotation(String f1, String f2, List<String> expectedErrors) {
        class SomeClass {
            @ExceptKeys({"a", "b", "c"})
            @SuppressWarnings("unused")
            public String base;

            @ExceptKeys("a")
            @SuppressWarnings("unused")
            public String removeKeys;

            @ExceptKeys({"a", "b", "c", "d", "e"})
            @SuppressWarnings("unused")
            public String addKeys;
        }

        ConfigAnnotation candidate = getAnnotation(SomeClass.class, f1, ExceptKeys.class.getName(), ExceptKeys.class);
        ConfigAnnotation current = getAnnotation(SomeClass.class, f2, ExceptKeys.class.getName(), ExceptKeys.class);

        AnnotationCompatibilityValidator validator = new ExceptKeysAnnotationCompatibilityValidator();

        List<String> errors = new ArrayList<>();
        validator.validate(candidate, current, errors);

        assertEquals(Set.copyOf(expectedErrors), Set.copyOf(errors));
    }

    private static Stream<Arguments> exceptKeysRules() {
        return Stream.of(
                Arguments.of("base", "base", List.of()),
                Arguments.of("base", "removeKeys", List.of()),
                Arguments.of("base", "addKeys", List.of("ExceptKeys: changed keys from [a, b, c] to [a, b, c, d, e]"))
        );
    }
}
