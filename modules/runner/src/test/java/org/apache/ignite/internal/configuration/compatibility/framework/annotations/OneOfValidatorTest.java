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
import java.util.stream.Stream;
import org.apache.ignite.configuration.validation.OneOf;
import org.apache.ignite.internal.configuration.compatibility.framework.AnnotationCompatibilityValidator;
import org.apache.ignite.internal.configuration.compatibility.framework.ConfigAnnotation;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for {@link OneOfAnnotationCompatibilityValidator}.
 */
public class OneOfValidatorTest extends BaseAnnotationCompatibilityValidatorSelfTest {

    @ParameterizedTest
    @MethodSource("oneOfRules")
    public void oneOfAnnotation(String f1, String f2, List<String> expectedErrors) {
        class SomeClass {
            @OneOf({"a", "b", "c"})
            @SuppressWarnings("unused")
            public String base;

            @OneOf("a")
            @SuppressWarnings("unused")
            public String removeKeys;

            @OneOf({"a", "b", "c", "d", "e"})
            @SuppressWarnings("unused")
            public String addKeys;

            @OneOf(value = {"a", "b", "c"}, caseSensitive = true)
            @SuppressWarnings("unused")
            public String caseSensitive;

            @OneOf({"a", "b", "c"})
            @SuppressWarnings("unused")
            public String caseInsensitive;

            @OneOf({"a", "b"})
            @SuppressWarnings("unused")
            public String caseInsensitiveRemoveKeys;
        }

        ConfigAnnotation candidate = getAnnotation(SomeClass.class, f1, OneOf.class.getName(), OneOf.class);
        ConfigAnnotation current = getAnnotation(SomeClass.class, f2, OneOf.class.getName(), OneOf.class);

        AnnotationCompatibilityValidator validator = new OneOfAnnotationCompatibilityValidator();

        List<String> errors = new ArrayList<>();
        validator.validate(candidate, current, errors);

        assertEquals(expectedErrors, errors);
    }

    private static Stream<Arguments> oneOfRules() {
        return Stream.of(
                Arguments.of("base", "base", List.of()),
                Arguments.of("base", "removeKeys", List.of("OneOf: changed keys from [a, b, c] to [a]")),
                Arguments.of("base", "addKeys", List.of()),
                Arguments.of("caseInsensitive", "caseSensitive", List.of()),
                Arguments.of("caseSensitive", "caseInsensitive", 
                        List.of(
                                "OneOf: case-insensitive validation can' become case-sensitive, which is more restrictive"
                        )
                ),
                Arguments.of("caseSensitive", "caseInsensitiveRemoveKeys",
                        List.of(
                                "OneOf: changed keys from [a, b, c] to [a, b]",
                                "OneOf: case-insensitive validation can' become case-sensitive, which is more restrictive"
                        )
                )
        );
    }
}
