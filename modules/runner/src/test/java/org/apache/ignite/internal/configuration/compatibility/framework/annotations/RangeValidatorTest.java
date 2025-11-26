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
import org.apache.ignite.configuration.validation.Range;
import org.apache.ignite.internal.configuration.compatibility.framework.AnnotationCompatibilityValidator;
import org.apache.ignite.internal.configuration.compatibility.framework.ConfigAnnotation;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for {@link RangeAnnotationCompatibilityValidator}.
 */
public class RangeValidatorTest extends BaseAnnotationCompatibilityValidatorSelfTest {

    @ParameterizedTest
    @MethodSource("rangeAnnotationRules")
    public void rangeAnnotation(String f1, String f2, Set<String> expectedErrors) {
        class SomeClass {
            @Range(min = 10)
            @SuppressWarnings("unused")
            public int minBase;
            @Range(min = 5)
            @SuppressWarnings("unused")
            public int minDecreased;
            @Range(min = 12)
            @SuppressWarnings("unused")
            public int minIncreased;

            @Range(max = 20)
            @SuppressWarnings("unused")
            public int maxBase;
            @Range(max = 15)
            @SuppressWarnings("unused")
            public int maxDecreased;
            @Range(max = 22)
            @SuppressWarnings("unused")
            public int maxIncreased;

            @Range
            @SuppressWarnings("unused")
            public int base;
            @Range(max = Long.MAX_VALUE)
            @SuppressWarnings("unused")
            public int maxToDefault;
            @Range(min = Long.MIN_VALUE)
            @SuppressWarnings("unused")
            public int minToDefault;
        }

        ConfigAnnotation candidate = getAnnotation(SomeClass.class, f1, Range.class.getName(), Range.class);
        ConfigAnnotation current = getAnnotation(SomeClass.class, f2, Range.class.getName(), Range.class);

        AnnotationCompatibilityValidator validator = new RangeAnnotationCompatibilityValidator();

        List<String> errors = new ArrayList<>();
        validator.validate(candidate, current, errors);

        assertEquals(Set.copyOf(expectedErrors), Set.copyOf(errors));
    }

    private static Stream<Arguments> rangeAnnotationRules() {
        return Stream.of(
                Arguments.of("minBase", "minBase", Set.of()),
                Arguments.of("minBase", "minDecreased", Set.of()),
                Arguments.of("minBase", "minIncreased", Set.of("Range: min changed from 10 to 12")),

                Arguments.of("maxBase", "maxBase", Set.of()),
                Arguments.of("maxBase", "maxIncreased", Set.of()),
                Arguments.of("maxBase", "maxDecreased", Set.of("Range: max changed from 20 to 15")),

                Arguments.of("base", "maxToDefault", Set.of()),
                Arguments.of("maxToDefault", "base", Set.of()),

                Arguments.of("base", "minToDefault", Set.of()),
                Arguments.of("minToDefault", "base", Set.of())
        );
    }
}
