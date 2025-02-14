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

package org.apache.ignite.internal.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.apache.ignite.lang.util.IgniteNameUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test cases to verify {@link IgniteNameUtils}.
 */
public class IgniteNameUtilsTest {
    @ParameterizedTest
    @CsvSource({
            "foo, FOO", "fOo, FOO", "FOO, FOO", "\"FOO\", FOO",
            "\"foo\", foo", "\"fOo\", fOo", "\"f.f\", f.f", "\"f\"\"f\", f\"f",
    })
    public void validSimpleNames(String source, String expected) {
        assertThat(IgniteNameUtils.parseSimpleName(source), equalTo(expected));
    }

    @Test
    public void parseSequenceOfSpaces() {
        assertThat(IgniteNameUtils.parseSimpleName("\"   \""), equalTo("   "));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "f.f", "f f", "f\"f", "f\"\"f", "\"foo", "\"fo\"o\"", "1o0", "@#$"
    })
    public void malformedSimpleNames(String source) {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> IgniteNameUtils.parseSimpleName(source));

        assertThat(ex.getMessage(), is(anyOf(
                equalTo("Fully qualified name is not expected [name=" + source + "]"),
                containsString("Malformed identifier [identifier=" + source))));
    }

    @ParameterizedTest
    @CsvSource({
            "foo, \"foo\"", "fOo, \"fOo\"", "FOO, FOO", "1o0, \"1o0\"",
            "@#$, \"@#$\"", "f16, \"f16\"", "F16, F16", "\"foo\", \"\"\"foo\"\"\"",
            "\"fOo\", \"\"\"fOo\"\"\"", "\"f.f\", \"\"\"f.f\"\"\"",
            "foo\"bar\", \"foo\"\"bar\"\"\"", "foo\"bar, \"foo\"\"bar\"",
    })
    public void quoteIfNeeded(String source, String expected) {
        assertThat(IgniteNameUtils.quoteIfNeeded(source), equalTo(expected));
    }

    @ParameterizedTest
    @MethodSource("parseNameData")
    public void parseName(String source, List<String> expected) {
        assertThat(IgniteNameUtils.parseName(source), equalTo(expected));
    }

    @Test
    public void parseNameNullArgument() {
        NullPointerException ex = assertThrows(NullPointerException.class, () -> IgniteNameUtils.parseName(null));
        assertThat(ex.getMessage(), equalTo("name"));
    }

    @ParameterizedTest
    @MethodSource("parseNameIllegalArguments")
    public void parseNameIllegalArgument(String name, String expectedError) {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> IgniteNameUtils.parseName(name));
        assertThat(ex.getMessage(), equalTo(expectedError));
    }

    private static List<Arguments> parseNameData() {
        return List.of(
                Arguments.of("foo", List.of("FOO")),
                Arguments.of("foo.", List.of("FOO", "")),
                Arguments.of(
                        "\"  \".foo.Bar.BAZ.\"qUx\".\"qu\"\"ux\"",
                        List.of("  ", "FOO", "BAR", "BAZ", "qUx", "qu\"ux")
                )
        );
    }

    private static List<Arguments> parseNameIllegalArguments() {
        return List.of(
                Arguments.of("", "Argument \"name\" can't be empty."),
                Arguments.of(" ", "Malformed identifier [identifier= , pos=0]"),
                Arguments.of(".", "Malformed identifier [identifier=., pos=0]"),
                Arguments.of("foo..", "Malformed identifier [identifier=foo.., pos=4]")
        );
    }
}
