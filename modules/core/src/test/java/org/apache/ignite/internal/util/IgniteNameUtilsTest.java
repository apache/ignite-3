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
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test cases to verify {@link IgniteNameUtils}.
 */
public class IgniteNameUtilsTest {
    @ParameterizedTest
    @MethodSource("validUnqoutedIdentifiers")
    @MethodSource("validQoutedIdentifiers")
    public void validIdentifiers(String source, String expected) {
        String parsed = IgniteNameUtils.parseIdentifier(source);

        assertThat(parsed, equalTo(expected));

        assertThat(IgniteNameUtils.parseIdentifier(IgniteNameUtils.quoteIfNeeded(parsed)), equalTo(parsed));
    }

    @ParameterizedTest
    @MethodSource("validUnqoutedIdentifiers")
    public void validNormalizedIdentifiers(String source) {
        assertThat(IgniteNameUtils.isValidNormalizedIdentifier(source), is(true));
    }

    private static Arguments[] validUnqoutedIdentifiers() {
        return new Arguments[] {
                Arguments.of("foo", "FOO"),
                Arguments.of("fOo", "FOO"),
                Arguments.of("FOO", "FOO"),
                Arguments.of("fo_o", "FO_O"),
                Arguments.of("_foo", "_FOO"),
        };
    }

    private static Arguments[] validQoutedIdentifiers() {
        return new Arguments[] {
                Arguments.of("\"FOO\"", "FOO"),
                Arguments.of("\"foo\"", "foo"),
                Arguments.of("\"fOo\"", "fOo"),
                Arguments.of("\"$fOo\"", "$fOo"),
                Arguments.of("\"f.f\"", "f.f"),
                Arguments.of("\"f\"\"f\"", "f\"f"),
                Arguments.of("\" \"", " "),
                Arguments.of("\"   \"", "   "),
                Arguments.of("\",\"", ","),
                Arguments.of("\"😅\"", "😅"),
                Arguments.of("\"f😅\"", "f😅")
        };
    }

    @ParameterizedTest
    @ValueSource(strings = {
            " ", "foo-1", "f.f", "f f", "f\"f", "f\"\"f", "\"foo", "\"fo\"o\"", "1o0", "@#$", "😅", "f😅", "$foo", "foo$"
    })
    public void malformedIdentifiers(String source) {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> IgniteNameUtils.parseIdentifier(source));

        assertThat(ex.getMessage(), is(anyOf(
                equalTo("Fully qualified name is not expected [name=" + source + "]"),
                containsString("Malformed identifier [identifier=" + source))));

        assertThat(IgniteNameUtils.isValidNormalizedIdentifier(source), is(false));
    }

    @ParameterizedTest
    @MethodSource("quoteIfNeededData")
    public void quoteIfNeeded(String source, String expected) {
        String quoted = IgniteNameUtils.quoteIfNeeded(source);

        assertThat(IgniteNameUtils.quoteIfNeeded(source), equalTo(expected));
        assertThat(IgniteNameUtils.quoteIfNeeded(IgniteNameUtils.parseIdentifier(quoted)), equalTo(expected));
    }

    private static Arguments[] quoteIfNeededData() {
        return new Arguments[]{
                Arguments.of("foo", "\"foo\""),
                Arguments.of("fOo", "\"fOo\""),
                Arguments.of("FOO", "FOO"),
                Arguments.of("1o0", "\"1o0\""),
                Arguments.of("@#$", "\"@#$\""),
                Arguments.of("f16", "\"f16\""),
                Arguments.of("F16", "F16"),
                Arguments.of("Ff16", "\"Ff16\""),
                Arguments.of("FF16", "FF16"),
                Arguments.of("_FF16", "_FF16"),
                Arguments.of("FF_16", "FF_16"),
                Arguments.of(" ", "\" \""),
                Arguments.of(" F", "\" F\""),
                Arguments.of(" ,", "\" ,\""),
                Arguments.of("😅", "\"😅\""),
                Arguments.of("\"foo\"", "\"\"\"foo\"\"\""),
                Arguments.of("\"fOo\"", "\"\"\"fOo\"\"\""),
                Arguments.of("\"f.f\"", "\"\"\"f.f\"\"\""),
                Arguments.of("foo\"bar\"", "\"foo\"\"bar\"\"\""),
                Arguments.of("foo\"bar", "\"foo\"\"bar\"")
        };
    }

    @ParameterizedTest
    @MethodSource("parseNameData")
    public void parseName(String source, List<String> expected) {
        assertThat(IgniteNameUtils.parseName(source), equalTo(expected));
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

    private static List<Arguments> parseNameIllegalArguments() {
        return List.of(
                Arguments.of("", "Argument \"name\" can't be empty."),
                Arguments.of(" ", "Malformed identifier [identifier= , pos=0]"),
                Arguments.of(".", "Malformed identifier [identifier=., pos=0]"),
                Arguments.of("foo..", "Malformed identifier [identifier=foo.., pos=4]")
        );
    }
}
