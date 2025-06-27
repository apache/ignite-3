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

package org.apache.ignite.table;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.ignite.lang.util.IgniteNameUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test cases to verify {@link QualifiedName}.
 */
public class QualifiedNameTest {
    private static Arguments[] validSimpleNamesArgs() {
        return new Arguments[]{
                // Uppercased
                Arguments.of("foo", "FOO"),
                Arguments.of("fOo", "FOO"),
                Arguments.of("FOO", "FOO"),
                Arguments.of("f23", "F23"),
                Arguments.of("\"23f\"", "23f"),
                Arguments.of("foo_", "FOO_"),
                Arguments.of("foo_1", "FOO_1"),
                Arguments.of("_foo", "_FOO"),
                Arguments.of("__foo", "__FOO"),

                // Quoted
                Arguments.of("\"FOO\"", "FOO"),
                Arguments.of("\"foo\"", "foo"),
                Arguments.of("\"fOo\"", "fOo"),
                Arguments.of("\"_foo\"", "_foo"),
                Arguments.of("\"$foo\"", "$foo"),
                Arguments.of("\"%foo\"", "%foo"),
                Arguments.of("\"foo_\"", "foo_"),
                Arguments.of("\"foo$\"", "foo$"),
                Arguments.of("\"foo%\"", "foo%"),
                Arguments.of("\"@#$\"", "@#$"),
                Arguments.of("\"f.f\"", "f.f"),
                Arguments.of("\"   \"", "   "),
                Arguments.of("\"😅\"", "😅"),

                // Escaped
                Arguments.of("\"f\"\"f\"", "f\"f"),
                Arguments.of("\"f\"\"\"\"f\"", "f\"\"f"),
                Arguments.of("\"\"\"bar\"\"\"", "\"bar\""),
                Arguments.of("\"\"\"\"\"bar\"\"\"", "\"\"bar\"")
        };
    }

    private static Arguments[] malformedSimpleNamesArgs() {
        return new Arguments[]{
                // Empty names
                Arguments.of(""),
                Arguments.of(" "),

                // Unexpected delimiters
                Arguments.of(".f"),
                Arguments.of("f."),
                Arguments.of("."),

                // Unquoted names with Invalid characters
                Arguments.of("f f"),
                Arguments.of("1o0"),
                Arguments.of("@#$"),
                Arguments.of("foo$"),
                Arguments.of("foo%"),
                Arguments.of("foo&"),
                Arguments.of("f😅"),
                Arguments.of("😅f"),

                // Invalid escape sequences
                Arguments.of("f\"f"),
                Arguments.of("f\"\"f"),
                Arguments.of("\"foo"),
                Arguments.of("\"fo\"o\"")
        };
    }

    private static Arguments[] malformedCanonicalNamesArgs() {
        return new Arguments[]{
                Arguments.of("foo."),
                Arguments.of(".bar"),
                Arguments.of("."),
                Arguments.of("foo..bar"),
                Arguments.of("foo.bar."),
                Arguments.of("foo.."),

                Arguments.of("@#$.bar"),
                Arguments.of("foo.@#$"),
                Arguments.of("@#$"),
                Arguments.of("1oo.bar"),
                Arguments.of("foo.1ar"),
                Arguments.of("1oo")
        };
    }

    private static Arguments[] validCanonicalNamesArgs() {
        return new Arguments[]{
                Arguments.of("\"foo.bar\".baz", "foo.bar", "BAZ"),
                Arguments.of("foo.\"bar.baz\"", "FOO", "bar.baz"),

                Arguments.of("\"foo.\"\"bar\"\"\".baz", "foo.\"bar\"", "BAZ"),
                Arguments.of("foo.\"bar.\"\"baz\"", "FOO", "bar.\"baz"),

                Arguments.of("_foo.bar", "_FOO", "BAR"),
                Arguments.of("foo._bar", "FOO", "_BAR")
        };
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    public void invalidNullNames() {
        assertThrows(NullPointerException.class, () -> QualifiedName.parse(null));
        assertThrows(NullPointerException.class, () -> QualifiedName.fromSimple(null));
        assertThrows(NullPointerException.class, () -> QualifiedName.of("s1", null));
        assertThrows(NullPointerException.class, () -> QualifiedName.of(null, null));
    }

    @Test
    public void defaultSchemaName() {
        assertEquals(QualifiedName.DEFAULT_SCHEMA_NAME, QualifiedName.parse("foo").schemaName());
        assertEquals(QualifiedName.DEFAULT_SCHEMA_NAME, QualifiedName.fromSimple("foo").schemaName());
        assertEquals(QualifiedName.DEFAULT_SCHEMA_NAME, QualifiedName.of(null, "foo").schemaName());
    }

    @Test
    public void canonicalForm() {
        assertThat(QualifiedName.parse("foo.bar").toCanonicalForm(), equalTo("FOO.BAR"));
        assertThat(QualifiedName.parse("\"foo\".\"bar\"").toCanonicalForm(), equalTo("\"foo\".\"bar\""));
    }

    @ParameterizedTest
    @MethodSource("validSimpleNamesArgs")
    void validSimpleNames(String actual, String expectedIdentifier) {
        QualifiedName simple = QualifiedName.fromSimple(actual);
        QualifiedName parsed = QualifiedName.parse(actual);
        QualifiedName of = QualifiedName.of(null, actual);

        assertThat(simple.objectName(), equalTo(expectedIdentifier));
        assertThat(parsed.objectName(), equalTo(expectedIdentifier));
        assertThat(of.objectName(), equalTo(expectedIdentifier));

        assertEquals(parsed, simple);
        assertEquals(of, simple);
    }

    @ParameterizedTest
    @MethodSource("validSimpleNamesArgs")
    public void validCanonicalNames(String source, String expectedIdentifier) {
        QualifiedName parsed = QualifiedName.parse(source + '.' + source);

        assertThat(parsed.schemaName(), equalTo(expectedIdentifier));
        assertThat(parsed.objectName(), equalTo(expectedIdentifier));

        QualifiedName of = QualifiedName.of(source, source);

        assertThat(of.schemaName(), equalTo(expectedIdentifier));
        assertThat(of.objectName(), equalTo(expectedIdentifier));

        assertEquals(of, parsed);

        // Canonical form should parsed to the equal object.
        assertEquals(parsed, QualifiedName.parse(parsed.toCanonicalForm()));
    }

    @ParameterizedTest
    @MethodSource("validCanonicalNamesArgs")
    public void validCanonicalNames(String source, String schemaIdentifier, String objectIdentifier) {
        QualifiedName parsed = QualifiedName.parse(source);

        assertThat(parsed.schemaName(), equalTo(schemaIdentifier));
        assertThat(parsed.objectName(), equalTo(objectIdentifier));

        assertThat(parsed.toCanonicalForm(), equalTo(IgniteNameUtils.canonicalName(schemaIdentifier, objectIdentifier)));

        // Canonical form should parsed to the equal object.
        assertEquals(parsed, QualifiedName.parse(parsed.toCanonicalForm()));
    }

    @ParameterizedTest
    @MethodSource("malformedSimpleNamesArgs")
    public void malformedSimpleNames(String source) {
        { // fromSimple
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> QualifiedName.fromSimple(source));

            assertThat(ex.getMessage(), is(anyOf(
                    equalTo("Object identifier can't be empty."),
                    equalTo("Fully qualified name is not expected [name=" + source + "]"),
                    containsString("Malformed identifier [identifier=" + source))));
        }

        { // parse
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> QualifiedName.parse(source));

            assertThat(ex.getMessage(), is(anyOf(
                    equalTo("Object identifier can't be empty."),
                    containsString("Malformed identifier [identifier=" + source))));
        }

        { // schemaName
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> QualifiedName.of(source, "bar"));

            assertThat(ex.getMessage(), is(anyOf(
                    equalTo("Fully qualified name is not expected [name=" + source + "]"),
                    equalTo("Schema identifier can't be empty."),
                    containsString("Malformed identifier [identifier=" + source))
            ));
        }

        { // objectName
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> QualifiedName.of(null, source));

            assertThat(ex.getMessage(), is(anyOf(
                    equalTo("Fully qualified name is not expected [name=" + source + "]"),
                    equalTo("Object identifier can't be empty."),
                    containsString("Malformed identifier [identifier=" + source))
            ));
        }
    }

    @Test
    public void unexpectedCanonicalName() {
        String canonicalName = "f.f";

        { // fromSimple
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> QualifiedName.fromSimple(canonicalName));

            assertThat(ex.getMessage(), equalTo("Fully qualified name is not expected [name=" + canonicalName + "]"));
        }

        { // schemaName
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> QualifiedName.of(canonicalName, "bar"));

            assertThat(ex.getMessage(), equalTo("Fully qualified name is not expected [name=" + canonicalName + "]"));
        }

        { // objectName
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> QualifiedName.of(null, canonicalName));

            assertThat(ex.getMessage(), equalTo("Fully qualified name is not expected [name=" + canonicalName + "]"));
        }
    }

    @ParameterizedTest
    @MethodSource("malformedCanonicalNamesArgs")
    public void malformedCanonicalNames(String source) {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> QualifiedName.parse(source));

        assertThat(ex.getMessage(), is(anyOf(
                equalTo("Object identifier can't be empty."),
                equalTo("Canonical name format mismatch: " + source),
                containsString("Malformed identifier [identifier=" + source)
        )));
    }
}
