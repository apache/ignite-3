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

import static org.apache.ignite.lang.util.IgniteNameUtils.quoteIfNeeded;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test cases to verify {@link QualifiedName}.
 */
public class QualifiedNameTest {
    @ParameterizedTest
    @CsvSource({
            "foo, FOO", "fOo, FOO", "FOO, FOO", "f23, F23", // Uppercased
            "\"FOO\", FOO", "\"foo\", foo", "\"fOo\", fOo", "\"@#$\", @#$", // Quoted
            "\"f.f\", f.f", "\"f\"\"f\", f\"f", "\"f\"\"bar\"\"f\", f\"bar\"f", // Escaped
    })
    public void validSimpleNames(String source, String expected) {
        QualifiedName simple = QualifiedName.fromSimple(source);
        QualifiedName parsed = QualifiedName.parse(source);

        assertThat(simple.name(), equalTo(expected));
        assertThat(parsed.name(), equalTo(expected));

        assertNull(simple.schemaName());
        assertNull(parsed.schemaName());

        assertEquals(parsed, simple);

        assertThat(parsed.toString(), equalTo(quoteIfNeeded(expected)));
    }

    @ParameterizedTest
    @CsvSource({
            "foo.bar, FOO, BAR", "fOo.bAr, FOO, BAR", "FOO.BAR, FOO, BAR", "foo.b23, FOO, B23", "f23.bar, F23, BAR",// Uppercased
            "\"FOO\".\"BAR\", FOO, BAR", "\"foo\".\"bar\", foo, bar", "\"fOo\".\"bAr\", fOo, bAr", "\"fOo\".bAr, fOo, BAR",
            "fOo.\"bAr\", FOO, bAr", "\"@#$\".bar, @#$, BAR", "foo.\"@#$\", FOO, @#$", // Quoted
            "\"foo.bar\".baz, foo.bar, BAZ", "foo.\"bar.baz\", FOO, bar.baz",
            "\"foo.\"\"bar\"\"\".baz, foo.\"bar\", BAZ", "foo.\"bar.\"\"baz\"\"\", FOO, bar.\"baz\"" // Escaped
    })
    public void validCanonicalNames(String source, String schemaName, String objectName) {
        QualifiedName parsed = QualifiedName.parse(source);

        assertThat(parsed.schemaName(), equalTo(schemaName));
        assertThat(parsed.name(), equalTo(objectName));

        assertThat(parsed.toString(), equalTo(quoteIfNeeded(schemaName) + '.' + quoteIfNeeded(objectName)));
    }


    @Test
    public void parseSequenceOfSpaces() {
        QualifiedName simple = QualifiedName.fromSimple("\"   \"");
        QualifiedName parsed = QualifiedName.parse("\"   \"");

        assertThat(simple.name(), equalTo("   "));
        assertThat(parsed.name(), equalTo("   "));

        assertNull(simple.schemaName());
        assertNull(parsed.schemaName());

        assertEquals(parsed, simple);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "f.f", "f f", "f\"f", "f\"\"f", "\"foo", "\"fo\"o\"", ".f", "f.", ".", "", "1o0, 1O0", "@#$, @#$",
    })
    public void malformedSimpleNames(String source) {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> QualifiedName.fromSimple(source));

        assertThat(ex.getMessage(), is(anyOf(
                equalTo("Object name can't be null or empty."),
                equalTo("Non-qualified identifier expected: " + source),
                containsString("Malformed name [name=" + source))));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "f f", "f\"f", "f\"\"f", "\"foo", "\"fo\"o\"", ".f", "f.", ".",
            "@#$.bar", "foo.@#$", "@#$", "1oo.bar", "foo.1ar", "1oo",
    })
    public void malformedNames(String source) {
        IllegalArgumentException ex1 = assertThrows(IllegalArgumentException.class, () -> QualifiedName.parse(source));

        assertThat(ex1.getMessage(), is(anyOf(
                equalTo("Schema name can't be empty."),
                equalTo("Object name can't be null or empty."),
                containsString("Malformed name [name=" + source))));
    }
}
