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
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.IgniteInternalException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test cases to verify {@link IgniteNameUtils}.
 */
public class IgniteNameUtilsTest {
    @ParameterizedTest
    @CsvSource({
            "foo, FOO", "fOo, FOO", "FOO, FOO", "1o0, 1O0", "@#$, @#$",
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
            "f.f", "f f", "f\"f", "f\"\"f", "\"foo", "\"fo\"o"
    })
    public void malformedSimpleNames(String source) {
        IgniteInternalException ex = assertThrows(IgniteInternalException.class, () -> IgniteNameUtils.parseSimpleName(source));

        assertThat(ex.getMessage(), ex.code(), equalTo(Common.ILLEGAL_ARGUMENT_ERR));
    }
}
