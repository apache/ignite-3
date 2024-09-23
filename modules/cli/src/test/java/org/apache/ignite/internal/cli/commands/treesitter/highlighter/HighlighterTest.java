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

package org.apache.ignite.internal.cli.commands.treesitter.highlighter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.of;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for the highlighting errors in JSON text. The colors itself are not tested.
 */
class HighlighterTest {

    private static Stream<Arguments> jsonTextInput() {
        return Stream.of(
                of("plain", "plain"),
                of("{\"key\": \"val\"}", "{\"key\": \"val\"}"),
                of("{\"key\": \"1%\"}", "{\"key\": \"1%\"}")
        );
    }

    @ParameterizedTest
    @MethodSource("jsonTextInput")
    void jsonHighlight(String input, String expected) {
        String result = JsonAnsiHighlighter.highlight(input);
        assertEquals(expected, result);
    }

    private static Stream<Arguments> hoconTextInput() {
        return Stream.of(
                of("plain", "plain"),
                of("{\"key\": \"val\"}", "{\"key\": \"val\"}"),
                of("key=val", "key=val")
        );
    }

    @ParameterizedTest
    @MethodSource("hoconTextInput")
    void hoconHighlight(String input, String expected) {
        String result = HoconAnsiHighlighter.highlight(input);
        assertEquals(expected, result);
    }
}
