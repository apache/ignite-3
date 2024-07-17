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

package org.apache.ignite.internal.cli.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ArrayUtilsTest {

    private static Stream<Arguments> wordsForFindLastNotEmptyWord() {
        return Stream.of(
                Arguments.of(new String[]{"one", "two", "three"}, "three"),
                Arguments.of(new String[]{"one", "two", "three", " "}, "three"),
                Arguments.of(new String[]{"one"}, "one"),
                Arguments.of(new String[]{" "}, ""),
                Arguments.of(new String[0], "")
        );
    }

    private static Stream<Arguments> wordsForFindLastNotEmptyWordBeforeWord() {
        return Stream.of(
                Arguments.of(new String[]{"one", "two", "three"}, "three", "two"),
                Arguments.of(new String[]{"one", "two", "three"}, "two", "one"),
                Arguments.of(new String[]{"one"}, "one", ""),
                Arguments.of(new String[]{" "}, "one", ""),
                Arguments.of(new String[0], "one", "")
        );
    }

    private static Stream<Arguments> wordsForFirstStartsWithSecond() {
        return Stream.of(
                Arguments.of(new String[]{}, new String[]{}, true),
                Arguments.of(new String[]{"one", "two", "three"}, new String[]{}, true),
                Arguments.of(new String[]{"one", "two", "three"}, new String[]{"one", "two"}, true),
                Arguments.of(new String[]{"one", "two", "three"}, new String[]{"one", "two", "three"}, true),
                Arguments.of(new String[]{"one", "two", "three"}, new String[]{"one", "two", "3"}, false),
                Arguments.of(new String[]{"one", "two", "three"}, new String[]{"one", "two", "three", "four"}, false)
        );
    }


    @ParameterizedTest
    @MethodSource("wordsForFindLastNotEmptyWord")
    void findLastNotEmptyWord(String[] words, String expectedWord) {
        assertEquals(expectedWord,
                ArrayUtils.findLastNotEmptyWord(words));
    }

    @ParameterizedTest
    @MethodSource("wordsForFindLastNotEmptyWordBeforeWord")
    void findLastNotEmptyWordBeforeWord(String[] words, String word, String expectedWord) {
        assertEquals(expectedWord,
                ArrayUtils.findLastNotEmptyWordBeforeWordFromEnd(words, word));
    }

    @Test
    void findLastNotEmptyWordBeforeBlankWord() {
        assertThrows(IllegalArgumentException.class,
                () -> ArrayUtils.findLastNotEmptyWordBeforeWordFromEnd(new String[]{"one", "two"}, " "));
    }

    @ParameterizedTest
    @MethodSource("wordsForFirstStartsWithSecond")
    void firstStartsWithSecond(String[] first, String[] second, boolean expected) {
        assertEquals(expected, ArrayUtils.firstStartsWithSecond(first, second));
    }
}
