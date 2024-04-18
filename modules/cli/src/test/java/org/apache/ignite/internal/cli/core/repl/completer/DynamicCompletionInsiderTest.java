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

package org.apache.ignite.internal.cli.core.repl.completer;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class DynamicCompletionInsiderTest {

    private DynamicCompletionInsider insider;

    private static Stream<Arguments> typedWordsSource() {
        return Stream.of(
                Arguments.of(new String[]{"cluster", "unit", "undeploy"}, false),
                Arguments.of(new String[]{"cluster", "unit", "undeploy", "unit.id", "--version"}, true),
                Arguments.of(new String[]{"cluster", "unit", "undeploy", "unit.id", "--version", "1.0.0"}, true),
                Arguments.of(new String[]{"node", "config", "show", ""}, false),
                Arguments.of(new String[]{"node", "config", "show", "conf."}, false)
        );
    }

    @BeforeEach
    void setUp() {
        insider = new DynamicCompletionInsider();
    }

    @ParameterizedTest
    @MethodSource("typedWordsSource")
    void testCompleter(String[] givenTypedWords, boolean expected) {
        boolean actual = insider.wasPositionalParameterCompleted(givenTypedWords);
        assertThat(actual).isEqualTo(expected);
    }
}
