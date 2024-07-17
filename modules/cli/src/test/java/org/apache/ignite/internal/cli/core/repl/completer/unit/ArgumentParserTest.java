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

package org.apache.ignite.internal.cli.core.repl.completer.unit;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ArgumentParserTest {
    private static Stream<Arguments> wordsSource() {
        return Stream.of(
                Arguments.of(new String[]{"cluster", "unit", "undeploy", "unit.id", "--version", "1.1.1"}, "unit.id"),
                // Picocli throws exception here. Probably we will need to use another parser.
                Arguments.of(new String[]{"cluster", "unit", "undeploy", "unit.id", "--version"}, null),
                Arguments.of(new String[]{"cluster", "undeploy", "unit.id", "--version"}, null),
                Arguments.of(new String[]{""}, null),
                Arguments.of(new String[]{"nosuch"}, null)
        );
    }

    @ParameterizedTest
    @MethodSource("wordsSource")
    void parseUnitId(String[] givenWords, String expectedUnitId) {
        // When parse
        String unitId = new ArgumentParser().parseFirstPositionalParameter(givenWords);

        // Then
        assertThat(unitId).isEqualTo(expectedUnitId);
    }
}
