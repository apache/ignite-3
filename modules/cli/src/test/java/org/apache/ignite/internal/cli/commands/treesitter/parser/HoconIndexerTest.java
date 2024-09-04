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

package org.apache.ignite.internal.cli.commands.treesitter.parser;

import static org.apache.ignite.internal.cli.commands.treesitter.parser.HoconTokenType.BOOL;
import static org.apache.ignite.internal.cli.commands.treesitter.parser.HoconTokenType.BRACKET;
import static org.apache.ignite.internal.cli.commands.treesitter.parser.HoconTokenType.COLON;
import static org.apache.ignite.internal.cli.commands.treesitter.parser.HoconTokenType.COMMA;
import static org.apache.ignite.internal.cli.commands.treesitter.parser.HoconTokenType.EQUALS;
import static org.apache.ignite.internal.cli.commands.treesitter.parser.HoconTokenType.NUMBER;
import static org.apache.ignite.internal.cli.commands.treesitter.parser.HoconTokenType.PATH;
import static org.apache.ignite.internal.cli.commands.treesitter.parser.HoconTokenType.QUOTE;
import static org.apache.ignite.internal.cli.commands.treesitter.parser.HoconTokenType.SPACE;
import static org.apache.ignite.internal.cli.commands.treesitter.parser.HoconTokenType.STRING;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for the indexing logic for HOCON.
 */
public class HoconIndexerTest {
    private static Stream<Arguments> hoconProvider() {
        return Stream.of(
                Arguments.of(
                        "{\"a\": 1, \"b\": \"t\", c=true}",
                        new HoconTokenType[]{
                                // {
                                BRACKET,
                                // "a"
                                PATH, PATH, PATH,
                                // :
                                COLON,
                                // 1
                                SPACE, NUMBER,
                                // ,
                                COMMA,
                                // "b"
                                SPACE, PATH, PATH, PATH,
                                // :
                                COLON,
                                // "t"
                                SPACE, QUOTE, STRING, QUOTE,
                                // ,
                                COMMA,
                                // c
                                SPACE, PATH,
                                // =
                                EQUALS,
                                // true
                                BOOL, BOOL, BOOL, BOOL,
                                // }
                                BRACKET
                        }
                )
        );
    }

    @ParameterizedTest
    @MethodSource("hoconProvider")
    void indexHocon(String text, HoconTokenType[] expectedTokens) {
        var tree = Parser.parseHocon(text);
        HoconTokenType[] tokens = Indexer.indexHocon(text, tree);
        assertArrayEquals(expectedTokens, tokens);
    }
}
