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

import static org.apache.ignite.internal.cli.commands.treesitter.parser.JsonTokenType.BOOL;
import static org.apache.ignite.internal.cli.commands.treesitter.parser.JsonTokenType.BRACKET;
import static org.apache.ignite.internal.cli.commands.treesitter.parser.JsonTokenType.COLON;
import static org.apache.ignite.internal.cli.commands.treesitter.parser.JsonTokenType.COMMA;
import static org.apache.ignite.internal.cli.commands.treesitter.parser.JsonTokenType.NUMBER;
import static org.apache.ignite.internal.cli.commands.treesitter.parser.JsonTokenType.QUOTE;
import static org.apache.ignite.internal.cli.commands.treesitter.parser.JsonTokenType.SPACE;
import static org.apache.ignite.internal.cli.commands.treesitter.parser.JsonTokenType.STRING_CONTENT;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for the indexing logic for JSON.
 */
public class JsonIndexerTest {
    private static Stream<Arguments> jsonProvider() {
        return Stream.of(
                Arguments.of(
                        "{\"a\": 1, \"b\": 2, \"c\": true}",
                        new JsonTokenType[]{
                                // {
                                BRACKET,
                                // "a"
                                QUOTE, STRING_CONTENT, QUOTE,
                                // :
                                COLON,
                                // 1
                                SPACE, NUMBER,
                                // ,
                                COMMA,
                                // "b"
                                SPACE, QUOTE, STRING_CONTENT, QUOTE,
                                // :
                                COLON,
                                // 2
                                SPACE, NUMBER,
                                // ,
                                COMMA,
                                // "c"
                                SPACE, QUOTE, STRING_CONTENT, QUOTE,
                                // :
                                COLON,
                                // true
                                SPACE, BOOL, BOOL, BOOL, BOOL,
                                // }
                                BRACKET
                        }
                )
        );
    }

    @ParameterizedTest
    @MethodSource("jsonProvider")
    void indexJson(String text, JsonTokenType[] expectedTokens) {
        var tree = Parser.parseJson(text);
        assertArrayEquals(expectedTokens, Indexer.indexJson(text, tree));
    }
}
