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

import static org.apache.ignite.internal.cli.commands.treesitter.parser.SqlTokenType.BRACKET;
import static org.apache.ignite.internal.cli.commands.treesitter.parser.SqlTokenType.COMMA;
import static org.apache.ignite.internal.cli.commands.treesitter.parser.SqlTokenType.EQUAL;
import static org.apache.ignite.internal.cli.commands.treesitter.parser.SqlTokenType.IDENTIFIER;
import static org.apache.ignite.internal.cli.commands.treesitter.parser.SqlTokenType.KEYWORD;
import static org.apache.ignite.internal.cli.commands.treesitter.parser.SqlTokenType.LITERAL;
import static org.apache.ignite.internal.cli.commands.treesitter.parser.SqlTokenType.SEMICOLON;
import static org.apache.ignite.internal.cli.commands.treesitter.parser.SqlTokenType.SPACE;
import static org.apache.ignite.internal.cli.commands.treesitter.parser.SqlTokenType.STAR;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SqlIndexerTest {
    private static Stream<Arguments> sqlProvider() {
        return Stream.of(
                Arguments.of(
                        "select * from table",
                        new SqlTokenType[]{
                                // select
                                KEYWORD, KEYWORD, KEYWORD, KEYWORD, KEYWORD, KEYWORD,
                                // *
                                SPACE, STAR, SPACE,
                                // from
                                KEYWORD, KEYWORD, KEYWORD, KEYWORD, SPACE,
                                // table
                                IDENTIFIER, IDENTIFIER, IDENTIFIER, IDENTIFIER, IDENTIFIER,
                        }),
                Arguments.of(
                        "select a ,b, c from table where a = 1 and b = 2 or c=3",
                        new SqlTokenType[]{
                                // select
                                KEYWORD, KEYWORD, KEYWORD, KEYWORD, KEYWORD, KEYWORD,
                                // a ,b, c
                                SPACE, IDENTIFIER, SPACE, COMMA, IDENTIFIER, COMMA, SPACE, IDENTIFIER, SPACE,
                                // from
                                KEYWORD, KEYWORD, KEYWORD, KEYWORD, SPACE,
                                // table
                                IDENTIFIER, IDENTIFIER, IDENTIFIER, IDENTIFIER, IDENTIFIER, SPACE,
                                // where
                                KEYWORD, KEYWORD, KEYWORD, KEYWORD, KEYWORD, SPACE,
                                // a = 1
                                IDENTIFIER, SPACE, EQUAL, SPACE, LITERAL, SPACE,
                                // and
                                KEYWORD, KEYWORD, KEYWORD, SPACE,
                                // b = 2
                                IDENTIFIER, SPACE, EQUAL, SPACE, LITERAL, SPACE,
                                // or
                                KEYWORD, KEYWORD, SPACE,
                                // c = 3
                                IDENTIFIER, EQUAL, LITERAL
                        }
                ),
                Arguments.of(
                        "select * from (select * from(select a from table));",
                        new SqlTokenType[]{
                                // select
                                KEYWORD, KEYWORD, KEYWORD, KEYWORD, KEYWORD, KEYWORD,
                                // *
                                SPACE, STAR, SPACE,
                                // from
                                KEYWORD, KEYWORD, KEYWORD, KEYWORD, SPACE,
                                // (
                                BRACKET,
                                // select
                                KEYWORD, KEYWORD, KEYWORD, KEYWORD, KEYWORD, KEYWORD,
                                // *
                                SPACE, STAR, SPACE,
                                // from
                                KEYWORD, KEYWORD, KEYWORD, KEYWORD,
                                // (
                                BRACKET,
                                // select
                                KEYWORD, KEYWORD, KEYWORD, KEYWORD, KEYWORD, KEYWORD, SPACE,
                                // a
                                IDENTIFIER, SPACE,
                                // from
                                KEYWORD, KEYWORD, KEYWORD, KEYWORD, SPACE,
                                // table
                                IDENTIFIER, IDENTIFIER, IDENTIFIER, IDENTIFIER, IDENTIFIER,
                                // ));
                                BRACKET, BRACKET, SEMICOLON
                        }
                )
        );
    }

    @ParameterizedTest
    @MethodSource("sqlProvider")
    void indexSql(String text, SqlTokenType[] expectedTokens) {
        var tree = Parser.parseSql(text);
        assertArrayEquals(expectedTokens, Indexer.indexSql(text, tree));
    }
}
