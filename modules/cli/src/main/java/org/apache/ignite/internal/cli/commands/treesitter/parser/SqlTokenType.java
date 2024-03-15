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

import org.treesitter.TSNode;

/**
 * Enum representing the different types of tokens in a SQL text.
 */
public enum SqlTokenType {
    KEYWORD,
    IDENTIFIER,
    STAR,
    LITERAL,
    BRACKET,
    SPACE,
    COMMA,
    EQUAL,
    SEMICOLON,
    UNKNOWN;

    static SqlTokenType fromNode(TSNode node) {
        switch (node.getType()) {
            case "keyword":
                return KEYWORD;
            case "identifier":
                return IDENTIFIER;
            case "literal":
                return LITERAL;
            case "(":
            case ")":
                return BRACKET;
            case ",":
                return COMMA;
            case "*":
                return STAR;
            case "=":
                return EQUAL;
            case ";":
                return SEMICOLON;
            default:
                if (node.getType().startsWith("keyword")) {
                    return KEYWORD;
                }
                return UNKNOWN;
        }
    }
}
