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
 * Enum representing the different types of tokens in a JSON text.
 */
public enum JsonTokenType {
    QUOTE,
    STRING_CONTENT,
    VALUE_STRING,
    NUMBER,
    LEFT_BRACKET,
    RIGHT_BRACKET,
    COLON,
    BRACKET,
    COMMA,
    UNKNOWN,
    BOOL,
    SPACE;

    static JsonTokenType fromNode(TSNode node) {
        switch (node.getType()) {
            case "\"":
                return QUOTE;
            case "string_content":
                return STRING_CONTENT;
            case "{":
            case "}":
            case "[":
            case "]":
                return BRACKET;
            case ",":
                return COMMA;
            case ":":
                return COLON;
            case "number":
                return NUMBER;
            case "true":
            case "false":
                return BOOL;
            default:
                return UNKNOWN;
        }
    }
}
