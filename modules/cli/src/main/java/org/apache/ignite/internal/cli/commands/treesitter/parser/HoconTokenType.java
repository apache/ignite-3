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
 * Enum representing the different types of tokens in a HOCON text.
 */
public enum HoconTokenType {
    QUOTE,
    STRING,
    PATH,
    NUMBER,
    COLON,
    EQUALS,
    BRACKET,
    COMMA,
    UNKNOWN,
    BOOL,
    SPACE;

    private static final String PATH_TYPE = "path";

    static HoconTokenType fromNode(TSNode node) {
        String nodeType = nodeType(node);
        switch (nodeType) {
            case "\"":
                return QUOTE;
            case PATH_TYPE:
                return PATH;
            case "string":
            case "unquoted_string":
                return STRING;
            case "{":
            case "}":
            case "[":
            case "]":
                return BRACKET;
            case ",":
                return COMMA;
            case ":":
                return COLON;
            case "=":
                return EQUALS;
            case "number":
                return NUMBER;
            case "true":
            case "false":
                return BOOL;
            default:
                return UNKNOWN;
        }
    }

    private static String nodeType(TSNode node) {
        TSNode parent = node;
        // Both key and value nodes could have the same type - string - but we want to highlight the key ("path") differently.
        // Proper fix would be to change the logic of tokenizing completely but this is enough for now.
        while (!parent.isNull()) {
            if (PATH_TYPE.equals(parent.getType())) {
                return PATH_TYPE;
            }
            parent = parent.getParent();
        }
        return node.getType();
    }
}
