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

import org.treesitter.TSParser;
import org.treesitter.TSTree;
import org.treesitter.TreeSitterJson;
import org.treesitter.TreeSitterSql;

/**
 * Parser class for parsing SQL and JSON. It uses TreeSitter to parse the input.
 * The result of parsing is an abstract syntax tree.
 */
public final class Parser {


    /**
     * Parses the input text as SQL.
     *
     * @param text The input text.
     * @return The abstract syntax tree.
     */
    public static TSTree parseSql(String text) {
        var parser = new TSParser();
        parser.setLanguage(new TreeSitterSql());
        return parser.parseString(null, text);
    }

    /**
     * Parses the input text as JSON.
     *
     * @param text The input text.
     * @return The abstract syntax tree.
     */
    public static TSTree parseJson(String text) {
        var parser = new TSParser();
        parser.setLanguage(new TreeSitterJson());
        return parser.parseString(null, text);
    }
}
