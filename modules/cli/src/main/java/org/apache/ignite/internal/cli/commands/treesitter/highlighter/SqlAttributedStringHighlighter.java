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

package org.apache.ignite.internal.cli.commands.treesitter.highlighter;

import org.apache.ignite.internal.cli.commands.treesitter.parser.Indexer;
import org.apache.ignite.internal.cli.commands.treesitter.parser.Parser;
import org.apache.ignite.internal.cli.commands.treesitter.parser.SqlTokenType;
import org.apache.ignite.internal.cli.core.style.AnsiStringSupport;
import org.apache.ignite.internal.cli.core.style.ColorScheme;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;

/**
 * Highlighter for SQL text. The highlighted text is returned as an AttributedString.
 */
public class SqlAttributedStringHighlighter {
    /**
     * Highlights the input SQL text with ANSI colors using the current color scheme.
     *
     * @param text The input SQL text.
     * @return The highlighted SQL text.
     */
    public static AttributedString highlight(String text) {
        return highlight(text, AnsiStringSupport.getColorScheme());
    }

    /**
     * Highlights the input SQL text with ANSI colors using the specified color scheme.
     *
     * @param text The input SQL text.
     * @param scheme The color scheme to use.
     * @return The highlighted SQL text.
     */
    public static AttributedString highlight(String text, ColorScheme scheme) {
        var as = new AttributedStringBuilder();

        var tree = Parser.parseSql(text);
        SqlTokenType[] tokens = Indexer.indexSql(text, tree);

        for (int i = 0; i < text.length(); i++) {
            SqlTokenType token = tokens[i];
            int color = getColorForToken(token, scheme);
            var style = AttributedStyle.DEFAULT.foreground(color);
            as.style(style).append(text.charAt(i));
        }

        return as.toAttributedString();
    }

    private static int getColorForToken(SqlTokenType token, ColorScheme scheme) {
        switch (token) {
            case KEYWORD:
                return scheme.keywordColor();
            case IDENTIFIER:
                return scheme.identifierColor();
            case LITERAL:
                return scheme.stringColor();
            case BRACKET:
            case COMMA:
            case EQUAL:
            case STAR:
            case SEMICOLON:
                return scheme.punctuationColor();
            case SPACE:
                return 0;
            case UNKNOWN:
            default:
                return scheme.punctuationColor();
        }
    }
}
