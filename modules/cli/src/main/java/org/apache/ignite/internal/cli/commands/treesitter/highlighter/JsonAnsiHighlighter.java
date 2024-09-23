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

import static org.apache.ignite.internal.cli.core.style.AnsiStringSupport.ansi;

import java.util.Map;
import org.apache.ignite.internal.cli.commands.treesitter.parser.Indexer;
import org.apache.ignite.internal.cli.commands.treesitter.parser.JsonTokenType;
import org.apache.ignite.internal.cli.commands.treesitter.parser.Parser;
import org.apache.ignite.internal.cli.core.style.AnsiStringSupport;
import org.apache.ignite.internal.cli.core.style.AnsiStringSupport.Color;

/**
 * Class for highlighting JSON text with ANSI colors.
 */
public class JsonAnsiHighlighter {

    private static final Map<JsonTokenType, AnsiStringSupport.Color> colorMap = Map.of(
            JsonTokenType.QUOTE, Color.GREEN,
            JsonTokenType.STRING_CONTENT, Color.GREEN,
            JsonTokenType.BRACKET, Color.WHITE,
            JsonTokenType.COMMA, Color.WHITE,
            JsonTokenType.COLON, Color.WHITE,
            JsonTokenType.NUMBER, Color.BLUE,
            JsonTokenType.BOOL, Color.YELLOW_DARK,
            JsonTokenType.UNKNOWN, Color.GRAY,
            JsonTokenType.SPACE, Color.GRAY
    );

    /**
     * Highlights the input JSON text with ANSI colors.
     *
     * @param text The input JSON text.
     * @return The highlighted JSON text.
     */
    public static String highlight(String text) {
        var tree = Parser.parseJson(text);
        JsonTokenType[] tokens = Indexer.indexJson(text, tree);
        var sb = new StringBuilder();

        for (int i = 0; i < text.length(); i++) {
            JsonTokenType token = tokens[i];
            Color color = colorMap.get(token);
            sb.append(AnsiStringSupport.fg(color).mark(String.valueOf(text.charAt(i))));
        }

        return ansi(sb.toString());
    }
}
