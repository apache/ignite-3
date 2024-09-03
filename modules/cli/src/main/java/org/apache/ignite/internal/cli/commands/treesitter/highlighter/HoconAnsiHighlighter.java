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

import java.util.EnumMap;
import java.util.Map;
import org.apache.ignite.internal.cli.commands.treesitter.parser.HoconTokenType;
import org.apache.ignite.internal.cli.commands.treesitter.parser.Indexer;
import org.apache.ignite.internal.cli.commands.treesitter.parser.Parser;
import org.apache.ignite.internal.cli.core.style.AnsiStringSupport;
import org.apache.ignite.internal.cli.core.style.AnsiStringSupport.Color;

/**
 * Class for highlighting HOCON text with ANSI colors.
 */
public class HoconAnsiHighlighter {

    private static final Map<HoconTokenType, Color> COLOR_MAP = new EnumMap<>(HoconTokenType.class);

    static {
        COLOR_MAP.put(HoconTokenType.QUOTE, Color.GREEN);
        COLOR_MAP.put(HoconTokenType.STRING, Color.GREEN);
        COLOR_MAP.put(HoconTokenType.PATH, Color.WHITE);
        COLOR_MAP.put(HoconTokenType.BRACKET, Color.GRAY);
        COLOR_MAP.put(HoconTokenType.COMMA, Color.GRAY);
        COLOR_MAP.put(HoconTokenType.COLON, Color.GRAY);
        COLOR_MAP.put(HoconTokenType.EQUALS, Color.GRAY);
        COLOR_MAP.put(HoconTokenType.NUMBER, Color.BLUE);
        COLOR_MAP.put(HoconTokenType.BOOL, Color.YELLOW_DARK);
        COLOR_MAP.put(HoconTokenType.UNKNOWN, Color.GRAY);
        COLOR_MAP.put(HoconTokenType.SPACE, Color.GRAY);
    }

    /**
     * Highlights the input HOCON text with ANSI colors.
     *
     * @param text The input HOCON text.
     * @return The highlighted HOCON text.
     */
    public static String highlight(String text) {
        var tree = Parser.parseHocon(text);
        HoconTokenType[] tokens = Indexer.indexHocon(text, tree);
        var sb = new StringBuilder();

        for (int i = 0; i < text.length(); i++) {
            HoconTokenType token = tokens[i];
            Color color = COLOR_MAP.get(token);
            sb.append(AnsiStringSupport.fg(color).mark(String.valueOf(text.charAt(i))));
        }

        return ansi(sb.toString());
    }
}
