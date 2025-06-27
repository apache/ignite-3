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

package org.apache.ignite.internal.cli.core.style;

import picocli.CommandLine.Help.Ansi;

/**
 * Utility class with ANSI string support.
 */
public final class AnsiStringSupport {
    private AnsiStringSupport() {}

    public static String ansi(String markupText) {
        return Ansi.AUTO.string(markupText);
    }

    public static Fg fg(Color color) {
        return new Fg(color);
    }

    /** Can mark the string as a ANSI string. */
    public interface Marker {
        String mark(String content);
    }

    /**
     * Dsl for ansi fg: takes color and marks string like: mytext -> @|fg(10) mytext|@ where 10 is the ansi color.
     */
    public static class Fg implements Marker {
        private final Color color;

        private Style style;

        private Fg(Color color) {
            this.color = color;
        }

        public Fg with(Style style) {
            this.style = style;
            return this;
        }

        /** Marks given text with the configured before style. */
        @Override
        public String mark(String textToMark) {
            if (style == Style.BOLD) {
                return String.format("@|fg(%d),bold %s|@", color.code, textToMark);
            }
            return String.format("@|fg(%d) %s|@", color.code, textToMark);
        }
    }

    /** Represents the text style. */
    public enum Style implements Marker {
        BOLD("bold"), UNDERLINE("underline");

        private final String value;

        Style(String value) {
            this.value = value;
        }

        @Override
        public String mark(String textToMark) {
            return String.format("@|%s %s|@", value, textToMark);
        }
    }

    /**
     * Represents ansi colors that are used in CLI.
     */
    public enum Color {
        RED(1),
        GREEN(2),
        YELLOW(3),
        BLUE(31),
        YELLOW_DARK(215),
        GREEN_DARK(22),
        GRAY(246),
        WHITE(252);

        Color(int code) {
            this.code = code;
        }

        private final int code;
    }
}
