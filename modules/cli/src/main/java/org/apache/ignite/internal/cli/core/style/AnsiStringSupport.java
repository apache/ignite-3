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

import java.util.concurrent.atomic.AtomicReference;
import picocli.CommandLine.Help.Ansi;

/**
 * Utility class with ANSI string support.
 */
public final class AnsiStringSupport {
    private static final AtomicReference<ColorSchemeProvider> SCHEME_PROVIDER =
            new AtomicReference<>(() -> ColorScheme.SOLARIZED_DARK);

    private AnsiStringSupport() {}

    /**
     * Sets the color scheme provider for dynamic color scheme resolution.
     *
     * @param provider Color scheme provider.
     */
    public static void setColorSchemeProvider(ColorSchemeProvider provider) {
        SCHEME_PROVIDER.set(provider != null ? provider : () -> ColorScheme.SOLARIZED_DARK);
    }

    /**
     * Returns the current color scheme from the provider.
     *
     * @return Current color scheme.
     */
    public static ColorScheme getColorScheme() {
        return SCHEME_PROVIDER.get().colorScheme();
    }

    public static String ansi(String markupText) {
        return Ansi.AUTO.string(markupText);
    }

    public static Fg fg(Color color) {
        return new Fg(color, getColorScheme());
    }

    public static Fg fg(Color color, ColorScheme scheme) {
        return new Fg(color, scheme);
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
        private final ColorScheme scheme;

        private Style style;

        private Fg(Color color, ColorScheme scheme) {
            this.color = color;
            this.scheme = scheme;
        }

        public Fg with(Style style) {
            this.style = style;
            return this;
        }

        /** Marks given text with the configured before style. */
        @Override
        public String mark(String textToMark) {
            int colorCode = color.getCode(scheme);
            if (style == Style.BOLD) {
                return String.format("@|fg(%d),bold %s|@", colorCode, textToMark);
            }
            return String.format("@|fg(%d) %s|@", colorCode, textToMark);
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
     * Represents semantic colors that are used in CLI.
     * Actual ANSI color codes are resolved based on the current color scheme.
     */
    public enum Color {
        /** Error color (red variants). */
        RED,
        /** Success color (green variants). */
        GREEN,
        /** Warning/option color (yellow variants). */
        YELLOW,
        /** Info color (blue variants). */
        BLUE,
        /** Keyword color for syntax highlighting. */
        YELLOW_DARK,
        /** String literal color for syntax highlighting. */
        GREEN_DARK,
        /** Secondary/muted text color. */
        GRAY,
        /** Primary text color. */
        WHITE;

        /**
         * Returns the ANSI color code for this semantic color in the given scheme.
         *
         * @param scheme Color scheme to use.
         * @return ANSI color code.
         */
        public int getCode(ColorScheme scheme) {
            switch (this) {
                case RED:
                    return scheme.errorColor();
                case GREEN:
                    return scheme.successColor();
                case YELLOW:
                    return scheme.warningColor();
                case BLUE:
                    return scheme.infoColor();
                case YELLOW_DARK:
                    return scheme.keywordColor();
                case GREEN_DARK:
                    return scheme.stringColor();
                case GRAY:
                    return scheme.mutedColor();
                case WHITE:
                    return scheme.primaryColor();
                default:
                    return scheme.primaryColor();
            }
        }
    }
}
