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

import java.util.Locale;
import org.jetbrains.annotations.Nullable;

/**
 * Defines color schemes for CLI output.
 * Each scheme provides ANSI 256-color codes optimized for different terminal backgrounds.
 */
public enum ColorScheme {
    /** Default color scheme for dark terminal backgrounds. */
    DARK(
            // UI colors
            1,    // error: bright red
            2,    // success: bright green
            3,    // warning: bright yellow
            33,   // info: bright blue
            246,  // muted: gray
            252,  // primary: white
            // Syntax highlighting colors
            214,  // keyword: orange
            2,    // string: bright green
            33,   // number: bright blue
            252,  // bracket: white
            246,  // punctuation: gray
            254   // identifier: bright white
    ),

    /** Default color scheme for light terminal backgrounds. */
    LIGHT(
            // UI colors
            124,  // error: dark red
            22,   // success: dark green
            130,  // warning: dark orange/brown
            18,   // info: dark blue
            242,  // muted: dark gray
            0,    // primary: black
            // Syntax highlighting colors
            130,  // keyword: dark orange/brown
            22,   // string: dark green
            18,   // number: dark blue
            0,    // bracket: black
            242,  // punctuation: dark gray
            0     // identifier: black
    ),

    /**
     * Solarized Dark theme by Ethan Schoonover.
     * Designed for dark backgrounds with carefully chosen colors for readability.
     * See: https://ethanschoonover.com/solarized/
     */
    SOLARIZED_DARK(
            // UI colors (Solarized accent colors)
            160,  // error: red (#dc322f)
            106,  // success: green (#859900)
            136,  // warning: yellow (#b58900)
            33,   // info: blue (#268bd2)
            246,  // muted: base0 (#839496)
            252,  // primary: bright white (for better visibility)
            // Syntax highlighting colors
            166,  // keyword: orange (#cb4b16)
            37,   // string: cyan (#2aa198)
            33,   // number: blue (#268bd2)
            252,  // bracket: bright white
            246,  // punctuation: base0 (#839496)
            252   // identifier: bright white
    ),

    /**
     * Solarized Light theme by Ethan Schoonover.
     * Designed for light backgrounds with carefully chosen colors for readability.
     * See: https://ethanschoonover.com/solarized/
     */
    SOLARIZED_LIGHT(
            // UI colors (Solarized accent colors)
            160,  // error: red (#dc322f)
            106,  // success: green (#859900)
            136,  // warning: yellow (#b58900)
            33,   // info: blue (#268bd2)
            247,  // muted: base1 (#93a1a1)
            66,   // primary: base00 (#657b83)
            // Syntax highlighting colors
            166,  // keyword: orange (#cb4b16)
            37,   // string: cyan (#2aa198)
            33,   // number: blue (#268bd2)
            66,   // bracket: base00 (#657b83)
            247,  // punctuation: base1 (#93a1a1)
            66    // identifier: base00 (#657b83)
    );

    private final int errorColor;
    private final int successColor;
    private final int warningColor;
    private final int infoColor;
    private final int mutedColor;
    private final int primaryColor;
    private final int keywordColor;
    private final int stringColor;
    private final int numberColor;
    private final int bracketColor;
    private final int punctuationColor;
    private final int identifierColor;

    ColorScheme(
            int errorColor,
            int successColor,
            int warningColor,
            int infoColor,
            int mutedColor,
            int primaryColor,
            int keywordColor,
            int stringColor,
            int numberColor,
            int bracketColor,
            int punctuationColor,
            int identifierColor
    ) {
        this.errorColor = errorColor;
        this.successColor = successColor;
        this.warningColor = warningColor;
        this.infoColor = infoColor;
        this.mutedColor = mutedColor;
        this.primaryColor = primaryColor;
        this.keywordColor = keywordColor;
        this.stringColor = stringColor;
        this.numberColor = numberColor;
        this.bracketColor = bracketColor;
        this.punctuationColor = punctuationColor;
        this.identifierColor = identifierColor;
    }

    /** Returns ANSI color code for error messages. */
    public int errorColor() {
        return errorColor;
    }

    /** Returns ANSI color code for success messages. */
    public int successColor() {
        return successColor;
    }

    /** Returns ANSI color code for warning/option messages. */
    public int warningColor() {
        return warningColor;
    }

    /** Returns ANSI color code for info messages. */
    public int infoColor() {
        return infoColor;
    }

    /** Returns ANSI color code for muted/secondary text. */
    public int mutedColor() {
        return mutedColor;
    }

    /** Returns ANSI color code for primary text. */
    public int primaryColor() {
        return primaryColor;
    }

    /** Returns ANSI color code for syntax keywords. */
    public int keywordColor() {
        return keywordColor;
    }

    /** Returns ANSI color code for string literals. */
    public int stringColor() {
        return stringColor;
    }

    /** Returns ANSI color code for number literals. */
    public int numberColor() {
        return numberColor;
    }

    /** Returns ANSI color code for brackets. */
    public int bracketColor() {
        return bracketColor;
    }

    /** Returns ANSI color code for punctuation (comma, colon, etc.). */
    public int punctuationColor() {
        return punctuationColor;
    }

    /** Returns ANSI color code for identifiers. */
    public int identifierColor() {
        return identifierColor;
    }

    /**
     * Parses a color scheme from string value.
     * Supports both underscore and hyphen separators (e.g., "solarized-dark" or "solarized_dark").
     *
     * @param value Color scheme name (case-insensitive).
     * @return Parsed color scheme or {@code null} if value is invalid.
     */
    @Nullable
    public static ColorScheme fromString(@Nullable String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        try {
            // Convert hyphens to underscores to support both "solarized-dark" and "solarized_dark"
            String normalized = value.toUpperCase(Locale.ROOT).replace('-', '_');
            return valueOf(normalized);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
