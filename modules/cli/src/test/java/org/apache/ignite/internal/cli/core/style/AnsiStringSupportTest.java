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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.ignite.internal.cli.core.style.AnsiStringSupport.Color;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class AnsiStringSupportTest {

    @AfterEach
    void tearDown() {
        // Reset to default provider
        AnsiStringSupport.setColorSchemeProvider(() -> ColorScheme.SOLARIZED_DARK);
    }

    @Test
    void defaultSchemeIsSolarizedDark() {
        AnsiStringSupport.setColorSchemeProvider(null);
        assertEquals(ColorScheme.SOLARIZED_DARK, AnsiStringSupport.getColorScheme());
    }

    @Test
    void providerIsCalledDynamically() {
        // Use a mutable holder to change the scheme dynamically
        final ColorScheme[] holder = {ColorScheme.DARK};
        AnsiStringSupport.setColorSchemeProvider(() -> holder[0]);

        assertEquals(ColorScheme.DARK, AnsiStringSupport.getColorScheme());

        // Change the scheme
        holder[0] = ColorScheme.LIGHT;
        assertEquals(ColorScheme.LIGHT, AnsiStringSupport.getColorScheme());
    }

    @Test
    void fgUsesCurrentSchemeColors() {
        AnsiStringSupport.setColorSchemeProvider(() -> ColorScheme.DARK);
        String darkResult = AnsiStringSupport.fg(Color.RED).mark("error");

        AnsiStringSupport.setColorSchemeProvider(() -> ColorScheme.LIGHT);
        String lightResult = AnsiStringSupport.fg(Color.RED).mark("error");

        // The color codes should be different
        assertTrue(darkResult.contains("fg(" + ColorScheme.DARK.errorColor() + ")"));
        assertTrue(lightResult.contains("fg(" + ColorScheme.LIGHT.errorColor() + ")"));
    }

    @Test
    void fgWithExplicitSchemeOverridesGlobal() {
        AnsiStringSupport.setColorSchemeProvider(() -> ColorScheme.DARK);

        // Even with DARK as global, we can explicitly use LIGHT
        String lightResult = AnsiStringSupport.fg(Color.RED, ColorScheme.LIGHT).mark("error");
        assertTrue(lightResult.contains("fg(" + ColorScheme.LIGHT.errorColor() + ")"));
    }

    @Test
    void fgWithBoldStyle() {
        AnsiStringSupport.setColorSchemeProvider(() -> ColorScheme.DARK);
        String result = AnsiStringSupport.fg(Color.GREEN).with(AnsiStringSupport.Style.BOLD).mark("success");
        assertTrue(result.contains("bold"));
        assertTrue(result.contains("fg(" + ColorScheme.DARK.successColor() + ")"));
    }

    @Test
    void colorEnumMapsToCorrectSchemeColors() {
        assertEquals(ColorScheme.DARK.errorColor(), Color.RED.getCode(ColorScheme.DARK));
        assertEquals(ColorScheme.DARK.successColor(), Color.GREEN.getCode(ColorScheme.DARK));
        assertEquals(ColorScheme.DARK.warningColor(), Color.YELLOW.getCode(ColorScheme.DARK));
        assertEquals(ColorScheme.DARK.infoColor(), Color.BLUE.getCode(ColorScheme.DARK));
        assertEquals(ColorScheme.DARK.keywordColor(), Color.YELLOW_DARK.getCode(ColorScheme.DARK));
        assertEquals(ColorScheme.DARK.stringColor(), Color.GREEN_DARK.getCode(ColorScheme.DARK));
        assertEquals(ColorScheme.DARK.mutedColor(), Color.GRAY.getCode(ColorScheme.DARK));
        assertEquals(ColorScheme.DARK.primaryColor(), Color.WHITE.getCode(ColorScheme.DARK));
    }
}
