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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class ColorSchemeTest {

    @Test
    void darkAndLightSchemesHaveDifferentColors() {
        assertNotEquals(ColorScheme.DARK.errorColor(), ColorScheme.LIGHT.errorColor());
        assertNotEquals(ColorScheme.DARK.successColor(), ColorScheme.LIGHT.successColor());
        assertNotEquals(ColorScheme.DARK.primaryColor(), ColorScheme.LIGHT.primaryColor());
    }

    @Test
    void solarizedSchemesExist() {
        assertEquals(160, ColorScheme.SOLARIZED_DARK.errorColor());
        assertEquals(106, ColorScheme.SOLARIZED_DARK.successColor());
        assertEquals(160, ColorScheme.SOLARIZED_LIGHT.errorColor());
        assertEquals(106, ColorScheme.SOLARIZED_LIGHT.successColor());
    }

    @Test
    void solarizedDarkAndLightHaveDifferentBaseColors() {
        // Primary text differs between solarized dark and light
        assertNotEquals(ColorScheme.SOLARIZED_DARK.primaryColor(), ColorScheme.SOLARIZED_LIGHT.primaryColor());
        // Muted text differs
        assertNotEquals(ColorScheme.SOLARIZED_DARK.mutedColor(), ColorScheme.SOLARIZED_LIGHT.mutedColor());
    }

    @Test
    void fromStringParsesValidSchemes() {
        assertEquals(ColorScheme.DARK, ColorScheme.fromString("dark"));
        assertEquals(ColorScheme.DARK, ColorScheme.fromString("DARK"));
        assertEquals(ColorScheme.LIGHT, ColorScheme.fromString("light"));
        assertEquals(ColorScheme.LIGHT, ColorScheme.fromString("LIGHT"));
    }

    @Test
    void fromStringParsesSolarizedWithHyphen() {
        assertEquals(ColorScheme.SOLARIZED_DARK, ColorScheme.fromString("solarized-dark"));
        assertEquals(ColorScheme.SOLARIZED_DARK, ColorScheme.fromString("SOLARIZED-DARK"));
        assertEquals(ColorScheme.SOLARIZED_LIGHT, ColorScheme.fromString("solarized-light"));
        assertEquals(ColorScheme.SOLARIZED_LIGHT, ColorScheme.fromString("SOLARIZED-LIGHT"));
    }

    @Test
    void fromStringParsesSolarizedWithUnderscore() {
        assertEquals(ColorScheme.SOLARIZED_DARK, ColorScheme.fromString("solarized_dark"));
        assertEquals(ColorScheme.SOLARIZED_DARK, ColorScheme.fromString("SOLARIZED_DARK"));
        assertEquals(ColorScheme.SOLARIZED_LIGHT, ColorScheme.fromString("solarized_light"));
        assertEquals(ColorScheme.SOLARIZED_LIGHT, ColorScheme.fromString("SOLARIZED_LIGHT"));
    }

    @Test
    void fromStringReturnsNullForInvalidValues() {
        assertNull(ColorScheme.fromString(null));
        assertNull(ColorScheme.fromString(""));
        assertNull(ColorScheme.fromString("invalid"));
        assertNull(ColorScheme.fromString("auto"));
    }

    @Test
    void lightSchemeHasDarkerColorsForLightBackgrounds() {
        // Primary text should be black (0) on light, white-ish on dark
        assertEquals(0, ColorScheme.LIGHT.primaryColor());
        assertEquals(252, ColorScheme.DARK.primaryColor());
    }

    @Test
    void solarizedUsesConsistentAccentColors() {
        // Solarized uses the same accent colors for both dark and light themes
        assertEquals(ColorScheme.SOLARIZED_DARK.errorColor(), ColorScheme.SOLARIZED_LIGHT.errorColor());
        assertEquals(ColorScheme.SOLARIZED_DARK.successColor(), ColorScheme.SOLARIZED_LIGHT.successColor());
        assertEquals(ColorScheme.SOLARIZED_DARK.warningColor(), ColorScheme.SOLARIZED_LIGHT.warningColor());
        assertEquals(ColorScheme.SOLARIZED_DARK.infoColor(), ColorScheme.SOLARIZED_LIGHT.infoColor());
    }
}
