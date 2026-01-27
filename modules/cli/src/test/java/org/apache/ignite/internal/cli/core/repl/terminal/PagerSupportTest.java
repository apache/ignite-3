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

package org.apache.ignite.internal.cli.core.repl.terminal;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.jline.terminal.Terminal;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link PagerSupport}.
 */
class PagerSupportTest {

    @Nested
    @DisplayName("countLines")
    class CountLinesTest {

        private final PagerSupport pager = createPagerSupport(24, true, "less");

        @Test
        @DisplayName("returns 0 for null input")
        void nullInput() {
            assertThat(pager.countLines(null), is(0));
        }

        @Test
        @DisplayName("returns 0 for empty string")
        void emptyString() {
            assertThat(pager.countLines(""), is(0));
        }

        @Test
        @DisplayName("returns 1 for single line without newline")
        void singleLineNoNewline() {
            assertThat(pager.countLines("hello"), is(1));
        }

        @Test
        @DisplayName("returns 1 for single line with trailing newline")
        void singleLineWithNewline() {
            assertThat(pager.countLines("hello\n"), is(1));
        }

        @Test
        @DisplayName("returns 2 for two lines")
        void twoLines() {
            assertThat(pager.countLines("hello\nworld"), is(2));
        }

        @Test
        @DisplayName("returns 2 for two lines with trailing newline")
        void twoLinesWithTrailingNewline() {
            assertThat(pager.countLines("hello\nworld\n"), is(2));
        }

        @Test
        @DisplayName("counts multiple lines correctly")
        void multipleLines() {
            String output = "line1\nline2\nline3\nline4\nline5";
            assertThat(pager.countLines(output), is(5));
        }

        @Test
        @DisplayName("handles empty lines in the middle")
        void emptyLinesInMiddle() {
            String output = "line1\n\nline3";
            assertThat(pager.countLines(output), is(3));
        }
    }

    @Nested
    @DisplayName("shouldUsePager")
    class ShouldUsePagerTest {

        @Test
        @DisplayName("returns false when pager is disabled")
        void pagerDisabled() {
            PagerSupport pager = createPagerSupport(24, false, "less");
            String output = generateLines(100);
            assertThat(pager.shouldUsePager(output), is(false));
        }

        @Test
        @DisplayName("returns false when output fits terminal")
        void outputFitsTerminal() {
            PagerSupport pager = createPagerSupport(24, true, "less");
            String output = generateLines(10);
            assertThat(pager.shouldUsePager(output), is(false));
        }

        @Test
        @DisplayName("returns true when output exceeds terminal height")
        void outputExceedsTerminal() {
            PagerSupport pager = createPagerSupport(24, true, "less");
            String output = generateLines(30);
            assertThat(pager.shouldUsePager(output), is(true));
        }

        @Test
        @DisplayName("returns false when output equals terminal height minus margin")
        void outputEqualsTerminalMinusMargin() {
            // Terminal height 24, margin 2, so threshold is 22
            PagerSupport pager = createPagerSupport(24, true, "less");
            String output = generateLines(22);
            assertThat(pager.shouldUsePager(output), is(false));
        }

        @Test
        @DisplayName("returns true when output is one line over threshold")
        void outputOneLineOverThreshold() {
            // Terminal height 24, margin 2, so threshold is 22
            PagerSupport pager = createPagerSupport(24, true, "less");
            String output = generateLines(23);
            assertThat(pager.shouldUsePager(output), is(true));
        }

        @Test
        @DisplayName("returns false for null output")
        void nullOutput() {
            PagerSupport pager = createPagerSupport(24, true, "less");
            assertThat(pager.shouldUsePager(null), is(false));
        }

        @Test
        @DisplayName("returns false for empty output")
        void emptyOutput() {
            PagerSupport pager = createPagerSupport(24, true, "less");
            assertThat(pager.shouldUsePager(""), is(false));
        }
    }

    @Nested
    @DisplayName("getPagerCommand")
    class GetPagerCommandTest {

        @Test
        @DisplayName("returns configured command when set")
        void configuredCommand() {
            PagerSupport pager = createPagerSupport(24, true, "more -d");
            assertThat(pager.getPagerCommand(), is("more -d"));
        }

        @Test
        @DisplayName("returns default command when config is null")
        void defaultWhenConfigNull() {
            PagerSupport pager = createPagerSupport(24, true, null);
            String expectedPager = PagerSupport.isWindows() ? PagerSupport.DEFAULT_PAGER_WINDOWS : PagerSupport.DEFAULT_PAGER;
            assertThat(pager.getPagerCommand(), is(expectedPager));
        }

        @Test
        @DisplayName("returns default command when config is empty")
        void defaultWhenConfigEmpty() {
            PagerSupport pager = createPagerSupport(24, true, "");
            String expectedPager = PagerSupport.isWindows() ? PagerSupport.DEFAULT_PAGER_WINDOWS : PagerSupport.DEFAULT_PAGER;
            assertThat(pager.getPagerCommand(), is(expectedPager));
        }
    }

    @Nested
    @DisplayName("isPagerEnabled")
    class IsPagerEnabledTest {

        @Test
        @DisplayName("returns true when enabled")
        void enabled() {
            PagerSupport pager = createPagerSupport(24, true, "less");
            assertThat(pager.isPagerEnabled(), is(true));
        }

        @Test
        @DisplayName("returns false when disabled")
        void disabled() {
            PagerSupport pager = createPagerSupport(24, false, "less");
            assertThat(pager.isPagerEnabled(), is(false));
        }
    }

    /**
     * Creates a PagerSupport with test configuration.
     *
     * @param terminalHeight terminal height in lines
     * @param pagerEnabled whether pager is enabled
     * @param pagerCommand pager command (null for default)
     * @return configured PagerSupport instance
     */
    private static PagerSupport createPagerSupport(int terminalHeight, boolean pagerEnabled, String pagerCommand) {
        Terminal terminal = mock(Terminal.class);
        when(terminal.getHeight()).thenReturn(terminalHeight);
        return new PagerSupport(terminal, pagerEnabled, pagerCommand);
    }

    /**
     * Generates a string with the specified number of lines.
     */
    private static String generateLines(int count) {
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= count; i++) {
            sb.append("Line ").append(i);
            if (i < count) {
                sb.append('\n');
            }
        }
        return sb.toString();
    }
}
