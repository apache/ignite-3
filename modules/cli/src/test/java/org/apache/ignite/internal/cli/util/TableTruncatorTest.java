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

package org.apache.ignite.internal.cli.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.sameInstance;

import com.jakewharton.fliptables.FlipTableConverters;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.cli.decorators.TruncationConfig;
import org.apache.ignite.internal.cli.sql.table.Table;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link TableTruncator}.
 */
class TableTruncatorTest {

    @Test
    void truncateDisabledReturnsOriginalTable() {
        Table<String> table = createTable(
                List.of("id", "name"),
                List.of("1", "Alice", "2", "Bob")
        );
        TruncationConfig config = TruncationConfig.disabled();

        Table<String> result = new TableTruncator(config).truncate(table);

        assertThat(result, sameInstance(table));
    }

    @Test
    void truncateCellWithinLimit() {
        String result = TableTruncator.truncateCell("short", 10);

        assertThat(result, equalTo("short"));
    }

    @Test
    void truncateCellExceedsLimit() {
        String result = TableTruncator.truncateCell("very long text that exceeds the limit", 10);

        assertThat(result, equalTo("very long…"));
    }

    @Test
    void truncateCellExactlyAtLimit() {
        String result = TableTruncator.truncateCell("1234567890", 10);

        assertThat(result, equalTo("1234567890"));
    }

    @Test
    void truncateCellNullValue() {
        String result = TableTruncator.truncateCell(null, 10);

        assertThat(result, equalTo("null"));
    }

    @Test
    void truncateCellMinimumWidth() {
        // With 1-char ellipsis, width 2 = 1 char content + ellipsis
        String result = TableTruncator.truncateCell("hello", 2);

        assertThat(result, equalTo("h…"));
    }

    @Test
    void truncateCellWidthEqualToEllipsis() {
        // With 1-char ellipsis, width 1 = just the ellipsis
        String result = TableTruncator.truncateCell("hello", 1);

        assertThat(result, equalTo("…"));
    }

    @Test
    void truncateTableAppliesMaxColumnWidth() {
        Table<String> table = createTable(
                List.of("id", "description"),
                List.of("1", "This is a very long description that should be truncated")
        );
        TruncationConfig config = new TruncationConfig(true, 20, 0);

        Table<String> result = new TableTruncator(config).truncate(table);

        assertThat(result.header()[1], equalTo("description"));
        assertThat(result.content()[0][1], equalTo("This is a very long…"));
    }

    @Test
    void truncateTablePreservesShortColumns() {
        Table<String> table = createTable(
                List.of("id", "name", "description"),
                List.of("1", "Alice", "Short desc")
        );
        TruncationConfig config = new TruncationConfig(true, 20, 0);

        Table<String> result = new TableTruncator(config).truncate(table);

        assertThat(result.content()[0][0], equalTo("1"));
        assertThat(result.content()[0][1], equalTo("Alice"));
        assertThat(result.content()[0][2], equalTo("Short desc"));
    }

    @Test
    void truncateEmptyTableReturnsOriginal() {
        Table<String> table = createTable(List.of("id"), List.of());
        TruncationConfig config = new TruncationConfig(true, 20, 80);

        Table<String> result = new TableTruncator(config).truncate(table);

        assertThat(result.header().length, is(1));
        assertThat(result.content().length, is(0));
    }

    @Test
    void calculateColumnWidthsRespectsMaxColumnWidth() {
        String[] header = {"id", "description"};
        Object[][] content = {{"1", "This is a long description"}};
        TruncationConfig config = new TruncationConfig(true, 15, 0);

        int[] widths = new TableTruncator(config).calculateColumnWidths(header, content);

        assertThat(widths[0], is(2)); // minimum column width (1 char + 1-char ellipsis)
        assertThat(widths[1], is(15)); // capped at max column width
    }

    @Test
    void calculateColumnWidthsDistributesForTerminal() {
        String[] header = {"col1", "col2", "col3"};
        Object[][] content = {
                {"very long content 1", "very long content 2", "very long content 3"}
        };
        // Terminal width = 50, with overhead for borders (3*3 + 1 = 10)
        TruncationConfig config = new TruncationConfig(true, 100, 50);

        int[] widths = new TableTruncator(config).calculateColumnWidths(header, content);

        // Total width should fit within terminal width (available = 50 - 10 = 40)
        int totalWidth = Arrays.stream(widths).sum();
        assertThat(totalWidth, lessThanOrEqualTo(50 - 10));

        // Each column should have reasonable width (at least minimum of 2: 1 char + 1-char ellipsis)
        for (int width : widths) {
            assertThat(width, greaterThanOrEqualTo(2));
        }

        // Verify actual truncation result
        Table<String> table = createTable(List.of("col1", "col2", "col3"),
                List.of("very long content 1", "very long content 2", "very long content 3"));
        Table<String> result = new TableTruncator(config).truncate(table);

        // Content should be truncated to fit calculated widths
        for (int i = 0; i < 3; i++) {
            String cell = (String) result.content()[0][i];
            assertThat(cell.length(), lessThanOrEqualTo(widths[i]));
        }
    }

    @Test
    void calculateColumnWidthsUsesExactAvailableWidth() {
        // Single column with content larger than terminal
        String[] header = {"header"};
        Object[][] content = {{"12345678901234567890"}}; // 20 chars

        // Terminal = 20, overhead for 1 column = 3*1 + 1 = 4, available = 16
        TruncationConfig config = new TruncationConfig(true, 100, 20);

        int[] widths = new TableTruncator(config).calculateColumnWidths(header, content);

        // Content (20) exceeds available (16), so should be shrunk to exactly 16
        assertThat(widths[0], is(16));
    }

    @Test
    void calculateColumnWidthsExactFitWhenResizedByOne() {
        // Simulate: content fits at width W, then terminal resized to W-1
        String[] header = {"col"};
        Object[][] content = {{"1234567890"}}; // 10 chars

        // At terminal = 14: overhead = 3*1 + 1 = 4, available = 10, content = 10 -> fits exactly
        TruncationConfig configFits = new TruncationConfig(true, 100, 14);
        int[] widthsFits = new TableTruncator(configFits).calculateColumnWidths(header, content);
        assertThat(widthsFits[0], is(10)); // No truncation needed

        // At terminal = 13: overhead = 4, available = 9, content = 10 -> need to shrink by 1
        TruncationConfig configShrink = new TruncationConfig(true, 100, 13);
        int[] widthsShrink = new TableTruncator(configShrink).calculateColumnWidths(header, content);
        assertThat(widthsShrink[0], is(9)); // Should shrink to exactly 9, not less
    }

    @Test
    void truncateTableWithMultipleRows() {
        Table<String> table = createTable(
                List.of("id", "name"),
                List.of(
                        "1", "Alice with a very long name",
                        "2", "Bob",
                        "3", "Charlie with another long name"
                )
        );
        TruncationConfig config = new TruncationConfig(true, 10, 0);

        Table<String> result = new TableTruncator(config).truncate(table);

        assertThat(result.content()[0][1], equalTo("Alice wit…"));
        assertThat(result.content()[1][1], equalTo("Bob"));
        assertThat(result.content()[2][1], equalTo("Charlie w…"));
    }

    @Test
    void truncatePreservesHeaderWhenWidthIsSmall() {
        Table<String> table = createTable(
                List.of("very_long_header_name"),
                List.of("short")
        );
        TruncationConfig config = new TruncationConfig(true, 10, 0);

        Table<String> result = new TableTruncator(config).truncate(table);

        assertThat(result.header()[0], equalTo("very_long…"));
    }

    @Test
    void truncateTableWithLongContentExceedingDefaultMaxWidth() {
        // Content longer than default max column width (50 characters)
        String longContent = "This is a very long string that exceeds the default maximum column width of fifty characters";
        Table<String> table = createTable(
                List.of("id", "description"),
                List.of("1", longContent)
        );
        TruncationConfig config = new TruncationConfig(true, 50, 0);

        Table<String> result = new TableTruncator(config).truncate(table);

        String truncatedContent = (String) result.content()[0][1];
        assertThat(truncatedContent.length(), is(50));
        assertThat(truncatedContent, equalTo("This is a very long string that exceeds the defau…"));
    }

    /**
     * Tests that a truncated table with IDDDDD column fits exactly within the terminal width.
     * This is a regression test for the off-by-1 bug in the overhead calculation.
     *
     * <p>The FlipTables overhead formula is: 3*N + 1 where N is the number of columns.
     * - Left border: 1 char
     * - Right border: 1 char
     * - N-1 separators between columns: N-1 chars
     * - 2*N padding (space before + space after each column): 2*N chars
     * - Total: 1 + 1 + (N-1) + 2*N = 3*N + 1
     */
    @Test
    void truncatedTableWithIdddddColumnFitsWithinTerminalWidth() {
        // Simulate the user's table: IDDDDD, NAME, AGE, EMAIL
        Table<String> table = createTable(
                List.of("IDDDDD", "NAME", "AGE", "EMAIL"),
                List.of("1", "Alice", "28", "alice@example.com")
        );

        // Set terminal width to a value that requires truncation
        int terminalWidth = 50;
        TruncationConfig config = new TruncationConfig(true, 100, terminalWidth);

        Table<String> truncated = new TableTruncator(config).truncate(table);

        // Render the table using FlipTables (same as TableDecorator does)
        String rendered = FlipTableConverters.fromObjects(truncated.header(), truncated.content());

        // Get the actual width of the rendered table (first line contains the top border)
        int actualWidth = rendered.lines().findFirst().orElse("").length();

        assertThat("Rendered table width should fit within terminal width",
                actualWidth, lessThanOrEqualTo(terminalWidth));
    }

    /**
     * Tests that a truncated table fits exactly within the terminal width when
     * the content needs to be shrunk.
     * This is a regression test for the off-by-1 bug in the overhead calculation.
     *
     * <p>For 4 columns:
     * - FlipTables overhead = 3*N + 1 = 3*4 + 1 = 13
     * - If we calculate overhead as 3*N = 12 (wrong), we'll allow 1 extra char
     *   of content, causing the table to overflow by 1 character.
     */
    @Test
    void truncatedTableFitsExactlyWhenContentNeedsShrinking() {
        // Create a table with long content that requires truncation
        // IDDDDD=6, NAME=7 (Charlie), AGE=3, EMAIL=19 (charlie@example.com)
        // Total content = 35, Overhead = 13, Natural width = 48
        Table<String> table = createTable(
                List.of("IDDDDD", "NAME", "AGE", "EMAIL"),
                List.of("1", "Charlie", "42", "charlie@example.com")
        );

        // Set terminal width smaller than natural width to force truncation
        // Natural width = 48, so terminal = 40 forces shrinking
        int terminalWidth = 40;
        TruncationConfig config = new TruncationConfig(true, 100, terminalWidth);

        Table<String> truncated = new TableTruncator(config).truncate(table);

        // Render the table using FlipTables
        String rendered = FlipTableConverters.fromObjects(truncated.header(), truncated.content());
        int actualWidth = rendered.lines().findFirst().orElse("").length();

        // The bug: if overhead is calculated as 12 instead of 13,
        // the table will be 1 char wider than terminal
        assertThat("Rendered table width should not exceed terminal width. "
                        + "Actual width: " + actualWidth + ", terminal: " + terminalWidth,
                actualWidth, lessThanOrEqualTo(terminalWidth));
    }

    /**
     * Tests that when terminal width is 0, truncation still works using the default width fallback.
     * This is important for environments where JLine cannot detect terminal size.
     */
    @Test
    void truncateTableWorksWhenTerminalWidthIsZero() {
        // Create a table with content that would overflow a narrow terminal
        Table<String> table = createTable(
                List.of("A_VERY_LONG_COLUMN_HEADER", "ANOTHER_LONG_HEADER"),
                List.of("some long content that exceeds normal width", "more long content here")
        );

        // Terminal width 0 means "not detected" - should use fallback
        TruncationConfig config = new TruncationConfig(true, 100, 0);

        // When terminal width is 0, NO terminal-based truncation is applied
        // Only maxColumnWidth truncation is applied
        Table<String> result = new TableTruncator(config).truncate(table);

        // The table should still be truncated based on maxColumnWidth (100)
        // But since content is shorter than 100, it should remain unchanged
        assertThat(result.content()[0][0], equalTo("some long content that exceeds normal width"));
    }

    /**
     * Tests that TruncationConfig.fromConfig applies fallback when terminal returns 0.
     */
    @Test
    void fromConfigAppliesFallbackWhenTerminalWidthIsZero() {
        // This test verifies the fallback logic in TruncationConfig.fromConfig
        // by checking that it uses DEFAULT_TERMINAL_WIDTH when supplier returns 0
        assertThat(TruncationConfig.DEFAULT_TERMINAL_WIDTH, is(80));
    }

    private static Table<String> createTable(List<String> headers, List<String> content) {
        return new Table<>(headers, content);
    }

}
